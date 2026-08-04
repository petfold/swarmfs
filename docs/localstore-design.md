# `swarmfs.localstore` — a local-first blob store for Swarm

**Status: design, agreed in discussion 2026-08-04. Not yet scheduled.**
Consumers with concrete needs today: **recordstore** and **swarmfs's own write
path**. Likely later: other content-addressed-on-Swarm projects (bee-py users,
canopy, ontodag-fs).

## Why (and why "local-first", not "cache")

Every Swarm-backed application in this stack currently picks exactly one home
for its blobs: memory, local disk, or a Bee node. Each choice sacrifices
something — memory forgets, disk doesn't publish, Bee makes every read a
network round trip and every commit a network (and postage) liability. The
obvious per-app fixes (an in-memory value cache here, a read cache there)
re-solve the same problem with slightly different bugs.

The distinction that shapes this design: a **cache** may drop anything,
because the remote is authoritative. A **local-first replica** is
authoritative for everything not yet safely on Swarm, and may only drop what
Swarm confirmedly holds. That difference is the whole design:

> **The invariant.** Every blob is durably held somewhere at all times.
> Blobs reachable only from roots not yet confirmed on Swarm are **pinned**
> locally — never evicted under any pressure. Blobs confirmed on Swarm are
> **evictable** — dropping one merely turns a local read into a lazy
> re-fetch.

Consequences, in order of importance:

- **Offline is the normal mode, not a degraded one.** Reads and commits are
  local-disk operations; sync happens when connected. Network disruption
  affects the push/fetch workers, never `commit()`.
- **Low storage cannot cause a correctness problem** — only back-pressure
  (see *Budget*).
- **Performance**: local reads replace network round trips; push coalescing
  means intermediate commits' orphaned blobs never cross the network
  (postage saved, not just time); and client-side BMT addressing
  (`swarmfs/splitter.py`) computes real Swarm references entirely offline,
  so commit latency is pure local-disk latency.

## Placement and the contract

A submodule of swarmfs (working name `swarmfs/localstore/`), because the
Swarm-specific organs it needs — Bee client, BMT addressing, tags,
stewardship, stamp TTL — already live here, and recordstore already depends
on swarmfs optionally (the `[stamps]` extra). A standalone package is the
fallback if a consumer ever wants local-first machinery without a Swarm
remote; starting there means versioning three repos in lockstep for no
current benefit.

**Zero fsspec dependency.** fsspec stays at swarmfs's edge (the adapter that
makes Swarm speak pandas). `localstore` depends on an HTTP client and the BMT
hasher, nothing else. This is half of the answer to the "fsspec is
Python-centric" concern; the other half is the format spec below.

The contract is deliberately tiny — recordstore's `BytesStore` protocol,
already proven by four independent implementations:

```
put(data) -> ref          get(ref) -> bytes
put_many(datas) -> [ref]  get_many(refs) -> {ref: bytes}
```

plus the local-first surface: `commit_root`, `push`/`sync`, `fetch`, `pin`,
`status`, budget configuration. **The layer never interprets blob contents.**
It does not know tries, Mantaray manifests, or records exist. Everything that
requires understanding a blob's structure is the application's job (see *The
boundary*).

## The durability ladder

"On Swarm" is not a boolean, and pretending it is would rebuild the ambiguity
this design exists to remove. With `Swarm-Deferred-Upload: true` (the common
default), Bee's 201 means *the local node has it, queued for background sync*;
a node that dies before syncing loses the data. The ladder is therefore
explicit:

```
staged → committed (local disk) → on-node (push accepted) → network-confirmed
```

- **committed**: blobs written to the local store, root recorded in the
  journal. Fast, offline-safe, never blocked by network or stamps.
- **on-node**: a push (deferred upload) was accepted by the Bee node.
- **network-confirmed**: verified out on the network — via the tags API
  (synced-chunk counts) or stewardship (`/stewardship/{ref}` retrievability
  check), or by using direct upload (`Swarm-Deferred-Upload: false`), whose
  success already means network. *(Exact confirmation mechanics to be
  verified against the current Bee API during implementation; the ladder does
  not depend on which one wins.)* Note that tags counts and stewardship
  responses are *the node's claim*; the cryptographically sound check is
  retrieve-and-verify — see *Verification and trust* for the policy.

**Only `network-confirmed` flips blobs from pinned to evictable.** That is
the single coupling between the ladder and the eviction engine, and it
inherits the fail-safe direction of the journal rule below.

**Auto-push is the default**, as background sync: `commit_root` returns after
the local write; a worker advances roots up the ladder, naturally coalescing
when commits outpace pushes (push whatever is newest — which also implements
the push-latest-only postage saving). Certainty on demand, not by making
every commit slow: `status()` reports each root's rung, and `sync()` is the
blocking barrier that returns only when everything is network-confirmed. The
`write()`/`fsync()` contract, deliberately.

### The one rule that makes bookkeeping safe

> **The journal must lag reality, never lead it.** Record "pushed" only after
> the push returned; "confirmed" only after the check passed.

Every crash then leaves the journal *under-claiming* durability, and recovery
is always the safe, idempotent action (re-push; content-addressing makes it a
no-op the remote dedups). The catastrophic direction — journal claims
confirmed, blobs evicted, Swarm never had them — is structurally impossible,
not merely unlikely.

## On-disk format: the spec is the artifact

What must be portable is the **format, not the Python code** — git's trick:
`.git` is the standard, git is one implementation of it. A short normative
format document (deliverable of phase L0) specifies the store directory so a
Go or JS implementation is a weekend against a spec. Rule: *authoritative
state must be trivially parseable in any language* (no pickles; binary only
where disposable).

One self-contained directory, rsync-able like `.git`:

```
<store>/
  blobs/ab/cdef…        # one file per blob, hash-fanned; immutable
  journal.jsonl         # authoritative, append-only events
  index/…               # derived, rebuildable, disposable
  lock                  # single-writer lock file (v1 scope)
```

Three tiers, split by mutability and by what losing them costs:

1. **The journal** — authoritative, append-only, tiny. JSONL events:
   `committed {root, parent, blobs: […]}` · `pushed {root}` ·
   `confirmed {root, batch, ttl_observed}` · `remote-root {remote, root}` ·
   `pin {name, refs}` / `unpin {name}` · periodic `snapshot` entries for
   compaction. Folding the events yields every per-root and per-remote fact.
   Append-only gives crash-safety nearly free, plus an audit trail. Note the
   `committed` event **carries the new-blob list** — the application's commit
   machinery already knows exactly which blobs are new (recordstore's
   `_flush`, swarmfs's commit engine), and recording it is what lets the
   layer compute the pinned set *without ever parsing a blob*.
2. **The blob files** — self-describing. Size and recency come from the
   filesystem's own `stat`; integrity is checkable from the filename
   (content-addressed). No per-blob metadata file at all.
3. **The derived index** — materializes the expensive queries (blob →
   pinning roots; LRU ordering; per-pin membership). Because it is derivable
   from tiers 1–2 it is allowed to be lost, corrupt, or version-skewed:
   delete and rebuild. This is the only tier where a binary format (SQLite)
   is acceptable — it never holds truth, only acceleration.

The journal doubles as the **reflog**: `committed` events carry parentage, so
root lineage and merge-base discovery (latest common entry with a feed's
update history) come from the same file. Compaction (fold into a `snapshot`
entry, write-new-then-rename) is the one place append-only purity bends; it
goes in the format spec from day one so all implementations handle it
identically.

## Budget, eviction, and outgrowing the disk

**Budget**: an explicit byte limit as the primary mechanism (predictable,
testable) plus an optional free-space floor via `statvfs` — "use at most
10 GB, and never leave less than 2 GB free."

**The limit is soft for pinned data** (decided): if unpushed data alone
exceeds the budget — e.g. a long offline stretch of heavy writing — the store
overruns the quota with loud warnings rather than refusing commits. Losing
the ability to save work is worse than temporarily exceeding a limit. The
pressure valve is always push: confirming roots converts pinned → evictable,
which frees space.

**Eviction** is LRU over evictable blobs, refined two ways:

- **Priority classes, not content inspection.** Applications declare, per
  commit, which of its new blobs are structure vs. payload (recorded in the
  `committed` journal event, so the classification survives reopening). Structure blobs — trie
  nodes, manifest nodes — are small, touched on every operation, and
  disproportionately valuable per byte: with structure resident and values
  remote, recordstore can diff or three-way-merge million-key roots locally
  on a store far larger than the disk (diff/merge compare refs, never
  values). Plain LRU approximates this (upper nodes are hot by construction);
  the hint makes it a guarantee. The layer stays ignorant of *why* a blob is
  structure.
- **TTL-aware.** Never evict a blob whose covering postage batch is near
  expiry — for the evicted portion, Swarm is truly authoritative, and an
  expired batch plus an evicted blob is permanent loss. Confirmation events
  record the batch and observed TTL; a periodic re-confirmation pass
  (stewardship) refreshes them, and must run *before* TTL-risky evictions,
  not after.

**When data outgrows storage** there is no cliff: the design has no
full-copy assumption. The hard floor is the pinned set plus breathing room;
everything else is working set. What is genuinely given up, and the layer's
corresponding duties:

- Total offline — a read of an evicted blob while disconnected fails, and it
  must fail with its own error ("exists, value on Swarm, you are offline"),
  distinguishable from not-found.
- Latency predictability — a read is microseconds or seconds depending on
  cache state; `status()` should make the working-set composition visible.
- **Swarm becomes authoritative for the evicted portion** — so `status()`
  reports, prominently: "N GB of this store exists *only* on Swarm, covered
  by batch B expiring in D days." Stamp maintenance graduates from
  housekeeping to the integrity story, and the software says so.

**User controls** (working set is now a user concern): named pins —
applications compute the refs (e.g. recordstore walks a key-prefix subtree;
"keep `users/` local" is one subtree walk) and register them; `fetch(refs)`
as the warm-up verb ("materialize this before I board the plane"). A blob
stays if *either* durability pinning or a named pin holds it. Deliberately
**no** predictive prefetching or access-pattern learning: 90%-LRU-plus-
explicit-pins is debuggable; anything smarter turns cache behavior into
weather, and its failure mode — surprise network reads — is exactly what
local-first exists to eliminate. Explicit beats clever.

## Verification and trust

The invariant quietly rests on a cryptographic property: a ref *is* a hash,
so possession of the ref is the ability to verify the bytes no matter who
serves them. That is why "evict it, Swarm holds it" is sound against an
untrusted open network. Four requirements and one deliberate non-use keep
that story honest:

1. **Verified re-fetch (L1 requirement, default on).** A lazy re-fetch of
   an evicted blob is verified against its ref before being served — reuse
   the verifying joiner (`swarmfs/join.py`) or the whole-blob check
   (`content_address(data) == ref`). Without this, the trust model silently
   degrades from "trust nothing but your own disk" to "trust whatever
   served the re-fetch." Cost: hash time on a path that already paid for a
   network round trip — small relatively, but BMT is pure Python and
   nonzero on large blobs, so it is a knob with swarmfs's own `verify`
   semantics as precedent: on by default, disableable for a trusted-node
   setup.
2. **Push ref-equality assertion (L1 requirement, not optional).** The
   reference the node returns for an upload MUST equal the locally
   computed one, or the push fails loudly. This is a free end-to-end
   tripwire for the known address-space fork: a node uploading with
   erasure coding on returns a *different* reference for the same bytes
   (parity changes every intermediate chunk), and `swarm` addressing is
   pinned to redundancy off. Free because the local ref already exists —
   it is the blob's filename — so the assertion is a string comparison;
   there is no cost to make optional.
3. **Confirmation is tiered by trust (L1 policy).** Node claims (tags,
   stewardship responses) promote a root cheaply to *on-node*;
   *network-confirmed* — the rung that unlocks eviction — requires
   retrieve-and-verify through the network path for at least a random
   sample of the root's blobs, with the sampling rate a paranoia knob.
   Since confirmation is precisely what permits deleting the local copy,
   the verification budget is spent exactly where the invariant lives.
   Cost: background bandwidth in the push worker, never application
   latency — confirmation is off the commit path by design. The knob's
   default is nonzero; setting it to `0` is a deliberate operator opt-out
   that reverts eviction safety to trusting the node's claims, and
   `status()` should say so when it is off.
   (Whether stewardship's check already exercises the network path rather
   than the node's own store is one of the L1 questions to settle live.)
4. **Local scrub (L4).** The format already mandates that a blob file not
   hashing to its name is corrupt and MUST be treated as absent — a
   `scrub()` verb (git-fsck style) makes that real against bitrot:
   re-hash local blobs; a corrupt *evictable* blob demotes to absent and
   heals by verified re-fetch, a corrupt *pinned* blob is a loud integrity
   error (the journal still under-claims correctly either way).

**Non-use, recorded:** the journal is not hash-chained or signed. It is
local, single-writer, on the owner's own disk — an attacker who can edit it
can delete the blob files directly, and torn writes are handled by the
truncation rule. Tamper-evidence becomes relevant only if journals are ever
shared between machines; at that point Swarm's *signed feed history* is the
right authenticated lineage, not a signed local file.

For downstream readers, this layer completes an existing chain of custody:
a signed SOC feed update (verified by swarmfs) names a root; recordstore's
`prove()`/`verify_proof()` hash-chains any inclusion/absence claim down
from that root with no store access — and structure-resident/values-remote
mode keeps exactly the blobs needed to *generate* proofs local, so even a
store far larger than the disk can mint proofs offline.

## The boundary: shared vs. app-specific

**In the layer**: blob get/put; the local mirror with budget, soft limit, and
two-class TTL-aware eviction; pinned/evictable tracking from journal events;
background push with the durability ladder; `sync()`; lazy re-fetch; named
pins and `fetch`; `status()`; the journal (including its reflog role).

**Staying with each application**: everything that interprets blobs —
recordstore's trie, `diff`, three-way `merge`; swarmfs's Mantaray manifests
and directory semantics; merge policy on refs (merging tries and merging
directory trees are different problems); which roots are live (each app feeds
its root set *into* the layer, the layer never discovers it); key encoding.

Gray zone, to be settled by implementation rather than upfront: how much of
the "mutable ref + history" machinery (recordstore's `Pointer`, bzzf feed
mounts) can share a ref-journal utility.

## Concurrency and open questions

- **Single writer per store in v1**, enforced by a lock file. Immutable blob
  files make concurrent reads trivially safe; POSIX unlink semantics mostly
  protect a reader racing an eviction, Windows does not — multi-process
  writing is explicitly out of v1 scope and honestly documented.
- Journal compaction across the pinned-index boundary needs care (spec'd, per
  above).
- History retention vs. push-latest-only: old roots that were never pushed
  are pinned *forever* by the invariant — "keep all history" silently becomes
  a disk commitment. Wants an explicit retention policy ("history older than
  X is either pushed or dropped") rather than letting the pinning rule decide
  by accident.
- Confirmation mechanics (tags vs. stewardship vs. direct upload) to be
  pinned down against the current Bee API, live-tested in the house style.
- Multiple remotes (several feeds/nodes) — the journal's `remote-root` events
  allow it; not a v1 goal.

## Phases

- **L0 — Format spec + core store (no network).** The normative format
  document; blob dir + journal + rebuildable index; budget with soft limit;
  pinned/evictable from `committed` events; priority-class LRU eviction; the
  bounded in-memory cache wrapper (which also answers recordstore's
  "value-blob cache" need as its degenerate case). *Acceptance:
  property-tested invariant — "no unpushed blob is ever evicted" under
  random workloads, crash-injection included (truncate the journal at any
  byte; the store recovers under-claiming).*
- **L1 — Push worker + durability ladder.** Deferred/direct upload, tag or
  stewardship confirmation (whichever the live tests validate), TTL
  recording, `sync()` barrier, `status()`. Verification requirements:
  verified re-fetch, the push ref-equality assertion, and sampled
  retrieve-and-verify as the network-confirmed check (see *Verification and
  trust*). *Acceptance: kill the process at any point during push; recovery
  re-pushes idempotently; live test against a real Bee.*
- **L2 — recordstore adoption.** See recordstore's ROADMAP (Local-first
  track): its local/Bee stores become adapters over `localstore`; the journal
  becomes its reflog; push/pull verbs on `RecordStore`.
- **L3 — swarmfs write-path adoption.** The commit engine's spool becomes a
  `localstore`; transactional commits become local-first with background
  push; `bzzf` feed update rides the same ladder.
- **L4 — Working-set controls.** Named pins, `fetch`, only-on-Swarm
  accounting surfaced in `status()`, retention policy, and `scrub()`
  (local bitrot detection with verified-re-fetch healing).

Each phase is independently useful; L0+L1 already give any consumer a
correct local-first store with certainty semantics.
