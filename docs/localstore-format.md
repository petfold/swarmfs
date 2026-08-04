# The localstore on-disk format, version 1

**Normative.** This document is the interoperability contract for a
local-first blob store directory (design and rationale:
[`localstore-design.md`](localstore-design.md)). The Python implementation
in `swarmfs/localstore.py` is *one* implementation of this format, the way
git is one implementation of `.git`. A conforming implementation in any
language MUST be able to open, read, extend, and recover a store written by
any other. Authoritative state is therefore restricted to formats trivially
parseable everywhere: UTF-8 text, JSON Lines, and raw blob files.

RFC 2119 keywords (MUST/SHOULD/MAY) are used with their usual meaning.

## Directory layout

```
<store>/
  format          # version + addressing line (see below)
  blobs/          # one file per blob, immutable, content-addressed
    ab/
      abcdef…     # full lowercase hex ref as the filename
  journal.jsonl   # authoritative append-only event log
  index/          # OPTIONAL, implementation-defined, always rebuildable
  lock            # advisory lock target
```

A store directory is self-contained: copying it (rsync, tar) copies the
store. Nothing outside the directory is authoritative.

### `format`

One line of UTF-8 text, three space-separated fields:

```
swarmfs-localstore 1 swarm
```

magic string, format version (integer), addressing scheme. Defined schemes:

- `swarm` — the ref is the Swarm reference of the blob (BMT chunk-tree root,
  erasure coding off), 64 lowercase hex chars. Shares Swarm's address space:
  a blob's local name equals what `POST /bytes` returns for it.
- `sha256` — the ref is the SHA-256 of the blob, 64 lowercase hex chars.

All refs in one store use the store's single scheme. Implementations MUST
refuse to open a store whose magic, version, or scheme they do not support.

### `blobs/`

Each blob is stored verbatim (no framing, no compression) at
`blobs/<ref[:2]>/<ref>`. Files are **immutable**: writers MUST create them
by writing a temporary file in the same directory and `rename(2)`-ing it
into place, and MUST NOT modify one after that. A blob file whose content
does not hash to its name is corrupt and MUST be treated as absent.

The file's modification time is the recency signal for eviction: readers
SHOULD bump it (e.g. `utime`) on access. Deleting a blob file is
**eviction** and is legal exactly when the journal proves the blob
evictable (see *Derived state*).

### `journal.jsonl`

The single source of truth. UTF-8, one JSON object per `\n`-terminated
line, append-only. Every object has an `ev` field naming the event type and
SHOULD have a `ts` field (seconds since the Unix epoch, informational only —
no consistency decision may depend on wall clocks). Writers MUST `fsync`
after appending events whose loss would over-claim durability (in practice:
all of them; they are rare and small).

**Recovery rule:** a final line that fails to parse or lacks its `\n`
terminator is a torn write; readers MUST discard it and accept everything
before it, and a writer MUST truncate the torn tail before appending
(otherwise the next event concatenates onto the fragment, turning a
recoverable torn line into corruption mid-file). A non-final line that
fails to parse is corruption and MUST be reported, not skipped. Unknown
`ev` values MUST be preserved on rewrite and otherwise ignored (forward
compatibility).

Event types:

```jsonc
{"ev": "committed", "root": "<ref>", "parent": "<ref>"|null,
 "blobs": ["<ref>", …], "structure": ["<ref>", …]?, "bytes": N?, "ts": T}
```
A new root exists locally. `blobs` lists every blob **new in this commit**
(the root blob itself included when new); blobs shared with the parent are
not repeated. `structure` (optional) is the subset of `blobs` that is index
structure rather than payload — an eviction-priority hint, not semantics.
`bytes` (optional) is the total size of `blobs`, kept so that
only-on-Swarm accounting survives eviction.

```jsonc
{"ev": "pushed", "root": "<ref>", "ts": T}
```
A push of this root's blobs was accepted by a Bee node (deferred upload:
the *node* has them; the network may not yet).

```jsonc
{"ev": "confirmed", "root": "<ref>", "batch": "<hex>"|null,
 "ttl": seconds|null, "ts": T}
```
This root's `blobs` list was verified retrievable from the Swarm network,
covered by postage batch `batch` with `ttl` seconds of observed remaining
validity at time `ts`. A root MUST NOT be confirmed before its parent:
"root R is network-confirmed" is then equivalent to "R and every ancestor
carry `confirmed` events", i.e. the whole tree is retrievable.

```jsonc
{"ev": "remote-root", "remote": "<name>", "root": "<ref>", "ts": T}
```
The named remote (a feed, a node) is known to point at `root` — the
remote-tracking ref.

```jsonc
{"ev": "pin",   "name": "<name>", "refs": ["<ref>", …], "ts": T}
{"ev": "unpin", "name": "<name>", "ts": T}
```
Named pins: the listed blobs are exempt from eviction while the pin stands.
A later `pin` with the same name replaces the earlier ref list.

```jsonc
{"ev": "rebased", "root": "<ref>", "blobs": ["<ref>", …],
 "structure": ["<ref>", …]?, "bytes": N?, "ts": T}
```
History retention (app-assisted squash): the lineage collapses onto
`root`, whose `blobs` list is its **full reachable set** — supplied by the
application, which unlike this layer can walk its own blob structure. On
fold, every other root is dropped (parent becomes null, `root`'s
durability rung and batch facts survive from its earlier events), and
blobs listed only by dropped roots become orphans, deletable by garbage
collection. A writer MUST refuse to append this event if any listed blob
is neither locally present nor listed by a confirmed root, since dropping
the older events would otherwise lose the last copy. Readers that do not
know this event ignore it (the general unknown-`ev` rule), which errs in
the safe direction: dropped roots stay pinned.

```jsonc
{"ev": "snapshot", "state": {…}, "ts": T}
```
Compaction: `state` is the full fold of every event before this line
(shape: the keys of *Derived state* below, with per-root blob lists
retained for roots not yet confirmed and for only-on-Swarm accounting).
Readers MAY start folding from the last `snapshot`. Compaction MUST be
performed by writing a complete replacement journal (snapshot event first,
then any events being retained verbatim) to a temporary file, `fsync`-ing,
and renaming over `journal.jsonl` while holding the lock.

**The lag rule (normative):** every event MUST be appended *after* the fact
it records is true. A crash may therefore leave the journal under-claiming
durability (safe: recovery re-pushes/re-confirms, idempotent under
content-addressing) and never over-claiming it (which could justify
evicting the only copy).

### `index/`

Implementation-defined acceleration (e.g. SQLite). It MUST be derivable
from `journal.jsonl` plus `blobs/` alone, and readers MUST tolerate its
absence, staleness, or corruption by rebuilding it. It holds no truth.

### `lock`

Implementations MUST hold an exclusive advisory lock (POSIX `flock` or
equivalent) on this file for the lifetime of any handle that writes.
Version 1 of the format assumes a **single writer per store**; concurrent
readers of immutable blob files are always safe.

## Derived state (the fold)

Folding the journal yields, deterministically:

- **Rungs**: per root — *committed*, *pushed*, or *confirmed* (highest event
  seen); a root is **network-confirmed** iff it and all ancestors are
  *confirmed*.
- **Remote-tracking roots**: last `remote-root` per remote name.
- **Lineage**: parent links from `committed` events (the reflog).
- **Pins**: last `pin` ref-list per name not followed by `unpin`.
- **Evictable blobs**: blobs listed in at least one `confirmed` root's
  `committed` event and in no named pin. (A blob shared by a confirmed and
  an unconfirmed root is evictable: confirmation means Swarm holds it, and a
  later push of the unconfirmed root need not re-upload it.)
- **Pinned blobs**: every locally present blob that is not evictable — which
  works out to: blobs whose *every* listing root is unconfirmed, named-pin
  refs, and blob files present in `blobs/` but absent from all `committed`
  events (orphans — conservative: they may be a commit in progress).

**The invariant** a conforming implementation maintains: a blob file may be
deleted only if it is evictable under the fold of the journal *as currently
on disk*. Combined with the lag rule, no sequence of crashes can delete the
last copy of anything.

Eviction order is not normative, but implementations SHOULD evict payload
blobs before `structure` blobs and SHOULD NOT evict blobs whose covering
batch (`confirmed.batch/ttl/ts`) is close to expiry.
