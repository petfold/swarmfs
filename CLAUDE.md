# swarmfs — CLAUDE.md

Persistent brief for Claude Code. Read this first, every session. Keep it updated as
decisions change; treat it as the source of truth over any single conversation.

## What this is

`swarmfs` is an [fsspec](https://filesystem-spec.readthedocs.io/) backend for
[Ethereum Swarm](https://docs.ethswarm.org/), talking to a [Bee](https://github.com/ethersphere/bee)
node over its HTTP API. Installing it makes Swarm a first-class storage backend for the
entire Python data ecosystem — pandas, dask, zarr, xarray, pyarrow, DuckDB — via URLs like
`bzz://<reference>/path/to/file.parquet`.

## Primary audience (drives priorities)

"Data people who should be able to ignore that it's Swarm." The read path and the
pandas/dask/zarr experience matter most. Swarm-native mutable-filesystem use
(feed-mounted writes) is a real but secondary audience — build the read story first.

## Names and identifiers (decided)

- Package / import name: `swarmfs` (confirmed unclaimed on PyPI).
- Main class: `SwarmFileSystem`.
- Protocols: `bzz://` (immutable, content-addressed) and `bzzf://` (feed-backed, mutable).
- File class: `SwarmFile`.

## The core impedance mismatches (why this isn't just another HTTP backend)

1. **Content-addressing vs. mutable paths.** Writing to Swarm produces a *new* reference;
   the old root is unchanged. Resolution: copy-on-write commit model. Writes are staged,
   then a commit builds a new Mantaray manifest, uploads only changed nodes+data, and
   yields a new root reference. Map this onto fsspec's existing `transaction` context
   manager. Every commit is automatically a snapshot (free versioning/rollback).
2. **Stable identity.** A root hash that changes on every write is hostile to config files
   and pipelines. Feeds provide a stable pointer: `bzzf://<owner-or-ens>/<topic>/path`
   resolves through a feed to the latest root manifest; commit updates the feed. This is
   the mutable filesystem; `bzz://` is immutable.
3. **Payment.** Writing costs money and needs a valid postage stamp (batch). Stamps live in
   `storage_options`. A stamp manager checks usability/TTL *before* a commit and fails early
   with a useful error, never a mid-write 402. Payment is also *ongoing*: when a batch
   expires its chunks stop being paid for and become the first candidates for eviction,
   so treat expiry as loss (the exact window is unpredictable) — see "Stamp lifecycle".

## The listing problem (CRITICAL architectural point)

Bee has **no server-side manifest-listing endpoint** today. To implement `ls`/`find`/`glob`
we must traverse the Mantaray trie **client-side**, fetching nodes via `/bytes` chunk by
chunk. This is the single biggest piece of real engineering in the project.

- A feature request for a server-side listing (+ mutation) endpoint has been filed upstream
  (full text in `docs/bee-feature-request.md`), tracked as **ethersphere/bee#5535**
  (https://github.com/ethersphere/bee/issues/5535). Open, no maintainer reply when
  filed; a read-only listing implementation is being prototyped on a bee fork (Go —
  new `pkg/api` handler over the existing `WalkNode`/`LookupNode` primitives, scoped to
  the read path with mutation as a follow-up). It is NOT on this project's critical path.
- **This whole approach assumes CURRENT Bee features (client-side trie traversal).** It should
  be revised depending on the status of issue #5535: if/when the server-side manifest listing
  (and mutation) endpoint ships, the design becomes significantly more efficient — listing
  collapses from O(trie nodes) round trips to O(pages), and the client-side Mantaray walk
  becomes a fallback rather than the primary path. Check the issue's status at the start of
  planning any listing/write work, and update this file and `docs/roadmap.md` accordingly.
- **Design for a dual read path with capability detection.** Probe for the server-side
  endpoint (via Bee version from `/health`, or by trying it once and caching the result per
  filesystem instance). If present, use it. If absent, fall back to client-side trie walking.
  When the endpoint eventually ships, the speedup arrives with no swarmfs release needed.
- v0 ships on the client-side path so it works against **today's** network and public gateways.

## The two hard engineering artifacts

1. **A Python Mantaray codec** (`swarmfs/mantaray/`). Bee gives no "list manifest" endpoint,
   so we parse and build the binary Mantaray trie ourselves. Needed for: listing (walk the
   trie via `/bytes`), and writes (patch the trie so changing one file in a big collection
   only re-uploads the affected path). Consider extracting as a standalone `mantaray-py`
   package later — it's independently useful. Reference implementations to study:
   `ethersphere/mantaray-js` and Bee's own `pkg/manifest/mantaray` (Go).
2. **The commit engine** (`swarmfs/commit.py`). Staging strategy (memory + local spool),
   parallel chunk uploads with tags for progress, building/patching the manifest, the
   feed-update step for `bzzf://`.

## v1 write semantics (decided, implemented)

- **Copy-on-write staged commits.** Writes stage on the filesystem instance; a commit
  validates the stamp first (fail early, never a mid-write 402), uploads data blobs in
  parallel, patches the Mantaray trie client-side (O(path depth) node re-uploads — proven
  against the real-Bee fixture), and yields a new root. Old roots are untouched: every
  commit is a snapshot.
- **Autocommit vs. transaction.** Outside a transaction every write op commits
  immediately. Inside ``with fs.transaction:`` everything is one commit per manifest
  lineage; rollback on exception discards staging having uploaded nothing.
- **Where does the new root go?** (old open decision — resolved: neither loudly nor
  quietly, but *queryably*.) The instance keeps an old→new root map: reads through the
  original URL see the latest committed state (read-your-writes), `fs.latest(ref)`
  returns the current head, `fs.commit_log` the history. Fresh manifests start at the
  pseudo-reference `bzz://new/...` (or `new-<suffix>` for several in one instance).
- **Lineage discipline.** Staging is keyed by each lineage's *origin* root and commits
  are serialized per instance, so concurrent writers (zarr writes chunks concurrently)
  extend one lineage instead of forking it. Content-addressing corner: committing
  identical content yields an identical root — never record an identity mapping
  (it makes head-resolution loop forever; found via xarray's double group-metadata write).
- **Metadata on write** (old open decision — resolved): emit bee-style `Content-Type`
  (guessed from the filename unless given) + `Filename`, matching what bee's own
  uploader produces (verified against the captured fixture).
- `mkdir`/`makedirs` are no-ops (directories are implicit in manifests). Write spool:
  `tempfile.SpooledTemporaryFile`, 16 MiB memory threshold (old open decision — resolved).
- Removing a directory's last file prunes the empty intermediate nodes (deliberate,
  small deviation from bee's Remove, which leaves empty nodes behind).

## Convenience surface & API tiers (decided, implemented)

- **`fs.upload(local_path) -> str` / `fs.download(rpath, lpath)`** are the
  hello-world one-liners; the README leads with them (the data-stack story comes
  second — nobody trusts the killer feature until the trivial round trip works).
  `upload` embraces the Swarm-native shape: the destination address is the
  *result* of a write, returned as the value. A single file is one direct
  `POST /bzz` through `SwarmClient` — deliberately NOT routed through the
  commit engine or fsspec's generic machinery (they add nothing for one file);
  a directory reuses the commit engine as a fresh manifest. Both paths hit
  `StampManager` first (fail early) and respect gateway policy via `_setup`.
  `upload(lpath, rpath)` (rpath given) keeps fsspec's base-class alias-of-put
  contract; `download` is an alias of `get`.
- **Generic `fs.put(local, "bzz://...")`** must never succeed in a way where the
  caller can't recover the reference. A bare/invalid destination raises a
  ValueError pointing at `fs.upload()` (and `bzz://new/…` + `fs.latest`). Put
  into an existing manifest path works normally (stage + commit). The generic
  `_get_file`/`_put_file` contract stays correct and tested — dask/rsync/
  third-party code calls it without knowing it's Swarm.
- **Three-tier public API**: raw HTTP (documented curl example, no shame in it)
  → `swarmfs.SwarmClient` (exported; direct async Bee calls with the shared
  endpoint resolution, no filesystem semantics) → `SwarmFileSystem`/fsspec.
  The middle tier has a blocking twin, `SyncSwarmClient` — the sync methods
  are generated from SwarmClient's coroutines (same signatures/docs, kept in
  lockstep by a test) and run on fsspec's shared background loop, the same
  trick fsspec uses for the fs object. Client-tier open items, deliberately
  not done yet: `stamp="auto"` resolution at this tier (safe — delegate to
  the same StampManager, explicit stamp skips resolution; just not needed
  yet) and exporting `VerifyingReader` for verified reads over an untrusted
  endpoint (gateway *refusal* stays fs-only by decision: SwarmClient
  endpoints are always explicit, so the silent-fallback risk it guards
  against doesn't exist at this tier).
  Convenience methods reach straight down to `SwarmClient`, skipping the middle
  layer when it adds nothing — but the fs object stays the single enforcement
  point for stamp/gateway/verification policy. No swarmfs CLI: that's
  swarm-cli's job (scope boundary, deliberate).
- **Exception taxonomy** (`swarmfs/exceptions.py`, exported from the package
  root): `SwarmError(OSError)` is the base for everything node/network —
  OSError so fsspec's and our own `except OSError` seams keep working.
  `BeeAPIError(SwarmError)` carries `.status`/`.url`/`.detail`;
  `BeePermissionError(BeeAPIError, PermissionError)` for 401/403 (gateway
  trust-detection catches it as before); 402 raises `StampError` — one type
  for "no usable stamp" whether caught locally by StampManager or as a node
  402. 404 stays builtin `FileNotFoundError` (fsspec semantics depend on it).
  `StampError` now lives in exceptions.py, re-exported from `swarmfs.stamps`.

## Stamp lifecycle: renewal (decided, implemented 2026-07-29)

Purchase was never the whole story — a batch expires and takes its content with
it. The lifecycle now lives in the same two tiers as everything else: raw
endpoints in `_client.py` (`stamp_topup`, `stamp_dilute`, `wallet`; sync twins
generated, lockstep enforced by `test_facade_mirrors_async_surface`), policy in
`stamps.py`. No Bee change was needed — unlike the listing gap, both PATCH
endpoints already exist.

- **Plan/apply pairs, mirroring `plan`/`buy`.** `plan_topup` (extend BY
  `ttl_secs`, TO `total_ttl_secs`, or for at most `budget_bzz` — the three
  questions publishers actually ask) + `topup`; `plan_dilute` + `dilute`. Plans
  are pure questions that spend nothing; only the verbs move money. Doctrine
  unchanged: swarmfs never spends implicitly, and after a transaction every
  failure path names the batch **and** the tx, or a paid-for operation becomes
  unverifiable. `topup` also refuses up front when the wallet can't cover the
  cost — the same fail-early stance as validating a stamp before an upload.
- **Facts learned live** (Bee 2.8.1, pinned by tests — the numbers are in
  `tests/test_stamps.py`, so don't "simplify" the arithmetic away):
  - A topup is **additive**: the applied `amount` delta equals exactly what was
    paid. Verified twice on one batch (+1 xBZZ → +16.08 d; +6 h → +0.25 d).
  - **`amount` is never a source of truth for remaining life** — two live
    findings, both caught by integration assertions failing. (1) It describes
    lifetime from `blockNumber`, elapsed part included: 27.78 d implied vs
    24.0 d reported on a 3.79-d-old batch. (2) It is the *local issuer's*
    bookkeeping, which bee's `HandleTopUp` increments in memory without
    persisting (`pkg/postage/service.go:186`); hours after two confirmed
    topups it had reverted to the creation value while `batchTTL` still
    reflected both. So no inequality between `amount` and `batchTTL` holds —
    the integration test asserts none, deliberately. `batchTTL` (from the
    batchstore, `estimateBatchTTLFromID`) is authoritative. This also forced
    `topup()` to accept a `batchTTL` jump as proof of application, not just an
    `amount` increase, or a paid-for topup would hang until timeout.
  - The node **indexes a topup ~40 s after the tx returns** (41.8 s measured),
    so an immediate read shows the old amount while the wallet is already
    debited — indistinguishable from a silent failure. `_await_applied` polls.
  - **The price drifts** (68657 → 68699 in a day), so any quoted TTL is an
    estimate; monitor `batchTTL` rather than trusting purchase-time maths.
  - **Dilution is paid for in TTL** (~halved per depth step) and only raises
    depth, so on a nearly-full *immutable* batch it must precede a topup.
    `plan_topup().warning` encodes that ordering instead of documenting it.
  - **An expired batch cannot be revived**: the node drops it, and a topup
    against it fails. Renewal is prevention, not repair.
  - **Bucket overflow does not destroy a batch** (corrected from Bee's own
    source, `pkg/postage/stampissuer.go:186` + `pkg/api/bzz.go:219`, after we
    had it wrong in comments and docs): a chunk hashing into a full bucket is
    refused on an **immutable** batch — `ErrBucketFull` → HTTP 402 "batch is
    overissued" — so the *upload* fails while the batch and everything it has
    already stamped survive, still paid for. Recovery is a dilution: depth+1
    doubles every bucket and the counters are preserved, so the retry
    succeeds and (addressing being deterministic) yields the same root. On a
    **mutable** batch the counter resets and the stamp index is reused,
    silently invalidating the chunk stamped there before — immutability buys
    a loud failure instead of a quiet one.
  - **Depth sizing is now derived, not folklore** (implemented; the three
    hardcoded tiers are gone). A batch is filled by *stamped* chunks, so
    `stamped_chunks()` counts them from bee's own appendix-F erasure tables
    (`pkg/file/redundancy/level.go`, ported verbatim) — leaves, per-level
    parity, intermediates and dispersed root replicas — and `suggest_depth`
    then solves the balls-into-buckets bound to an explicit `risk`
    (`DEFAULT_RISK = 1%`). Both `redundancy` and `encrypted` change the
    answer: inflation is 1.08/1.20/1.32/3.45 plain for MEDIUM/STRONG/INSANE/
    PARANOID and higher encrypted, up to **1.7× more at PARANOID** — an
    earlier claim here that encryption was ~1% was wrong (it came from
    misreading which table `maxParity` uses; bee's `New()` takes it from the
    *plain* table even for encrypted uploads, redundancy.go:47-55).
  - **Better still, sizing can be exact.** `depth_for_addresses()` builds the
    real bucket histogram from known chunk addresses — `bucket_histogram()`
    reproduces bee's `toBucket` exactly (`BigEndian.Uint32(addr[:4]) >> 16`,
    stampissuer.go:384) — so a plain upload gets a 0%-risk depth from
    `split()` output, and only node-generated parity stays probabilistic
    (pass it as `extra_chunks`). Worth it: 2 MB of random data sizes to depth
    17 exactly where the byte estimate says 18, i.e. half the cost. The live
    demo batch made the same point in reverse — the estimate called depth 18
    a 34% risk for content whose true histogram fit it exactly.
    `StampManager.buckets()` (`GET /stamps/{id}/buckets`, 65536 counters)
    gives the same truth for a batch you already own.
  - **The shallowest sellable depth is 17.** Verified live: `POST /stamps/1/16`
    is rejected with `{"field": "depth", "error": "want min:17"}`. Careful with
    the name across libraries: swarm-bee/bee-js call the *bucket depth*
    `MIN_DEPTH` (16) and require `depth > MIN_DEPTH` — the same rule, stated
    the other way round. A first draft of this note called their constant a
    bug; it is not, and the mistaken claim nearly became a public issue on
    their tracker. Read the enforcement, not the constant.
  - **Re-stamping a chunk already stamped by the same batch is free.**
    `stamper.Stamp` looks up `StampItem{BatchID, chunkAddress}` and, when it
    exists, refreshes the timestamp and reuses the stored batch index without
    calling `increment` (stamper.go:47-58) — so uploading identical content
    twice on one batch consumes no extra bucket slots, and address-keyed
    dedup in `split()` matches what the node will actually stamp.
- **Live tests are opt-in by cost**: the inspection/planning integration test
  spends nothing (runs on `SWARMFS_TEST_BEE` alone); the one test that really
  tops up is gated on `SWARMFS_TEST_SPEND=<xBZZ budget>` — an explicit amount
  doubling as consent.
- **Scope boundary holds**: monitoring/CLI/expiry policy and the
  batch↔publication mapping belong to callers (swarmlite), not here. swarmfs
  offers `list_batches()` + `StampInfo.problem(min_ttl)` as the primitive and
  stops there — there is still no swarmfs CLI, by decision.

## `modified()` (decided, implemented)

`AbstractFileSystem.modified()` raises `NotImplementedError` by default;
DuckDB's fsspec bridge calls it unconditionally, so `read_parquet` over a
registered swarmfs filesystem failed outright until this was overridden.
`SwarmFileSystem.modified(path)` checks the path exists (like `info`) and
returns a fixed constant (the epoch) — the honest answer, since `bzz://`
content is content-addressed and immutable at a fixed reference: there is no
real last-modified time to report, and a constant can never spuriously
invalidate a downstream cache. `bzzf://` mounts inherit this unchanged; it
does **not** reflect a feed's most recent update (the SOC payload's
timestamp is parsed in `feeds.py` but currently discarded) — a real
per-feed `modified()` is a reasonable future addition but wasn't in scope
for this fix.

## Base class and async

Subclass `fsspec.asyn.AsyncFileSystem` (the s3fs/gcsfs pattern) over `aiohttp`. fsspec
generates the sync interface automatically. Range requests: Bee supports HTTP Range on
downloads — implement `_fetch_range` so fsspec block caching / readahead work, which is what
makes Parquet predicate pushdown and zarr chunk reads viable.

## v2 feed semantics (decided, implemented)

- **Path model**: `bzzf://<owner>/<topic>/path` — owner is a 40-hex ethereum address
  (0x-prefix tolerated), topic is a human string (keccak256'd, bee-js
  `Topic.fromString` convention) or a raw 64-hex topic. ENS owners deferred.
- **Read** needs no keys: Bee's server-side sequence lookup (`GET /feeds`, headers only
  via `Swarm-Only-Root-Chunk`) finds the current index; we fetch the SOC chunk at that
  index ourselves and parse the payload — handling bee-js's `timestamp‖ref` format, a
  bare ref, and the wrapped-root-chunk format (via our BMT hasher).
- **Write** reuses the v1 commit machinery unchanged — a feed is just another lineage
  whose head advances — plus an `_after_commit` hook that publishes a client-side-signed
  SOC feed update (bee-js `timestamp‖ref` format, same postage batch as the commit).
  Requires `signer=<private key hex>` in storage_options and the optional `feeds` extra
  (`eth-keys` + `eth-hash[pycryptodome]`; core deps stay lean). Missing/mismatched
  signers fail at *staging* time, before anything uploads.
- **`swarmfs/bmt.py`**: BMT chunk addressing in pure Python — required for SOC signing
  (the signature covers the wrapped chunk's address), validated against the real
  references in the captured manifest fixture, and the primitive for the future opt-in
  chunk-verification mode.
- **Freshness/concurrency**: feed resolution is TTL-cached per instance (`feed_ttl`,
  default 15 s); own commits refresh it immediately; other writers' updates are adopted
  when seen (roots this instance committed are never rolled back by a stale lookup).
  Feeds are last-write-wins — documented, not papered over.
- **Listings stay in feed coordinates** (`<owner>/<topic>/…`), preserving the stable-URL
  illusion instead of leaking resolved root hashes.

## Prior art: ipfsspec (study, don't copy wholesale)

`ipfsspec` (IPFS backend in the official fsspec org) is the closest existing analog and
confirms our core choices: it subclasses `fsspec.asyn.AsyncFileSystem`, implements
`_cat_file`/`_ls`/etc. over an HTTP gateway, and registers `ipfs://` via entry points —
exactly our pattern. Two instructive contrasts:

1. It has stayed read-only, partly because writing to IPFS is awkward. Feeds + postage
   stamps give us a genuinely writable `bzzf://` — we can *exceed* the IPFS analog, not
   just match it.
2. Its one big unfinished piece is UnixFS HAMT support (sharded large-directory listing)
   — the direct analog of our Mantaray codec. This independently confirms that the
   manifest/trie codec is the load-bearing, bug-prone part: tests first, never mock the
   trie format.

Before designing the v1 commit engine, also look at `ipfspy` (Algovera) — rougher, but it
has a local-node write path. One ipfsspec pattern we deliberately do NOT adopt: public
gateway selection/fallback (see next section).

## Gateways, light nodes, and content verification (decided, implemented)

- **Endpoint resolution order**, consistent across the codebase (same shape as
  ipfsspec's convention): explicit `storage_options` (`api_url`) → an injected client's
  endpoint → `BEE_API_URL` environment variable → default `http://localhost:1633`.
- **Design stance: encourage running a light node, discourage gateways** — encoded in
  the software. First contact (`_setup`, once per instance) pings `/health`: an
  unreachable endpoint fails with an error pointing at light-node setup, never a silent
  gateway fallback. Trust detection: localhost is trusted; elsewhere the node-owner API
  (`/stamps`) is probed — blocked means "gateway", refused unless `allow_gateway=True`.
- **Content verification** (`swarmfs/join.py`): a verifying joiner walks the Swarm hash
  tree over `/chunks`, BMT-checking every chunk against the reference it was fetched by;
  range reads descend only the subtrees they need, so Parquet/zarr access stays viable.
  Manifest walks verify too (the listing loader routes through the same reader), and
  bzzf feed updates get full SOC verification (address + owner-signature recovery).
  `verify=None` (default) auto-resolves: **on for gateways, off for a trusted node**;
  either can be forced. Facts learned live: the BMT address covers the stored span
  as-is (erasure-coding level bits included), and intermediate chunks carry parity refs
  after the `ceil(span/unit)` data refs — traversal takes only the data refs. Bare-ref
  reads (`/bzz` index-document resolution) are refused under verification — they resolve
  server-side and cannot be checked.

## What falls out for free (validate these as acceptance demos)

- `fs.get_mapper("bzz://ref/store")` → MutableMapping → **zarr on Swarm**. This is the
  flagship demo for the data audience.
- `simplecache::bzz://ref/big.parquet` → local caching via fsspec URL chaining, zero code.
- Entry-point registration → every fsspec consumer understands `bzz://` after `pip install`.

## Constraints / environment

- Assume a local Bee node at `http://localhost:1633` by default; configurable per the
  resolution order above. Gateway reads (read-only, no stamp) may exist for the
  "no node of my own" crowd, but only as an explicit opt-in — the answer we lead with is
  "run a light node" (see the gateways section above).
- Target modern Python (3.11+ — floor raised from the original 3.10+ once CI showed
  `zarr>=3`, a test dependency, has no release supporting 3.10; see Packaging & CI).
  Keep runtime deps lean: `fsspec`, `aiohttp`. Everything else (numpy/zarr/pandas) is
  test/dev-only and optional.
- Peter's context: comfortable with content-addressed tries over chunks (cf. his OntoDAG
  `recordstore` work). Don't over-explain Swarm internals; do surface API-shape decisions.

## Packaging & CI (decided, implemented)

- **Version**: `0.6.0` (localstore retention primitives: `rebase_root`
  — the app-assisted squash, with the `rebased` journal event now in the
  format spec — plus `gc_orphans`, `has_root`/`latest_root`, and the
  `durability=` commit-boundary fsync batching. recordstore 0.18.0's
  `squash_history` depends on exactly this release). Earlier: `0.5.0`
  the local-first store (localstore + localsync, L0–L2); `0.4.0` derived
  batch sizing + the topup-detection fix; `0.3.0` the stamp lifecycle;
  `0.2.0` local addressing; `0.1.0` the first real one. Live in both `pyproject.toml` and `swarmfs/__init__.py` —
  keep these two in sync on every bump. `.devN`/pre-release suffixes are
  excluded from `pip install` by default; a plain version with the "Alpha"
  classifier is the intended shape — the classifier signals maturity, the
  version string doesn't need to.
- **CI**: `.github/workflows/tests.yml` runs the offline suite across Python
  3.11–3.12 on push/PR (integration tests self-skip without
  `SWARMFS_TEST_BEE`, so no live Bee node is needed in CI), plus a `package`
  job that builds both artifacts, runs `twine check`, and asserts the sdist
  contains `LICENSE` and never contains `.claude/` — a direct regression
  guard for the packaging leak caught before the `0.1.0` release (see the
  git history around the `LICENSE`/packaging-fixes commit).
- **Publish**: `.github/workflows/publish.yml` triggers on pushing a `v*` tag
  (uniform across the stack; it no longer waits for a GitHub Release), re-runs
  tests, builds, and publishes via PyPI trusted publishing (OIDC — no stored
  API token) from the `pypi` environment. So a release is: bump both version
  strings → commit → `git tag vX.Y.Z && git push origin vX.Y.Z`. The one-time
  manual step (register `petfold/swarmfs`, workflow `publish.yml`, environment
  `pypi` at https://pypi.org/manage/account/publishing/) is **done** — 0.1.0
  and 0.2.0 published through it.

## Phase plan

See `docs/roadmap.md`. Short version:
- **v0** read-only `bzz://`: client + Mantaray parse + range reads. Enough for pandas/dask.
- **v1** stamps + immutable writes via the transactional commit engine.
- **v2** `bzzf://` feed-mounted mutability.
- **v3 (shipped L0–L4, 2026-08-04)** `swarmfs.localstore`/`localsync` — local-first blob
  store (pinned-until-confirmed invariant, durability ladder, p2p-native confirmation,
  push/sync; design: `docs/localstore-design.md`, format: `docs/localstore-format.md`).
  recordstore adopted it (its 0.17/0.18); swarmfs's own write path too:
  `SwarmFileSystem(local_store=..., redundancy=0)` commits offline via
  `LocalFirstCommitEngine`, `fs.sync()` is the barrier, bzzf feeds publish only after
  network confirmation, and reads are local-first for known refs (offline
  read-your-writes incl. `ls` and ranges; foreign refs still read through the node).
- **later** encrypted refs (128-hex), ACT, redundancy level as write kwarg, gateway fallback,
  wire up the server-side listing endpoint when it lands.

## Working agreements for Claude Code

- Update this file and `docs/roadmap.md` when a decision changes. They outlive any chat.
- Tests first for the Mantaray codec — it's the load-bearing, bug-prone part. Build against
  known fixtures (upload a small collection to a real Bee node, capture the reference, assert
  the codec's parse matches). Don't mock away the trie format; that's where the bugs hide.
- Keep the capability-detection seam clean: listing/mutation go through an internal interface
  with two implementations (client-side, server-side) so the server path drops in later.
- Prefer real integration tests against a local Bee over heavy mocking, but keep a fast unit
  layer that runs without a node (fixture-based).
