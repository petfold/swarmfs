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
   with a useful error, never a mid-write 402.

## The listing problem (CRITICAL architectural point)

Bee has **no server-side manifest-listing endpoint** today. To implement `ls`/`find`/`glob`
we must traverse the Mantaray trie **client-side**, fetching nodes via `/bytes` chunk by
chunk. This is the single biggest piece of real engineering in the project.

- A feature request for a server-side listing (+ mutation) endpoint has been filed upstream
  (see `docs/bee-feature-request.md`), tracked as **ethersphere/bee#5535**
  (https://github.com/ethersphere/bee/issues/5535). It is NOT on this project's critical path.
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

- **Version**: `0.1.0` (bumped from the placeholder `0.1.0.dev0` in both
  `pyproject.toml` and `swarmfs/__init__.py` — keep these two in sync on every
  bump). `.devN`/pre-release suffixes are excluded from `pip install` by
  default; `0.1.0` with the existing "Alpha" classifier is the intended shape
  for a first real release — the classifier signals maturity, the version
  string doesn't need to.
- **CI**: `.github/workflows/tests.yml` runs the offline suite across Python
  3.11–3.12 on push/PR (integration tests self-skip without
  `SWARMFS_TEST_BEE`, so no live Bee node is needed in CI), plus a `package`
  job that builds both artifacts, runs `twine check`, and asserts the sdist
  contains `LICENSE` and never contains `.claude/` — a direct regression
  guard for the packaging leak caught before the `0.1.0` release (see the
  git history around the `LICENSE`/packaging-fixes commit).
- **Publish**: `.github/workflows/publish.yml` triggers on a published GitHub
  Release, re-runs tests, builds, and publishes via PyPI trusted publishing
  (OIDC — no stored API token). **Requires a one-time manual step only the
  repo owner can do**: register `petfold/swarmfs`, workflow `publish.yml`,
  environment `pypi` as a (pending) trusted publisher at
  https://pypi.org/manage/account/publishing/ before the first release is
  cut — until then the `publish` job will fail at the OIDC exchange step.

## Phase plan

See `docs/roadmap.md`. Short version:
- **v0** read-only `bzz://`: client + Mantaray parse + range reads. Enough for pandas/dask.
- **v1** stamps + immutable writes via the transactional commit engine.
- **v2** `bzzf://` feed-mounted mutability.
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
