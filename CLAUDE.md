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

## Base class and async

Subclass `fsspec.asyn.AsyncFileSystem` (the s3fs/gcsfs pattern) over `aiohttp`. fsspec
generates the sync interface automatically. Range requests: Bee supports HTTP Range on
downloads — implement `_fetch_range` so fsspec block caching / readahead work, which is what
makes Parquet predicate pushdown and zarr chunk reads viable.

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

## Gateways, light nodes, and content verification (decided)

- **Endpoint resolution order**, consistent across the codebase (same shape as
  ipfsspec's convention): explicit `storage_options` (`api_url`) → `BEE_API_URL`
  environment variable → default `http://localhost:1633`.
- **Design stance: encourage running a light node, discourage gateways** — and the
  software should encode this. The local-node default is itself the nudge. No
  gateway-selection or silent-fallback behavior: when no node is reachable, fail with an
  error that points the user toward running a light node. If gateway reads are supported
  at all, they are an explicit opt-in (e.g. `allow_gateway=True`), never automatic.
- **Content verification.** ipfsspec verifies fetched data against the CID; Swarm's
  analog is that a reference is the BMT hash of the content, so chunks can be verified
  client-side against their address. Against a trusted local Bee this is unnecessary
  overhead; on the (discouraged, opt-in) gateway path it is what makes reads actually
  trustless, and a real differentiator. Plan: an opt-in verification mode, likely v0/v1
  for the gateway path, not necessarily on by default for local nodes. Frame it as
  *mitigation for the discouraged gateway path*, not an endorsement of gateways.

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
- Target modern Python (3.10+). Keep runtime deps lean: `fsspec`, `aiohttp`. Everything else
  (numpy/zarr/pandas) is test/dev-only and optional.
- Peter's context: comfortable with content-addressed tries over chunks (cf. his OntoDAG
  `recordstore` work). Don't over-explain Swarm internals; do surface API-shape decisions.

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
