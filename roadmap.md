# swarmfs — Roadmap

Phased so that each milestone is independently useful and nothing sits on the Bee release
cycle's critical path. Read alongside `CLAUDE.md`.

## v0 — Read-only `bzz://` (the pandas/dask demo)

Goal: `pd.read_parquet("bzz://<ref>/data.parquet")` and
`dd.read_parquet("bzz://<ref>/dataset/")` work against a real network / public gateway.

- [x] Package skeleton, `pyproject.toml`, entry-point registration for `bzz` protocol.
- [x] `SwarmClient` — thin async aiohttp wrapper over Bee endpoints: `/bytes/{ref}`,
      `/bzz/{ref}/{path}`, `/chunks/{ref}`, `/health`. Range-request support on downloads.
- [x] `swarmfs/mantaray/` codec — **parse** first (build comes in v1):
      - [x] Node deserialization from raw chunk bytes (obfuscation key, version header,
            fork metadata, entry references, metadata).
      - [x] Recursive walk via `/bytes` fetches to enumerate entries under a prefix.
      - [x] Fixture-based unit tests (see Testing). Note: `build`+`save` (the in-memory
            half of the v1 write path) landed early — offline fixtures needed them.
            Fixtures captured from a real Bee node still wanted as a cross-check.
- [x] `SwarmFileSystem(AsyncFileSystem)`:
      - [x] `_ls`, `_info`, `_walk`, `_glob`, `_find` over the Mantaray walk.
      - [x] `_cat_file` / `_get_file` with range support.
      - [x] `SwarmFile` with `_fetch_range` for block caching / readahead.
      - [x] Capability-detection seam (`ListingBackend` interface in `swarmfs/_listing.py`,
            client-side impl only for now).
- [ ] Read-only against a live node, and against a **public gateway** as an explicit
      opt-in (`allow_gateway=True` or similar — never automatic; when no node is reachable,
      fail with a message pointing at running a light node. See the gateways section in
      `CLAUDE.md`). Integration tests exist in `tests/test_integration.py`, gated on
      `SWARMFS_TEST_BEE`; not yet run against a live node.
- [ ] Demos as tests: pandas single Parquet ✓; dask partitioned Parquet (exercises `find`)
      — not yet; `simplecache::bzz://…` chaining ✓.

Exit criterion: dask reads a multi-file Parquet dataset from Swarm end to end.

## v1 — Immutable writes + stamps

Goal: write a collection, get a new root reference back, read it.

- [ ] Mantaray **build + patch**: construct a trie from entries; patch an existing trie so a
      single-file change re-uploads only the affected path.
- [ ] `StampManager`: list batches (`/stamps`), select/validate, check usability + TTL,
      fail early with actionable errors. `stamp="auto"` and explicit batch-id modes.
- [ ] `CommitEngine`: stage writes (memory + local spool), parallel chunk upload with tags
      (`/tags`) for progress, build/patch manifest, return new root. Before designing it,
      review `ipfspy` (Algovera) — rough, but has a local-node write path worth studying.
- [ ] Opt-in client-side chunk verification (BMT hash of fetched data vs. its reference)
      for the gateway read path — mitigation for the discouraged-but-supported gateway
      mode, not on by default for local nodes. May pull forward into v0 if gateway opt-in
      lands there.
- [ ] Wire fsspec `transaction` → deferred commit. `_pipe_file`, `_put_file`, `_rm`, `_mkdir`
      semantics defined for copy-on-write.
- [ ] `get_mapper` write path → **zarr write demo** (flagship). Round-trip a zarr array.

Exit criterion: create a zarr store on Swarm, read it back with xarray.

## v2 — `bzzf://` feed-mounted mutability

Goal: a stable, writable mount where the URL doesn't change as contents change.

- [ ] Feed read: resolve `bzzf://<owner-or-ens>/<topic>/path` → latest root via feed lookup.
      ENS resolution for owner where applicable.
- [ ] Feed write: after a commit, update the feed to the new root (needs signer config in
      `storage_options`).
- [ ] TTL/caching of feed resolution per filesystem instance.
- [ ] Concurrency note: feeds are last-write-wins; document it, don't pretend otherwise.

Exit criterion: two processes mounting the same `bzzf://` see each other's committed changes.

## Later / opportunistic

- [ ] Server-side listing endpoint support: when the upstream endpoint (ethersphere/bee#5535,
      https://github.com/ethersphere/bee/issues/5535) ships, add the server-side
      (status checked 2026-07-12: open, no maintainer response yet — v0 proceeds client-side)
      `_ListingBackend` impl behind the existing capability seam. No API change. Revisit the
      v0/v1 design if the issue progresses — it makes listing and writes materially cheaper.
- [ ] Server-side mutation endpoint support (same seam, write side).
- [ ] Encrypted references (128-hex) — decryption in the load path.
- [ ] ACT-protected content (pass the `swarm-act-*` headers through).
- [ ] Redundancy level as a write kwarg (erasure coding).
- [ ] Extract `swarmfs/mantaray/` as a standalone `mantaray-py` package.
- [ ] Optional: contribute to / consume an S3-compatible gateway effort (separate project;
      shares the same node-side primitives).

## Testing strategy

- **Fast unit layer (no node):** Mantaray codec against captured fixtures. Upload known small
  collections to a real Bee once, capture (reference, raw chunk bytes, expected entry list),
  commit the fixtures, and assert parse/walk correctness offline. This is where bugs live.
- **Integration layer (local Bee):** full read/write round trips against `http://localhost:1633`.
  Gate behind an env var / marker so unit tests run without a node.
- Property-style tests for the codec: build a trie from random entry sets, serialize, parse
  back, assert round-trip equality.

## Open decisions to revisit

- Exact `storage_options` schema (api url, stamp, signer, gateway mode, redundancy).
  Endpoint resolution order is decided (`storage_options` → `BEE_API_URL` → localhost:1633);
  still open: the gateway opt-in flag name (`allow_gateway`?), how to detect "this endpoint
  is a gateway, not my node", and the error copy that points users at light-node setup.
- Whether `bzz://` writes should error loudly ("captured the new ref?") vs. return it quietly.
- Where the local write spool lives and its cleanup policy.
- Mantaray metadata key conventions to emit on write (align with any upstream standardization).
