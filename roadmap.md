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
      - [x] Cross-check against a manifest captured from a **real Bee node** (2.8.1):
            `tests/capture_fixture.py` records the raw nodes, `tests/test_real_fixture.py`
            asserts our codec parses Bee's own bytes offline. Confirmed metadata shape
            (`Content-Type` + `Filename` per file fork) and mid-edge dir splits.
- [x] `SwarmFileSystem(AsyncFileSystem)`:
      - [x] `_ls`, `_info`, `_walk`, `_glob`, `_find` over the Mantaray walk.
      - [x] `_cat_file` / `_get_file` with range support.
      - [x] `SwarmFile` with `_fetch_range` for block caching / readahead.
      - [x] Capability-detection seam (`ListingBackend` interface in `swarmfs/_listing.py`,
            client-side impl only for now).
- [x] Read-only against a live node — integration tests in `tests/test_integration.py`
      (gated on `SWARMFS_TEST_BEE`/`SWARMFS_TEST_STAMP`) pass against a real Bee 2.8.1 node:
      upload collection → find/ls/cat/range read round-trips.
- [x] **Public gateway** as an explicit opt-in (`allow_gateway=True` — never automatic;
      unreachable endpoints fail with a message pointing at running a light node; trust
      detection probes the node-owner API). See the gateways section in `CLAUDE.md`.
- [x] Demos as tests: pandas single Parquet ✓; dask partitioned Parquet (exercises `find`)
      ✓ offline *and* against a live node; `simplecache::bzz://…` chaining ✓.

Exit criterion: dask reads a multi-file Parquet dataset from Swarm end to end. **MET** —
`test_dask_partitioned_parquet_live` uploads a 3-partition dataset to a real Bee node and
reads it back through dask+swarmfs.

## v1 — Immutable writes + stamps

Goal: write a collection, get a new root reference back, read it.

- [x] Mantaray **build + patch**: construct a trie from entries; patch an existing trie so a
      single-file change re-uploads only the affected path. (One canonical async
      add/remove in `mantaray/build.py`, load-on-demand + copy-on-write; patch tests run
      against the captured real-Bee manifest and assert ≤5 node re-uploads out of 16.)
- [x] `StampManager`: list batches (`/stamps`), select/validate, check usability + TTL +
      fullness, fail early with actionable errors. `stamp="auto"` and explicit batch-id
      modes. (Fail-early proved its worth immediately: caught a 100%-utilized batch
      before any byte was uploaded, in the first live run.)
- [x] `CommitEngine`: stage writes (memory + local spool), parallel `/bytes` uploads,
      build/patch manifest, return new root. (Reviewed `ipfspy` first: it proxies IPFS's
      server-side MFS per-op — no staging, no atomicity — confirming our client-side
      staged-commit contrast.) Tags/progress reporting not wired yet — later.
- [x] Opt-in client-side chunk verification (BMT hash of fetched data vs. its reference):
      `swarmfs/join.py`, a verifying joiner with subtree-pruned range reads; manifest
      walks and bzzf SOC updates verified too. Auto: on for gateways, off for trusted
      nodes; forcible either way. Validated live (incl. erasure-coded spans and parity
      refs in intermediate chunks — both discovered against the real node).
- [x] Wire fsspec `transaction` → deferred commit (one commit per manifest lineage;
      rollback discards without uploading). `_pipe_file`, `_put_file`, `_rm`, `_mkdir`,
      `open("wb")`, `_cp_file` defined for copy-on-write. Root lineage model: `bzz://new/…`
      pseudo-refs, per-instance old→new root map (read-your-writes), `fs.latest()` +
      `fs.commit_log`. See "v1 write semantics" in `CLAUDE.md`.
- [x] `get_mapper` write path → **zarr write demo** (flagship): zarr 3's `FsspecStore`
      drives the async interface directly; round-trips offline.

Exit criterion: create a zarr store on Swarm, read it back with xarray. **MET** — offline
(`tests/test_zarr.py::test_xarray_dataset_roundtrip`) *and* against a real Bee 2.8.1 node
(`test_zarr_xarray_roundtrip_live`), plus a live transactional write/rm/snapshot round
trip. Notes from the live run: the old postage batch filled up and the fail-early
`StampManager` caught it before any upload; bought `swarmfs-tests` (depth-18; NB Bee's
`POST /stamps` takes `Immutable` as a *header*, not a query param, so it came out
immutable — fine at this depth).

## v2 — `bzzf://` feed-mounted mutability

Goal: a stable, writable mount where the URL doesn't change as contents change.

- [x] Feed read: resolve `bzzf://<owner>/<topic>/path` → latest root via feed lookup
      (server-side sequence lookup + client-side SOC parse; all three payload formats).
      ENS resolution for owner still deferred (needs a resolver-enabled node).
- [x] Feed write: after a commit, update the feed to the new root — client-side signed
      SOC (`signer` in `storage_options`, `feeds` extra). Includes `swarmfs/bmt.py`
      (BMT chunk addressing, validated against real captured references) — also the
      primitive for the future chunk-verification mode.
- [x] TTL/caching of feed resolution per filesystem instance (`feed_ttl`, default 15 s;
      own commits refresh immediately, external updates adopted).
- [x] Concurrency note: feeds are last-write-wins; documented in `CLAUDE.md` and the
      module docstring, not pretended otherwise.

Exit criterion: two processes mounting the same `bzzf://` see each other's committed
changes. **MET** — offline (`test_feedfs.py::test_update_cycle_two_writers`, with real
signature verification in the fake node) *and* live against Bee 2.8.1
(`test_bzzf_two_mounts_live`): three mounts, real SOC signing, real network propagation
(measured ~6 s on a light node — reads poll because Swarm is eventually consistent).
The live run also flushed out a staleness bug: fsspec's dircache made `ls` skip feed
re-resolution entirely, so listings never honored `feed_ttl`; bzzf now refreshes the
feed before consulting the listing cache.

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

- Exact `storage_options` schema — mostly settled now: `api_url`, `stamp`, `pin`,
  `signer`, `feed_ttl`, `allow_gateway`, `verify`, `block_size`, `timeout`, `headers`.
  Gateway detection = probe of the node-owner API (`/stamps`); error copy points at
  light-node setup. Remaining: redundancy level as a write kwarg (still "later").
- Whether `bzz://` writes should error loudly ("captured the new ref?") vs. return it quietly.
- Where the local write spool lives and its cleanup policy.
- Mantaray metadata key conventions to emit on write (align with any upstream standardization).
