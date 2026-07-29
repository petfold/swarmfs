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

## Stamp lifecycle: renewal (2026-07-29)

- [x] **The batch lifecycle after purchase**, driven by a real need: a published demo's
      batch had 24 days left and no way to extend it except raw `curl`. Client tier
      (`_client.py`) gained the three missing endpoints — `stamp_topup`, `stamp_dilute`,
      `wallet` (sync twins auto-generated, enforced by `test_facade_mirrors_async_surface`).
      Policy tier (`stamps.py`) gained pure arithmetic (`ttl_to_amount`, `amount_to_ttl`,
      `batch_cost_bzz`), inspection (`list_batches`, `get_batch`, `balance_bzz`), and
      plan/apply pairs: `plan_topup` (extend BY `ttl_secs`, TO `total_ttl_secs`, or for at
      most `budget_bzz`) + `topup`, `plan_dilute` + `dilute`. Same doctrine as `buy`:
      plans are pure questions, only the verbs spend, and every post-transaction failure
      path names the batch *and* the tx.
      *Findings pinned by tests (all measured live against Bee 2.8.1):*
      1. **A topup is additive** — the applied `amount` delta equals exactly what was
         paid; it does not restart the clock. Verified twice (+1 xBZZ → +16.08 d, and
         +6 h → +0.25 d on the same batch).
      2. **`amount` is cumulative from `blockNumber`**, not remaining balance — so
         `amount / currentPrice * 5` is *total* lifetime and the elapsed part is spent.
         Discovered by an integration assertion failing by exactly the batch's age
         (27.78 d implied vs 24.0 d reported, 3.79 d old). Remaining life is `batchTTL`.
      3. **The node indexes a topup ~40 s after the tx returns** (41.8 s measured), so an
         immediate read shows the old amount while the wallet is already debited —
         indistinguishable from a silent failure. Hence `_await_applied` polls.
      4. **The price moves**: 68657 → 68699 within one day, so a quoted TTL is an
         estimate. Monitoring `batchTTL` beats trusting a purchase-time calculation.
      5. **Dilution is paid for in TTL** (~halved per depth step) and only ever raises
         depth, so on a nearly-full immutable batch it must precede a topup —
         `plan_topup().warning` says so rather than leaving it to documentation.
      Live spending stays opt-in: `SWARMFS_TEST_SPEND=<xBZZ>` gates the one test that
      really tops up; the inspection/planning integration test spends nothing and runs
      on `SWARMFS_TEST_BEE` alone. Renewal *policy* (a CLI, expiry warnings, mapping
      batches to publications) belongs to callers — swarmfs has no CLI by decision.

## Batch sizing: derived, and exact where possible (2026-07-29)

- [x] **Depth sizing from bee's own numbers.** The three hardcoded size→depth tiers are
      gone. `stamped_chunks()` counts what a batch actually holds — leaves, per-level
      erasure parity, intermediates, dispersed root replicas — from bee's appendix-F
      tables (`pkg/file/redundancy/level.go`, ported verbatim), and `suggest_depth()`
      solves the balls-into-buckets bound to an explicit `risk` (default 1%).
      `plan()` forwards `redundancy`/`encrypted`/`risk` and records them on `BatchPlan`,
      or takes `depth=` outright.
- [x] **Exact sizing.** `bucket_histogram()` reproduces bee's `toBucket`
      (`BigEndian.Uint32(addr[:4]) >> 16`) and `depth_for_addresses()` returns the
      0%-risk depth when every address is known — which `split()` provides for a plain
      upload — leaving only node-generated parity to `extra_chunks`. Measured: 2 MB of
      random data needs depth 17 exactly, where the byte estimate says 18 (half the cost).
- [x] **Bucket truth from the node.** `SwarmClient.stamp_buckets()` +
      `StampManager.buckets() -> BucketStats` (`max_load`, `headroom`, `risk_for(n)`),
      cross-validated live against `/stamps` (its `utilization` *is* the fullest bucket).
- [x] **402 "batch is overissued" now explains the recovery** (dilute one depth, retry,
      top up) instead of reading like a dead stamp.
      *Findings pinned by tests:* encryption raises the chunk count by up to **1.7×** at
      PARANOID, not the ~1% claimed earlier (that came from misreading which table
      `maxParity` uses); the shallowest sellable depth is **17**, verified by the node
      rejecting depth 16 (`want min:17`) — note swarm-bee/bee-js name the *bucket
      depth* `MIN_DEPTH` (16) and require `depth > MIN_DEPTH`, which is the same rule
      inverted, not a disagreement; a bucket-full batch is **not**
      destroyed and dilution reopens it because the counters are preserved; and
      re-stamping an address the same batch already stamped costs no bucket slot
      (`stamper.Stamp` reuses the stored index, stamper.go:47-58).

## Local addressing (2026-07-28)

- [x] **Splitter** (`swarmfs/splitter.py`): `split(data) -> (root, chunks)` and
      `content_address(data)` build the Swarm chunk tree and its BMT addresses
      offline — the inverse of the verifying joiner, which previously only
      existed as a test helper. Verified against a live Bee 2.8.1 at every tree
      shape: with erasure coding off, the local reference equals what
      `POST /bytes` returns, exactly.
      *Finding pinned by that test:* nodes (and swarmfs itself, `redundancy=2`)
      commonly default to erasure coding, whose roots differ because parity
      chunks change every intermediate — Bee marks them by setting the span's
      top byte to `0x80 | level`. The test asserts both directions so nobody
      later "fixes" the splitter to chase a default it cannot reproduce.
      This is also the erasure-coding shape that broke `join.py`'s 128-fanout
      assumption (fixed the same week).

## Later / opportunistic

- [ ] Server-side listing endpoint support: when the upstream endpoint (ethersphere/bee#5535,
      https://github.com/ethersphere/bee/issues/5535) ships, add the server-side
      (status checked 2026-07-12: open, no maintainer response yet — v0 proceeds client-side)
      `_ListingBackend` impl behind the existing capability seam. No API change. Revisit the
      v0/v1 design if the issue progresses — it makes listing and writes materially cheaper.
- [ ] Server-side mutation endpoint support (same seam, write side).
- [ ] Encrypted references (128-hex) — decryption in the load path.
- [ ] ACT-protected content (pass the `swarm-act-*` headers through).
- [x] Redundancy level as a write option (erasure coding): `redundancy=0..4` storage
      option (default **2**; 0 disables, None = node default), passed as
      `swarm-redundancy-level` on all commit uploads. Live-validated: the root chunk's
      span carries the level and verified reads handle the parity refs.
- [ ] Extract `swarmfs/mantaray/` as a standalone `mantaray-py` package.
- [ ] **Extract postage management** — the pure half of `stamps.py` (51% of it, measured:
      erasure tables, sizing, arithmetic, parsers, plan builders, recovery messages) as a
      zero-dependency module, leaving each library its own transport. Boundary, layout and
      migration in `docs/postage-extraction.md`; blocked on
      [ethswarm-tools/bee-py#3](https://github.com/ethswarm-tools/bee-py/issues/3), where
      the same logic was offered to the Python Bee client that already exists (two of
      Peter's projects already depend on it). Trigger: no reply / not maintained, **and** a
      second consumer that wants stamps without a filesystem. Motivating smell: recordstore
      imports `swarmfs._client` — a private module — just to reach `StampManager`.
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
