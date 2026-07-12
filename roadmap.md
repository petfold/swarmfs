# swarmfs — Roadmap

Phased so that each milestone is independently useful and nothing sits on the Bee release
cycle's critical path. Read alongside `CLAUDE.md`.

## v0 — Read-only `bzz://` (the pandas/dask demo)

Goal: `pd.read_parquet("bzz://<ref>/data.parquet")` and
`dd.read_parquet("bzz://<ref>/dataset/")` work against a real network / public gateway.

- [ ] Package skeleton, `pyproject.toml`, entry-point registration for `bzz` protocol.
- [ ] `SwarmClient` — thin async aiohttp wrapper over Bee endpoints: `/bytes/{ref}`,
      `/bzz/{ref}/{path}`, `/chunks/{ref}`, `/health`. Range-request support on downloads.
- [ ] `swarmfs/mantaray/` codec — **parse** first (build comes in v1):
      - [ ] Node deserialization from raw chunk bytes (obfuscation key, version header,
            fork metadata, entry references, metadata).
      - [ ] Recursive walk via `/bytes` fetches to enumerate entries under a prefix.
      - [ ] Fixture-based unit tests (see Testing).
- [ ] `SwarmFileSystem(AsyncFileSystem)`:
      - [ ] `_ls`, `_info`, `_walk`, `_glob`, `_find` over the Mantaray walk.
      - [ ] `_cat_file` / `_get_file` with range support.
      - [ ] `SwarmFile` with `_fetch_range` for block caching / readahead.
      - [ ] Capability-detection seam (`_ListingBackend` interface, client-side impl only for now).
- [ ] Read-only against a **public gateway** with no stamp (the "no node of my own" path).
- [ ] Demos as tests: pandas single Parquet; dask partitioned Parquet (exercises `find`);
      `simplecache::bzz://…` chaining.

Exit criterion: dask reads a multi-file Parquet dataset from Swarm end to end.

## v1 — Immutable writes + stamps

Goal: write a collection, get a new root reference back, read it.

- [ ] Mantaray **build + patch**: construct a trie from entries; patch an existing trie so a
      single-file change re-uploads only the affected path.
- [ ] `StampManager`: list batches (`/stamps`), select/validate, check usability + TTL,
      fail early with actionable errors. `stamp="auto"` and explicit batch-id modes.
- [ ] `CommitEngine`: stage writes (memory + local spool), parallel chunk upload with tags
      (`/tags`) for progress, build/patch manifest, return new root.
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
- Whether `bzz://` writes should error loudly ("captured the new ref?") vs. return it quietly.
- Where the local write spool lives and its cleanup policy.
- Mantaray metadata key conventions to emit on write (align with any upstream standardization).
