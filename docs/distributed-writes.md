# swarmfs — Distributed writes, pinned mounts, listing at scale

Design handoff, 2026-09-22. Companion to `CLAUDE.md` and `ROADMAP.md`; the
checklist lives in `ROADMAP.md` and the decisions in `CLAUDE.md` (transplanted
2026-09-22), so this file is the reasoning behind them, not a second to-do list.

Origin: a review of what swarmfs still lacks for the *living* part of the
Blaze ecosystem (Dask; and, by extension, the engines that replaced Blaze —
Ibis, DuckDB, table formats). The verdict was that fsspec already covers the
read side completely. The gaps are all on the write side and in listing cost,
plus one small read-side feature (pinned feed mounts) that a table layer
needs. A new sibling project, `brash` (Apache Iceberg on Swarm), is the first
consumer of everything below; keep its needs in view but do not let table
semantics leak into swarmfs — the scope boundary stays where `CLAUDE.md`
draws it.

## 1. The gap, precisely

`fs.transaction` is per-process. Inside one process concurrent writers extend
one lineage (lineage discipline, `CLAUDE.md`), and zarr/xarray ride on that.
Across processes — a Dask cluster writing a partitioned Parquet dataset —
each worker holds its own `SwarmFileSystem`, stages into its own lineage, and
commits its own root. Nothing assembles those roots into one manifest, and
the driver never learns the references. `dd.to_parquet("bzz://new/ds/")`
therefore "works" and produces N unrelated roots and N stamp validations.

Three facts make the fix small:

- Content addressing composes. A Mantaray entry is a *reference*; it does not
  care who uploaded the data or with which batch. A manifest built on the
  driver can point at blobs uploaded by any worker through any node.
- `SwarmClient.bytes_post` already returns a bare data reference (not a
  wrapping manifest), which is exactly what a file fork's entry needs.
- The commit engine already separates "upload staged payloads" from "build
  the trie" — `CommitEngine.commit` uploads `writes` then patches. A staged
  entry that is *already* a reference just skips the first step.

## 2. Primitives (filesystem tier)

### 2.1 `fs.put_blob(data, *, content_type=None, stamp=None) -> str`

Upload one payload and return its data reference. `POST /bytes` through
`SwarmClient.bytes_post` with the instance's `pin`/`redundancy`/`encrypt`
policy; `StampManager` resolves the batch first (fail early, as everywhere).
No manifest, no lineage, no staging. `data` is `bytes` or a binary
file-like (spooled like `_put_file`). This is the worker-side call.

Under `local_store=` it lands in the store and is journaled as a root of
its own (a one-blob root), so the usual push/confirm ladder applies and
`fs.sync()` is the barrier — see §5 on why that matters for workers.

Refused: `act=True` instances (a bare blob is not a root to wrap; the
Iceberg-style consumer protects the *manifest* root, not blobs). Encrypted
instances return 128-hex references, as `upload` does.

### 2.2 `fs.link(path, reference, *, size=None, metadata=None)`

Stage a manifest entry that points at an existing reference. Same staging
table as `pipe_file`, same lineage rules (`bzz://new/…`, existing roots,
`bzzf://` heads), same transaction semantics. Implementation: a
`StagedLink(reference, size, metadata)` alongside `StagedWrite`; the commit
engine's upload step passes links straight through to the trie patch.

- `size` is optional and advisory (for `info()` before the first read); when
  absent, `info()` reads the span from the root chunk as it does for any
  entry.
- `metadata` defaults to bee-style `Content-Type` (guessed from `path`) +
  `Filename`, via `_guess_metadata`, exactly like a written file.
- A link must match the lineage's refBytesSize (64-hex into a plain
  lineage, 128-hex into an encrypted one); mixing is refused with the
  existing "a lineage cannot mix" error.
- Under `local_store=`: a linked reference is *foreign* by construction —
  not persisted, not pushed — the same rule as foreign-lineage parents.
  Correct because the uploader is responsible for its own blob's
  network residency (§5).

`CommitResult.written` already maps `path -> data reference`; links appear
there unchanged, so `fs.commit_log` tells the truth about where each entry
came from.

### 2.3 Commit is unchanged

`with fs.transaction: fs.link(...); fs.link(...)` → one root. Nothing new to
learn; `fs.latest("new")` returns it, bzzf publishes it. That is the whole
driver-side protocol.

## 3. `swarmfs.dask` (helper module, optional dependency)

Two entry points, both thin:

```python
import swarmfs.dask as sd

root = sd.to_parquet(ddf, "bzz://new/ds", storage_options={"stamp": "auto"})
# -> "c0ffee…" — one manifest, one commit, N partitions uploaded by workers

root = sd.to_parquet(ddf, "bzzf://<owner>/ds", storage_options={..., "signer": key})
# -> feed advanced once, after the single commit
```

Mechanics: `map_partitions` writes each partition to bytes with pyarrow and
calls `fs.put_blob` on the worker (the worker's own `SwarmFileSystem`, built
from the same `storage_options`); the results `(path, reference, size)` come
back to the driver, which does `link` for each inside one transaction and
returns `fs.latest(...)`. `name_function`, `partition_on`, `write_index`,
`schema`, `write_metadata_file` follow dask's own `to_parquet` signature where
they make sense (`partition_on` produces hive-style paths; the optional
`_metadata`/`_common_metadata` files are written on the driver as ordinary
`pipe_file`s in the same transaction).

The same shape gives `to_zarr`-style array writes for free later; do not
build it until something needs it.

**Why a helper and not the generic path.** `dd.to_parquet` calls
`fs.open(path, "wb")` on workers; there is no channel back to the driver
except side effects. One could recover references after the fact
(`distributed.Client.run` to collect a per-worker `fs.staged_links` table),
and a `stage_only=True` storage option could make `open("wb").close()` do
`put_blob` and record the link instead of committing. Keep that as an
*opportunistic* follow-up behind the explicit helper: it is fragile (worker
restarts lose the table), scheduler-specific, and the helper covers the
real use. Document the generic-path behaviour honestly in the User Guide
(N roots, N stamps) so nobody is surprised.

*Checked 2026-09-22*: the User Guide's Dask section covers reading a
partitioned dataset and writing one inside a single-process
`with fs.transaction:`; it says nothing about `dd.to_parquet` to a `bzz://`
URL. So the honest paragraph is an **addition**, not an edit — it belongs
next to the existing `scheduler="threads"` caveat at the end of
[dask.bag](USER_GUIDE.md#daskbag), which is the other place where "the
filesystem object does not cross a process boundary" bites.

## 4. Stamps across workers

`put_blob` needs a usable batch **on the node the worker talks to**. A batch
is owned by one node (the issuer signs), so:

- One Bee node for the cluster (the usual case: a cluster-local light node,
  or the driver's node exposed to workers): every worker's `stamp="auto"`
  resolves against the same `/stamps`; nothing to configure.
- Several nodes: each worker's node needs its own batch. The manifest still
  composes — but **the dataset's lifetime is the shortest `batchTTL` among
  the batches that stamped its parts**, and the driver cannot see the
  workers' batches. `sd.to_parquet` therefore returns the set of
  `(node, batch)` pairs used, and `fs.commit_log` records the driver's
  batch as today. Renewal policy stays with callers (`StampInfo.problem`,
  `list_batches`); swarmfs does not chase foreign batches.

Bucket exhaustion is per batch and per upload; a worker hitting
`ErrBucketFull` fails its partition with the existing 402 recovery message,
dask retries the task, and the retry stamps identical content for free (the
re-stamp rule in `CLAUDE.md`). Nothing new.

## 5. Local-first workers

Workers with `local_store=` commit their blob offline and push later.
A driver that `link`s a reference the network does not yet hold produces a
root that resolves on the driver's node only once every worker has synced.
Rule for the helper: `sd.to_parquet` calls `fs.sync()` on each worker
*before* returning references when the worker filesystem is local-first
(cheap when it is not). A bzzf publish of a root with un-synced links
would otherwise advertise unreachable content — the same invariant the
localstore's publish-after-confirmation listener protects, one level up.

## 6. Listing at scale

Dask's `find()` over a dataset with thousands of partitions walks the trie
node by node. `MantarayListingBackend` already shares a reference-keyed
`NodeStore` (content-addressed, safe across roots). Two steps, in order:

1. **Measure, then parallelise.** If the walk is sequential per level, make
   `iter_files` a bounded-concurrency BFS (reuse the commit engine's
   semaphore pattern, `concurrency=8` default). Expect the round-trip count
   to stay the same and the wall time to drop by the fan-out. Add a
   benchmark test over the fake node with a 2,000-file synthetic manifest
   so regressions are visible.

   *Checked 2026-09-22*: the walk **is** sequential — `mantaray/walk.py`
   recurses depth-first through `_iter_fork` (and `list_directory.process`),
   awaiting one `store.resolve` at a time, so a level's children are fetched
   one after another. Step 1 is therefore real work, not just the benchmark.
2. **Optional root index** (`index=True` on commit, default off): the
   commit writes `.swarmfs/index.json` at the manifest root — every file's
   path, data reference, size and metadata — and the listing backend, if it
   finds that entry, answers `find`/`ls`/`info` from one fetch. Off by
   default because it changes the root (the reference no longer equals a
   plain bee upload of the same tree, and canonical-revisit rules in the
   localstore must treat the index as structure). Consumers with big
   datasets (brash, `sd.to_parquet`) turn it on. This is a third
   `ListingBackend` behind the existing seam, so when bee#5535 ships all
   three coexist.

Do not add a client-side prefix cache beyond fsspec's `dircache`; bzz roots
are immutable, so the reference-keyed node cache is already the right cache.

## 7. Pinned and time-travelled feed mounts (read side)

A bzzf mount is a live view. A table layer needs two more readings of the
same URL, and both are one lookup away:

```python
fsspec.filesystem("bzzf", at_root="<64-hex>")        # freeze: bzzf://o/t/… resolves against this root
fsspec.filesystem("bzzf", at="2026-09-01T12:00Z")    # feed as of a time (Bee: GET /feeds?at=)
```

`at_root` bypasses feed resolution entirely (a bzzf path is then just a bzz
path with a stable prefix); `at=` resolves once via the feed's `at`
lookup and then behaves like `at_root`. Writes are refused on both (they
are views). The point: every fsspec consumer — DuckDB's registered
filesystem, dask, pyarrow — gets tamper-evident, reproducible reads of a
mutable URL with no path rewriting, because the *paths* Iceberg records are
`bzzf://` and the *pin* is a storage option.

While here: `SwarmFileSystem.modified()` returns the epoch for bzzf too
(`CLAUDE.md` notes the feed timestamp is parsed and discarded). Return the
feed update's timestamp for bzzf roots. DuckDB and Iceberg readers use it
for cache invalidation; a live view that never changes its mtime defeats
them.

## 8. Metadata keys — closing the open decision

`ROADMAP.md` lists "Mantaray metadata key conventions to emit on write" as
open. Decision proposed: **swarmfs emits only bee's own keys**
(`Content-Type`, `Filename`) and **passes caller `metadata=` through
untouched**; it defines no keys of its own. Layers above namespace their
keys with their package name (`brash.*`, `swarmlite.*`, `ontodag.*`). The
`info()` result already surfaces `metadata`, so nothing else is needed. If
Bee standardises keys upstream, swarmfs follows bee; it does not lead.

## 9. Upstream: `fsspec.registry.known_implementations`

swarmfs registers `bzz`/`bzzf` via entry points, so installed it works. Not
installed, `fsspec.filesystem("bzz")` fails with a generic protocol error.
A two-line PR to fsspec's `registry.py` adding
`"bzz": {"class": "swarmfs.SwarmFileSystem", "err": "Install swarmfs to access Swarm"}`
(and `bzzf`) turns that into an actionable message for every user of every
fsspec-based tool. Do it once the package is out of Alpha; ipfsspec is the
precedent entry to copy.

## 10. Order of work

1. §2 primitives + tests (offline against the fake node: links appear in
   the trie with the right entry, `info()` sizes, encrypted/plain refusal,
   local-first foreign rule; live: a worker-style `put_blob` from a second
   process then `link`+commit from the first). Small, self-contained,
   unblocks brash.
2. §7 `at_root`/`at` + bzzf `modified()`. Also small; unblocks brash reads.
3. §3 `swarmfs.dask` helper with the §4/§5 rules, and the User Guide's
   honest paragraph about the generic path.
4. §6 step 1 (measure/parallelise); step 2 only when a consumer asks.
5. §8 decision recorded; §9 when the version story allows.

## Tests to add (names as guidance)

- `test_put_blob_returns_bare_reference`, `test_link_into_new_lineage`,
  `test_link_into_existing_root_patches_minimally` (≤ path-depth node
  re-uploads, like the write patch tests), `test_link_refuses_mixed_refsize`,
  `test_link_local_first_not_persisted`, `test_commit_log_records_links`.
- `test_dask_to_parquet_single_root` (local threaded scheduler, fake node;
  assert one commit, N `bytes_post`), `test_dask_to_parquet_bzzf_publishes_once`,
  `test_dask_local_first_syncs_before_link`.
- `test_bzzf_at_root_bypasses_feed`, `test_bzzf_at_time_resolves_once`,
  `test_bzzf_modified_is_feed_timestamp`, `test_pinned_view_refuses_writes`.
- `bench_find_2000_files` (marked `bench`, not run in CI by default).

---

## Transplanted (2026-09-22)

The roadmap entries and CLAUDE.md additions that used to live here have been
moved to their homes — one source of truth: `ROADMAP.md` §"Distributed writes
& pinned mounts (planned 2026-09-22)" and `CLAUDE.md` §"Distributed writes".
