# swarmfs

[![tests](https://github.com/petfold/swarmfs/actions/workflows/tests.yml/badge.svg)](https://github.com/petfold/swarmfs/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/swarmfs)](https://pypi.org/project/swarmfs/)
[![license](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)

An [fsspec](https://filesystem-spec.readthedocs.io/) backend for
[Ethereum Swarm](https://docs.ethswarm.org/), talking to a
[Bee](https://github.com/ethersphere/bee) node (or public gateway) over its
HTTP API. Installing it makes Swarm a first-class storage backend for the
Python data ecosystem — pandas, dask, zarr, xarray, pyarrow, DuckDB — via
URLs like `bzz://<reference>/path/to/file.parquet`.

**Status: v3.** Read-only `bzz://` access, transactional copy-on-write
writes (postage stamps, every commit a snapshot), mutable feed-backed
`bzzf://` mounts, and a **local-first mode**: commits land on local disk
instantly and sync to Swarm in the background — offline is the normal
mode, `fs.sync()` is the certainty barrier. See the
[roadmap](docs/roadmap.md) and the local-first design in
[docs/localstore-design.md](docs/localstore-design.md).

New to swarmfs? This README is the quick tour — the
**[User Guide](docs/USER_GUIDE.md)** walks through a worked example for every
library above (pandas, all three Dask collection types, Zarr, xarray,
PyArrow, DuckDB) and explains the content-addressing model in plain terms.
For lookup — a storage option, a signature, an error, a default — use the
**[Reference](docs/REFERENCE.md)**: definition-first tables, pinned against
the code by the test suite, and the right document to hand to an AI agent.

## Install

```bash
pip install swarmfs            # or: pip install "swarmfs[feeds]" for signed feeds
```

## Upload and download a file

You need a running [Bee light node](https://docs.ethswarm.org/docs/bee/installation/getting-started/)
(`http://localhost:1633` by default) and, for uploads, a usable
[postage stamp](https://docs.ethswarm.org/docs/develop/access-the-swarm/buy-a-stamp-batch):

```python
import fsspec

fs = fsspec.filesystem("bzz", stamp="auto")

ref = fs.upload("photo.jpg")                    # → "c0ffee…" (64 hex chars)
fs.download(f"bzz://{ref}/photo.jpg", "copy.jpg")
```

On Swarm the address of new content is the *result* of a write, not its
input — `upload` returns the new reference, and that reference is permanent:
it names this exact content forever. Directories work the same way and come
back as a single reference for the whole tree:

```python
ref = fs.upload("dataset/")
fs.ls(f"bzz://{ref}")
fs.download(f"bzz://{ref}", "dataset-copy/", recursive=True)
```

`upload` accepts `content_type=` (otherwise guessed from the filename),
`encrypt=True` (single files; the returned 128-hex reference includes the
decryption key), and `redundancy=0–4` (erasure coding, default 2). The stamp
is validated before any byte moves, so a missing or expired stamp fails
immediately with an actionable error.

## The data ecosystem

The point of being an fsspec backend: everything that speaks fsspec now
speaks Swarm, with zero extra code.

```python
import pandas as pd

df = pd.read_parquet("bzz://<64-hex-reference>/data.parquet")

# local caching via URL chaining
df = pd.read_parquet("simplecache::bzz://<reference>/big.parquet")
```

```python
import fsspec

fs = fsspec.filesystem("bzz")          # api_url=..., default $BEE_API_URL or localhost:1633
fs.ls("bzz://<reference>/")            # client-side Mantaray trie walk
fs.find("bzz://<reference>/dataset/")  # recursive listing (dask uses this)
fs.cat("bzz://<reference>/hello.txt")

with fs.open("bzz://<reference>/big.parquet", block_size=2**20) as f:
    f.seek(-8, 2)                      # range requests: only the bytes you touch
    f.read(8)
```

Same story for any other fsspec-based tool — Intake, DVC, Kedro, pyxet,
Hugging Face Datasets, petl, and more (see the [User Guide](docs/USER_GUIDE.md#also-works-with)).

## Transactional writes

For anything beyond a one-shot upload — building a dataset in place, changing
one file inside a large collection — writes are copy-on-write commits: each
commit patches the manifest trie client-side, re-uploads only what changed,
and yields a new root. Old roots are untouched, so every commit is a snapshot.

```python
fs = fsspec.filesystem("bzz", stamp="auto")
with fs.transaction:
    fs.pipe_file("bzz://new/dataset/a.parquet", data_a)
    fs.pipe_file("bzz://new/dataset/b.parquet", data_b)
root = fs.latest("new")          # share this reference; it never changes
```

## Local-first writes

Add `local_store=` and the network leaves the write path entirely: commits
land in a local store directory instantly — they work on a plane — and a
background worker pushes them to Swarm and *confirms* arrival peer-to-peer.
Reads of anything the store holds are served from disk too (offline
read-your-writes, including `ls` and ranged reads); foreign references
still read through the node.

```python
fs = fsspec.filesystem("bzz", local_store="~/.myapp/store", redundancy=0)
with fs.transaction:
    fs.pipe_file("bzz://new/dataset/a.parquet", data_a)   # instant, offline-safe
root = fs.latest("new")
fs.sync()                        # optional barrier: confirmed ON the network
print(fs.sync_status())          # pinned vs evictable bytes, batch expiries
```

No postage stamp is needed at commit time (the push owns postage), local
disk becomes a budgeted working set (unpushed data is pinned; only
Swarm-confirmed blobs evict, and evicted reads heal by verified re-fetch),
and on `bzzf://` mounts the feed update publishes only once the network
provably serves the new root. `redundancy=0` is required — erasure coding
would fork the node's references from the local address space. Full design:
[docs/localstore-design.md](docs/localstore-design.md).

## Mutable feeds (`bzzf://`)

A feed gives you a stable URL whose contents you can update — the mutable
filesystem on top of immutable commits:

```python
ffs = fsspec.filesystem("bzzf", stamp="auto", signer="<private key hex>")
ffs.pipe_file(f"bzzf://{owner}/my-app/config.json", b'{"v": 2}')
# readers need no keys — and the URL never changes
```

## Addressing content offline, and buying stamps

Two things you can do without uploading anything:

```python
import swarmfs

ref = swarmfs.content_address(open("photo.jpg", "rb").read())   # no node needed
root, chunks = swarmfs.split(data)   # the whole chunk tree, keyed by address
```

`content_address` computes the reference Bee would return for those bytes, so
you can check whether the network already has content, name blobs by their
Swarm address in a local store, or build test fixtures at real addresses. It
needs the `feeds` extra (keccak256).

**One caveat that bites in practice:** this is the reference for a *plain*
upload. Erasure coding changes every intermediate chunk and therefore the root,
and parity is the node's to generate — so a redundant upload's reference cannot
be predicted offline. swarmfs writes with `redundancy=2` by default and many
nodes default to redundancy too, so pass `redundancy=0` if you want the
uploaded reference to match the one you computed.

Stamps can also be handled programmatically — selection is automatic
(`stamp="auto"` picks the usable batch with the longest life), and spending is
available but never implicit. Every `plan_*` call is a pure question; only the
verbs move money:

```python
from swarmfs.stamps import StampManager, depth_for_addresses, stamped_chunks

plan = await mgr.plan(size_bytes, ttl_secs)   # depth, amount, cost in xBZZ
batch = await mgr.buy(plan.amount, plan.depth)  # spends the node wallet's xBZZ

# depth depends on how you will upload: erasure parity and encryption add
# stamped chunks, and a batch dies of a full *bucket*, not of full capacity
plan = await mgr.plan(size_bytes, ttl_secs, redundancy=4, encrypted=True)
# ...or skip the statistics entirely — a plain upload's addresses are known
root, chunks = swarmfs.split(data)
plan = await mgr.plan(size_bytes, ttl_secs, depth=depth_for_addresses(chunks))

# renewal: extend BY a duration, TO a total, or for at most a budget
plan = await mgr.plan_topup(batch, ttl_secs=30 * 86400)
print(plan.cost_bzz, plan.total_ttl_secs, plan.warning)
info = await mgr.topup(batch, plan.added_amount)   # waits until the node applies it

# capacity, not time: dilution costs gas, and is paid for in TTL
print((await mgr.plan_dilute(batch, 20)).ttl_after_secs)   # ~halved per step
```

Four things about renewal that are easy to get wrong, so swarmfs encodes them:
a topup **adds** to the remaining life rather than restarting it; a nearly-full
**immutable** batch should be diluted *before* topping up, or the dilution
halves away part of what you just bought (`plan_topup().warning` says so);
remaining life is the node's `batchTTL` and *never* `amount / currentPrice`
(that field describes lifetime from the creation block, and is local
bookkeeping that can revert); and an expired batch cannot be revived, so renew
while it lives. The node also takes ~40 s to index a topup — `topup()` polls,
because reading straight after the transaction shows the old value and looks
like a silent failure.

For monitoring, `mgr.list_batches()` plus `StampInfo.problem(min_ttl)` turns
"still usable" into "needs renewing" at whatever threshold you choose, and
`mgr.buckets(batch)` reports the true per-bucket headroom that bounds the next
upload — `utilizationRatio` only summarises it.

`plan` sizes the batch for your upload (bucket-overflow-aware) and prices it
from the chain; `buy` purchases and waits until the batch is usable. Nothing in
swarmfs buys on its own — deciding to spend is the caller's.

## Which API should I use?

Three tiers, all backed by the same endpoint resolution
(`api_url=...` → `$BEE_API_URL` → `http://localhost:1633`):

- **`SwarmFileSystem` / fsspec URLs** — the default. Filesystem semantics,
  transactions, verification, and the whole data ecosystem for free.
- **`swarmfs.SyncSwarmClient` / `swarmfs.SwarmClient`** — direct calls
  against the Bee API (upload a blob, fetch bytes, post a feed update)
  without filesystem semantics. `SyncSwarmClient` is the blocking twin for
  plain scripts; `SwarmClient` is the same surface as coroutines for
  asyncio code:

  ```python
  from swarmfs import SyncSwarmClient

  with SyncSwarmClient() as client:            # async? use SwarmClient + await
      ref = client.bzz_post(open("photo.jpg", "rb"), stamp=batch_id)
      data = client.bzz_get(ref, "photo.jpg")
  ```

- **Raw HTTP** — the Bee API is plain HTTP; no library needed:

  ```bash
  curl -X POST -H "Swarm-Postage-Batch-Id: <batch>" \
       --data-binary @photo.jpg http://localhost:1633/bzz?name=photo.jpg
  ```

  What the library adds over this: stamp validation up front, chunk
  verification, gateway policy, better errors — the edge cases.

## Nodes, gateways, verification

The recommended setup is a local light node — reads then come straight from
the network with nothing to trust in between. Pointing `api_url` at a public
gateway is discouraged and requires an explicit `allow_gateway=True` — on
that path swarmfs verifies every fetched chunk client-side against its BMT
address (a Swarm reference *is* the content hash), so even an untrusted
gateway can't tamper with what you read. Verification can also be forced
on/off with `verify=True/False`.

## How it works

Swarm has no server-side directory listing today, so `swarmfs` parses the
binary [Mantaray](https://github.com/ethersphere/bee/tree/master/pkg/manifest/mantaray)
manifest trie itself, fetching nodes on demand via `/bytes` (see
`swarmfs/mantaray/` — a self-contained pure-Python codec). File reads resolve
the path to its data reference once, then use HTTP range requests against
`/bytes`, which is what makes Parquet predicate pushdown and zarr chunk reads
viable. When Bee grows a server-side listing endpoint
([ethersphere/bee#5535](https://github.com/ethersphere/bee/issues/5535)) it
will slot in behind the existing capability seam with no API change.

## Compared to ipfsspec

[ipfsspec](https://github.com/fsspec/ipfsspec), the closest analog in the
fsspec ecosystem, is read-only by its own admission. Postage stamps make
paid writes tractable on Swarm, so swarmfs adds a transactional write path
and, via `bzzf://` feeds, a stable URL you can actually mutate — not just
read.

## Development

```bash
pip install -e ".[test]"
pytest                                   # offline unit tests (no node needed)
SWARMFS_TEST_BEE=http://localhost:1633 \
SWARMFS_TEST_STAMP=<batch-id> pytest tests/test_integration.py
```
