# swarmfs

An [fsspec](https://filesystem-spec.readthedocs.io/) backend for
[Ethereum Swarm](https://docs.ethswarm.org/), talking to a
[Bee](https://github.com/ethersphere/bee) node (or public gateway) over its
HTTP API. Installing it makes Swarm a first-class storage backend for the
Python data ecosystem — pandas, dask, zarr, xarray, pyarrow, DuckDB — via
URLs like `bzz://<reference>/path/to/file.parquet`.

**Status: v2.** Read-only `bzz://` access, transactional copy-on-write
writes (postage stamps, every commit a snapshot), and mutable feed-backed
`bzzf://` mounts. See the [roadmap](roadmap.md).

New to swarmfs? This README is a quick reference — the
**[User Guide](USER_GUIDE.md)** walks through a worked example for every
library above (pandas, all three Dask collection types, Zarr, xarray,
PyArrow, DuckDB) and explains the content-addressing model in plain terms.

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

## Mutable feeds (`bzzf://`)

A feed gives you a stable URL whose contents you can update — the mutable
filesystem on top of immutable commits:

```python
ffs = fsspec.filesystem("bzzf", stamp="auto", signer="<private key hex>")
ffs.pipe_file(f"bzzf://{owner}/my-app/config.json", b'{"v": 2}')
# readers need no keys — and the URL never changes
```

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
