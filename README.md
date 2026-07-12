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

```python
import fsspec

# immutable, content-addressed: every commit yields a new root reference
fs = fsspec.filesystem("bzz", stamp="auto")
with fs.transaction:
    fs.pipe_file("bzz://new/dataset/a.parquet", data_a)
    fs.pipe_file("bzz://new/dataset/b.parquet", data_b)
root = fs.latest("new")          # share this reference; it never changes

# mutable, feed-backed: a stable URL whose contents you can update
ffs = fsspec.filesystem("bzzf", stamp="auto", signer="<private key hex>")
ffs.pipe_file(f"bzzf://{owner}/my-app/config.json", b'{"v": 2}')
# readers need no keys — and the URL never changes
```

## Usage

The recommended setup is a local [Bee light node](https://docs.ethswarm.org/docs/bee/installation/getting-started/)
— reads then come straight from the network with nothing to trust in between:

```python
import pandas as pd

# against your local Bee node (http://localhost:1633 by default)
df = pd.read_parquet("bzz://<64-hex-reference>/data.parquet")
```

The endpoint resolves as: `storage_options={"api_url": ...}` → the `BEE_API_URL`
environment variable → `http://localhost:1633`. Pointing `api_url` at a public
gateway is discouraged and requires an explicit `allow_gateway=True` — on that
path swarmfs verifies every fetched chunk client-side against its BMT address
(a Swarm reference *is* the content hash), so even an untrusted gateway can't
tamper with what you read. Verification can also be forced on/off with
`verify=True/False`.

Or with fsspec directly:

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

URL chaining and mappers work out of the box:

```python
# local caching, zero code
pd.read_parquet("simplecache::bzz://<reference>/big.parquet")
```

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

## Development

```bash
pip install -e ".[test]"
pytest                                   # offline unit tests (no node needed)
SWARMFS_TEST_BEE=http://localhost:1633 \
SWARMFS_TEST_STAMP=<batch-id> pytest tests/test_integration.py
```
