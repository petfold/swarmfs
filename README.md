# swarmfs

An [fsspec](https://filesystem-spec.readthedocs.io/) backend for
[Ethereum Swarm](https://docs.ethswarm.org/), talking to a
[Bee](https://github.com/ethersphere/bee) node (or public gateway) over its
HTTP API. Installing it makes Swarm a first-class storage backend for the
Python data ecosystem — pandas, dask, zarr, xarray, pyarrow, DuckDB — via
URLs like `bzz://<reference>/path/to/file.parquet`.

**Status: v0 — read-only `bzz://`.** Writes (postage stamps, transactional
commits) and mutable feed-backed `bzzf://` are on the
[roadmap](roadmap.md).

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
gateway works for reads but is discouraged (you're trusting the gateway);
opt-in client-side content verification for that path is on the roadmap.

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
