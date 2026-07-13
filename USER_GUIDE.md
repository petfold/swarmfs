# swarmfs user guide

This is the long-form companion to the [README](README.md). The README is a
quick reference; this guide walks through *why* things work the way they do
and gives a runnable example for every major library in the Python data
stack — pandas, Dask (DataFrame, Array, and Bag), Zarr, xarray, PyArrow, and
DuckDB. You don't need to know anything about Swarm going in.

Every example on this page was actually run against a real Bee node while
writing this guide. If you hit something that doesn't match what's shown
here, that's a bug — please open an issue.

## Table of contents

- [Setup](#setup)
- [The mental model, in plain terms](#the-mental-model-in-plain-terms)
- [pandas](#pandas)
- [Dask](#dask)
  - [dask.dataframe](#daskdataframe)
  - [dask.array (via Zarr)](#daskarray-via-zarr)
  - [dask.bag](#daskbag)
- [Zarr](#zarr)
- [xarray](#xarray)
- [PyArrow](#pyarrow)
- [DuckDB](#duckdb)
- [Writing more than one file at a time](#writing-more-than-one-file-at-a-time)
- [Getting a stable URL: feeds](#getting-a-stable-url-feeds)
- [Also works with](#also-works-with)
- [Troubleshooting](#troubleshooting)
- [Where to go next](#where-to-go-next)

## Setup

You need two things before any of the examples below will work:

1. **A running Bee node.** swarmfs talks to Bee's HTTP API at
   `http://localhost:1633` by default. The
   [quick-start guide](https://docs.ethswarm.org/docs/bee/installation/quick-start)
   walks through installing and funding a light node — it takes about ten
   minutes and a small amount of xDAI/xBZZ to get going. You can point
   swarmfs at a different address with `api_url="..."` or the `BEE_API_URL`
   environment variable; see [Troubleshooting](#troubleshooting) for the
   public-gateway option if you don't want to run a node at all.

2. **A postage stamp**, if you're going to write anything (every example on
   this page writes at least once). A stamp is Swarm's name for "storage
   you've paid for" — without one, uploads fail immediately with an error
   telling you how to buy one. If you already have Bee running:

   ```bash
   swarm-cli stamp buy --depth 20 --amount 100000000
   ```

   Reading existing content needs no stamp at all — only writes do.

Install swarmfs itself, plus whichever data libraries you want to try:

```bash
pip install swarmfs pandas "dask[dataframe,array,bag]" zarr xarray pyarrow duckdb
```

## The mental model, in plain terms

If you've used S3 or a local filesystem with fsspec before, there's exactly
one idea you need to add, and it explains almost everything else in this
guide.

**On Swarm, the address of your data *is* a hash of its content.** A
64-character hex string like `c0ffee1234…` isn't a path you chose — it's
computed from what you uploaded. Upload the same bytes twice, get the same
address twice. Change one byte, get a completely different address.

This has a consequence that's different from S3-style storage: **you can't
write to an address that doesn't exist yet, because the address is the
*output* of writing, not the input.** With S3 you decide `s3://my-bucket/data.csv`
and then put bytes there. With Swarm you put bytes in, and the network hands
you back where they ended up. Every write in swarmfs follows this shape —
you'll see it in every example below as `ref = fs.upload(...)` or
`root = fs.latest("new")`.

The upside is enormous for anything read-heavy, which is most of the data
stack: **once you have a reference, that content will never silently
change out from under you.** A Parquet file, a Zarr array, a whole
directory tree — the reference is a permanent, verifiable name for exactly
those bytes. That's what makes `simplecache::bzz://<ref>/big.parquet`
perfectly safe to cache forever, and it's why nothing in this guide worries
about cache invalidation.

Two more pieces complete the picture:

- **`bzz://`** is the immutable protocol: `bzz://<reference>/path/inside`.
  Everything in this guide uses it except the very last section.
- **`bzzf://`** is the mutable one, for when you want a URL that *doesn't*
  change even though its contents do (a config file, a dataset that gets
  new partitions weekly). It's a thin layer on top of `bzz://` — see
  [Getting a stable URL: feeds](#getting-a-stable-url-feeds).

With that out of the way, everything below is just: point your favorite
library at a `bzz://` URL, or ask `fs.upload()` for one.

## pandas

The simplest possible round trip: write a Parquet file, get a reference,
read it straight back.

```python
import pandas as pd
import fsspec

df = pd.DataFrame({"id": range(5), "value": [1.1, 2.2, 3.3, 4.4, 5.5]})

fs = fsspec.filesystem("bzz", stamp="auto")
with fs.transaction:
    with fs.open("bzz://new/example.parquet", "wb") as f:
        df.to_parquet(f)
ref = fs.latest("new")
print(ref)  # e.g. "ffff5f25...4c" — this is now the permanent address

df2 = pd.read_parquet(f"bzz://{ref}/example.parquet")
```

Why the `with fs.transaction:` around a single file? It isn't strictly
necessary here — `fs.open(..., "wb")` would commit on its own the moment
you close the file. The transaction matters once you're writing more than
one file (see [Writing more than one file at a time](#writing-more-than-one-file-at-a-time));
it's shown here so the pattern looks the same everywhere in this guide. For
literally one file, `fs.upload("local.parquet")` (see the
[README](README.md#upload-and-download-a-file)) is one line shorter and
skips the manifest machinery entirely.

`stamp="auto"` tells swarmfs to pick whichever of your postage batches has
the longest remaining lifetime — pass an explicit batch ID if you have
several and care which one is used.

## Dask

Dask has three distinct collection types, and swarmfs supports all of them
— but each one talks to storage a little differently, so each gets its own
example.

### dask.dataframe

This is the one most people reach for: a Parquet *dataset* split across
several files, read back as a single lazy DataFrame.

```python
import pandas as pd
import dask.dataframe as dd
import fsspec

fs = fsspec.filesystem("bzz", stamp="auto")
with fs.transaction:
    for i in range(3):
        part = pd.DataFrame({"id": range(i * 100, (i + 1) * 100), "part": i})
        with fs.open(f"bzz://new/sales/part.{i}.parquet", "wb") as f:
            part.to_parquet(f)
root = fs.latest("new")

ddf = dd.read_parquet(f"bzz://{root}/sales")
print(ddf.npartitions)  # 3 — swarmfs's client-side directory listing found each file
out = ddf.compute()
```

The three files went up as one transaction (one commit, one new reference
for the whole `sales/` directory). `dd.read_parquet` on a directory needs to
list what's inside it — that's swarmfs walking Swarm's manifest trie behind
the scenes (see [How it works](README.md#how-it-works) in the README), and
it's exactly the same mechanism `fs.find()` and `fs.ls()` use directly.

### dask.array (via Zarr)

Dask's array type doesn't read Parquet — it reads chunked array storage,
and the standard chunked-array format in this ecosystem is Zarr. So a Dask
array on Swarm is really "a Zarr store on Swarm, opened with Dask instead of
NumPy":

```python
import dask.array as da
import zarr
from zarr.storage import FsspecStore
from swarmfs import SwarmFileSystem

fs = SwarmFileSystem(stamp="auto", asynchronous=True)
arr = da.random.default_rng(7).normal(size=(200, 200), chunks=(50, 50))

z = zarr.open(
    FsspecStore(fs, path="new/array"), mode="w",
    shape=arr.shape, chunks=(50, 50), dtype=arr.dtype,
)
da.store(arr, z)
root = fs.latest("new")

fs2 = SwarmFileSystem(asynchronous=True)
z2 = zarr.open(FsspecStore(fs2, read_only=True, path=f"{root}/array"), mode="r")
out = da.from_zarr(z2)
out.compute()
```

Two things worth calling out:

- **`asynchronous=True`** is needed here (unlike the pandas/dask.dataframe
  examples) because `FsspecStore` talks to the filesystem through its async
  interface directly rather than through the ordinary sync `fs.open()`
  wrapper. If you forget it, Zarr will raise a clear error rather than
  silently misbehaving.
- Each Dask chunk you write or read maps to one Zarr chunk, which maps to
  one Swarm reference inside the array's manifest. Reading a slice of a
  huge array only fetches the chunks that slice touches — this is the same
  range-request machinery that makes Parquet predicate pushdown viable,
  applied to N-dimensional arrays instead of columns.

### dask.bag

Bag is Dask's collection for unstructured or semi-structured data — lines
of text, JSON records, log files. It reads a glob of files:

```python
import json
import dask.bag as db
import fsspec

fs = fsspec.filesystem("bzz", stamp="auto")
records = [{"id": i, "tag": "a" if i % 2 else "b"} for i in range(6)]
with fs.transaction:
    for i, rec in enumerate(records):
        fs.pipe_file(f"bzz://new/events/log-{i}.json", json.dumps(rec).encode())
root = fs.latest("new")

bag = db.read_text(f"bzz://{root}/events/*.json").map(json.loads)
out = bag.compute(scheduler="threads")
```

**Use `scheduler="threads"` (or leave Dask's default single-machine
scheduler in place) rather than the multiprocessing scheduler.** The
`fs` object holds a live aiohttp session and an event loop thread; those
can't be pickled across a process boundary, which is what Dask's
multiprocessing scheduler needs to do. This isn't a swarmfs limitation as
such — it's true of any fsspec backend with real network state — but it's
easy to trip over since multiprocessing is Dask's default scheduler in some
configurations.

## Zarr

The Dask example above went through Zarr already, but Zarr stands on its
own — plenty of scientific and ML workflows use chunked arrays without
Dask at all, just NumPy:

```python
import numpy as np
import zarr
from zarr.storage import FsspecStore
from swarmfs import SwarmFileSystem

fs = SwarmFileSystem(stamp="auto", asynchronous=True)
z = zarr.open(
    FsspecStore(fs, path="new/grid"), mode="w",
    shape=(1000,), chunks=(100,), dtype="f8",
)
z[:] = np.arange(1000, dtype="f8")
root = fs.latest("new")

fs2 = SwarmFileSystem(asynchronous=True)
z2 = zarr.open(FsspecStore(fs2, read_only=True, path=f"{root}/grid"), mode="r")
z2[200:210]  # fetches only the one 100-element chunk that slice falls in
```

This is the flagship demo for why swarmfs exists: `fs.get_mapper()` /
`FsspecStore` turn Swarm into a `MutableMapping`, and Zarr doesn't need to
know or care that the storage underneath is a content-addressed P2P
network rather than a local disk.

## xarray

xarray builds labeled, multi-dimensional datasets on top of Zarr, so this
example looks almost identical to the Zarr one — xarray just adds
coordinates and variable names:

```python
import numpy as np
import xarray as xr
from zarr.storage import FsspecStore
from swarmfs import SwarmFileSystem

fs = SwarmFileSystem(stamp="auto", asynchronous=True)
ds = xr.Dataset(
    {"temperature": (("x", "y"), np.random.default_rng(11).normal(15, 3, (8, 12)))},
    coords={"x": np.arange(8), "y": np.arange(12)},
)
ds.to_zarr(FsspecStore(fs, path="new/climate"), mode="w", consolidated=False)
root = fs.latest("new")

fs2 = SwarmFileSystem(asynchronous=True)
out = xr.open_zarr(
    FsspecStore(fs2, read_only=True, path=f"{root}/climate"), consolidated=False,
).load()
```

`consolidated=False` is there because xarray's "consolidated metadata"
optimization pre-fetches the whole store's metadata as one JSON blob — a
nice trick for slow object storage, but unnecessary here since swarmfs
already batches metadata lookups efficiently, and it avoids an extra
round trip through the manifest for a store this small.

## PyArrow

PyArrow underlies pandas' own Parquet reader, but using it directly gets
you two things pandas doesn't expose as easily: reading/writing Arrow
tables without going through pandas at all, and `pyarrow.dataset`, which
pushes filters down to the file level.

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import fsspec

fs = fsspec.filesystem("bzz", stamp="auto")
table = pa.table({"id": range(10), "city": ["ny", "sf"] * 5})
with fs.transaction:
    with fs.open("bzz://new/city.parquet", "wb") as f:
        pq.write_table(table, f)
root = fs.latest("new")

with fs.open(f"bzz://{root}/city.parquet", "rb") as f:
    t2 = pq.read_table(f)

# predicate pushdown: only the row groups matching the filter are fetched
dataset = ds.dataset(f"{root}/city.parquet", filesystem=fs, format="parquet")
ny_only = dataset.to_table(filter=ds.field("city") == "ny")
```

`pyarrow.dataset.dataset(..., filesystem=fs)` is the same pattern
`dask.dataframe` and DuckDB (below) use internally — pass swarmfs's `fs`
object as `filesystem=`, and the library's own predicate-pushdown and
range-request logic does the rest, hitting Swarm through swarmfs's
`_fetch_range` under the hood.

## DuckDB

DuckDB can query Parquet files directly off Swarm by registering the
swarmfs filesystem object with its fsspec bridge:

```python
import pandas as pd
import fsspec
import duckdb

fs = fsspec.filesystem("bzz", stamp="auto")
df = pd.DataFrame({"id": range(20), "amount": [i * 1.5 for i in range(20)]})
with fs.transaction:
    with fs.open("bzz://new/orders.parquet", "wb") as f:
        df.to_parquet(f)
root = fs.latest("new")

con = duckdb.connect()
con.register_filesystem(fs)
con.execute(
    f"SELECT sum(amount) AS total FROM read_parquet('bzz://{root}/orders.parquet')"
).fetchone()
```

`register_filesystem` is the one line that's different from a local file —
everything after it is ordinary SQL, unaware that `bzz://` isn't a local
path.

## Writing more than one file at a time

Every example above that writes more than one file wraps the writes in
`with fs.transaction:`. Two things this buys you:

- **One commit instead of many.** Without a transaction, each `pipe_file` /
  `open(..., "wb")` call commits (and re-uploads the manifest) on its own.
  Inside a transaction, all the writes land in one commit at the end — one
  new manifest patch, one new reference.
- **All-or-nothing.** If an exception is raised inside the `with` block,
  nothing is uploaded at all — staged writes are simply discarded. You
  never end up with a half-written dataset.

If you only ever write one file (or one directory as a unit), you don't
need a transaction — `fs.upload("local_file_or_dir")` is the one-line
version, covered in the [README](README.md#upload-and-download-a-file).

Every commit, transactional or not, leaves the *previous* reference
completely untouched — it's a new manifest, not an edit of the old one.
That means every commit doubles as a free snapshot: keep the old reference
around and you have a permanent before-picture, with no extra bookkeeping.

## Getting a stable URL: feeds

Everything above produces a new reference every time you write. That's
exactly right for one-shot datasets, but awkward for anything you expect to
update — a live dashboard's backing data, an application's config file,
a dataset that gains a new partition every day. For that, swarmfs has
`bzzf://`, the mutable protocol:

```python
ffs = fsspec.filesystem("bzzf", stamp="auto", signer="<private key hex>")
ffs.pipe_file(f"bzzf://{owner}/my-app/config.json", b'{"v": 2}')
# readers use the same URL, no key needed, and always see the latest write
data = fsspec.filesystem("bzzf").cat(f"bzzf://{owner}/my-app/config.json")
```

Under the hood a feed is just an extra layer on top of everything above:
writes still go through the same commit machinery and produce a new `bzz://`
reference each time, but that reference gets published to a small piece of
state (the "feed") that a stable `owner/topic` URL always resolves to the
latest version of. Reading needs no key; writing does, because only the
feed's owner is allowed to update it. See the
[feeds section](README.md#mutable-feeds-bzzf) of the README for the full
picture, including what "last-write-wins" means if two processes update the
same feed concurrently.

## Also works with

Everything above got a full runnable example because they're the libraries
most people in the target audience reach for first. But any library that
consumes an fsspec filesystem (or a `path`/`filesystem` pair, or an fsspec
URL) gets Swarm support the same way, with no swarmfs-specific code. Some
other libraries worth knowing about:

- **[Intake](https://intake.readthedocs.io/)** — data source cataloguing and
  loading. A catalog entry pointing at a `bzz://` URL works the same as one
  pointing at `s3://` or a local path.
- **[DVC](https://dvc.org/)** — version control for machine learning
  projects. DVC's fsspec-based remotes can target `bzz://`/`bzzf://` the
  same way they target any other remote, giving DVC-tracked datasets a
  content-addressed, verifiable backing store.
- **[Kedro](https://kedro.org/)** — a framework for reproducible, modular
  data science pipelines. Kedro's `DataCatalog` datasets accept an fsspec
  `filepath`/`protocol`, so pipeline inputs and outputs can live on Swarm
  without touching pipeline code.
- **[pyxet](https://github.com/xetdata/pyxet)** — mounts and accesses very
  large datasets from XetHub; another fsspec-filesystem consumer in the
  same shape as the examples above.
- **[Hugging Face 🤗 Datasets](https://huggingface.co/docs/datasets/)** —
  loading and manipulating data for deep learning models. `load_dataset`
  and friends accept fsspec URLs, so training data can be read straight off
  Swarm.
- **[petl](https://petl.readthedocs.io/)** — a general-purpose package for
  extracting, transforming, and loading tables of data, also built on
  fsspec for its file-based sources and sinks.

(pandas, Zarr, xarray, and PyArrow are also fsspec-based — see their
dedicated sections above.)

## Troubleshooting

**"no usable postage stamp"** — you're trying to write without a valid
stamp. Buy one (`swarm-cli stamp buy --depth 20 --amount 100000000`) or
check `fs.info()`-style tools against `GET /stamps` on your node to see why
an existing one isn't usable (expired, still syncing, or full).

**"looks like a public gateway"** — swarmfs's default stance is "run your
own node"; an endpoint where it can't detect that you own it (the
node-owner `/stamps` API is unreachable) is treated as a shared public
gateway and refused unless you pass `allow_gateway=True`. On that path,
every chunk you read is verified client-side against its content hash, so
even an untrusted gateway can't hand you tampered data — but running your
own light node avoids the question entirely.

**Everything hangs or the wrong data comes back** — check `fs.trusted` and
`fs.verify_active` on your filesystem instance; if you expected verification
and it's off (or vice versa), pass `verify=True` / `verify=False` explicitly
rather than relying on the gateway/node auto-detection.

**A library complains it can't pickle the filesystem, or spawns processes
that fail oddly** — this is the same issue called out in the
[dask.bag section](#daskbag): the `fs` object holds live network state
(an aiohttp session, an event loop thread) that can't cross a process
boundary. Use threads, not processes, for parallelism against swarmfs.

## Where to go next

- [README.md](README.md) — the quick reference: installation, the
  upload/download one-liners, the three API tiers (fsspec / `SwarmClient` /
  raw HTTP), and how the Mantaray manifest trie works under the hood.
- [roadmap.md](roadmap.md) — what's implemented, what's planned, and the
  status of Bee's upstream server-side listing feature request
  ([ethersphere/bee#5535](https://github.com/ethersphere/bee/issues/5535)),
  which will make listing large collections significantly faster once it
  ships.
- The [Bee documentation](https://docs.ethswarm.org/docs/) for everything
  below the Python layer: running and funding a node, how postage stamps
  and storage pricing work, and the Swarm network itself.
