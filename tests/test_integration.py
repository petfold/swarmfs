"""Integration tests against a real Bee node.

Skipped unless SWARMFS_TEST_BEE is set (e.g. http://localhost:1633).
Uploading the fixture additionally needs SWARMFS_TEST_STAMP (a usable
postage batch id); without it, set SWARMFS_TEST_REF to a known collection
reference to run the read-side assertions against existing content.
"""

from __future__ import annotations

import io
import os
import subprocess
import sys
import tarfile
import textwrap
import time
import urllib.request

import pytest

BEE = os.environ.get("SWARMFS_TEST_BEE")
STAMP = os.environ.get("SWARMFS_TEST_STAMP")
KNOWN_REF = os.environ.get("SWARMFS_TEST_REF")

pytestmark = pytest.mark.skipif(
    not BEE, reason="set SWARMFS_TEST_BEE=<bee api url> to run integration tests"
)

FILES = {
    "hello.txt": b"hello swarm\n",
    "data/a.bin": bytes(range(256)) * 64,
    "data/b.bin": b"b" * 10_000,
}


def upload_collection() -> str:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in FILES.items():
            ti = tarfile.TarInfo(name=name)
            ti.size = len(content)
            tar.addfile(ti, io.BytesIO(content))
    req = urllib.request.Request(
        f"{BEE}/bzz",
        data=buf.getvalue(),
        headers={
            "Content-Type": "application/x-tar",
            "Swarm-Postage-Batch-Id": STAMP,
            "Swarm-Collection": "true",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        import json

        return json.loads(resp.read())["reference"]


@pytest.fixture(scope="module")
def root_ref() -> str:
    if STAMP:
        return upload_collection()
    if KNOWN_REF:
        return KNOWN_REF
    pytest.skip("need SWARMFS_TEST_STAMP (to upload) or SWARMFS_TEST_REF (existing)")


@pytest.fixture()
def fs():
    from swarmfs import SwarmFileSystem

    return SwarmFileSystem(api_url=BEE, skip_instance_cache=True)


@pytest.mark.skipif(not STAMP, reason="upload fixture needs SWARMFS_TEST_STAMP")
def test_roundtrip_ls_and_cat(fs, root_ref):
    assert sorted(fs.find(f"bzz://{root_ref}")) == sorted(
        f"{root_ref}/{p}" for p in FILES
    )
    entries = {e["name"]: e for e in fs.ls(f"bzz://{root_ref}/data")}
    assert entries[f"{root_ref}/data/a.bin"]["size"] == len(FILES["data/a.bin"])
    assert fs.cat_file(f"bzz://{root_ref}/hello.txt") == FILES["hello.txt"]
    content = FILES["data/a.bin"]
    assert fs.cat_file(f"bzz://{root_ref}/data/a.bin", start=100, end=200) == content[100:200]
    with fs.open(f"bzz://{root_ref}/data/b.bin", block_size=2048) as f:
        f.seek(5000)
        assert f.read(100) == FILES["data/b.bin"][5000:5100]


def test_read_existing_reference(fs, root_ref):
    files = fs.find(f"bzz://{root_ref}")
    assert files, "manifest lists at least one file"
    info = fs.info(files[0])
    assert info["type"] == "file"
    data = fs.cat_file(files[0])
    if info["size"] is not None:
        assert len(data) == info["size"]


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_write_roundtrip_live():
    """v1 against a real node: fresh manifest, transactional batch, rm, patch."""
    from swarmfs import SwarmFileSystem

    fs = SwarmFileSystem(api_url=BEE, stamp=STAMP, skip_instance_cache=True)

    # fresh manifest through the pseudo-root
    fs.pipe_file("bzz://new/hello.txt", b"hello from swarmfs v1\n")
    root1 = fs.latest("new")
    assert len(root1) == 64
    assert fs.cat_file(f"bzz://{root1}/hello.txt") == b"hello from swarmfs v1\n"

    # transactional batch: one commit for three ops
    ncommits = len(fs.commit_log)
    with fs.transaction:
        fs.pipe_file("bzz://new/data/a.bin", bytes(range(256)) * 8)
        fs.pipe_file("bzz://new/data/b.bin", b"b" * 5000)
        fs.rm_file("bzz://new/hello.txt")
    assert len(fs.commit_log) == ncommits + 1
    root2 = fs.latest("new")

    # a fresh instance (no root map, no staging) sees the committed state
    fresh = SwarmFileSystem(api_url=BEE, skip_instance_cache=True)
    assert fresh.find(f"bzz://{root2}") == sorted(
        [f"{root2}/data/a.bin", f"{root2}/data/b.bin"]
    )
    assert fresh.cat_file(f"bzz://{root2}/data/b.bin", start=100, end=105) == b"bbbbb"
    # the first snapshot is untouched
    assert fresh.cat_file(f"bzz://{root1}/hello.txt") == b"hello from swarmfs v1\n"

    # metadata written bee-style
    info = fresh.info(f"bzz://{root2}/data/a.bin")
    assert info["size"] == 2048
    assert info["metadata"]["Filename"] == "a.bin"


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_upload_download_live(tmp_path):
    """The hello-world: one-line upload returning a reference, download back."""
    from swarmfs import SwarmFileSystem

    fs = SwarmFileSystem(api_url=BEE, stamp=STAMP, skip_instance_cache=True)
    local = tmp_path / "hello.txt"
    local.write_bytes(b"hello from fs.upload\n")

    ref = fs.upload(str(local))
    assert len(ref) == 64
    fs.download(f"bzz://{ref}/hello.txt", str(tmp_path / "copy.txt"))
    assert (tmp_path / "copy.txt").read_bytes() == b"hello from fs.upload\n"
    # single-file uploads resolve as the manifest's index document too
    assert fs.cat(f"bzz://{ref}") == b"hello from fs.upload\n"

    # a directory goes through the commit engine and yields one reference
    d = tmp_path / "dataset"
    (d / "sub").mkdir(parents=True)
    (d / "a.txt").write_bytes(b"aaa")
    (d / "sub" / "b.bin").write_bytes(b"\x00\x01\x02")
    dref = fs.upload(str(d))
    fresh = SwarmFileSystem(api_url=BEE, skip_instance_cache=True)
    assert fresh.find(f"bzz://{dref}") == sorted([f"{dref}/a.txt", f"{dref}/sub/b.bin"])
    assert fresh.cat_file(f"bzz://{dref}/sub/b.bin") == b"\x00\x01\x02"


def test_sync_client_live():
    """The blocking middle tier against a real node."""
    from swarmfs import SyncSwarmClient

    with SyncSwarmClient(api_url=BEE) as client:
        assert client.health()["status"] == "ok"
        if STAMP:
            ref = client.bzz_post(b"sync facade\n", STAMP, filename="s.txt")
            assert client.bzz_get(ref, "s.txt") == b"sync facade\n"


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_zarr_xarray_roundtrip_live():
    """v1 exit criterion on a real node: zarr store on Swarm, read via xarray."""
    np = pytest.importorskip("numpy")
    xr = pytest.importorskip("xarray")
    pytest.importorskip("zarr")
    from zarr.storage import FsspecStore

    from swarmfs import SwarmFileSystem

    fs = SwarmFileSystem(
        api_url=BEE, stamp=STAMP, asynchronous=True, skip_instance_cache=True
    )
    ds = xr.Dataset(
        {"temperature": (("x", "y"), np.random.default_rng(11).normal(15, 3, (8, 12)))},
        coords={"x": np.arange(8), "y": np.arange(12)},
    )
    ds.to_zarr(FsspecStore(fs, path="new/climate"), mode="w", consolidated=False)
    root = fs.latest("new")
    assert len(root) == 64

    fs2 = SwarmFileSystem(api_url=BEE, asynchronous=True, skip_instance_cache=True)
    out = xr.open_zarr(
        FsspecStore(fs2, read_only=True, path=f"{root}/climate"), consolidated=False
    ).load()
    xr.testing.assert_identical(out, ds)


@pytest.mark.skipif(not STAMP, reason="upload fixture needs SWARMFS_TEST_STAMP")
def test_verified_reads_live(root_ref):
    """verify=True against real content: manifest walk, full/range reads and
    sizes all go through BMT-checked chunk fetches (incl. erasure-coded
    spans and parity refs on multi-chunk files)."""
    from swarmfs import SwarmFileSystem

    vfs = SwarmFileSystem(api_url=BEE, verify=True, skip_instance_cache=True)
    assert vfs.find(f"bzz://{root_ref}") == sorted(f"{root_ref}/{p}" for p in FILES)
    content = FILES["data/a.bin"]  # 16 KiB -> multi-chunk tree
    assert vfs.info(f"bzz://{root_ref}/data/a.bin")["size"] == len(content)
    assert vfs.cat_file(f"bzz://{root_ref}/data/a.bin") == content
    assert vfs.cat_file(
        f"bzz://{root_ref}/data/a.bin", start=4000, end=8200
    ) == content[4000:8200]
    with vfs.open(f"bzz://{root_ref}/data/a.bin", block_size=2048) as f:
        f.seek(-100, 2)
        assert f.read() == content[-100:]
    assert vfs.verify_active is True and vfs.trusted is True


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_redundancy_write_live():
    """redundancy= writes erasure-coded content: the root chunk's span
    carries the level, and verified reads handle the parity refs."""
    pytest.importorskip("eth_hash")
    from swarmfs import SwarmFileSystem
    from swarmfs.join import decode_span

    content = bytes(range(256)) * 80  # 20480 bytes -> 5 data chunks
    fs = SwarmFileSystem(api_url=BEE, stamp=STAMP, redundancy=2, skip_instance_cache=True)
    fs.pipe_file("bzz://new/ec/data.bin", content)
    root = fs.latest("new")

    # the file's data reference points at a root chunk with level 2 encoded
    info = fs.info(f"bzz://{root}/ec/data.bin")
    assert info["size"] == len(content)
    from fsspec.asyn import sync

    chunk = sync(fs.loop, fs.client.chunk_get, info["reference"])
    assert chunk[7] > 128, "span does not carry a redundancy level"
    assert chunk[7] & 0x7F == 2, f"expected level 2, got {chunk[7] & 0x7F}"
    assert decode_span(chunk[:8]) == len(content)

    # verified read-back of our own erasure-coded write
    vfs = SwarmFileSystem(api_url=BEE, verify=True, skip_instance_cache=True)
    assert vfs.cat_file(f"bzz://{root}/ec/data.bin") == content
    assert vfs.cat_file(f"bzz://{root}/ec/data.bin", start=5000, end=9000) == content[5000:9000]


def _poll(fn, expect, timeout=90, interval=3):
    """Feed updates propagate through the network before they resolve
    (~6 s on a light node measured); poll until visible or timed out."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last == expect:
                return last
        except FileNotFoundError:
            last = None
        time.sleep(interval)
    raise AssertionError(f"feed did not converge within {timeout}s: last={last!r}")


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_bzzf_two_mounts_live():
    """v2 exit criterion on a real node: two mounts of the same bzzf:// feed
    see each other's committed changes."""
    pytest.importorskip("eth_keys")
    import secrets

    from swarmfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedSigner

    key = secrets.token_hex(32)  # fresh feed per run
    owner = FeedSigner(key).owner_hex
    url = f"bzzf://{owner}/swarmfs-integration/state.txt"

    a = SwarmFeedFileSystem(
        api_url=BEE, stamp=STAMP, signer=key, feed_ttl=0, skip_instance_cache=True
    )
    a.pipe_file(url, b"written by mount A")

    # a keyless reader resolves the feed (eventually — Swarm is a network)
    reader = SwarmFeedFileSystem(api_url=BEE, feed_ttl=0, skip_instance_cache=True)
    _poll(lambda: reader.cat_file(url), b"written by mount A")

    # a second writer updates; the first mount sees it (last-write-wins)
    c = SwarmFeedFileSystem(
        api_url=BEE, stamp=STAMP, signer=key, feed_ttl=0, skip_instance_cache=True
    )
    c.pipe_file(url, b"updated by mount C")
    c.pipe_file(f"bzzf://{owner}/swarmfs-integration/extra.txt", b"more")
    _poll(lambda: a.cat_file(url), b"updated by mount C")
    _poll(
        lambda: sorted(a.ls(f"bzzf://{owner}/swarmfs-integration", detail=False)),
        [
            f"{owner}/swarmfs-integration/extra.txt",
            f"{owner}/swarmfs-integration/state.txt",
        ],
    )


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_bzzf_pinned_views_live():
    """Frozen and time-travelled mounts of a live feed (§7): the same URL,
    read as of a root or as of a moment, against a real node."""
    pytest.importorskip("eth_keys")
    import datetime
    import secrets

    from swarmfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedError, FeedSigner

    key = secrets.token_hex(32)
    owner = FeedSigner(key).owner_hex
    topic = "swarmfs-pinned"
    url = f"bzzf://{owner}/{topic}/state.txt"

    w = SwarmFeedFileSystem(api_url=BEE, stamp=STAMP, signer=key, feed_ttl=0,
                            skip_instance_cache=True)
    w.pipe_file(url, b"version one")
    first_root = w.latest(f"bzzf://{owner}/{topic}")
    # modified() is the feed update's publication time, not a constant
    t1 = w.modified(url)
    assert t1 > datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)

    time.sleep(2)
    w.pipe_file(url, b"version two")
    t2 = w.modified(url)
    assert t2 > t1
    _poll(lambda: SwarmFeedFileSystem(
        api_url=BEE, feed_ttl=0, skip_instance_cache=True).cat_file(url),
        b"version two")

    # frozen at a root: the feed is never consulted
    frozen = SwarmFeedFileSystem(api_url=BEE, at_root=first_root,
                                 skip_instance_cache=True)
    assert frozen.cat_file(url) == b"version one"
    assert frozen.ls(f"bzzf://{owner}/{topic}", detail=False) == [
        f"{owner}/{topic}/state.txt"]

    # as of a moment between the two updates
    between = int(t1.timestamp()) + 1
    past = SwarmFeedFileSystem(api_url=BEE, at=between, skip_instance_cache=True)
    assert past.cat_file(url) == b"version one"
    assert past.modified(url) == t1
    now = SwarmFeedFileSystem(api_url=BEE, at=int(t2.timestamp()) + 60,
                              skip_instance_cache=True)
    assert now.cat_file(url) == b"version two"
    before = SwarmFeedFileSystem(api_url=BEE, at=int(t1.timestamp()) - 3600,
                                 skip_instance_cache=True)
    with pytest.raises(FileNotFoundError, match="did not exist yet"):
        before.cat_file(url)

    # a view is read-only even holding the key
    ro = SwarmFeedFileSystem(api_url=BEE, stamp=STAMP, signer=key,
                             at_root=first_root, skip_instance_cache=True)
    with pytest.raises(FeedError, match="pinned read-only view"):
        ro.pipe_file(url, b"nope")


@pytest.mark.skipif(not STAMP, reason="upload fixture needs SWARMFS_TEST_STAMP")
def test_dask_partitioned_parquet_live(fs):
    """The v0 exit criterion against a *real* node: upload a partitioned
    Parquet dataset as a Swarm collection, read it back with dask."""
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    pytest.importorskip("pyarrow")

    frames, tar_files = [], {}
    for i in range(3):
        part = pd.DataFrame({"id": range(i * 100, (i + 1) * 100), "part": i})
        frames.append(part)
        buf = io.BytesIO()
        part.to_parquet(buf)
        tar_files[f"dataset/part.{i}.parquet"] = buf.getvalue()
    expected = pd.concat(frames, ignore_index=True)

    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w") as t:
        for name, content in tar_files.items():
            ti = tarfile.TarInfo(name=name)
            ti.size = len(content)
            t.addfile(ti, io.BytesIO(content))
    req = urllib.request.Request(
        f"{BEE}/bzz",
        data=tar.getvalue(),
        headers={
            "Content-Type": "application/x-tar",
            "Swarm-Postage-Batch-Id": STAMP,
            "Swarm-Collection": "true",
        },
        method="POST",
    )
    import json

    with urllib.request.urlopen(req) as resp:
        root = json.loads(resp.read())["reference"]

    ddf = dd.read_parquet(f"bzz://{root}/dataset", storage_options={"api_url": BEE})
    out = ddf.compute().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(out[["id", "part"]], expected[["id", "part"]])


@pytest.mark.skipif(not STAMP, reason="uploading needs SWARMFS_TEST_STAMP")
def test_distributed_write_live(fs):
    """Two processes, one manifest (docs/distributed-writes.md §2).

    A separate *process* uploads the partitions with ``put_blob`` and
    prints their references; this one ``link``s them into a single commit.
    Nothing crosses the boundary but 64-hex strings — no shared staging,
    no shared filesystem object — and a third instance that saw none of it
    reads the finished dataset back with dask.
    """
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    pytest.importorskip("pyarrow")

    frames = [pd.DataFrame({"id": range(i * 100, (i + 1) * 100), "part": i})
              for i in range(3)]
    expected = pd.concat(frames, ignore_index=True)

    worker = textwrap.dedent("""
        import io, sys
        import pandas as pd
        from swarmfs import SwarmFileSystem

        fs = SwarmFileSystem(api_url=sys.argv[1], stamp=sys.argv[2])
        for i in range(3):
            buf = io.BytesIO()
            pd.DataFrame({"id": range(i * 100, (i + 1) * 100),
                          "part": i}).to_parquet(buf)
            print(fs.put_blob(buf.getvalue()))
    """)
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, PYTHONPATH=repo + os.pathsep + os.environ.get("PYTHONPATH", ""))
    out = subprocess.run([sys.executable, "-c", worker, BEE, STAMP],
                         capture_output=True, text=True, env=env, timeout=300)
    assert out.returncode == 0, out.stderr
    refs = out.stdout.split()
    assert len(refs) == 3 and all(len(r) == 64 for r in refs), out.stdout

    from swarmfs import SwarmFileSystem

    driver = SwarmFileSystem(api_url=BEE, stamp=STAMP, skip_instance_cache=True)
    with driver.transaction:
        for i, ref in enumerate(refs):
            driver.link(f"bzz://new/dataset/part.{i}.parquet", ref)
    root = driver.latest("new")
    assert len(driver.commit_log) == 1          # one commit for the dataset
    res = driver.commit_log[0]
    assert list(res.written.values()) == refs   # the workers' own references

    ddf = dd.read_parquet(f"bzz://{root}/dataset", storage_options={"api_url": BEE})
    got = ddf.compute().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(got[["id", "part"]], expected[["id", "part"]])


@pytest.mark.skipif(not STAMP, reason="writes need SWARMFS_TEST_STAMP")
def test_dask_helper_live():
    """§3 against a real node, in real *processes*: swarmfs.dask writes the
    partitions on multiprocessing workers (nothing unpicklable crosses the
    boundary — each builds its own filesystem from storage_options) and the
    driver turns their references into one manifest with one commit. Then
    the same, published through a feed: the driver holds the signer, the
    workers never see it."""
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    pytest.importorskip("pyarrow")
    pytest.importorskip("eth_keys")
    import secrets

    import swarmfs.dask as sd
    from swarmfs.feeds import FeedSigner

    df = pd.DataFrame({"id": range(300), "part": [i // 100 for i in range(300)]})
    ddf = dd.from_pandas(df, npartitions=3)
    options = {"api_url": BEE, "stamp": STAMP}

    res = sd.to_parquet(ddf, "bzz://new/dataset", storage_options=options,
                        compute_kwargs={"scheduler": "processes"})

    assert len(res) == 3 and len(res.root) == 64
    assert res.paths == ["part.0.parquet", "part.1.parquet", "part.2.parquet"]
    assert res.batches == {(BEE, STAMP)}  # one node, one batch

    back = dd.read_parquet(f"bzz://{res.root}/dataset",
                           storage_options={"api_url": BEE}).compute()
    pd.testing.assert_frame_equal(
        back.sort_values("id").reset_index(drop=True)[["id", "part"]],
        df[["id", "part"]])

    # …and onto a stable URL: one feed update for the whole dataset
    key = secrets.token_hex(32)
    owner = FeedSigner(key).owner_hex
    feed_url = f"bzzf://{owner}/swarmfs-dask/dataset"
    fed = sd.to_parquet(ddf, feed_url,
                        storage_options={**options, "signer": key},
                        compute_kwargs={"scheduler": "processes"})
    assert fed.root == res.root  # same content, same reference (deterministic)
    _poll(lambda: len(dd.read_parquet(
        feed_url, storage_options={"api_url": BEE}).compute()), 300)


@pytest.mark.skipif(not STAMP, reason="uploading needs SWARMFS_TEST_STAMP")
def test_local_split_matches_bee():
    """The splitter's claim, checked against the only authority that matters:
    the reference Bee computes for the same bytes.

    `POST /bytes` returns the *content* reference (no manifest wrapping), so
    this compares directly with `split()`'s root. Two things are pinned:

    1. With erasure coding off, the local reference equals Bee's — exactly,
       at every tree shape. That is what makes "address before you upload"
       and Swarm-addressed local stores valid.
    2. With redundancy on, multi-chunk roots *differ*, and legitimately so:
       parity chunks are the node's to generate and they change every
       intermediate. Pinning this stops someone "fixing" the splitter to
       chase a node default it cannot reproduce. Bee marks such a root by
       setting the span's top byte to 0x80 | level.
    """
    import asyncio
    import random

    from swarmfs._client import SwarmClient
    from swarmfs.splitter import content_address

    async def main():
        client = SwarmClient(BEE)
        try:
            redundant_differed = False
            for size in (0, 1, 4095, 4096, 4097, 8192, 100_000, 600_000):
                data = random.Random(size).randbytes(size)
                local = content_address(data).hex()

                plain = await client.bytes_post(data, STAMP, redundancy=0)
                assert local == plain, (
                    f"size {size}: local {local[:16]}… != bee {plain[:16]}…")

                default = await client.bytes_post(data, STAMP)
                if default != local:
                    redundant_differed = True
                    chunk = await client.chunk_get(default)
                    assert chunk[7] > 128, (
                        "a root that differs from the plain split should be "
                        f"erasure-coded, but span is {chunk[:8].hex()}")
            assert redundant_differed, (
                "this node did not add redundancy by default, so claim 2 went "
                "unexercised — not a failure of the splitter")
        finally:
            await client.close()

    asyncio.run(main())


# ---------------------------------------------------------------------------
# stamp renewal against a live node
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not STAMP, reason="needs SWARMFS_TEST_STAMP to inspect")
def test_stamp_inspection_and_planning_live():
    """Read-only: parse a real batch and price extensions against the real
    chainstate. Spends nothing — every ``plan_*`` is a pure question.
    """
    import asyncio

    from swarmfs._client import SwarmClient
    from swarmfs.stamps import StampManager, amount_to_ttl, batch_cost_bzz

    async def main():
        client = SwarmClient(BEE)
        try:
            mgr = StampManager(client)

            batches = await mgr.list_batches()
            assert any(b.batch_id.lower() == STAMP.lower() for b in batches)

            info = await mgr.get_batch(STAMP)
            assert info.depth >= info.bucket_depth
            assert info.amount > 0
            assert info.bucket_capacity == 2 ** (info.depth - info.bucket_depth)

            balance = await mgr.balance_bzz()
            assert balance >= 0

            # a week's extension, priced three ways that must agree
            week = 7 * 86400
            by_week = await mgr.plan_topup(STAMP, ttl_secs=week)
            assert by_week.added_ttl_secs == pytest.approx(week, rel=1e-3)
            assert by_week.cost_bzz == pytest.approx(
                batch_cost_bzz(by_week.added_amount, info.depth), rel=1e-9)

            to_total = await mgr.plan_topup(STAMP, total_ttl_secs=max(info.ttl, 0) + week)
            assert to_total.added_ttl_secs == pytest.approx(week, rel=1e-3)

            spend = await mgr.plan_topup(STAMP, budget_bzz=by_week.cost_bzz)
            assert spend.added_ttl_secs == pytest.approx(week, rel=1e-2)

            # topping up ADDS: the plan's total is remaining + purchased
            assert by_week.total_ttl_secs == max(info.ttl, 0) + by_week.added_ttl_secs

            # `amount` is NOT a source of truth for remaining life — it
            # describes lifetime from the creation block, and it is the local
            # issuer's bookkeeping, which bee increments in memory on a topup
            # without persisting (seen live to revert to the creation value
            # while the topups stayed in effect). Both assertions below failed
            # against a real batch before being weakened to what actually
            # holds: amount only ever implies a lifetime, never a remainder.
            chain = await client.chainstate()
            price = int(chain["currentPrice"])
            age_secs = (int(chain["block"]) - info.block_number) * 5
            implied = amount_to_ttl(info.amount, price)
            assert implied > 0
            assert info.ttl > 0
            # Deliberately NO assertion relating the two. Two successive
            # attempts here failed against this very batch: first
            # `implied == ttl` (off by exactly the batch's age), then
            # `ttl <= implied + age` (broken once the issuer's amount reverted
            # while topped-up TTL remained — live: implied 2396540 + age
            # 340940 against a real 3465968). The node's own bookkeeping makes
            # amount and batchTTL independent; only batchTTL is authoritative.
            assert age_secs >= 0

            # dilution is priced in TTL, and only ever upward
            dil = await mgr.plan_dilute(STAMP, info.depth + 1)
            assert dil.ttl_after_secs == pytest.approx(max(info.ttl, 0) / 2, rel=1e-3)
            with pytest.raises(Exception, match="only increases depth"):
                await mgr.plan_dilute(STAMP, info.depth)
        finally:
            await client.close()

    asyncio.run(main())


@pytest.mark.skipif(
    not (STAMP and os.environ.get("SWARMFS_TEST_SPEND")),
    reason="set SWARMFS_TEST_SPEND=<xBZZ budget, e.g. 0.01> to really top up "
           "a batch — this SPENDS the node wallet's xBZZ and is irreversible",
)
def test_topup_extends_a_real_batch_live():
    """The one test that spends money: top up ``SWARMFS_TEST_STAMP`` by
    ``SWARMFS_TEST_SPEND`` xBZZ and assert the batch gained exactly the
    purchased amount (the additive property) once the node indexes it.
    """
    import asyncio

    from swarmfs._client import SwarmClient
    from swarmfs.stamps import StampManager

    budget = float(os.environ["SWARMFS_TEST_SPEND"])

    async def main():
        client = SwarmClient(BEE)
        try:
            mgr = StampManager(client)
            before = await mgr.get_batch(STAMP)
            plan = await mgr.plan_topup(STAMP, budget_bzz=budget)
            assert plan.cost_bzz <= budget * 1.001
            if plan.cost_bzz > await mgr.balance_bzz():
                pytest.skip(f"wallet cannot cover {plan.cost_bzz:.4f} xBZZ")

            after = await mgr.topup(STAMP, plan.added_amount)
            # batchTTL is the authoritative signal: it must have grown by
            # roughly what was bought, allowing for drain during the ~40 s the
            # node takes to index the event.
            gained = after.ttl - before.ttl
            assert gained == pytest.approx(plan.added_ttl_secs, rel=0.05), (
                f"expected ~{plan.added_ttl_secs}s more life, got {gained}s")
            assert after.depth == before.depth  # topup never changes capacity
            # `amount` is the local issuer's bookkeeping (bee increments it in
            # memory without persisting), so it may or may not have moved —
            # when it does, the delta must be exactly what was paid for.
            if after.amount != before.amount:
                assert after.amount - before.amount == plan.added_amount, (
                    "a topup must ADD to the batch's balance, not replace it")
        finally:
            await client.close()

    asyncio.run(main())


@pytest.mark.skipif(not STAMP, reason="needs SWARMFS_TEST_STAMP to inspect")
def test_bucket_occupancy_agrees_with_the_summary_live():
    """Read-only: GET /stamps/{id}/buckets parsed into BucketStats must agree
    with what GET /stamps/{id} summarises, cross-checking our parsing against
    two independent endpoints — and confirming that `utilization` really is
    the fullest bucket's load, which is what bounds the next upload.
    """
    import asyncio

    from swarmfs._client import SwarmClient
    from swarmfs.stamps import BUCKET_DEPTH, StampManager, suggest_depth

    async def main():
        client = SwarmClient(BEE)
        try:
            mgr = StampManager(client)
            info = await mgr.get_batch(STAMP)
            stats = await mgr.buckets(STAMP)

            assert stats.depth == info.depth
            assert stats.bucket_depth == info.bucket_depth == BUCKET_DEPTH
            assert stats.capacity == 1 << (info.depth - info.bucket_depth)
            assert sum(stats.loads.values()) == 1 << BUCKET_DEPTH
            # the summary's utilization IS the fullest bucket
            assert stats.max_load == info.utilization
            if info.utilization_ratio is not None:
                assert stats.max_load / stats.capacity == pytest.approx(
                    info.utilization_ratio, rel=1e-6)
            assert stats.headroom == stats.capacity - stats.max_load
            assert 0.0 <= stats.risk_for(1) <= 1.0

            # sizing this batch's own content from bytes alone is pessimistic
            # compared with what the true histogram shows actually fits
            from_bytes = suggest_depth(stats.chunks * 4096, redundancy=0)
            exact_enough = BUCKET_DEPTH + max(stats.max_load - 1, 1).bit_length()
            assert exact_enough <= from_bytes
        finally:
            await client.close()

    asyncio.run(main())


@pytest.mark.skipif(not STAMP, reason="ACT round trip uploads; needs SWARMFS_TEST_STAMP")
def test_act_roundtrip_live(tmp_path):
    """ACT against a real node: protect a directory, read it back through a
    fresh instance holding only the history (publisher = this node), watch
    it stay invisible without the headers, continue the history with a
    second commit and a single-file upload, and manage a grantee list.
    Pins the facts in swarmfs/act.py: opaque same-length references, the
    root-only wrapping, the mandatory publisher, the 1-second patch rule."""
    from swarmfs import SwarmFileSystem
    from swarmfs.act import GranteeList
    from swarmfs.exceptions import BeeAPIError

    keys = pytest.importorskip("eth_keys").keys
    # real curve points: Bee validates Swarm-Act-Publisher and grantee keys
    # as secp256k1 points and answers 400 "invalid public key" otherwise
    other_key = keys.PrivateKey(bytes(range(1, 33))).public_key.to_compressed_bytes().hex()
    grantee = keys.PrivateKey(bytes(range(101, 133))).public_key.to_compressed_bytes().hex()

    d = tmp_path / "private"
    (d / "data").mkdir(parents=True)
    (d / "readme.txt").write_bytes(b"members only\n")
    (d / "data" / "blob.bin").write_bytes(bytes(range(256)) * 40)

    pub = SwarmFileSystem(api_url=BEE, stamp=STAMP, act=True, redundancy=0,
                          skip_instance_cache=True)
    root = pub.upload(str(d))
    history = pub.act_history
    assert len(root) == 128 and history and len(history) == 64  # act ⇒ encrypt
    assert pub.commit_log[-1].act_history == history
    assert pub.cat_file(f"bzz://{root}/readme.txt") == b"members only\n"

    # a fresh reader with the history only — the publisher is this node
    reader = SwarmFileSystem(api_url=BEE, act_history=history, skip_instance_cache=True)
    assert sorted(reader.find(f"bzz://{root}")) == [f"{root}/data/blob.bin", f"{root}/readme.txt"]
    assert reader.cat_file(f"bzz://{root}/data/blob.bin", start=256, end=260) == bytes(range(4))
    assert reader.info(f"bzz://{root}/data/blob.bin")["size"] == 256 * 40
    assert reader.act_publisher == pub.publisher_key()

    # invisible without the headers, and with the wrong publisher
    blind = SwarmFileSystem(api_url=BEE, skip_instance_cache=True)
    with pytest.raises(FileNotFoundError):
        blind.ls(f"bzz://{root}")
    wrong = SwarmFileSystem(api_url=BEE, act_history=history,
                            act_publisher=other_key, skip_instance_cache=True)
    with pytest.raises(FileNotFoundError):
        wrong.ls(f"bzz://{root}")
    # a well-formed key that is not a curve point is a 400 from the node,
    # not a 404 — surfaced as BeeAPIError, not as "not found"
    malformed = SwarmFileSystem(api_url=BEE, act_history=history,
                                act_publisher="02" + "cd" * 32, skip_instance_cache=True)
    with pytest.raises(BeeAPIError, match="invalid public key"):
        malformed.ls(f"bzz://{root}")

    # continuing the history: a second commit and a single-file upload
    pub.pipe_file(f"bzz://{root}/data/more.txt", b"second commit")
    head = pub.latest(root)
    assert head != root and pub.act_history == history
    assert reader.cat_file(f"bzz://{head}/data/more.txt") == b"second commit"
    one = tmp_path / "one.txt"
    one.write_bytes(b"single protected file")
    ref1 = pub.upload(str(one))
    assert pub.act_history == history
    assert reader.cat_file(f"bzz://{ref1}/one.txt") == b"single protected file"

    # grantees: create, inspect, patch (Bee refuses two updates in one second)
    gl = pub.create_grantees([grantee])
    assert isinstance(gl, GranteeList) and len(gl.history) == 64
    assert pub.grantees(gl.reference) == [grantee]
    time.sleep(1.2)
    gl2 = pub.patch_grantees(gl.reference, gl.history, add=[pub.publisher_key()])
    assert gl2.history != gl.history
    assert sorted(pub.grantees(gl2.reference)) == sorted([grantee, pub.publisher_key()])
    # publishing into the grantee history: readable with that history
    member = SwarmFileSystem(api_url=BEE, stamp=STAMP, act=True, act_history=gl2.history,
                             redundancy=0, skip_instance_cache=True)
    r2 = member.upload(str(d))
    assert member.act_history == gl2.history
    assert SwarmFileSystem(api_url=BEE, act_history=gl2.history, skip_instance_cache=True
                           ).cat_file(f"bzz://{r2}/readme.txt") == b"members only\n"
