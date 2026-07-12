"""Integration tests against a real Bee node.

Skipped unless SWARMFS_TEST_BEE is set (e.g. http://localhost:1633).
Uploading the fixture additionally needs SWARMFS_TEST_STAMP (a usable
postage batch id); without it, set SWARMFS_TEST_REF to a known collection
reference to run the read-side assertions against existing content.
"""

from __future__ import annotations

import io
import os
import tarfile
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
