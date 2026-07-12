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
