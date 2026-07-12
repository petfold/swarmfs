"""SwarmFileSystem tests through the *sync* fsspec API, so the whole async
plumbing (event loop thread, sync wrappers, dircache) is exercised."""

from __future__ import annotations

import io

import fsspec
import pytest

from swarmfs import SwarmFileSystem

from conftest import FILES


def test_protocol_registered():
    assert fsspec.get_filesystem_class("bzz") is SwarmFileSystem


def test_strip_protocol():
    ref = "ab" * 32
    assert SwarmFileSystem._strip_protocol(f"bzz://{ref}/a/b/") == f"{ref}/a/b"
    assert SwarmFileSystem._strip_protocol(f"{ref}/a") == f"{ref}/a"


def test_invalid_reference_rejected(fs):
    fs, _ = fs
    with pytest.raises(ValueError, match="reference"):
        fs.ls("bzz://not-a-reference/x")


def test_ls_root(fs):
    fs, root = fs
    names = fs.ls(f"bzz://{root}", detail=False)
    assert names == sorted(
        [f"{root}/index.html", f"{root}/a", f"{root}/assets", f"{root}/data", f"{root}/data-old"]
    )


def test_ls_detail(fs):
    fs, root = fs
    entries = {e["name"]: e for e in fs.ls(f"bzz://{root}/data")}
    e0 = entries[f"{root}/data/part-00000.parquet"]
    assert e0["type"] == "file"
    assert e0["size"] == len(FILES["data/part-00000.parquet"])
    assert e0["mimetype"] == "application/octet-stream"
    e1 = entries[f"{root}/data/part-00001.parquet"]
    assert e1["size"] == len(FILES["data/part-00001.parquet"])


def test_ls_of_file_lists_itself(fs):
    fs, root = fs
    entries = fs.ls(f"bzz://{root}/index.html")
    assert len(entries) == 1
    assert entries[0]["type"] == "file"
    assert entries[0]["name"] == f"{root}/index.html"


def test_info(fs):
    fs, root = fs
    info = fs.info(f"bzz://{root}/index.html")
    assert info["type"] == "file"
    assert info["size"] == len(FILES["index.html"])
    assert info["mimetype"] == "text/html; charset=utf-8"

    assert fs.info(f"bzz://{root}/assets")["type"] == "directory"
    assert fs.info(f"bzz://{root}")["type"] == "directory"
    # "data" only exists mid-edge (data/..., data-old/...) — still a directory
    assert fs.info(f"bzz://{root}/data")["type"] == "directory"

    with pytest.raises(FileNotFoundError):
        fs.info(f"bzz://{root}/missing.txt")


def test_predicates(fs):
    fs, root = fs
    assert fs.isfile(f"bzz://{root}/index.html")
    assert not fs.isdir(f"bzz://{root}/index.html")
    assert fs.isdir(f"bzz://{root}/assets")
    assert fs.exists(f"bzz://{root}/data/part-00001.parquet")
    assert not fs.exists(f"bzz://{root}/data/part-00002.parquet")


def test_cat_full_and_ranges(fs):
    fs, root = fs
    content = FILES["data/part-00000.parquet"]
    url = f"bzz://{root}/data/part-00000.parquet"
    assert fs.cat_file(url) == content
    assert fs.cat_file(url, start=10, end=20) == content[10:20]
    assert fs.cat_file(url, start=100) == content[100:]
    assert fs.cat_file(url, end=7) == content[:7]
    with pytest.raises(IsADirectoryError):
        fs.cat_file(f"bzz://{root}/data")


def test_open_read_and_seek(fs):
    fs, root = fs
    content = FILES["data/part-00001.parquet"]
    with fs.open(f"bzz://{root}/data/part-00001.parquet", block_size=1024) as f:
        assert f.size == len(content)
        assert f.read(16) == content[:16]
        f.seek(4000)
        assert f.read(100) == content[4000:4100]
        f.seek(-5, 2)
        assert f.read() == content[-5:]


def test_find(fs):
    fs, root = fs
    assert fs.find(f"bzz://{root}") == sorted(f"{root}/{p}" for p in FILES)
    assert fs.find(f"bzz://{root}/assets") == [
        f"{root}/assets/css/site.css",
        f"{root}/assets/img/logo.png",
    ]
    # maxdepth prunes by directory level
    assert fs.find(f"bzz://{root}", maxdepth=1) == [f"{root}/index.html"]
    # find on a file returns the file
    assert fs.find(f"bzz://{root}/index.html") == [f"{root}/index.html"]
    # withdirs synthesizes the intermediate directories
    withdirs = fs.find(f"bzz://{root}/assets", withdirs=True)
    assert f"{root}/assets/css" in withdirs and f"{root}/assets/img" in withdirs


def test_glob(fs):
    fs, root = fs
    assert fs.glob(f"bzz://{root}/data/*.parquet") == [
        f"{root}/data/part-00000.parquet",
        f"{root}/data/part-00001.parquet",
    ]
    assert fs.glob(f"bzz://{root}/**/*.css") == [f"{root}/assets/css/site.css"]


def test_du(fs):
    fs, root = fs
    assert fs.du(f"bzz://{root}/data") == sum(
        len(FILES[p]) for p in FILES if p.startswith("data/")
    )


def test_writes_rejected(fs):
    fs, root = fs
    with pytest.raises(NotImplementedError, match="read-only"):
        fs.pipe_file(f"bzz://{root}/new.txt", b"data")
    with pytest.raises(NotImplementedError, match="read-only"):
        fs.open(f"bzz://{root}/new.txt", "wb")
    with pytest.raises(NotImplementedError, match="read-only"):
        fs.rm(f"bzz://{root}/index.html")


def test_get_file_download(fs, tmp_path):
    fs, root = fs
    local = tmp_path / "logo.png"
    fs.get_file(f"bzz://{root}/assets/img/logo.png", str(local))
    assert local.read_bytes() == FILES["assets/img/logo.png"]


def test_name_that_is_both_file_and_directory():
    """A Mantaray trie can hold a value at "a" and entries under "a/";
    ls must not emit duplicate names, and the file wins."""
    from conftest import FakeClient, build_manifest

    root, store = build_manifest({"a": b"file-a", "a/b.txt": b"file-b"})
    fs = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)

    entries = fs.ls(f"bzz://{root}")
    assert [e["name"] for e in entries] == [f"{root}/a"]
    assert entries[0]["type"] == "file"
    assert fs.cat_file(f"bzz://{root}/a") == b"file-a"
    assert fs.ls(f"bzz://{root}/a", detail=False) == [f"{root}/a/b.txt"]
    assert fs.cat_file(f"bzz://{root}/a/b.txt") == b"file-b"


def test_simplecache_chaining(manifest, tmp_path):
    """`simplecache::bzz://…` — local caching via URL chaining, zero code."""
    root, store = manifest
    from conftest import FakeClient

    with fsspec.open(
        f"simplecache::bzz://{root}/index.html",
        bzz={"client": FakeClient(store), "skip_instance_cache": True},
        simplecache={"cache_storage": str(tmp_path)},
    ) as f:
        assert f.read() == FILES["index.html"]
    assert any(tmp_path.iterdir()), "file was cached locally"


def test_pandas_parquet_demo():
    """The acceptance demo for the data audience:
    pd.read_parquet("bzz://<ref>/df.parquet") end to end (offline)."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    from conftest import FakeClient, build_manifest

    df = pd.DataFrame({"x": range(1000), "y": [f"row-{i}" for i in range(1000)]})
    buf = io.BytesIO()
    df.to_parquet(buf)
    root, store = build_manifest({"dataset/df.parquet": buf.getvalue()})

    out = pd.read_parquet(
        f"bzz://{root}/dataset/df.parquet",
        storage_options={"client": FakeClient(store), "skip_instance_cache": True},
    )
    pd.testing.assert_frame_equal(out, df)
