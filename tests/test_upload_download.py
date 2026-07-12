"""The one-shot convenience surface: fs.upload / fs.download, plus the
generic fsspec get/put contract third-party code depends on — offline,
against the fake client."""

from __future__ import annotations

import pytest

from swarmfs import SwarmFileSystem
from swarmfs.stamps import StampError

from conftest import FILES, FakeClient, FakeGatewayClient, build_manifest


@pytest.fixture()
def ufs():
    """A filesystem over an empty store (uploads don't need prior content)."""
    client = FakeClient({})
    return SwarmFileSystem(client=client, skip_instance_cache=True), client


def test_upload_single_file_roundtrip(ufs, tmp_path):
    fs, client = ufs
    local = tmp_path / "photo.jpg"
    local.write_bytes(b"jpeg bytes")

    ref = fs.upload(str(local))

    assert isinstance(ref, str) and len(ref) == 64
    # bee-style wrapping: the file sits at its filename inside the manifest
    assert fs.cat_file(f"bzz://{ref}/photo.jpg") == b"jpeg bytes"
    # content type guessed from the filename, like the staged write path
    assert client.bzz_uploads[-1] == ("photo.jpg", "image/jpeg", False)
    # single file = single direct POST (content only; no manifest-node
    # uploads from the commit engine — Bee builds the wrapper server-side)
    assert len(client.uploads) == 1
    assert len(fs.commit_log) == 0

    fs.download(f"bzz://{ref}/photo.jpg", str(tmp_path / "copy.jpg"))
    assert (tmp_path / "copy.jpg").read_bytes() == b"jpeg bytes"


def test_upload_kwargs_passed_through(ufs, tmp_path):
    fs, client = ufs
    local = tmp_path / "blob"
    local.write_bytes(b"x" * 100)
    fs.upload(str(local), content_type="text/plain", encrypt=True, redundancy=3)
    assert client.bzz_uploads[-1] == ("blob", "text/plain", True)
    assert client.redundancies[-1] == 3
    with pytest.raises(ValueError, match="redundancy"):
        fs.upload(str(local), redundancy=7)


def test_upload_directory(ufs, tmp_path):
    fs, client = ufs
    (tmp_path / "dataset" / "sub").mkdir(parents=True)
    (tmp_path / "dataset" / "a.csv").write_bytes(b"a,b\n1,2\n")
    (tmp_path / "dataset" / "sub" / "b.bin").write_bytes(b"\x00\x01")

    ref = fs.upload(str(tmp_path / "dataset"))

    assert len(ref) == 64
    assert sorted(fs.find(f"bzz://{ref}")) == [f"{ref}/a.csv", f"{ref}/sub/b.bin"]
    assert fs.cat_file(f"bzz://{ref}/a.csv") == b"a,b\n1,2\n"
    assert fs.info(f"bzz://{ref}/a.csv")["metadata"]["Content-Type"] == "text/csv"
    # the directory path goes through the commit engine
    assert len(fs.commit_log) == 1 and fs.commit_log[0].new_root == ref

    with pytest.raises(NotImplementedError, match="encrypt"):
        fs.upload(str(tmp_path / "dataset"), encrypt=True)


def test_upload_fails_early_without_stamp(tmp_path):
    fs = SwarmFileSystem(client=FakeClient({}, stamps=[]), skip_instance_cache=True)
    local = tmp_path / "f.txt"
    local.write_bytes(b"data")
    with pytest.raises(StampError):
        fs.upload(str(local))
    assert len(fs.client.uploads) == 0  # nothing hit the node


def test_upload_refused_on_gateway(tmp_path):
    fs = SwarmFileSystem(client=FakeGatewayClient({}), skip_instance_cache=True)
    local = tmp_path / "f.txt"
    local.write_bytes(b"data")
    with pytest.raises(PermissionError, match="gateway"):
        fs.upload(str(local))


def test_upload_with_rpath_is_generic_put(tmp_path):
    """upload(lpath, rpath) keeps fsspec's base-class alias-of-put contract."""
    root, store = build_manifest(FILES)
    fs = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    local = tmp_path / "extra.txt"
    local.write_bytes(b"via put")
    out = fs.upload(str(local), f"bzz://{root}/extra.txt")
    assert not isinstance(out, str)  # put's return, not a reference
    assert fs.cat_file(f"bzz://{root}/extra.txt") == b"via put"


def test_generic_get_and_put(tmp_path):
    """The plain fsspec interface (what dask/rsync/third parties call)."""
    root, store = build_manifest(FILES)
    fs = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)

    fs.get(f"bzz://{root}/index.html", str(tmp_path / "index.html"))
    assert (tmp_path / "index.html").read_bytes() == FILES["index.html"]

    local = tmp_path / "new.csv"
    local.write_bytes(b"x,y\n")
    fs.put(str(local), f"bzz://{root}/data/new.csv")
    assert fs.cat_file(f"bzz://{root}/data/new.csv") == b"x,y\n"

    # recursive download of a directory through the alias
    fs.download(f"bzz://{root}/assets", str(tmp_path / "assets"), recursive=True)
    assert (tmp_path / "assets" / "css" / "site.css").read_bytes() == FILES[
        "assets/css/site.css"
    ]


def test_put_to_bare_protocol_directs_to_upload(tmp_path):
    """A write with no reference cannot succeed silently — the error says
    where the answer lives."""
    root, store = build_manifest(FILES)
    fs = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    local = tmp_path / "f.txt"
    local.write_bytes(b"data")
    with pytest.raises(ValueError, match="fs.upload"):
        fs.put(str(local), "bzz://")
