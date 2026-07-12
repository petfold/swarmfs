"""v1 write path: staging, commits, transactions, root mapping — offline,
through the sync fsspec API against the fake client."""

from __future__ import annotations

import pytest

from swarmfs import SwarmFileSystem
from swarmfs.stamps import StampError

from conftest import FILES, FakeClient, build_manifest


@pytest.fixture()
def wfs(manifest):
    root_hex, store = manifest
    client = FakeClient(store)
    fs = SwarmFileSystem(client=client, skip_instance_cache=True)
    return fs, root_hex, client


def test_pipe_autocommit_and_read_your_writes(wfs):
    fs, root, client = wfs
    fs.pipe_file(f"bzz://{root}/notes/hello.txt", b"written to swarm")

    assert len(fs.commit_log) == 1
    new_root = fs.latest(root)
    assert new_root != root and len(new_root) == 64

    # read through the ORIGINAL url (root map) and through the new root
    assert fs.cat_file(f"bzz://{root}/notes/hello.txt") == b"written to swarm"
    assert fs.cat_file(f"bzz://{new_root}/notes/hello.txt") == b"written to swarm"
    # pre-existing content is still there in the new root
    assert fs.cat_file(f"bzz://{new_root}/index.html") == FILES["index.html"]
    # the old root itself is untouched (immutable snapshot)
    fresh = SwarmFileSystem(client=client, skip_instance_cache=True)
    assert not fresh.exists(f"bzz://{root}/notes/hello.txt")

    info = fs.info(f"bzz://{root}/notes/hello.txt")
    assert info["type"] == "file" and info["size"] == 16
    assert info["metadata"]["Content-Type"] == "text/plain"
    assert info["metadata"]["Filename"] == "hello.txt"


def test_transaction_batches_into_one_commit(wfs):
    fs, root, client = wfs
    with fs.transaction:
        fs.pipe_file(f"bzz://{root}/batch/a.txt", b"aaa")
        fs.pipe_file(f"bzz://{root}/batch/b.txt", b"bbb")
        fs.rm_file(f"bzz://{root}/index.html")

        # staged state is readable before the commit
        assert fs.cat_file(f"bzz://{root}/batch/a.txt") == b"aaa"
        assert fs.info(f"bzz://{root}/batch/a.txt")["staged"] is True
        assert fs.isdir(f"bzz://{root}/batch")
        names = fs.ls(f"bzz://{root}/batch", detail=False)
        assert sorted(names) == [f"{root}/batch/a.txt", f"{root}/batch/b.txt"]
        with pytest.raises(FileNotFoundError):
            fs.info(f"bzz://{root}/index.html")
        assert len(fs.commit_log) == 0

    assert len(fs.commit_log) == 1
    res = fs.commit_log[0]
    assert set(res.written) == {"batch/a.txt", "batch/b.txt"}
    assert res.removed == ["index.html"]
    assert fs.cat_file(f"bzz://{root}/batch/b.txt") == b"bbb"
    assert not fs.exists(f"bzz://{root}/index.html")
    assert fs.exists(f"bzz://{fs.latest(root)}/batch/a.txt")


def test_transaction_rollback_discards(wfs):
    fs, root, client = wfs
    uploads_before = len(client.uploads)
    with pytest.raises(RuntimeError, match="boom"):
        with fs.transaction:
            fs.pipe_file(f"bzz://{root}/doomed.txt", b"never")
            raise RuntimeError("boom")
    assert len(fs.commit_log) == 0
    assert len(client.uploads) == uploads_before  # nothing hit the node
    assert fs.latest(root) == root
    assert not fs.exists(f"bzz://{root}/doomed.txt")


def test_open_wb(wfs):
    fs, root, client = wfs
    with fs.open(f"bzz://{root}/out/blob.bin", "wb") as f:
        f.write(b"chunk one|")
        f.write(b"chunk two")
    assert fs.cat_file(f"bzz://{root}/out/blob.bin") == b"chunk one|chunk two"
    assert len(fs.commit_log) == 1


def test_put_file(wfs, tmp_path):
    fs, root, client = wfs
    local = tmp_path / "upload.csv"
    local.write_bytes(b"a,b\n1,2\n")
    fs.put_file(str(local), f"bzz://{root}/data/upload.csv")
    assert fs.cat_file(f"bzz://{root}/data/upload.csv") == b"a,b\n1,2\n"
    info = fs.info(f"bzz://{root}/data/upload.csv")
    assert info["metadata"]["Content-Type"] == "text/csv"


def test_rm_and_errors(wfs):
    fs, root, client = wfs
    fs.rm_file(f"bzz://{root}/data/part-00000.parquet")
    assert not fs.exists(f"bzz://{root}/data/part-00000.parquet")
    assert fs.exists(f"bzz://{root}/data/part-00001.parquet")
    with pytest.raises(FileNotFoundError):
        fs.rm_file(f"bzz://{root}/no-such-file.txt")


def test_rm_last_file_prunes_directory(wfs):
    fs, root, client = wfs
    fs.rm_file(f"bzz://{root}/data-old/readme.md")
    assert not fs.exists(f"bzz://{root}/data-old")
    names = fs.ls(f"bzz://{root}", detail=False)
    assert f"{fs.latest(root)}/data-old" not in [
        n.replace(root, fs.latest(root)) for n in names
    ]


def test_new_pseudo_root(wfs):
    fs, _, client = wfs
    fs.pipe_file("bzz://new/greeting.txt", b"hi")
    real = fs.latest("new")
    assert len(real) == 64
    assert fs.cat_file("bzz://new/greeting.txt") == b"hi"
    assert fs.cat_file(f"bzz://{real}/greeting.txt") == b"hi"
    # separate fresh lineages don't collide
    fs.pipe_file("bzz://new-other/x.txt", b"x")
    assert fs.latest("new-other") != fs.latest("new")
    assert not fs.exists("bzz://new/x.txt")


def test_find_sees_staged_and_removed(wfs):
    fs, root, client = wfs
    with fs.transaction:
        fs.pipe_file(f"bzz://{root}/data/part-00002.parquet", b"P2")
        fs.rm_file(f"bzz://{root}/data/part-00000.parquet")
        found = fs.find(f"bzz://{root}/data")
        assert f"{root}/data/part-00002.parquet" in found
        assert f"{root}/data/part-00000.parquet" not in found
        assert f"{root}/data/part-00001.parquet" in found


def test_cp_file_preserves_content_type(wfs):
    fs, root, client = wfs
    fs.cp_file(f"bzz://{root}/index.html", f"bzz://{root}/copy.html")
    assert fs.cat_file(f"bzz://{root}/copy.html") == FILES["index.html"]
    meta = fs.info(f"bzz://{root}/copy.html")["metadata"]
    assert meta["Content-Type"] == "text/html; charset=utf-8"


def test_stamp_failure_is_early_and_lossless(manifest):
    root, store = manifest
    client = FakeClient(store, stamps=[])
    fs = SwarmFileSystem(client=client, skip_instance_cache=True)
    with pytest.raises(StampError, match="no postage stamps"):
        fs.pipe_file(f"bzz://{root}/x.txt", b"data")
    assert len(client.uploads) == 0  # failed before any byte was uploaded
    # the staged write survived the failed commit: fix the stamp and retry
    client.stamps.append(dict(__import__("conftest").GOOD_STAMP))
    fs.commit_all()
    assert fs.cat_file(f"bzz://{root}/x.txt") == b"data"


def test_explicit_stamp_validation(manifest):
    from conftest import GOOD_STAMP

    root, store = manifest
    bad = dict(GOOD_STAMP, usable=False, batchID="cd" * 32)
    client = FakeClient(store, stamps=[bad, dict(GOOD_STAMP)])
    fs = SwarmFileSystem(client=client, stamp="cd" * 32, skip_instance_cache=True)
    with pytest.raises(StampError, match="not usable"):
        fs.pipe_file(f"bzz://{root}/x.txt", b"data")

    fs2 = SwarmFileSystem(client=client, stamp="ab" * 32, skip_instance_cache=True)
    fs2.pipe_file(f"bzz://{root}/x.txt", b"data")
    assert client.uploads and all(s == "ab" * 32 for s, _ in client.uploads)


def test_patch_reuploads_only_affected_nodes(wfs):
    """The commit engine re-uploads O(path depth) manifest nodes, not the trie."""
    fs, root, client = wfs
    fs.pipe_file(f"bzz://{root}/index2.html", b"<h1>two</h1>")
    # uploads = 1 data blob + the nodes along the changed path
    assert 2 <= len(client.uploads) <= 6, client.uploads


def test_overwrite_existing_file(wfs):
    fs, root, client = wfs
    fs.pipe_file(f"bzz://{root}/index.html", b"<h1>updated</h1>", content_type="text/html")
    assert fs.cat_file(f"bzz://{root}/index.html") == b"<h1>updated</h1>"
    # unrelated files untouched
    assert fs.cat_file(f"bzz://{root}/data/part-00001.parquet") == FILES[
        "data/part-00001.parquet"
    ]


def test_mkdir_is_noop(wfs):
    fs, root, client = wfs
    fs.mkdir(f"bzz://{root}/whatever")
    fs.makedirs(f"bzz://{root}/a/b/c", exist_ok=True)
    assert len(fs.commit_log) == 0
