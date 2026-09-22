"""Distributed writes: ``put_blob`` + ``link`` (docs/distributed-writes.md §2).

The shape under test is the cross-process one: a worker uploads a payload
through its own node and gets a bare data reference back; the driver stages
that reference as a manifest entry and commits once. Content addressing is
what makes it compose — a Mantaray entry is a reference and does not care
who uploaded the data, or with which batch. Offline, against the fake node:
two ``FakeClient``s over one store are two processes on one swarm.
"""

from __future__ import annotations

import io

import pytest

from swarmfs import SwarmFileSystem

from conftest import FILES, GOOD_STAMP, FakeClient


@pytest.fixture()
def wfs(manifest):
    root_hex, store = manifest
    client = FakeClient(store)
    fs = SwarmFileSystem(client=client, skip_instance_cache=True)
    return fs, root_hex, client


def _sizes(client, since=0):
    return [n for _, n in client.uploads[since:]]


# -- put_blob ---------------------------------------------------------------


def test_put_blob_returns_bare_reference(wfs):
    fs, root, client = wfs
    data = b"partition bytes"
    ref = fs.put_blob(data)

    assert len(ref) == 64
    # a *data* reference, not a wrapping manifest: the bytes come back as-is
    assert fs.read_reference(ref) == data
    assert client.uploads[-1] == (GOOD_STAMP["batchID"], len(data))
    assert client.redundancies[-1] == 2  # the instance's write policy applies
    # no manifest, no lineage, no staging
    assert fs.commit_log == []
    assert not fs._staged


def test_put_blob_accepts_a_file_object(wfs):
    fs, root, client = wfs
    buf = io.BytesIO(b"from a file object")
    buf.read(4)  # position is irrelevant: the whole payload goes up
    ref = fs.put_blob(buf)
    assert fs.read_reference(ref) == b"from a file object"


def test_put_blob_refused_on_act_instance(manifest):
    root, store = manifest
    fs = SwarmFileSystem(client=FakeClient(store), act=True,
                         skip_instance_cache=True)
    with pytest.raises(ValueError, match="ACT"):
        fs.put_blob(b"secret")


# -- link -------------------------------------------------------------------


def test_link_into_new_lineage(wfs):
    fs, root, client = wfs
    payload = b"P" * 5000  # bigger than a manifest node, so uploads are telling
    ref = fs.put_blob(payload)
    mark = len(client.uploads)

    fs.link("bzz://new/ds/part.0.parquet", ref, size=len(payload))
    new_root = fs.latest("new")
    assert len(new_root) == 64

    # the commit uploaded manifest nodes only — the payload was never re-sent
    assert len(payload) not in _sizes(client, mark)
    assert fs.cat_file(f"bzz://{new_root}/ds/part.0.parquet") == payload

    info = fs.info(f"bzz://{new_root}/ds/part.0.parquet")
    assert info["size"] == len(payload)
    assert info["reference"] == ref
    assert info["metadata"]["Filename"] == "part.0.parquet"


def test_link_into_existing_root_patches_minimally(wfs):
    fs, root, client = wfs
    payload = b"NEW" * 2000
    ref = fs.put_blob(payload)
    mark = len(client.uploads)

    fs.link(f"bzz://{root}/data/part-00002.parquet", ref)

    # O(path depth) node re-uploads, like a written file — and no data blob
    assert 1 <= len(client.uploads) - mark <= 5, client.uploads[mark:]
    assert len(payload) not in _sizes(client, mark)
    assert fs.cat_file(f"bzz://{root}/data/part-00002.parquet") == payload
    # the rest of the manifest came along untouched
    assert fs.cat_file(f"bzz://{root}/data/part-00001.parquet") == (
        FILES["data/part-00001.parquet"])
    assert fs.cat_file(f"bzz://{root}/index.html") == FILES["index.html"]


def test_transaction_collects_links_into_one_commit(wfs):
    fs, root, client = wfs
    refs = [fs.put_blob(f"part {i}".encode() * 1000) for i in range(3)]
    mark = len(client.uploads)

    with fs.transaction:
        for i, ref in enumerate(refs):
            fs.link(f"bzz://new/ds/part.{i}.parquet", ref)
        assert fs.commit_log == []  # nothing until the transaction closes

    assert len(fs.commit_log) == 1
    new_root = fs.latest("new")
    for i, ref in enumerate(refs):
        assert fs.cat_file(f"bzz://{new_root}/ds/part.{i}.parquet") == (
            f"part {i}".encode() * 1000)
    # one commit: the only uploads were that commit's manifest nodes
    assert all(n <= 4096 for n in _sizes(client, mark))


def test_commit_log_records_links(wfs):
    fs, root, client = wfs
    ref = fs.put_blob(b"linked payload")
    fs.link("bzz://new/ds/part.0.parquet", ref)

    res = fs.commit_log[-1]
    assert res.written == {"ds/part.0.parquet": ref}  # where the entry came from
    assert res.old_root is None and res.new_root == fs.latest("new")
    assert res.batch == GOOD_STAMP["batchID"]  # the driver's own batch


def test_link_refuses_mixed_refsize(manifest):
    root, store = manifest
    plain = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    enc = SwarmFileSystem(client=FakeClient(store), encrypt=True,
                          skip_instance_cache=True)

    with pytest.raises(ValueError, match="cannot mix"):
        plain.link("bzz://new/x.bin", "ab" * 64)  # 128-hex into a plain lineage
    with pytest.raises(ValueError, match="cannot mix"):
        enc.link("bzz://new/x.bin", "ab" * 32)  # 64-hex into an encrypted one
    with pytest.raises(ValueError, match="invalid swarm reference"):
        plain.link("bzz://new/x.bin", "not-a-reference")

    # a refused link stages nothing and commits nothing
    assert not plain._staged and not enc._staged
    assert plain.commit_log == [] and enc.commit_log == []


def test_link_refuses_the_manifest_root(wfs):
    fs, root, client = wfs
    ref = fs.put_blob(b"x")
    with pytest.raises(IsADirectoryError):
        fs.link("bzz://new", ref)


def test_link_size_is_advisory(wfs):
    fs, root, client = wfs
    ref = fs.put_blob(b"x" * 321)
    with fs.transaction:
        fs.link("bzz://new/sized/a.bin", ref)  # no size given
        fs.link("bzz://new/sized/b.bin", ref, size=321)

        # staged, not yet committed: a.bin's span is read from the node
        assert fs.info("bzz://new/sized/a.bin")["size"] == 321
        assert fs.info("bzz://new/sized/b.bin")["size"] == 321
        assert fs.info("bzz://new/sized/a.bin")["staged"] is True
        assert {e["size"] for e in fs.ls("bzz://new/sized")} == {321}
        assert all(e["size"] == 321
                   for e in fs.find("bzz://new/sized", detail=True).values())
        # and a staged link reads through its reference, ranges included
        assert fs.cat_file("bzz://new/sized/a.bin", start=2, end=5) == b"xxx"

    new_root = fs.latest("new")
    assert fs.size(f"bzz://{new_root}/sized/a.bin") == 321


def test_link_overwrites_and_unstages_like_a_write(wfs):
    fs, root, client = wfs
    ref = fs.put_blob(b"replacement")
    fs.link(f"bzz://{root}/index.html", ref, metadata={"Content-Type": "text/html"})
    assert fs.cat_file(f"bzz://{root}/index.html") == b"replacement"
    assert fs.info(f"bzz://{root}/index.html")["metadata"]["Content-Type"] == "text/html"

    # rollback discards a staged link without uploading anything
    mark = len(client.uploads)
    with pytest.raises(RuntimeError, match="boom"):
        with fs.transaction:
            fs.link(f"bzz://{root}/doomed.bin", ref)
            raise RuntimeError("boom")
    assert len(client.uploads) == mark
    assert not fs.exists(f"bzz://{root}/doomed.bin")


# -- the cross-process shape ------------------------------------------------


def test_two_processes_one_manifest(manifest):
    """The whole driver-side protocol: N workers upload, one driver links."""
    root, store = manifest
    workers = [SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
               for _ in range(2)]
    driver = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)

    parts = {}
    for i in range(4):
        payload = f"part{i}".encode() * 500
        parts[f"ds/part.{i}.parquet"] = (
            workers[i % 2].put_blob(payload), payload)

    mark = len(driver.client.uploads)
    with driver.transaction:
        for path, (ref, _) in parts.items():
            driver.link(f"bzz://new/{path}", ref)

    new_root = driver.latest("new")
    assert len(driver.commit_log) == 1
    # the driver uploaded manifest nodes only; the data went up on the workers
    assert all(n <= 4096 for n in _sizes(driver.client, mark))

    # a process that saw none of it reads the finished dataset
    reader = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    assert sorted(reader.find(f"bzz://{new_root}")) == sorted(
        f"{new_root}/{path}" for path in parts)
    for path, (_, payload) in parts.items():
        assert reader.cat_file(f"bzz://{new_root}/{path}") == payload


def test_encrypted_lineage_links_128_hex_references(manifest):
    """An encrypted lineage needs encrypted blobs: the worker's instance
    must carry the same ``encrypt`` policy, or the refBytesSize check
    catches the mismatch before anything is staged."""
    root, store = manifest
    worker = SwarmFileSystem(client=FakeClient(store), encrypt=True,
                             skip_instance_cache=True)
    driver = SwarmFileSystem(client=FakeClient(store), encrypt=True,
                             skip_instance_cache=True)

    ref = worker.put_blob(b"encrypted partition")
    assert len(ref) == 128  # address ‖ key

    driver.link("bzz://new/ds/part.0.bin", ref)
    new_root = driver.latest("new")
    assert len(new_root) == 128
    assert driver.cat_file(f"bzz://{new_root}/ds/part.0.bin") == b"encrypted partition"


def test_link_into_act_protected_lineage(manifest):
    """Only the root is wrapped, so linked children are ordinary references
    — a protected manifest composes from blobs the publisher never uploaded."""
    root, store = manifest
    worker = SwarmFileSystem(client=FakeClient(store), encrypt=True,
                             skip_instance_cache=True)
    publisher = SwarmFileSystem(client=FakeClient(store), act=True,
                                skip_instance_cache=True)

    ref = worker.put_blob(b"protected partition")
    publisher.link("bzz://new/ds/part.0.bin", ref)
    new_root = publisher.latest("new")

    assert publisher.act_history  # the commit created one
    assert publisher.cat_file(f"bzz://{new_root}/ds/part.0.bin") == b"protected partition"

    # without the history the root is invisible, as for any protected commit
    blind = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    with pytest.raises(FileNotFoundError):
        blind.cat_file(f"bzz://{new_root}/ds/part.0.bin")


def test_link_into_feed_requires_the_signer(manifest):
    pytest.importorskip("eth_keys")
    from swarmfs.feedfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedError

    root, store = manifest
    client = FakeClient(store)
    unsigned = SwarmFeedFileSystem(client=client, skip_instance_cache=True)
    ref = unsigned.put_blob(b"feed partition")
    owner = "11" * 20
    with pytest.raises(FeedError, match="signer"):
        unsigned.link(f"bzzf://{owner}/ds/part.0.bin", ref)
    assert not unsigned._staged  # refused at staging: nothing uploaded

    fs = SwarmFeedFileSystem(client=client, signer="11" * 32,
                             skip_instance_cache=True)
    path = f"bzzf://{fs.signer.owner_hex}/ds/part.0.bin"
    fs.link(path, ref)
    assert len(fs.commit_log) == 1  # one commit, one feed update
    assert fs.cat_file(path) == b"feed partition"
