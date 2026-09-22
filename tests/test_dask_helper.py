"""swarmfs.dask — a dask dataset written to Swarm as ONE manifest.

Offline, against the fake node. The point under test is the channel the
generic path lacks: partitions upload on workers, their references come
back to the driver, and a single commit assembles them.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pandas")
pytest.importorskip("dask.dataframe")
pytest.importorskip("pyarrow")

import dask.dataframe as dd  # noqa: E402
import pandas as pd  # noqa: E402

import swarmfs.dask as sd  # noqa: E402
from swarmfs.stamps import StampError  # noqa: E402

from conftest import GOOD_STAMP, FakeClient  # noqa: E402


def frame(rows: int = 300, parts: int = 3):
    df = pd.DataFrame({"id": range(rows),
                       "part": [i * parts // rows for i in range(rows)]})
    return dd.from_pandas(df, npartitions=parts), df


@pytest.fixture()
def opts(manifest):
    """Storage options a worker could rebuild from — and, because fsspec
    caches instances by them, the driver's own filesystem is what
    ``fsspec.filesystem(**opts)`` hands back here (each test's FakeClient is
    a distinct object, so the tests do not share instances)."""
    _, store = manifest
    client = FakeClient(store)
    return {"client": client}, client


def test_dask_to_parquet_single_root(opts):
    so, client = opts
    ddf, df = frame()
    mark = len(client.uploads)

    res = sd.to_parquet(ddf, "bzz://new/sales", storage_options=so)

    assert len(res.root) == 64
    assert res.paths == ["part.0.parquet", "part.1.parquet", "part.2.parquet"]
    assert len(res) == 3 and all(len(r) == 64 for r in res.written.values())
    # every partition went up as a blob, and the commit added only manifest
    # nodes on top — no partition was uploaded twice
    sizes = [n for _, n in client.uploads[mark:]]
    for path, size in res.sizes.items():
        assert sizes.count(size) == 1, path

    # one manifest, one commit — that is the whole point
    fs = __import__("fsspec").filesystem("bzz", **so)
    assert fs.find(f"bzz://{res.root}/sales") == [
        f"{res.root}/sales/{p}" for p in res.paths]

    back = dd.read_parquet(f"bzz://{res.root}/sales",
                           storage_options=so).compute()
    pd.testing.assert_frame_equal(
        back.sort_values("id").reset_index(drop=True)[["id", "part"]],
        df[["id", "part"]])


def test_driver_commits_once_and_reports_batches(opts):
    so, client = opts
    ddf, _ = frame(rows=120, parts=4)
    res = sd.to_parquet(ddf, "bzz://new/ds", storage_options=so)

    import fsspec

    fs = fsspec.filesystem("bzz", **so)
    # the driver instance is the cached one: exactly one commit for 4 parts
    assert len(fs.commit_log) == 1
    assert set(fs.commit_log[0].written) == {f"ds/{p}" for p in res.paths}
    # and the (node, batch) pairs the parts were stamped with
    assert res.batches == {("fake://", GOOD_STAMP["batchID"])}


def test_stamp_is_validated_on_the_driver_before_workers_run(manifest):
    _, store = manifest
    client = FakeClient(store, stamps=[])  # no usable batch anywhere
    so = {"client": client}
    ddf, _ = frame()
    with pytest.raises(StampError, match="no postage stamps"):
        sd.to_parquet(ddf, "bzz://new/ds", storage_options=so)
    assert client.uploads == []  # nothing was spent, nowhere


def test_partition_on_writes_hive_paths(opts):
    so, client = opts
    ddf, _ = frame(rows=90, parts=3)
    res = sd.to_parquet(ddf, "bzz://new/hive", storage_options=so,
                        partition_on=["part"], write_index=False)

    assert all(p.startswith("part=") for p in res.paths), res.paths
    assert {p.split("/")[0] for p in res.paths} == {"part=0", "part=1", "part=2"}

    import fsspec

    fs = fsspec.filesystem("bzz", **so)
    one = res.paths[0]
    frame_back = pd.read_parquet(
        __import__("io").BytesIO(fs.cat_file(f"bzz://{res.root}/hive/{one}")))
    # the partitioning column lives in the path, not in the data (as in dask)
    assert list(frame_back.columns) == ["id"]


def test_signer_never_reaches_the_workers(manifest, monkeypatch):
    pytest.importorskip("eth_keys")
    from swarmfs.feeds import FeedSigner

    _, store = manifest
    key = bytes(range(1, 33)).hex()
    owner = FeedSigner(key).owner_hex
    so = {"client": FakeClient(store), "signer": key}

    seen: list[dict] = []
    real = sd._write_partition

    def spy(df, filename, protocol, storage_options, *a, **kw):
        seen.append(storage_options)
        return real(df, filename, protocol, storage_options, *a, **kw)

    monkeypatch.setattr(sd, "_write_partition", spy)
    ddf, _ = frame(rows=60, parts=2)
    res = sd.to_parquet(ddf, f"bzzf://{owner}/sales", storage_options=so)

    assert seen and all("signer" not in s for s in seen)
    assert all("client" in s for s in seen)  # everything else does travel
    assert len(res.root) == 64


def test_dask_to_parquet_bzzf_publishes_once(manifest):
    pytest.importorskip("eth_keys")
    from swarmfs.feeds import FeedSigner, topic_bytes

    _, store = manifest
    client = FakeClient(store)
    key = bytes(range(1, 33)).hex()
    owner = FeedSigner(key).owner_hex
    so = {"client": client, "signer": key}

    ddf, df = frame(rows=150, parts=3)
    res = sd.to_parquet(ddf, f"bzzf://{owner}/sales", storage_options=so)

    import asyncio

    head = asyncio.run(FakeClient(store).feed_head(
        owner, topic_bytes("sales").hex()))
    assert head is not None
    assert int.from_bytes(bytes.fromhex(head[0]), "big") == 0  # ONE update

    # the stable URL now serves the dataset — a reader that saw none of it
    reader_opts = {"client": FakeClient(store), "skip_instance_cache": True}
    back = dd.read_parquet(f"bzzf://{owner}/sales",
                           storage_options=reader_opts).compute()
    assert len(back) == len(df)
    reader = __import__("fsspec").filesystem("bzzf", **reader_opts)
    reader.ls(f"bzzf://{owner}/sales")  # resolves the feed on this instance
    assert reader.latest(f"bzzf://{owner}/sales") == res.root


def test_write_into_an_existing_manifest(opts, manifest):
    so, client = opts
    root, _ = manifest
    ddf, _ = frame(rows=60, parts=2)

    res = sd.to_parquet(ddf, f"bzz://{root}/new-dataset", storage_options=so)

    import fsspec

    fs = fsspec.filesystem("bzz", **so)
    # the dataset joined the existing manifest instead of replacing it
    assert fs.cat_file(f"bzz://{res.root}/index.html").startswith(b"<h1>")
    assert len(fs.find(f"bzz://{res.root}/new-dataset")) == 2


def test_refusals(opts):
    so, _ = opts
    ddf, _ = frame(rows=30, parts=1)
    with pytest.raises(ValueError, match="not a Swarm destination"):
        sd.to_parquet(ddf, "s3://bucket/ds", storage_options=so)
    with pytest.raises(NotImplementedError, match="write_metadata_file"):
        sd.to_parquet(ddf, "bzz://new/ds", storage_options=so,
                      write_metadata_file=True)


def test_local_first_workers_sync_before_linking(manifest, tmp_path, monkeypatch):
    """§5: a local-first worker's blob must be on the network *before* the
    driver links it — a manifest may not name content only one worker's
    disk holds."""
    pytest.importorskip("eth_hash")
    from conftest import BMTFakeClient
    from swarmfs.core import SwarmFileSystem

    store: dict = {}
    client = BMTFakeClient(store)
    so = {"client": client, "local_store": str(tmp_path / "store"),
          "redundancy": 0}

    order: list[str] = []
    real_sync, real_link = SwarmFileSystem.sync, SwarmFileSystem.link

    def spy_sync(self, *a, **kw):
        order.append("sync")
        return real_sync(self, *a, **kw)

    def spy_link(self, *a, **kw):
        order.append("link")
        return real_link(self, *a, **kw)

    monkeypatch.setattr(SwarmFileSystem, "sync", spy_sync)
    monkeypatch.setattr(SwarmFileSystem, "link", spy_link)

    ddf, df = frame(rows=60, parts=2)
    res = sd.to_parquet(ddf, "bzz://new/offline", storage_options=so)

    assert "sync" in order and "link" in order
    assert order.index("sync") < order.index("link")
    # …and it really is out there: every partition reached the fake node
    for reference in res.written.values():
        assert bytes.fromhex(reference) in store
    # the batch reported is the one the *push* spent, not a commit-time one
    assert res.batches == {("fake://", GOOD_STAMP["batchID"])}

    back = dd.read_parquet(f"bzz://{res.root}/offline",
                           storage_options=so).compute()
    assert len(back) == len(df)


def test_indexed_dataset_lists_in_one_fetch(opts):
    """The named consumer for the root index (§6): a partitioned dataset big
    enough that walking the trie hurts."""
    so, client = opts
    ddf, _ = frame(rows=200, parts=8)
    res = sd.to_parquet(ddf, "bzz://new/sales",
                        storage_options={**so, "index": True})

    import fsspec

    from swarmfs.commit import parse_index

    reader = fsspec.filesystem("bzz", client=FakeClient(client.store),
                               skip_instance_cache=True)
    assert len(reader.find(f"bzz://{res.root}/sales")) == 8
    entries = parse_index(reader.cat_file(f"bzz://{res.root}/.swarmfs/index.json"))
    assert set(entries) == {f"sales/{p}" for p in res.paths}
    assert all(e["s"] > 0 for e in entries.values())  # sizes, for free
