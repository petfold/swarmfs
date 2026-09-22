"""The optional root index (docs/distributed-writes.md §6 step 2).

`index=True` makes each commit maintain `.swarmfs/index.json`: every entry's
path, data reference, size and metadata in one file, so a reader answers
ls/find/info from a single fetch instead of one round trip per trie node.
The tests that matter are the ones that would catch it *lying* — an index
that disagrees with the trie is worse than no index at all.
"""

from __future__ import annotations

import json

import pytest

from swarmfs import SwarmFileSystem
from swarmfs.commit import INDEX_PATH, parse_index

from conftest import FILES, FakeClient


class CountingClient(FakeClient):
    """Counts reads, so a test can show the index is doing the work."""

    def __init__(self, store):
        super().__init__(store)
        self.gets = 0
        self.sizes = 0

    async def bytes_get(self, ref, start=None, end=None, act=None):
        self.gets += 1
        return await super().bytes_get(ref, start, end, act)

    async def bytes_size(self, ref, act=None):
        self.sizes += 1
        return await super().bytes_size(ref, act)


def fs_for(store, **kw):
    return SwarmFileSystem(client=CountingClient(store), skip_instance_cache=True, **kw)


def content(i: int) -> bytes:
    """Deliberately not all the same length — a size test that passes on
    equal-sized files proves nothing."""
    return f"row {i}".encode() * (3 + i % 4)


def dataset(fs, url, n=50, prefix="sales"):
    with fs.transaction:
        for i in range(n):
            fs.pipe_file(f"{url}/{prefix}/part.{i:04d}.parquet", content(i))
    return fs.latest(url)


def read_index(fs, root):
    return parse_index(fs.cat_file(f"bzz://{root}/{INDEX_PATH}"))


# -- writing ----------------------------------------------------------------


def test_index_written_on_commit(manifest):
    _, store = manifest
    fs = fs_for(store, index=True)
    with fs.transaction:
        fs.pipe_file("bzz://new/a.txt", b"alpha")
        fs.pipe_file("bzz://new/dir/b.bin", b"betabeta")
    root = fs.latest("new")

    entries = read_index(fs, root)
    assert set(entries) == {"a.txt", "dir/b.bin"}
    assert entries["a.txt"]["s"] == 5 and entries["dir/b.bin"]["s"] == 8
    assert entries["a.txt"]["m"]["Filename"] == "a.txt"
    # the references in the index are the real ones
    assert fs.cat_file(f"bzz://{root}/dir/b.bin") == b"betabeta"
    assert entries["dir/b.bin"]["r"] == fs.info(f"bzz://{root}/dir/b.bin")["reference"]
    assert INDEX_PATH not in entries  # an index never lists itself


def test_index_is_off_by_default(manifest):
    _, store = manifest
    fs = fs_for(store)
    fs.pipe_file("bzz://new/a.txt", b"alpha")
    with pytest.raises(FileNotFoundError):
        fs.cat_file(f"bzz://{fs.latest('new')}/{INDEX_PATH}")


def test_index_is_maintained_incrementally(manifest):
    _, store = manifest
    fs = fs_for(store, index=True)
    root = dataset(fs, "bzz://new", n=30)

    before = fs.client.gets
    with fs.transaction:
        fs.pipe_file(f"bzz://{root}/sales/part.0030.parquet", b"new row")
        fs.rm_file(f"bzz://{root}/sales/part.0000.parquet")
    head = fs.latest(root)
    cost = fs.client.gets - before

    entries = read_index(fs, head)
    assert "sales/part.0030.parquet" in entries
    assert "sales/part.0000.parquet" not in entries
    assert len(entries) == 30
    # the previous index was read (one fetch) and edited, not re-derived by
    # walking the trie — which alone would have cost one fetch per node, and
    # a 30-file dataset has ~35 of them
    assert cost < 15, cost


def test_index_adopts_an_existing_manifest(manifest):
    """Turning it on later costs one full walk, once — and then covers the
    files that were already there."""
    root, store = manifest
    plain = fs_for(store)
    plain.pipe_file(f"bzz://{root}/late.txt", b"added without an index")
    head = plain.latest(root)

    indexed = fs_for(store, index=True)
    indexed.pipe_file(f"bzz://{head}/first-indexed.txt", b"now with an index")
    entries = read_index(indexed, indexed.latest(head))

    assert "first-indexed.txt" in entries
    assert "late.txt" in entries
    for path in FILES:  # everything the original manifest held
        assert path in entries


def test_commit_without_index_drops_a_stale_one(manifest):
    """The trap this feature would otherwise set: an index left behind by a
    writer that stopped maintaining it would answer listings with content
    that is no longer there."""
    _, store = manifest
    indexed = fs_for(store, index=True)
    root = dataset(indexed, "bzz://new", n=5)
    assert read_index(indexed, root)

    plain = fs_for(store)
    plain.pipe_file(f"bzz://{root}/sales/part.0005.parquet", b"written without one")
    head = plain.latest(root)

    with pytest.raises(FileNotFoundError):
        plain.cat_file(f"bzz://{head}/{INDEX_PATH}")
    # …and a fresh reader now sees all six files, from the trie
    reader = fs_for(store)
    assert len(reader.find(f"bzz://{head}/sales")) == 6


# -- reading ----------------------------------------------------------------


def test_index_and_trie_agree(manifest):
    """The load-bearing test: every listing answered from the index must
    match the one the trie gives, on a tree with the awkward shapes
    (shared prefixes, nesting, mid-edge splits)."""
    _, store = manifest
    indexed = fs_for(store, index=True)
    with indexed.transaction:
        for path, data in FILES.items():
            indexed.pipe_file(f"bzz://new/{path}", data)
    root = indexed.latest("new")

    # the same content, committed without an index: the trie is the oracle
    plain = fs_for(store)
    with plain.transaction:
        for path, data in FILES.items():
            plain.pipe_file(f"bzz://new/{path}", data)
    plain_root = plain.latest("new")

    a, b = fs_for(store), fs_for(store)
    assert sorted(p[len(root):] for p in a.find(f"bzz://{root}")) == sorted(
        p[len(plain_root):] for p in b.find(f"bzz://{plain_root}"))
    for path in FILES:
        ai, bi = a.info(f"bzz://{root}/{path}"), b.info(f"bzz://{plain_root}/{path}")
        assert ai["reference"] == bi["reference"]
        assert ai["size"] == bi["size"] == len(FILES[path])
        assert ai["metadata"] == bi["metadata"]
        assert a.cat_file(f"bzz://{root}/{path}") == FILES[path]
    for d in ("assets", "assets/css", "data", "a/very/deeply"):
        assert sorted(x[len(root):] for x in a.ls(f"bzz://{root}/{d}", detail=False)) == \
            sorted(x[len(plain_root):] for x in b.ls(f"bzz://{plain_root}/{d}", detail=False))
    assert a.isdir(f"bzz://{root}/data") and not a.isdir(f"bzz://{root}/index.html")
    assert not a.exists(f"bzz://{root}/nope.txt")


def test_listing_costs_one_fetch_not_one_per_node(manifest):
    _, store = manifest
    writer = fs_for(store, index=True)
    root = dataset(writer, "bzz://new", n=200)

    indexed = fs_for(store)
    found = indexed.find(f"bzz://{root}/sales")
    assert len(found) == 200
    # root node + the trie hops to .swarmfs/index.json + the index itself
    assert indexed.client.gets <= 6, indexed.client.gets
    assert indexed.client.sizes == 0  # sizes came from the index, no HEADs

    # the same listing without an index: one round trip per trie node
    plain_writer = fs_for(store)
    plain_root = dataset(plain_writer, "bzz://new", n=200)
    plain = fs_for(store)
    assert len(plain.find(f"bzz://{plain_root}/sales")) == 200
    assert plain.client.gets > 200, plain.client.gets


def test_detailed_listing_sizes_come_from_the_index(manifest):
    _, store = manifest
    writer = fs_for(store, index=True)
    root = dataset(writer, "bzz://new", n=20)

    reader = fs_for(store)
    entries = {e["name"].rsplit("/", 1)[1]: e
               for e in reader.ls(f"bzz://{root}/sales", detail=True)}
    assert len(entries) == 20
    for i in range(20):
        assert entries[f"part.{i:04d}.parquet"]["size"] == len(content(i))
    assert reader.client.sizes == 0  # would be 20 HEAD requests without it


def test_reserved_directory_is_hidden_but_readable(manifest):
    _, store = manifest
    fs = fs_for(store, index=True)
    root = dataset(fs, "bzz://new", n=3)

    reader = fs_for(store)
    assert all(".swarmfs" not in p for p in reader.find(f"bzz://{root}"))
    assert all(".swarmfs" not in p for p in reader.ls(f"bzz://{root}", detail=False))
    # …but it is a real file, and reading it is how you debug an index
    doc = json.loads(reader.cat_file(f"bzz://{root}/{INDEX_PATH}"))
    assert doc["swarmfs-index"] == 1 and len(doc["entries"]) == 3
    assert reader.info(f"bzz://{root}/{INDEX_PATH}")["type"] == "file"


def test_an_index_we_do_not_understand_is_ignored(manifest):
    """Forward compatibility: a future version number means 'walk the trie',
    not 'crash'."""
    _, store = manifest
    fs = fs_for(store, index=True)
    root = dataset(fs, "bzz://new", n=4)

    future = json.dumps({"swarmfs-index": 99, "entries": {}}).encode()
    plain = fs_for(store)
    plain.pipe_file(f"bzz://{root}/{INDEX_PATH}", future)
    head = plain.latest(root)

    reader = fs_for(store)
    assert len(reader.find(f"bzz://{head}/sales")) == 4  # from the trie
    assert parse_index(future) is None


def test_index_survives_the_feed_and_the_dask_helper(manifest):
    pytest.importorskip("eth_keys")
    from swarmfs.feedfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedSigner

    _, store = manifest
    key = bytes(range(1, 33)).hex()
    owner = FeedSigner(key).owner_hex
    fs = SwarmFeedFileSystem(client=CountingClient(store), signer=key,
                             index=True, feed_ttl=0, skip_instance_cache=True)
    with fs.transaction:
        for i in range(5):
            fs.pipe_file(f"bzzf://{owner}/ds/part.{i}.bin", f"p{i}".encode())

    reader = SwarmFeedFileSystem(client=CountingClient(store), feed_ttl=0,
                                 skip_instance_cache=True)
    assert len(reader.find(f"bzzf://{owner}/ds")) == 5
    assert all(".swarmfs" not in p for p in reader.find(f"bzzf://{owner}/ds"))
