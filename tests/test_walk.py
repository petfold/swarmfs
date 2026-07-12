"""Walker tests against tries built by the builder and served from a dict —
plus a randomized build→save→walk round-trip, which is the property-style
check that the three codec pieces agree with each other."""

from __future__ import annotations

import asyncio
import hashlib
import random

from swarmfs.mantaray import Node, NodeStore, add, iter_files, list_directory, locate, save

from conftest import FILES, METADATA, build_manifest


def make_store(store: dict[bytes, bytes]) -> NodeStore:
    async def load(ref: bytes) -> bytes:
        return store[ref]

    return NodeStore(load)


def run(coro):
    return asyncio.run(coro)


async def collect(aiter):
    return [e async for e in aiter]


def test_iter_files_full_walk(manifest):
    root_hex, store = manifest
    entries = run(collect(iter_files(make_store(store), bytes.fromhex(root_hex))))
    assert sorted(e.path.decode() for e in entries) == sorted(FILES)
    by_path = {e.path.decode(): e for e in entries}
    # metadata rides on the fork records
    assert by_path["index.html"].metadata == METADATA["index.html"]
    assert by_path["assets/css/site.css"].metadata is None
    # the "/" root-metadata marker has a zero entry and must not surface
    assert "/" not in by_path and "" not in by_path


def test_iter_files_under_prefix(manifest):
    root_hex, store = manifest
    entries = run(collect(iter_files(make_store(store), bytes.fromhex(root_hex), b"data/")))
    assert sorted(e.path.decode() for e in entries) == [
        "part-00000.parquet",
        "part-00001.parquet",
    ]


def test_iter_files_missing_prefix(manifest):
    root_hex, store = manifest
    entries = run(collect(iter_files(make_store(store), bytes.fromhex(root_hex), b"nope/")))
    assert entries == []


def test_list_directory_root(manifest):
    root_hex, store = manifest
    files, dirs = run(list_directory(make_store(store), bytes.fromhex(root_hex), b""))
    assert [f.path.decode() for f in files] == ["index.html"]
    assert dirs == [b"a", b"assets", b"data", b"data-old"]


def test_list_directory_prunes_at_separator(manifest):
    """data vs data-old share the edge "data"; listing must split them."""
    root_hex, store = manifest
    ns = make_store(store)
    root = bytes.fromhex(root_hex)

    files, dirs = run(list_directory(ns, root, b"data"))
    assert sorted(f.path.decode() for f in files) == [
        "part-00000.parquet",
        "part-00001.parquet",
    ]
    assert dirs == []

    files, dirs = run(list_directory(ns, root, b"data-old"))
    assert [f.path.decode() for f in files] == ["readme.md"]

    files, dirs = run(list_directory(ns, root, b"assets"))
    assert files == []
    assert dirs == [b"css", b"img"]


def test_list_directory_not_a_directory(manifest):
    root_hex, store = manifest
    ns = make_store(store)
    root = bytes.fromhex(root_hex)
    assert run(list_directory(ns, root, b"missing")) is None
    assert run(list_directory(ns, root, b"index.html")) is None


def test_locate_exact_file(manifest):
    root_hex, store = manifest
    ns = make_store(store)
    loc = run(locate(ns, bytes.fromhex(root_hex), b"data/part-00000.parquet"))
    assert loc is not None and loc.fork is not None and not loc.leftover
    assert loc.fork.is_value


def test_locate_mid_edge_is_leftover(manifest):
    root_hex, store = manifest
    ns = make_store(store)
    loc = run(locate(ns, bytes.fromhex(root_hex), b"data/part"))
    assert loc is not None and loc.leftover  # ended inside the edge
    assert run(locate(ns, bytes.fromhex(root_hex), b"data/nope")) is None


def test_random_tree_roundtrip():
    """Build random path sets, persist, walk back, compare exactly."""
    rng = random.Random(20260712)
    segments = ["a", "bb", "ccc", "data", "data-old", "part-0", "x" * 40, "f.bin"]
    for _ in range(20):
        n = rng.randrange(1, 30)
        paths = set()
        while len(paths) < n:
            depth = rng.randrange(1, 5)
            paths.add("/".join(rng.choice(segments) for _ in range(depth)))
        # a path can't be both a file and a directory prefix in this generator's
        # accounting; keep only maximal paths to keep expectations exact
        paths = {p for p in paths if not any(q != p and q.startswith(p + "/") for q in paths)}

        expected: dict[str, bytes] = {}
        store: dict[bytes, bytes] = {}

        async def saver(data: bytes) -> bytes:
            ref = hashlib.sha256(data).digest()
            store[ref] = data
            return ref

        async def build_and_walk():
            root = Node()
            for p in sorted(paths):
                entry = hashlib.sha256(p.encode()).digest()
                expected[p] = entry
                add(root, p.encode(), entry)
            root_ref = await save(root, saver)
            ns = make_store(store)
            return {
                e.path.decode(): e.reference async for e in iter_files(ns, root_ref)
            }

        assert run(build_and_walk()) == expected
