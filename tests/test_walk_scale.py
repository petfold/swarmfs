"""The trie walk at dataset scale: concurrency, pruning, and a benchmark.

Bee has no server-side listing endpoint (ethersphere/bee#5535), so listing a
manifest costs one ``/bytes`` round trip per trie node — 2,224 of them for a
2,000-file dataset. That count is irreducible here; what the walk can do is
stop paying for them one at a time. These tests pin both halves of that:
the count and the order must not change, and the wall time must.
"""

from __future__ import annotations

import asyncio
import hashlib
import time

import pytest

from swarmfs.mantaray import Node, NodeStore, add, iter_files, list_directory, save


def build(paths: list[str]) -> tuple[bytes, dict[bytes, bytes]]:
    store: dict[bytes, bytes] = {}

    def put(data: bytes) -> bytes:
        ref = hashlib.sha256(data).digest()
        store[ref] = data
        return ref

    async def saver(data: bytes) -> bytes:
        return put(data)

    async def go() -> bytes:
        root = Node()
        for path in paths:
            await add(root, path.encode(), put(b"data:" + path.encode()), None)
        return await save(root, saver)

    return asyncio.run(go()), store


def counting_store(store, concurrency, latency=0.0):
    """A NodeStore that records every fetch and the peak number in flight."""
    stats = {"fetches": 0, "in_flight": 0, "peak": 0, "refs": []}

    async def load(ref: bytes) -> bytes:
        stats["fetches"] += 1
        stats["refs"].append(bytes(ref))
        stats["in_flight"] += 1
        stats["peak"] = max(stats["peak"], stats["in_flight"])
        if latency:
            await asyncio.sleep(latency)
        stats["in_flight"] -= 1
        return store[ref]

    return NodeStore(load, concurrency=concurrency), stats


def test_concurrency_changes_neither_the_count_nor_the_order():
    """Prefetching must be invisible in the result: same entries, same order,
    same number of round trips — only overlapped in time."""
    root, store = build([f"dataset/part.{i:04d}.parquet" for i in range(300)])

    runs = {}
    for concurrency in (1, 16):
        # a hair of latency, so a fetch actually suspends: without one the
        # tasks run to completion the instant they are scheduled and nothing
        # ever overlaps — which would make the peak below meaningless
        node_store, stats = counting_store(store, concurrency, latency=0.001)

        async def walk():
            return [e.path async for e in iter_files(node_store, root, b"")]

        runs[concurrency] = (asyncio.run(walk()), stats)

    (seq_paths, seq_stats), (par_paths, par_stats) = runs[1], runs[16]
    assert seq_paths == par_paths  # canonical sorted order, preserved
    assert seq_paths == sorted(seq_paths)
    assert len(seq_paths) == 300
    assert par_stats["fetches"] == seq_stats["fetches"]  # no extra fetches
    assert seq_stats["peak"] == 1  # …and the sequential one really was
    assert par_stats["peak"] > 1  # …while the parallel one overlapped


def test_listing_a_directory_does_not_fetch_subdirectory_nodes():
    """The pruning `list_directory` depends on must survive prefetching: a
    directory listing names its subdirectories, it does not descend into
    them, so their nodes must never be fetched."""
    paths = [f"d{i:02d}/f{j:02d}.bin" for i in range(20) for j in range(5)]
    root, store = build(paths)
    node_store, stats = counting_store(store, concurrency=16)

    files, dirs = asyncio.run(list_directory(node_store, root, b""))

    assert files == []  # the root holds only directories
    assert dirs == [f"d{i:02d}".encode() for i in range(20)]
    # the root node plus the handful of trie nodes along its own level —
    # nowhere near the 20 subdirectory nodes, let alone the 100 files
    assert stats["fetches"] <= 10, stats["fetches"]


def test_a_shared_fetch_is_issued_once():
    """Two walkers on one store share a fetch in flight instead of doubling
    it — the cache is keyed by reference, so this is always safe."""
    root, store = build([f"dir/f{i:03d}.bin" for i in range(50)])
    node_store, stats = counting_store(store, concurrency=16, latency=0.002)

    async def both():
        async def walk():
            return [e.path async for e in iter_files(node_store, root, b"")]

        return await asyncio.gather(walk(), walk())

    first, second = asyncio.run(both())
    assert first == second and len(first) == 50
    assert len(stats["refs"]) == len(set(stats["refs"]))  # no reference twice


@pytest.mark.bench
def test_bench_find_2000_files():
    """The regression signal for §6: walking a 2,000-file dataset must stay
    several times faster than fetching its nodes one at a time.

    Run with `pytest -m bench` (deselected by default — it sleeps on purpose).
    The 1 ms is *pure waiting*, which models a remote endpoint; against your
    own node most of the per-fetch cost is this process's CPU instead, so
    the real local speedup is smaller (measured: 1.7x on a local Bee, 5.9x
    through a public gateway). The point of the number below is to catch a
    regression in the overlap, not to predict anyone's wall time.
    """
    paths = [f"dataset/part.{i:05d}.parquet" for i in range(2000)]
    root, store = build(paths)

    def run(concurrency):
        node_store, stats = counting_store(store, concurrency, latency=0.001)

        async def walk():
            return [e.path async for e in iter_files(node_store, root, b"")]

        start = time.perf_counter()
        entries = asyncio.run(walk())
        return len(entries), stats, time.perf_counter() - start

    n_seq, seq, t_seq = run(1)
    n_par, par, t_par = run(16)

    assert n_seq == n_par == 2000
    assert par["fetches"] == seq["fetches"] == 2224  # the irreducible cost
    print(f"\n2000 files, {seq['fetches']} round trips @1ms: "
          f"sequential {t_seq:.2f}s, concurrent {t_par:.2f}s "
          f"({t_seq / t_par:.1f}x, peak in flight {par['peak']})")
    assert t_par * 3 < t_seq, f"only {t_seq / t_par:.1f}x faster"
