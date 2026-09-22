"""Walking persisted Mantaray tries by fetching nodes on demand.

Everything here is transport-agnostic: a ``load`` coroutine (reference ->
node bytes, i.e. GET ``/bytes/{ref}``) is the only I/O dependency, which keeps
the codec testable offline and lets the filesystem layer own HTTP concerns.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Iterable

from .node import Fork, Node, unmarshal

Loader = Callable[[bytes], Awaitable[bytes]]

# Measured 2026-09-22 against real nodes, on a real 2,000-file dataset
# (2,224 node fetches):
#   local Bee 2.8.2   concurrency 1 → 2.6 s, 4 → 1.7 s, 8 → 1.6 s, 16 → 1.7 s
#   public gateway    (100-file slice, 114 fetches at 158 ms each)
#                     concurrency 1 → 18.0 s, 16 → 3.1 s — 5.9x
# Against your own node the ceiling is this process's own CPU (~1.2 ms per
# fetch of HTTP plus unmarshal), not waiting, so 4-8 already saturates and
# the gain is a modest 1.7x. Against anything remote the waiting dominates
# and concurrency is the whole game. 16 is the middle: free locally, worth
# a lot over a gateway. A second ceiling is structural — prefetching warms
# one node's children at a time, so no more than the trie's fan-out is ever
# in flight (a flat part.00000… dataset branches by decimal digit: ten).
DEFAULT_CONCURRENCY = 16


class NodeStore:
    """Fetch-and-parse cache for manifest nodes, keyed by reference.

    References are content-addressed, so cached nodes never go stale and the
    cache can safely be shared across manifests.

    Fetches run concurrently, up to ``concurrency`` at a time: a walk is a
    long chain of small ``/bytes`` requests, so its cost is round trips, not
    bytes (measured on a 2,000-file dataset: 2,224 fetches — one per trie
    node). ``prefetch`` starts a node's children before the walk reaches
    them, and ``get`` joins a fetch already in flight instead of issuing a
    second one — so the traversal below stays a plain, ordered depth-first
    walk while the waiting overlaps. The number of fetches is unchanged;
    only the time spent waiting for them is.
    """

    def __init__(self, load: Loader, cache_size: int = 4096,
                 concurrency: int = DEFAULT_CONCURRENCY):
        self._load = load
        self._cache: OrderedDict[bytes, Node] = OrderedDict()
        self._cache_size = cache_size
        self._concurrency = concurrency
        self._sem: asyncio.Semaphore | None = None  # bound to the running loop
        self._inflight: dict[bytes, asyncio.Future] = {}

    async def get(self, ref: bytes) -> Node:
        key = bytes(ref)
        node = self._cache.get(key)
        if node is not None:
            self._cache.move_to_end(key)
            return node
        task = self._inflight.get(key) or self._start(key)
        # shield: several walkers may wait on one fetch, and one of them
        # giving up must not cancel it for the others
        return await asyncio.shield(task)

    async def resolve(self, fork: Fork) -> Node:
        if fork.node is None:
            fork.node = await self.get(fork.ref)
        return fork.node

    def prefetch(self, refs: Iterable[bytes]) -> None:
        """Start fetching these references now (bounded, deduplicated).

        Fire-and-forget: the walk awaits them through ``get``/``resolve``
        when it gets there. Cheap to over-call — anything cached or already
        in flight is skipped.
        """
        for ref in refs:
            key = bytes(ref)
            if key not in self._cache and key not in self._inflight:
                self._start(key).add_done_callback(_retrieved)

    def _start(self, key: bytes) -> asyncio.Future:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self._concurrency)

        async def fetch() -> Node:
            try:
                async with self._sem:
                    node = unmarshal(await self._load(key))
                self._cache[key] = node
                while len(self._cache) > self._cache_size:
                    self._cache.popitem(last=False)
                return node
            finally:
                self._inflight.pop(key, None)

        task = asyncio.ensure_future(fetch())
        self._inflight[key] = task
        return task

    def warm(self, node: Node) -> list[Fork]:
        """A node's forks in canonical order, with their children already
        being fetched — the one line that turns a sequential descent into a
        parallel one."""
        forks = [node.forks[b] for b in sorted(node.forks)]
        self.prefetch(f.ref for f in forks if f.node is None)
        return forks


def _retrieved(task: asyncio.Future) -> None:
    """Consume a prefetch's exception so a fetch nobody waited for does not
    surface as 'Task exception was never retrieved'. A later ``get`` on the
    same reference still fails loudly: it re-fetches, and that one raises."""
    if not task.cancelled():
        task.exception()


@dataclass
class FileEntry:
    """A value entry found in a trie: ``path`` is relative to the walk root.

    ``size`` is None from a trie walk — a manifest entry records a reference,
    not a length, so the size costs a request. A root index records it, which
    is how an indexed listing answers without one per file.
    """

    path: bytes
    reference: bytes
    metadata: dict[str, str] | None
    size: int | None = None


@dataclass
class Location:
    """Where a lookup path landed in the trie.

    Exactly one of ``node``/``fork`` is set. ``leftover`` is the unconsumed
    tail of the fork's prefix when the path ended in the middle of an edge
    (e.g. looking up ``data/`` when the only entry is ``data/part1.parquet``).
    """

    node: Node | None = None
    fork: Fork | None = None
    leftover: bytes = b""


async def locate(store: NodeStore, root: Node | bytes, needle: bytes) -> Location | None:
    node = root if isinstance(root, Node) else await store.get(root)
    while True:
        if not needle:
            return Location(node=node)
        f = node.forks.get(needle[0])
        if f is None:
            return None
        if needle.startswith(f.prefix):
            needle = needle[len(f.prefix) :]
            if not needle:
                return Location(fork=f)
            if not f.is_edge:
                return None
            node = await store.resolve(f)
        elif f.prefix.startswith(needle):
            return Location(fork=f, leftover=f.prefix[len(needle) :])
        else:
            return None


async def _iter_fork(store: NodeStore, fork: Fork, acc: bytes) -> AsyncIterator[FileEntry]:
    if fork.is_value:
        child = await store.resolve(fork)
        if child.has_entry:
            yield FileEntry(path=acc, reference=child.entry, metadata=fork.metadata)
    if fork.is_edge:
        child = await store.resolve(fork)
        for f in store.warm(child):
            async for e in _iter_fork(store, f, acc + f.prefix):
                yield e


async def iter_files(
    store: NodeStore, root: Node | bytes, prefix: bytes = b""
) -> AsyncIterator[FileEntry]:
    """Yield every file entry under ``prefix``, paths relative to it.

    Entries come out in canonical (sorted) path order, one at a time, while
    the fetches behind them overlap: each node's children are started
    together (``NodeStore.warm``) and awaited as the walk reaches them.

    Note: cost is O(trie nodes) round trips to ``/bytes`` — Bee has no
    server-side listing endpoint yet (ethersphere/bee#5535). Concurrency
    hides the latency; it does not reduce the count.
    """
    loc = await locate(store, root, prefix)
    if loc is None:
        return
    if loc.node is not None:
        if loc.node.has_entry:
            yield FileEntry(path=b"", reference=loc.node.entry, metadata=loc.node.metadata)
        for f in store.warm(loc.node):
            async for e in _iter_fork(store, f, f.prefix):
                yield e
    else:
        async for e in _iter_fork(store, loc.fork, loc.leftover):
            yield e


async def list_directory(
    store: NodeStore, root: Node | bytes, dirpath: bytes
) -> tuple[list[FileEntry], list[bytes]] | None:
    """Immediate children of a directory: (files, subdirectory names).

    Returns None when ``dirpath`` does not exist or is not a directory.
    Descent is pruned at the first path separator, so this touches only the
    nodes along the directory's own level, not the whole subtree.
    """
    needle = dirpath + b"/" if dirpath else b""
    loc = await locate(store, root, needle)
    if loc is None:
        return None

    files: list[FileEntry] = []
    dirs: set[bytes] = set()

    async def process(fork: Fork, acc: bytes) -> None:
        i = acc.find(b"/")
        if i == 0:
            # bee stores root-level metadata (index document etc.) under "/";
            # an empty child name is never a real entry
            return
        if i > 0:
            dirs.add(acc[:i])
            return
        if fork.is_value:
            child = await store.resolve(fork)
            if child.has_entry:
                files.append(FileEntry(path=acc, reference=child.entry, metadata=fork.metadata))
        if fork.is_edge:
            child = await store.resolve(fork)
            for f in _warm_level(store, child, acc):
                await process(f, acc + f.prefix)

    if loc.node is not None:
        for f in _warm_level(store, loc.node, b""):
            await process(f, f.prefix)
    elif loc.leftover:
        await process(loc.fork, loc.leftover)
    else:
        # needle ended exactly on a fork boundary: "dir/" resolved to a node
        if not loc.fork.is_edge:
            return None
        child = await store.resolve(loc.fork)
        for f in _warm_level(store, child, b""):
            await process(f, f.prefix)

    return files, sorted(dirs)


def _warm_level(store: NodeStore, node: Node, acc: bytes) -> list[Fork]:
    """Like ``NodeStore.warm``, but only for the forks a *directory listing*
    will actually resolve.

    ``list_directory`` prunes at the first separator: a fork whose path
    already names a subdirectory contributes the name and nothing else, and
    its node is never read. Prefetching those would undo the pruning — the
    listing of a directory with many subdirectories would fetch every one of
    them — so the prefetch follows the same rule as the descent.
    """
    forks = [node.forks[b] for b in sorted(node.forks)]
    store.prefetch(f.ref for f in forks
                   if f.node is None and (acc + f.prefix).find(b"/") < 0)
    return forks
