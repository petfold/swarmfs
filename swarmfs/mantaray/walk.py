"""Walking persisted Mantaray tries by fetching nodes on demand.

Everything here is transport-agnostic: a ``load`` coroutine (reference ->
node bytes, i.e. GET ``/bytes/{ref}``) is the only I/O dependency, which keeps
the codec testable offline and lets the filesystem layer own HTTP concerns.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable

from .node import Fork, Node, unmarshal

Loader = Callable[[bytes], Awaitable[bytes]]


class NodeStore:
    """Fetch-and-parse cache for manifest nodes, keyed by reference.

    References are content-addressed, so cached nodes never go stale and the
    cache can safely be shared across manifests.
    """

    def __init__(self, load: Loader, cache_size: int = 4096):
        self._load = load
        self._cache: OrderedDict[bytes, Node] = OrderedDict()
        self._cache_size = cache_size

    async def get(self, ref: bytes) -> Node:
        key = bytes(ref)
        node = self._cache.get(key)
        if node is not None:
            self._cache.move_to_end(key)
            return node
        node = unmarshal(await self._load(key))
        self._cache[key] = node
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return node

    async def resolve(self, fork: Fork) -> Node:
        if fork.node is None:
            fork.node = await self.get(fork.ref)
        return fork.node


@dataclass
class FileEntry:
    """A value entry found in a trie: ``path`` is relative to the walk root."""

    path: bytes
    reference: bytes
    metadata: dict[str, str] | None


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
        for b in sorted(child.forks):
            f = child.forks[b]
            async for e in _iter_fork(store, f, acc + f.prefix):
                yield e


async def iter_files(
    store: NodeStore, root: Node | bytes, prefix: bytes = b""
) -> AsyncIterator[FileEntry]:
    """Yield every file entry under ``prefix``, paths relative to it.

    Note: cost is O(trie nodes) round trips to ``/bytes`` — Bee has no
    server-side listing endpoint yet (ethersphere/bee#5535).
    """
    loc = await locate(store, root, prefix)
    if loc is None:
        return
    if loc.node is not None:
        if loc.node.has_entry:
            yield FileEntry(path=b"", reference=loc.node.entry, metadata=loc.node.metadata)
        for b in sorted(loc.node.forks):
            f = loc.node.forks[b]
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
            for b in sorted(child.forks):
                f = child.forks[b]
                await process(f, acc + f.prefix)

    if loc.node is not None:
        for b in sorted(loc.node.forks):
            f = loc.node.forks[b]
            await process(f, f.prefix)
    elif loc.leftover:
        await process(loc.fork, loc.leftover)
    else:
        # needle ended exactly on a fork boundary: "dir/" resolved to a node
        if not loc.fork.is_edge:
            return None
        child = await store.resolve(loc.fork)
        for b in sorted(child.forks):
            f = child.forks[b]
            await process(f, f.prefix)

    return files, sorted(dirs)
