"""Listing/lookup backends behind a capability-detection seam.

Bee has no server-side manifest listing endpoint today, so the only real
implementation walks the Mantaray trie client-side over ``/bytes``. When
ethersphere/bee#5535 ships, add a ``ServerSideListingBackend`` here and teach
``detect_listing_backend`` to probe for it (Bee version via ``/health``, or a
one-shot request with the result cached per filesystem instance). Nothing
above this module should need to change.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AsyncIterator

from .commit import INDEX_PATH, RESERVED_DIR, parse_index
from .mantaray import (DEFAULT_CONCURRENCY, FileEntry, NodeStore, iter_files,
                       list_directory, locate)


def _b(path: str) -> bytes:
    return path.encode("utf-8", "surrogateescape")


def _s(path: bytes) -> str:
    return path.decode("utf-8", "surrogateescape")


@dataclass
class Stat:
    kind: str  # "file" | "directory"
    reference: bytes | None = None  # the file's data reference (entry)
    metadata: dict[str, str] | None = None
    size: int | None = None  # known only from a root index (see below)


class ListingBackend(ABC):
    @abstractmethod
    async def stat(self, root: str, path: str) -> Stat | None:
        """Resolve a path inside a manifest; None if it does not exist."""

    @abstractmethod
    async def list_dir(
        self, root: str, path: str
    ) -> tuple[list[FileEntry], list[str]] | None:
        """Immediate children (files, dir names); None if not a directory."""

    @abstractmethod
    def iter_files(self, root: str, prefix: str) -> AsyncIterator[FileEntry]:
        """All file entries under a prefix, paths relative to it."""


class MantarayListingBackend(ListingBackend):
    """Client-side Mantaray trie traversal via GET /bytes (works against
    today's Bee and public gateways).

    ``reader`` is the raw client or a VerifyingReader — with the latter,
    manifest nodes are chunk-verified too, so listings are trustless."""

    def __init__(self, reader, cache_size: int = 4096,
                 concurrency: int = DEFAULT_CONCURRENCY):
        # keyed by reference (content-addressed), safe to share across roots;
        # `concurrency` bounds how many node fetches are in flight at once
        self.store = NodeStore(load=lambda ref: reader.bytes_get(ref.hex()),
                               cache_size=cache_size, concurrency=concurrency)

    async def stat(self, root: str, path: str) -> Stat | None:
        root_ref = bytes.fromhex(root)
        if not path:
            await self.store.get(root_ref)  # validates the ref parses as a manifest
            return Stat(kind="directory")
        loc = await locate(self.store, root_ref, _b(path))
        if loc is None:
            return None
        if loc.fork is not None and not loc.leftover:
            if loc.fork.is_value:
                child = await self.store.resolve(loc.fork)
                if child.has_entry:
                    return Stat(kind="file", reference=child.entry, metadata=loc.fork.metadata)
            if loc.fork.is_edge:
                return Stat(kind="directory")
            return None
        if loc.leftover:
            # path ended mid-edge; it is a directory iff the edge continues
            # with a separator (e.g. "data" inside "data/part1.parquet")
            return Stat(kind="directory") if loc.leftover.startswith(b"/") else None
        return Stat(kind="directory")

    async def list_dir(
        self, root: str, path: str
    ) -> tuple[list[FileEntry], list[str]] | None:
        res = await list_directory(self.store, bytes.fromhex(root), _b(path))
        if res is None:
            return None
        files, dirs = res
        return files, [_s(d) for d in dirs]

    async def iter_files(self, root: str, prefix: str) -> AsyncIterator[FileEntry]:
        async for e in iter_files(self.store, bytes.fromhex(root), _b(prefix)):
            yield e


class IndexedListingBackend(ListingBackend):
    """Answers from a root's ``.swarmfs/index.json`` when it has one.

    The index is one file listing every entry, so a listing costs a single
    fetch instead of one per trie node — and the sizes come with it, which
    also spares ``ls -l``-style calls a HEAD per file. Manifests without an
    index (most of them) fall through to the trie walk, and the probe that
    decides which is nearly free: it compares one byte against the root
    node's forks, which are in hand already.

    On trust: the index is reached *through* the manifest, so under
    verification its chunks are checked like any other. It is the
    publisher's own claim about their own manifest — an inconsistent index
    is the publisher misleading readers about their own content, not a
    third party tampering with it, which is what verification defends
    against. The bytes you then read are still checked against the
    reference the index gave.
    """

    def __init__(self, inner: MantarayListingBackend, reader):
        self.inner = inner
        self.reader = reader
        self._indexes: dict[str, dict[str, dict] | None] = {}  # root -> entries

    async def _index(self, root: str, load: bool = True):
        """The parsed index for ``root``; None when it has none. With
        ``load=False`` only an already-parsed one is returned — so a single
        ``stat`` does not pull a whole index it may not need."""
        if root in self._indexes:
            return self._indexes[root]
        if not load:
            return None
        entries = None
        st = await self.inner.stat(root, INDEX_PATH)
        if st is not None and st.kind == "file" and st.reference is not None:
            entries = parse_index(await self.reader.bytes_get(st.reference.hex()))
        self._indexes[root] = entries
        return entries

    @staticmethod
    def _entry(path: str, info: dict) -> FileEntry:
        return FileEntry(path=_b(path), reference=bytes.fromhex(info["r"]),
                         metadata=info.get("m"), size=info.get("s"))

    @staticmethod
    def _reserved(path: str) -> bool:
        """swarmfs's own bookkeeping directory. Listings never mention it —
        it is not the publisher's content — but asking for it by name is
        answered honestly from the trie, because the file is really there
        and reading it is how you debug an index."""
        return path == RESERVED_DIR or path.startswith(RESERVED_DIR + "/")

    async def stat(self, root: str, path: str) -> Stat | None:
        if self._reserved(path):
            return await self.inner.stat(root, path)
        entries = await self._index(root, load=False)
        if entries is None:
            return await self.inner.stat(root, path)
        if not path:
            return Stat(kind="directory")
        info = entries.get(path)
        if info is not None:
            return Stat(kind="file", reference=bytes.fromhex(info["r"]),
                        metadata=info.get("m"), size=info.get("s"))
        prefix = path + "/"
        if any(p.startswith(prefix) for p in entries):
            return Stat(kind="directory")
        return None

    async def list_dir(
        self, root: str, path: str
    ) -> tuple[list[FileEntry], list[str]] | None:
        if self._reserved(path):
            return await self.inner.list_dir(root, path)
        entries = await self._index(root)
        if entries is None:
            return await self.inner.list_dir(root, path)
        prefix = f"{path}/" if path else ""
        files: list[FileEntry] = []
        dirs: set[str] = set()
        found = False
        for p in sorted(entries):
            if prefix and not p.startswith(prefix):
                continue
            found = True
            rel = p[len(prefix):]
            head, sep, _ = rel.partition("/")
            if sep:
                dirs.add(head)
            elif rel:
                files.append(self._entry(rel, entries[p]))
        if not found and path:
            return None  # not a directory (or nothing there)
        return files, sorted(dirs)

    async def iter_files(self, root: str, prefix: str) -> AsyncIterator[FileEntry]:
        entries = None if self._reserved(prefix.rstrip("/")) else await self._index(root)
        if entries is None:
            async for e in self.inner.iter_files(root, prefix):
                yield e
            return
        for p in sorted(entries):
            if prefix and not p.startswith(prefix):
                continue
            yield self._entry(p[len(prefix):], entries[p])


async def detect_listing_backend(reader) -> ListingBackend:
    # TODO(bee#5535): probe for the server-side listing endpoint and prefer it.
    # Until then: the trie walk, with a root index short-circuiting it when
    # the publisher wrote one (docs/distributed-writes.md §6).
    return IndexedListingBackend(MantarayListingBackend(reader), reader)
