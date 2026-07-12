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

from ._client import SwarmClient
from .mantaray import FileEntry, NodeStore, iter_files, list_directory, locate


def _b(path: str) -> bytes:
    return path.encode("utf-8", "surrogateescape")


def _s(path: bytes) -> str:
    return path.decode("utf-8", "surrogateescape")


@dataclass
class Stat:
    kind: str  # "file" | "directory"
    reference: bytes | None = None  # the file's data reference (entry)
    metadata: dict[str, str] | None = None


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
    today's Bee and public gateways)."""

    def __init__(self, client: SwarmClient, cache_size: int = 4096):
        # keyed by reference (content-addressed), safe to share across roots
        self.store = NodeStore(load=lambda ref: client.bytes_get(ref.hex()), cache_size=cache_size)

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


async def detect_listing_backend(client: SwarmClient) -> ListingBackend:
    # TODO(bee#5535): probe for the server-side listing endpoint and prefer it.
    return MantarayListingBackend(client)
