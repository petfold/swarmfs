"""The transactional commit engine: staged writes → a new root reference.

A commit is copy-on-write: file blobs are uploaded in parallel, then the
manifest trie is patched client-side (only nodes along changed paths are
re-serialized and re-uploaded) and the new root reference is returned. The
old root is untouched — every commit is automatically a snapshot.
"""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass, field
from typing import IO, Iterable

from ._client import SwarmClient
from .mantaray import Node, add, remove, save, unmarshal
from .stamps import StampManager

SPOOL_MAX_MEMORY = 16 * 2**20  # staged writes larger than this spill to disk


@dataclass
class StagedWrite:
    """One staged file: bytes in memory, or a spooled temporary file."""

    data: bytes | IO[bytes]
    size: int
    metadata: dict[str, str] | None = None

    def payload(self) -> bytes:
        if isinstance(self.data, bytes):
            return self.data
        self.data.seek(0)
        return self.data.read()

    def close(self) -> None:
        if not isinstance(self.data, bytes):
            self.data.close()

    @classmethod
    def spooled(cls) -> IO[bytes]:
        return tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY)


@dataclass
class CommitResult:
    old_root: str | None
    new_root: str
    written: dict[str, str] = field(default_factory=dict)  # path -> data reference
    removed: list[str] = field(default_factory=list)
    batch: str = ""  # the postage batch the commit used


class CommitEngine:
    def __init__(
        self,
        client: SwarmClient,
        stamps: StampManager,
        concurrency: int = 8,
        pin: bool = False,
        redundancy: int | None = None,
    ):
        self.client = client
        self.stamps = stamps
        self.concurrency = concurrency
        self.pin = pin
        self.redundancy = redundancy

    async def commit(
        self,
        root: str | None,
        writes: dict[str, StagedWrite],
        removes: Iterable[str],
        stamp: str | None = None,
    ) -> CommitResult:
        """Apply staged operations against ``root`` (None = fresh manifest).

        The stamp is validated before any byte is uploaded.
        """
        removes = sorted(removes)
        if not writes and not removes:
            raise ValueError("nothing staged to commit")
        batch = await self.stamps.resolve(stamp)

        sem = asyncio.Semaphore(self.concurrency)

        async def upload(path: str, sw: StagedWrite) -> tuple[str, str]:
            async with sem:
                ref = await self.client.bytes_post(
                    sw.payload(), batch, pin=self.pin, redundancy=self.redundancy
                )
            return path, ref

        uploaded = dict(
            await asyncio.gather(*(upload(p, sw) for p, sw in writes.items()))
        )

        async def load(ref: bytes) -> bytes:
            return await self.client.bytes_get(ref.hex())

        if root is not None:
            node = unmarshal(await load(bytes.fromhex(root)))
        else:
            node = Node()

        for path in removes:
            await remove(node, _b(path), load)
        for path, sw in writes.items():
            await add(node, _b(path), bytes.fromhex(uploaded[path]), sw.metadata, load)

        async def saver(data: bytes) -> bytes:
            # manifest nodes are single chunks; parity applies to multi-chunk
            # trees, but the header is harmless and keeps behavior uniform
            return bytes.fromhex(
                await self.client.bytes_post(
                    data, batch, pin=self.pin, redundancy=self.redundancy
                )
            )

        new_root = await save(node, saver)
        for sw in writes.values():
            sw.close()
        return CommitResult(
            old_root=root,
            new_root=new_root.hex(),
            written=uploaded,
            removed=removes,
            batch=batch,
        )


def _b(path: str) -> bytes:
    return path.encode("utf-8", "surrogateescape")
