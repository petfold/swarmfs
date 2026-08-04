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
        encrypt: bool = False,
    ):
        self.client = client
        self.stamps = stamps
        self.concurrency = concurrency
        self.pin = pin
        self.redundancy = redundancy
        self.encrypt = encrypt

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
        if root is not None and self.encrypt != (len(root) == 128):
            # A lineage is encrypted or it isn't: manifest nodes carry ONE
            # refBytesSize, so a 64-byte (encrypted) child ref cannot live
            # in a 32-byte-ref parent, or vice versa.
            raise ValueError(
                "encrypt=%s but the manifest %s… is %sencrypted — a "
                "lineage cannot mix; publish a fresh manifest instead"
                % (self.encrypt, root[:8],
                   "" if len(root) == 128 else "un"))
        batch = await self.stamps.resolve(stamp)

        sem = asyncio.Semaphore(self.concurrency)

        async def upload(path: str, sw: StagedWrite) -> tuple[str, str]:
            async with sem:
                ref = await self.client.bytes_post(
                    sw.payload(), batch, pin=self.pin,
                    redundancy=self.redundancy, encrypt=self.encrypt
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
                    data, batch, pin=self.pin,
                    redundancy=self.redundancy, encrypt=self.encrypt
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


class LocalFirstCommitEngine(CommitEngine):
    """The commit engine over a local-first store (L3): staged files and
    manifest nodes land on local disk — BMT-addressed by the splitter, so
    the refs equal what the node would return with erasure coding off —
    and the new root is journaled with its exact new-blob list (manifest
    nodes classified as eviction-priority structure). The attached Syncer
    pushes and confirms in the background; `commit()` itself never touches
    the network for writes and needs **no stamp** — postage is the push's
    concern, which is why offline commits are the normal mode.

    Reads during the manifest patch are local-first: journal-known refs
    come from disk (healing by verified re-fetch if evicted), refs from a
    foreign lineage (a manifest that was never local, e.g. opening
    ``bzz://<remote-ref>`` and writing into it) fall back to the node
    transiently — deliberately not persisted, so foreign parents don't
    accumulate as forever-pinned orphans.

    Erasure coding must stay off for this store's uploads (parity forks
    the address space — the push's ref-equality assertion would trip), so
    the constructor refuses a redundancy setting.
    """

    def __init__(self, local, client: SwarmClient, concurrency: int = 8):
        self.local = local  # swarmfs.localstore.LocalStore, addressing="swarm"
        self.client = client
        self.concurrency = concurrency
        if getattr(local, "addressing", "swarm") != "swarm":
            raise ValueError(
                'a local-first commit engine needs addressing="swarm" '
                "(its refs must equal the node's)")

    async def commit(
        self,
        root: str | None,
        writes: dict[str, StagedWrite],
        removes: Iterable[str],
        stamp: str | None = None,  # unused: postage belongs to the push
    ) -> CommitResult:
        removes = sorted(removes)
        if not writes and not removes:
            raise ValueError("nothing staged to commit")
        sem = asyncio.Semaphore(self.concurrency)

        async def put_local(data: bytes) -> str:
            async with sem:
                return await asyncio.to_thread(self.local.put, data)

        async def upload(path: str, sw: StagedWrite) -> tuple[str, str]:
            return path, await put_local(sw.payload())

        uploaded = dict(
            await asyncio.gather(*(upload(p, sw) for p, sw in writes.items()))
        )

        async def load(ref: bytes) -> bytes:
            hexref = ref.hex()
            try:
                return await asyncio.to_thread(self.local.get, hexref)
            except KeyError:
                return await self.client.bytes_get(hexref)  # foreign lineage

        if root is not None:
            node = unmarshal(await load(bytes.fromhex(root)))
        else:
            node = Node()

        for path in removes:
            await remove(node, _b(path), load)
        for path, sw in writes.items():
            await add(node, _b(path), bytes.fromhex(uploaded[path]),
                      sw.metadata, load)

        node_refs: list[str] = []

        async def saver(data: bytes) -> bytes:
            ref = await put_local(data)
            node_refs.append(ref)
            return bytes.fromhex(ref)

        new_root = (await save(node, saver)).hex()
        for sw in writes.values():
            sw.close()

        if new_root != root and not self.local.has_root(new_root):
            parent = root if (root is not None
                              and self.local.has_root(root)) else None
            blobs = set(uploaded.values()) | set(node_refs)
            await asyncio.to_thread(
                self.local.commit_root, new_root, parent, sorted(blobs),
                sorted(set(node_refs)))
        return CommitResult(
            old_root=root,
            new_root=new_root,
            written=uploaded,
            removed=removes,
            batch="",  # no stamp spent: the push worker owns postage
        )


def _b(path: str) -> bytes:
    return path.encode("utf-8", "surrogateescape")
