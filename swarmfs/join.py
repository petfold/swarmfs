"""A verifying joiner: reads content chunk-by-chunk via ``/chunks`` and
checks every chunk's BMT address against the reference it was fetched by.

This is what makes reads through an untrusted endpoint (a public gateway)
actually trustless: a Swarm reference *is* the hash of the content, so a
tampering gateway is caught on the first bad chunk. Range reads descend only
the subtrees they need, so Parquet/zarr access stays viable.

Facts this implementation relies on (verified against a live Bee 2.8.1):

- the BMT address covers the stored chunk bytes as-is — including the span
  whose top byte may carry the erasure-coding redundancy level;
- an intermediate chunk's payload holds the *data* child references first,
  then parity references (when redundancy is on): exactly
  ``ceil(span/unit)`` of them are data, the rest are ignored here.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import AsyncIterator

from .bmt import CHUNK_PAYLOAD_SIZE, chunk_address

REF_SIZE = 32
BRANCHES = CHUNK_PAYLOAD_SIZE // REF_SIZE  # 128


class VerificationError(OSError):
    """Fetched data does not hash to the reference it was requested by."""


def decode_span(span: bytes) -> int:
    # bee encodes the redundancy level in the top byte (span[7] > 128)
    if span[7] > 128:
        span = span[:7] + b"\x00"
    return int.from_bytes(span, "little")


class VerifyingReader:
    """Drop-in for the read side of SwarmClient (`bytes_get`/`bytes_size`/
    `bytes_iter`), with every chunk verified. Chunks are content-addressed,
    so the verification cache never goes stale."""

    def __init__(self, client, cache_size: int = 1024):
        self.client = client
        self._cache: OrderedDict[bytes, bytes] = OrderedDict()
        self._cache_size = cache_size

    async def _chunk(self, ref: bytes) -> bytes:
        cached = self._cache.get(ref)
        if cached is not None:
            self._cache.move_to_end(ref)
            return cached
        data = await self.client.chunk_get(ref.hex())
        if chunk_address(data) != ref:
            raise VerificationError(
                f"chunk {ref.hex()} failed verification: content does not hash "
                "to its address (tampering, corruption, or a bad endpoint)"
            )
        self._cache[ref] = data
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return data

    @staticmethod
    def _root_ref(ref: str) -> bytes:
        if len(ref) != 64:
            raise VerificationError(
                f"cannot verify reference {ref[:16]}…: encrypted (128-hex) "
                "references are not supported by the verifying reader yet"
            )
        return bytes.fromhex(ref)

    async def bytes_size(self, ref: str) -> int:
        return decode_span((await self._chunk(self._root_ref(ref)))[:8])

    async def bytes_get(self, ref: str, start: int | None = None, end: int | None = None) -> bytes:
        chunk = await self._chunk(self._root_ref(ref))
        size = decode_span(chunk[:8])
        s = min(start or 0, size)
        e = size if end is None else min(end, size)
        if e <= s:
            return b""
        out = bytearray()
        await self._read(chunk, s, e, out)
        return bytes(out)

    async def _read(self, chunk: bytes, start: int, end: int, out: bytearray) -> None:
        """Append ``[start, end)`` of this subtree to ``out`` (both relative
        to the subtree), fetching and verifying only the chunks needed."""
        size = decode_span(chunk[:8])
        payload = chunk[8:]
        if size <= CHUNK_PAYLOAD_SIZE:
            out += payload[start:end]
            return
        unit = CHUNK_PAYLOAD_SIZE
        while unit * BRANCHES < size:
            unit *= BRANCHES
        n_data = -(-size // unit)
        if len(payload) < n_data * REF_SIZE:
            raise VerificationError(
                f"intermediate chunk carries {len(payload) // REF_SIZE} references "
                f"but its span requires {n_data}"
            )
        for i in range(start // unit, n_data):
            lo = i * unit
            if lo >= end:
                break
            hi = min(lo + unit, size)
            child = await self._chunk(payload[i * REF_SIZE : (i + 1) * REF_SIZE])
            if decode_span(child[:8]) != hi - lo:
                raise VerificationError(
                    f"child chunk span {decode_span(child[:8])} does not match "
                    f"its position in the tree (expected {hi - lo})"
                )
            await self._read(child, max(start, lo) - lo, min(end, hi) - lo, out)

    async def bytes_iter(self, ref: str, chunk_size: int = 1 << 20) -> AsyncIterator[bytes]:
        """Stream verified content leaf by leaf (for downloads)."""
        chunk = await self._chunk(self._root_ref(ref))
        async for piece in self._iter_tree(chunk):
            yield piece

    async def _iter_tree(self, chunk: bytes) -> AsyncIterator[bytes]:
        size = decode_span(chunk[:8])
        payload = chunk[8:]
        if size <= CHUNK_PAYLOAD_SIZE:
            yield payload[:size]
            return
        unit = CHUNK_PAYLOAD_SIZE
        while unit * BRANCHES < size:
            unit *= BRANCHES
        n_data = -(-size // unit)
        if len(payload) < n_data * REF_SIZE:
            raise VerificationError("intermediate chunk is missing child references")
        for i in range(n_data):
            child = await self._chunk(payload[i * REF_SIZE : (i + 1) * REF_SIZE])
            async for piece in self._iter_tree(child):
                yield piece
