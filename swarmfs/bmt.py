"""Binary Merkle Tree chunk addressing (Swarm's content addressing for chunks).

The BMT chunk address is keccak256 of the 8-byte span and the root of a
binary Merkle tree over the 32-byte segments of the payload, zero-padded to
4096 bytes. Needed client-side for single-owner chunks (feed updates sign the
wrapped chunk's address) and, later, for opt-in chunk verification.

Requires the ``feeds`` extra (``pip install swarmfs[feeds]``) for keccak256.
"""

from __future__ import annotations

CHUNK_PAYLOAD_SIZE = 4096
SEGMENT_SIZE = 32

try:
    from eth_hash.auto import keccak as _keccak

    def keccak256(data: bytes) -> bytes:
        return _keccak(data)

except ImportError:  # pragma: no cover

    def keccak256(data: bytes) -> bytes:
        raise ImportError(
            "keccak256 requires the 'feeds' extra: pip install 'swarmfs[feeds]'"
        )


def bmt_root(payload: bytes) -> bytes:
    """Root of the binary Merkle tree over 32-byte segments (zero-padded)."""
    if len(payload) > CHUNK_PAYLOAD_SIZE:
        raise ValueError(f"payload size {len(payload)} exceeds chunk payload {CHUNK_PAYLOAD_SIZE}")
    # segments of the payload zero-padded to 4096 bytes
    level = [
        bytes(payload[i : i + SEGMENT_SIZE]).ljust(SEGMENT_SIZE, b"\x00")
        for i in range(0, CHUNK_PAYLOAD_SIZE, SEGMENT_SIZE)
    ]
    while len(level) > 1:
        level = [keccak256(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
    return level[0]


def chunk_address(chunk_data: bytes) -> bytes:
    """BMT address of a content-addressed chunk (``chunk_data`` = span + payload)."""
    span, payload = chunk_data[:8], chunk_data[8:]
    return keccak256(span + bmt_root(payload))


def cac_data(payload: bytes) -> bytes:
    """Wrap a payload as content-addressed chunk data (little-endian span + payload)."""
    if len(payload) > CHUNK_PAYLOAD_SIZE:
        raise ValueError(f"payload size {len(payload)} exceeds chunk payload {CHUNK_PAYLOAD_SIZE}")
    return len(payload).to_bytes(8, "little") + payload
