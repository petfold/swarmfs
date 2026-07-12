"""Binary codec for Mantaray manifest nodes.

Wire format (mirrors bee's ``pkg/manifest/mantaray``, versions "mantaray:0.1"
and "mantaray:0.2")::

    obfuscation key   32 bytes, stored in plaintext
    version hash      31 bytes   keccak256("mantaray:0.x")[:31]
    refBytesSize      1 byte     32, or 64 for encrypted references
    entry             refBytesSize bytes (all zeros = no entry)
    fork bitmap       32 bytes   bit b set = fork whose prefix starts with byte b
    forks             for each set bit, in ascending byte order:
        nodeType      1 byte     bitfield: 2 value | 4 edge | 8 path-sep | 16 metadata
        prefixLen     1 byte     1..30
        prefix        30 bytes   zero-padded
        ref           refBytesSize bytes (reference of the child node)
        -- only v0.2 nodes, only when nodeType & 16 --
        metaSize      2 bytes    big-endian
        metadata      metaSize bytes of JSON, '\\n'-padded to 32-byte blocks

Everything after the obfuscation key is XORed with the key (repeated).
The root node's own nodeType and metadata are not persisted; forks carry
the type/metadata of the child node they reference.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

OBFUSCATION_KEY_SIZE = 32
VERSION_HASH_SIZE = 31
HEADER_SIZE = OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE + 1  # 64
FORK_HEADER_SIZE = 2
FORK_PRE_REFERENCE_SIZE = 32
PREFIX_MAX_SIZE = FORK_PRE_REFERENCE_SIZE - FORK_HEADER_SIZE  # 30
METADATA_SIZE_SIZE = 2

# keccak256("mantaray:0.1") / keccak256("mantaray:0.2"), truncated to 31 bytes,
# as pre-computed in bee's pkg/manifest/mantaray/marshal.go.
VERSION_01_HASH = bytes.fromhex(
    "025184789d63635766d78c41900196b57d7400875ebe4d9b5d1e76bd9652a9b7"
)[:VERSION_HASH_SIZE]
VERSION_02_HASH = bytes.fromhex(
    "5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f7b"
)[:VERSION_HASH_SIZE]

NT_VALUE = 0x02
NT_EDGE = 0x04
NT_WITH_PATH_SEPARATOR = 0x08
NT_WITH_METADATA = 0x10

ZERO32 = bytes(32)


class MantarayFormatError(ValueError):
    """Raised when bytes do not decode as a Mantaray node."""


@dataclass
class Fork:
    """A fork record inside a parent node.

    ``node_type`` and ``metadata`` describe the *child* node the fork points
    at (the wire format persists them here, not in the child). ``node`` is an
    in-memory child link used by the builder and as a resolution cache when
    walking; ``ref`` is the child's saved reference.
    """

    node_type: int = 0
    prefix: bytes = b""
    ref: bytes = b""
    metadata: dict[str, str] | None = None
    node: "Node | None" = None

    @property
    def is_value(self) -> bool:
        return bool(self.node_type & NT_VALUE)

    @property
    def is_edge(self) -> bool:
        return bool(self.node_type & NT_EDGE)

    @property
    def has_metadata(self) -> bool:
        return bool(self.node_type & NT_WITH_METADATA)


@dataclass
class Node:
    entry: bytes = b""
    ref_bytes_size: int = 0
    obfuscation_key: bytes = ZERO32
    forks: dict[int, Fork] = field(default_factory=dict)
    version: int = 2
    # In-memory only (builder state / assigned by save()):
    node_type: int = 0
    metadata: dict[str, str] | None = None
    ref: bytes | None = None

    @property
    def has_entry(self) -> bool:
        return any(self.entry)


def _xor(data: bytes | memoryview, key: bytes) -> bytes:
    if key == ZERO32 or not len(data):
        return bytes(data)
    reps = -(-len(data) // len(key))
    keystream = (key * reps)[: len(data)]
    n = len(data)
    return (int.from_bytes(data, "big") ^ int.from_bytes(keystream, "big")).to_bytes(n, "big")


def unmarshal(data: bytes) -> Node:
    """Decode one serialized Mantaray node (as fetched from ``/bytes/{ref}``)."""
    if len(data) < HEADER_SIZE:
        raise MantarayFormatError(f"serialized node too short: {len(data)} bytes")

    key = bytes(data[:OBFUSCATION_KEY_SIZE])
    body = _xor(memoryview(data)[OBFUSCATION_KEY_SIZE:], key)

    version_hash = body[:VERSION_HASH_SIZE]
    if version_hash == VERSION_01_HASH:
        version = 1
    elif version_hash == VERSION_02_HASH:
        version = 2
    else:
        raise MantarayFormatError(f"unknown version hash: {version_hash.hex()}")

    rbs = body[VERSION_HASH_SIZE]
    off = VERSION_HASH_SIZE + 1
    if len(body) < off + rbs + 32:
        raise MantarayFormatError("serialized node too short for entry + fork bitmap")
    entry = body[off : off + rbs]
    off += rbs
    bitmap = body[off : off + 32]
    off += 32

    node = Node(entry=entry, ref_bytes_size=rbs, obfuscation_key=key, version=version)
    if any(bitmap):
        node.node_type |= NT_EDGE

    for b in range(256):
        if not (bitmap[b >> 3] >> (b & 7)) & 1:
            continue
        if version == 2 and rbs == 0:
            # bee skips fork parsing entirely in this (degenerate) case
            continue
        base = FORK_PRE_REFERENCE_SIZE + rbs
        if len(body) < off + base:
            raise MantarayFormatError(f"truncated fork record for byte {b:#04x}")
        node_type = body[off]
        prefix_len = body[off + 1]
        if prefix_len == 0 or prefix_len > PREFIX_MAX_SIZE:
            raise MantarayFormatError(f"invalid fork prefix length: {prefix_len}")
        prefix = body[off + FORK_HEADER_SIZE : off + FORK_HEADER_SIZE + prefix_len]
        ref = body[off + FORK_PRE_REFERENCE_SIZE : off + base]

        metadata = None
        size = base
        if version == 2 and node_type & NT_WITH_METADATA:
            if len(body) < off + base + METADATA_SIZE_SIZE:
                raise MantarayFormatError(f"truncated fork metadata size for byte {b:#04x}")
            msize = int.from_bytes(body[off + base : off + base + METADATA_SIZE_SIZE], "big")
            size = base + METADATA_SIZE_SIZE + msize
            if len(body) < off + size:
                raise MantarayFormatError(f"truncated fork metadata for byte {b:#04x}")
            raw = body[off + base + METADATA_SIZE_SIZE : off + size]
            try:
                metadata = json.loads(raw)  # tolerates the '\n' padding
            except json.JSONDecodeError as e:
                raise MantarayFormatError(f"bad fork metadata JSON: {e}") from e

        node.forks[b] = Fork(node_type=node_type, prefix=prefix, ref=ref, metadata=metadata)
        off += size

    return node


def _infer_ref_bytes_size(node: Node) -> int:
    if node.ref_bytes_size:
        return node.ref_bytes_size
    if node.entry:
        return len(node.entry)
    for f in node.forks.values():
        if f.ref:
            return len(f.ref)
    return 32


def marshal(node: Node, obfuscation_key: bytes | None = None) -> bytes:
    """Serialize a node (always as version 0.2).

    Fork ``node_type``/``ref``/``metadata`` fields must be up to date; the
    builder's ``save()`` syncs them from in-memory children before calling this.
    """
    key = obfuscation_key if obfuscation_key is not None else node.obfuscation_key
    if len(key) != OBFUSCATION_KEY_SIZE:
        raise ValueError(f"obfuscation key must be {OBFUSCATION_KEY_SIZE} bytes")
    rbs = _infer_ref_bytes_size(node)
    if len(node.entry) not in (0, rbs):
        raise ValueError(f"entry size {len(node.entry)} does not match refBytesSize {rbs}")

    out = bytearray()
    out += key
    out += VERSION_02_HASH
    out.append(rbs)
    out += node.entry.ljust(rbs, b"\x00")

    bitmap = bytearray(32)
    for b in node.forks:
        bitmap[b >> 3] |= 1 << (b & 7)
    out += bitmap

    for b in sorted(node.forks):
        out += _fork_bytes(node.forks[b], rbs)

    obfuscated = bytes(out[:OBFUSCATION_KEY_SIZE]) + _xor(
        memoryview(out)[OBFUSCATION_KEY_SIZE:], key
    )
    return obfuscated


def _fork_bytes(fork: Fork, rbs: int) -> bytes:
    if not fork.prefix or len(fork.prefix) > PREFIX_MAX_SIZE:
        raise ValueError(f"invalid fork prefix length: {len(fork.prefix)}")
    if len(fork.ref) != rbs:
        raise ValueError(
            f"fork reference size {len(fork.ref)} does not match refBytesSize {rbs} "
            "(save children before marshalling)"
        )
    node_type = fork.node_type
    if fork.metadata:
        node_type |= NT_WITH_METADATA

    out = bytearray()
    out.append(node_type)
    out.append(len(fork.prefix))
    out += fork.prefix.ljust(PREFIX_MAX_SIZE, b"\x00")
    out += fork.ref

    if node_type & NT_WITH_METADATA:
        payload = json.dumps(
            fork.metadata or {}, separators=(",", ":"), sort_keys=True
        ).encode()
        # bee pads the JSON with '\n' so that (2-byte size + JSON) fills
        # 32-byte blocks — including a full extra block when already aligned.
        with_size = len(payload) + METADATA_SIZE_SIZE
        if with_size < OBFUSCATION_KEY_SIZE:
            pad = OBFUSCATION_KEY_SIZE - with_size
        elif with_size > OBFUSCATION_KEY_SIZE:
            pad = OBFUSCATION_KEY_SIZE - (with_size % OBFUSCATION_KEY_SIZE)
        else:
            pad = 0
        payload += b"\n" * pad
        if len(payload) > 0xFFFF:
            raise ValueError(f"metadata too large: {len(payload)} bytes")
        out += len(payload).to_bytes(METADATA_SIZE_SIZE, "big")
        out += payload

    return bytes(out)
