"""Codec tests. The hand-crafted fixtures pin the wire format itself
(byte-for-byte, per bee's pkg/manifest/mantaray) independently of our own
serializer, so a marshal/unmarshal bug can't cancel itself out."""

from __future__ import annotations

import random

import pytest

from swarmfs.mantaray import (
    Fork,
    MantarayFormatError,
    Node,
    marshal,
    unmarshal,
)
from swarmfs.mantaray.node import (
    NT_EDGE,
    NT_VALUE,
    NT_WITH_METADATA,
    NT_WITH_PATH_SEPARATOR,
    VERSION_01_HASH,
    VERSION_02_HASH,
)


def bitmap(byte_values) -> bytes:
    bm = bytearray(32)
    for b in byte_values:
        bm[b >> 3] |= 1 << (b & 7)
    return bytes(bm)


REF_A = bytes([0xAA]) * 32
REF_B = bytes([0xBB]) * 32
REF_C = bytes([0xCC]) * 32


def handcrafted_v02_body() -> bytes:
    """A v0.2 node with three forks, laid out by hand from the format spec."""
    meta_json = b'{"Content-Type":"text/plain"}'
    # (2-byte size + json) padded with '\n' to fill a 32-byte block
    meta_padded = meta_json + b"\n" * (32 - 2 - len(meta_json))

    body = bytearray()
    body += VERSION_02_HASH
    body.append(32)  # refBytesSize
    body += bytes(32)  # zero entry: the root is a directory
    body += bitmap([ord("h"), ord("m"), ord("s")])
    # fork 'h': value leaf "hello.txt"
    body += bytes([NT_VALUE, 9]) + b"hello.txt".ljust(30, b"\x00") + REF_A
    # fork 'm': value leaf "meta.txt" with metadata
    body += bytes([NT_VALUE | NT_WITH_METADATA, 8]) + b"meta.txt".ljust(30, b"\x00") + REF_C
    body += len(meta_padded).to_bytes(2, "big") + meta_padded
    # fork 's': edge "sub/"
    body += bytes([NT_EDGE | NT_WITH_PATH_SEPARATOR, 4]) + b"sub/".ljust(30, b"\x00") + REF_B
    return bytes(body)


def test_parse_handcrafted_v02():
    data = bytes(32) + handcrafted_v02_body()  # zero obfuscation key: XOR is a no-op
    node = unmarshal(data)

    assert node.version == 2
    assert node.ref_bytes_size == 32
    assert not node.has_entry
    assert sorted(node.forks) == [ord("h"), ord("m"), ord("s")]

    h = node.forks[ord("h")]
    assert h.prefix == b"hello.txt" and h.ref == REF_A
    assert h.is_value and not h.is_edge and h.metadata is None

    m = node.forks[ord("m")]
    assert m.prefix == b"meta.txt" and m.ref == REF_C
    assert m.is_value and m.metadata == {"Content-Type": "text/plain"}

    s = node.forks[ord("s")]
    assert s.prefix == b"sub/" and s.ref == REF_B
    assert s.is_edge and not s.is_value


def test_parse_handcrafted_v02_obfuscated():
    """The same node under a non-trivial obfuscation key must parse identically."""
    key = bytes(range(1, 33))
    body = handcrafted_v02_body()
    obfuscated = key + bytes(b ^ key[i % 32] for i, b in enumerate(body))
    node = unmarshal(obfuscated)
    assert node.obfuscation_key == key
    assert sorted(node.forks) == [ord("h"), ord("m"), ord("s")]
    assert node.forks[ord("m")].metadata == {"Content-Type": "text/plain"}


def test_parse_handcrafted_v01():
    body = bytearray()
    body += VERSION_01_HASH
    body.append(32)
    body += REF_A  # v0.1 value node with an entry
    body += bitmap([ord("x")])
    body += bytes([NT_VALUE, 1]) + b"x".ljust(30, b"\x00") + REF_B
    node = unmarshal(bytes(32) + bytes(body))
    assert node.version == 1
    assert node.entry == REF_A and node.has_entry
    assert node.forks[ord("x")].prefix == b"x"
    assert node.forks[ord("x")].ref == REF_B


def test_unknown_version_rejected():
    data = bytes(32) + bytes(31) + bytes([32]) + bytes(64)
    with pytest.raises(MantarayFormatError, match="version"):
        unmarshal(data)


def test_too_short_rejected():
    with pytest.raises(MantarayFormatError, match="short"):
        unmarshal(b"\x00" * 63)


def test_truncated_fork_rejected():
    data = bytes(32) + handcrafted_v02_body()
    with pytest.raises(MantarayFormatError):
        unmarshal(data[:-10])


def test_marshal_roundtrip_simple():
    node = Node(entry=b"", ref_bytes_size=32)
    node.forks[ord("f")] = Fork(node_type=NT_VALUE, prefix=b"file.bin", ref=REF_A)
    node.forks[ord("d")] = Fork(
        node_type=NT_EDGE | NT_WITH_PATH_SEPARATOR, prefix=b"dir/", ref=REF_B
    )
    parsed = unmarshal(marshal(node))
    assert parsed.ref_bytes_size == 32
    assert parsed.forks[ord("f")].prefix == b"file.bin"
    assert parsed.forks[ord("f")].ref == REF_A
    assert parsed.forks[ord("d")].is_edge


def test_marshal_roundtrip_with_obfuscation_key():
    rng = random.Random(1234)
    key = bytes(rng.randrange(256) for _ in range(32))
    node = Node(entry=REF_C, ref_bytes_size=32, obfuscation_key=key)
    node.forks[ord("a")] = Fork(
        node_type=NT_VALUE,
        prefix=b"a.txt",
        ref=REF_A,
        metadata={"Content-Type": "text/plain"},
    )
    data = marshal(node)
    assert data[:32] == key
    parsed = unmarshal(data)
    assert parsed.entry == REF_C
    assert parsed.forks[ord("a")].metadata == {"Content-Type": "text/plain"}


@pytest.mark.parametrize("value_len", list(range(0, 81)) + [1000])
def test_metadata_padding_boundaries(value_len):
    """Sweep metadata JSON sizes across the 32-byte padding boundaries,
    including bee's pad-a-full-extra-block-when-aligned quirk."""
    meta = {"k": "v" * value_len}
    node = Node(ref_bytes_size=32)
    node.forks[ord("a")] = Fork(node_type=NT_VALUE, prefix=b"a", ref=REF_A, metadata=meta)
    parsed = unmarshal(marshal(node))
    assert parsed.forks[ord("a")].metadata == meta


def test_marshal_encrypted_ref_width():
    """refBytesSize 64 (encrypted references) round-trips."""
    entry = bytes([0xEE]) * 64
    ref = bytes([0xDD]) * 64
    node = Node(entry=entry, ref_bytes_size=64)
    node.forks[ord("e")] = Fork(node_type=NT_VALUE, prefix=b"enc.bin", ref=ref)
    parsed = unmarshal(marshal(node))
    assert parsed.ref_bytes_size == 64
    assert parsed.entry == entry
    assert parsed.forks[ord("e")].ref == ref


def test_zero_entry_means_no_entry():
    node = unmarshal(marshal(Node(ref_bytes_size=32)))
    assert node.entry == bytes(32)
    assert not node.has_entry
