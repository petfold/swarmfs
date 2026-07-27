"""The splitter is the joiner's inverse, at genuine Swarm addresses.

Correctness here means two things, and both are checked: the tree reads back
byte-for-byte through the *verifying* joiner (which recomputes every chunk's
BMT address, so a wrong tree cannot pass), and the root matches the reference
Bee itself returns for the same bytes — that live check is in
tests/test_integration.py, since it needs a node.
"""

from __future__ import annotations

import asyncio
import random

import pytest

pytest.importorskip("eth_hash")

from swarmfs.join import VerifyingReader  # noqa: E402
from swarmfs.splitter import content_address, split  # noqa: E402

from conftest import FakeClient, split_content  # noqa: E402


def payload(n: int, seed: int = 3) -> bytes:
    return random.Random(seed).randbytes(n)


# every tree shape: empty, sub-leaf, leaf boundaries, two levels, three levels
SIZES = [0, 1, 4095, 4096, 4097, 8192, 100_000,
         128 * 4096, 128 * 4096 + 1, 600_000]


@pytest.mark.parametrize("size", SIZES)
def test_roundtrips_through_the_verifying_joiner(size):
    data = payload(size)
    root, chunks = split(data)
    reader = VerifyingReader(FakeClient(chunks))
    assert asyncio.run(reader.bytes_size(root.hex())) == size
    assert asyncio.run(reader.bytes_get(root.hex())) == data


@pytest.mark.parametrize("size", [10_000, 600_000])
def test_range_reads_of_a_split_tree(size):
    data = payload(size)
    root, chunks = split(data)
    reader = VerifyingReader(FakeClient(chunks))
    for start, end in [(0, 10), (4090, 4100), (size - 5, size), (0, size)]:
        got = asyncio.run(reader.bytes_get(root.hex(), start, end))
        assert got == data[start:end], (start, end)


@pytest.mark.parametrize("size", SIZES)
def test_agrees_with_the_reference_test_splitter(size):
    """conftest.split_content predates this module and is what the existing
    verification fixtures were built with; the library version must not drift
    from it."""
    data = payload(size)
    store: dict = {}
    assert split(data)[0] == split_content(data, store)


@pytest.mark.parametrize("size", SIZES)
def test_deterministic_and_chunks_are_self_addressed(size):
    from swarmfs.bmt import chunk_address

    data = payload(size)
    root, chunks = split(data)
    assert split(data)[0] == root                      # pure function
    assert content_address(data) == root
    for ref, chunk in chunks.items():
        assert chunk_address(chunk) == ref             # every key is its content
    assert root in chunks


def test_single_child_levels_are_promoted_not_wrapped():
    """A level of one must not gain a one-child intermediate: the joiner reads
    span <= 4096 as a leaf, so wrapping would change how the tree is read."""
    root, chunks = split(payload(4096))
    assert len(chunks) == 1                            # exactly the leaf


def test_empty_content():
    root, chunks = split(b"")
    reader = VerifyingReader(FakeClient(chunks))
    assert asyncio.run(reader.bytes_size(root.hex())) == 0
    assert asyncio.run(reader.bytes_get(root.hex())) == b""
