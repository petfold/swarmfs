"""Client-side chunk verification and the gateway opt-in.

Fixtures store content under genuine BMT addresses (split_content is the
inverse of the verifying joiner), so verification here is real: flip one
byte anywhere and the read must fail.
"""

from __future__ import annotations

import asyncio
import random

import pytest

pytest.importorskip("eth_hash")

from swarmfs import SwarmFileSystem  # noqa: E402
from swarmfs.join import VerificationError, VerifyingReader  # noqa: E402

from conftest import FakeClient, FakeGatewayClient, build_manifest_ca, split_content  # noqa: E402


def run(coro):
    return asyncio.run(coro)


def payload(n: int, seed: int = 42) -> bytes:
    return random.Random(seed).randbytes(n)


# sizes across every tree shape: empty, single leaf, leaf boundaries,
# two-level, and a three-level tree (> 128 * 4096)
SIZES = [0, 1, 4095, 4096, 4097, 10_000, 128 * 4096, 128 * 4096 + 1, 600_000]


@pytest.mark.parametrize("size", SIZES)
def test_roundtrip_and_size(size):
    data = payload(size)
    store: dict = {}
    root = split_content(data, store)
    reader = VerifyingReader(FakeClient(store))
    assert run(reader.bytes_size(root.hex())) == size
    assert run(reader.bytes_get(root.hex())) == data


@pytest.mark.parametrize("size", [10_000, 600_000])
def test_range_reads(size):
    data = payload(size)
    store: dict = {}
    root = split_content(data, store).hex()
    client = FakeClient(store)
    reader = VerifyingReader(client)
    for start, end in [
        (0, 10),
        (4090, 4100),  # crosses a leaf boundary
        (size - 5, size),
        (size // 2, size // 2 + 9000),  # spans several leaves
        (0, size),
        (size + 10, size + 20),  # beyond EOF -> empty
    ]:
        assert run(reader.bytes_get(root, start, end)) == data[start:end], (start, end)


def test_range_reads_fetch_only_needed_subtrees():
    data = payload(600_000)  # 3-level tree, 147 leaves
    store: dict = {}
    root = split_content(data, store).hex()

    class CountingClient(FakeClient):
        def __init__(self, store):
            super().__init__(store)
            self.chunk_fetches = 0

        async def chunk_get(self, ref):
            self.chunk_fetches += 1
            return await super().chunk_get(ref)

    client = CountingClient(store)
    reader = VerifyingReader(client)
    assert run(reader.bytes_get(root, 8192, 8292)) == data[8192:8292]
    # root + one intermediate + one leaf — not the whole tree
    assert client.chunk_fetches <= 4, client.chunk_fetches


def test_bytes_iter_streams_verified():
    data = payload(50_000)
    store: dict = {}
    root = split_content(data, store).hex()
    reader = VerifyingReader(FakeClient(store))

    async def collect():
        return b"".join([piece async for piece in reader.bytes_iter(root)])

    assert run(collect()) == data


@pytest.mark.parametrize("victim", ["leaf", "root"])
def test_corruption_detected(victim):
    data = payload(50_000)
    store: dict = {}
    root = split_content(data, store)
    # corrupt one byte of one stored chunk
    if victim == "root":
        target = root
    else:
        target = next(r for r, c in store.items() if len(c) == 4104 and r != root)
    chunk = bytearray(store[target])
    chunk[100] ^= 0xFF
    store[target] = bytes(chunk)

    reader = VerifyingReader(FakeClient(store))
    with pytest.raises(VerificationError, match="failed verification"):
        run(reader.bytes_get(root.hex()))


def test_encrypted_refs_refused():
    reader = VerifyingReader(FakeClient({}))
    with pytest.raises(VerificationError, match="encrypted"):
        run(reader.bytes_get("ab" * 64))


# ---------------------------------------------------------------- fs level


FILES = {
    "docs/readme.md": payload(500, seed=1),
    "data/big.bin": payload(20_000, seed=2),
}


def test_verified_filesystem_end_to_end():
    root, store = build_manifest_ca(FILES)
    fs = SwarmFileSystem(client=FakeClient(store), verify=True, skip_instance_cache=True)
    assert fs.verify is True

    # listing walks the manifest through verified fetches
    assert fs.find(f"bzz://{root}") == sorted(f"{root}/{p}" for p in FILES)
    info = fs.info(f"bzz://{root}/data/big.bin")
    assert info["size"] == 20_000
    assert fs.cat_file(f"bzz://{root}/data/big.bin") == FILES["data/big.bin"]
    assert (
        fs.cat_file(f"bzz://{root}/data/big.bin", start=5000, end=6000)
        == FILES["data/big.bin"][5000:6000]
    )
    with fs.open(f"bzz://{root}/data/big.bin", block_size=4096) as f:
        f.seek(12_345)
        assert f.read(100) == FILES["data/big.bin"][12_345:12_445]


def test_verified_filesystem_detects_corrupt_manifest_node():
    root, store = build_manifest_ca(FILES)
    # corrupt the root manifest node itself
    root_ref = bytes.fromhex(root)
    chunk = bytearray(store[root_ref])
    chunk[-1] ^= 0x01
    store[root_ref] = bytes(chunk)
    fs = SwarmFileSystem(client=FakeClient(store), verify=True, skip_instance_cache=True)
    with pytest.raises(VerificationError):
        fs.ls(f"bzz://{root}")


# ------------------------------------------------------------ gateway gate


def test_gateway_refused_without_opt_in(manifest):
    root, store = manifest
    fs = SwarmFileSystem(client=FakeGatewayClient(store), skip_instance_cache=True)
    with pytest.raises(PermissionError, match="allow_gateway=True"):
        fs.ls(f"bzz://{root}")


def test_gateway_opt_in_enables_verification_by_default():
    root, store = build_manifest_ca(FILES)
    fs = SwarmFileSystem(
        client=FakeGatewayClient(store), allow_gateway=True, skip_instance_cache=True
    )
    # reads work, and are verified by default on the gateway path
    assert fs.cat_file(f"bzz://{root}/docs/readme.md") == FILES["docs/readme.md"]
    assert fs.verify_active is True
    assert fs.trusted is False


def test_gateway_verification_can_be_disabled_explicitly(manifest):
    root, store = manifest  # sha256-keyed store: only readable unverified
    fs = SwarmFileSystem(
        client=FakeGatewayClient(store),
        allow_gateway=True,
        verify=False,
        skip_instance_cache=True,
    )
    assert fs.ls(f"bzz://{root}", detail=False)
    assert fs.verify_active is False


def test_own_node_trusted_and_unverified_by_default(manifest):
    root, store = manifest
    fs = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    fs.ls(f"bzz://{root}")
    assert fs.trusted is True
    assert fs.verify_active is False
