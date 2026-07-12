"""Patching persisted tries — exercised against the manifest a real Bee node
produced (fixtures/real_manifest.json), so copy-on-write resolution and the
minimal-re-upload property are proven on Bee's own bytes."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from swarmfs.mantaray import NodeStore, add, iter_files, list_directory, remove, save, unmarshal

FIXTURE = Path(__file__).parent / "fixtures" / "real_manifest.json"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(),
    reason="run tests/capture_fixture.py against a live Bee node to generate the fixture",
)


def run(coro):
    return asyncio.run(coro)


class PatchStore:
    """A loader/saver over the fixture nodes plus anything newly saved."""

    def __init__(self):
        data = json.loads(FIXTURE.read_text())
        self.root = data["root"]
        self.nodes = {bytes.fromhex(k): bytes.fromhex(v) for k, v in data["nodes"].items()}
        self.expected = data["expected_files"]
        self.saved: list[bytes] = []  # refs written during the patch

    async def load(self, ref: bytes) -> bytes:
        return self.nodes[ref]

    async def saver(self, data: bytes) -> bytes:
        ref = hashlib.sha256(data).digest()
        self.nodes[ref] = data
        self.saved.append(ref)
        return ref

    async def patch_root(self):
        return unmarshal(await self.load(bytes.fromhex(self.root)))

    def walk(self, root_ref: bytes) -> dict[str, dict]:
        async def go():
            store = NodeStore(self.load)
            return {
                e.path.decode(): {"reference": e.reference.hex(), "metadata": e.metadata}
                async for e in iter_files(store, root_ref)
            }

        return run(go())


NEW_ENTRY = bytes([0x42]) * 32


def test_add_file_to_real_manifest_minimal_reupload():
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await add(
            root,
            b"data/part-00002.csv",
            NEW_ENTRY,
            {"Content-Type": "text/csv; charset=utf-8", "Filename": "part-00002.csv"},
            ps.load,
        )
        return await save(root, ps.saver)

    new_root = run(patch())

    # only the nodes along data/part-00002.csv were re-serialized — a small
    # constant, far below the 16 nodes of the whole trie
    assert 1 < len(ps.saved) <= 5, f"re-uploaded {len(ps.saved)} nodes"

    listing = ps.walk(new_root)
    assert listing.pop("data/part-00002.csv") == {
        "reference": NEW_ENTRY.hex(),
        "metadata": {"Content-Type": "text/csv; charset=utf-8", "Filename": "part-00002.csv"},
    }
    # every pre-existing file survives untouched, metadata included
    assert listing == ps.expected


def test_overwrite_keeps_metadata_when_not_given():
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await add(root, b"index.html", NEW_ENTRY, None, ps.load)
        return await save(root, ps.saver)

    listing = ps.walk(run(patch()))
    assert listing["index.html"]["reference"] == NEW_ENTRY.hex()
    # the old fork's metadata was seeded into the materialized node
    assert listing["index.html"]["metadata"] == ps.expected["index.html"]["metadata"]
    others = {p: v for p, v in listing.items() if p != "index.html"}
    assert others == {p: v for p, v in ps.expected.items() if p != "index.html"}


def test_split_edge_add():
    """data / data-archive share an edge; adding data-live/x splits it again."""
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await add(root, b"data-live/x.bin", NEW_ENTRY, None, ps.load)
        return await save(root, ps.saver)

    new_root = run(patch())
    listing = ps.walk(new_root)
    assert listing["data-live/x.bin"]["reference"] == NEW_ENTRY.hex()
    assert set(ps.expected) < set(listing)

    files, dirs = run(list_directory(NodeStore(ps.load), new_root, b""))
    assert b"data" in dirs and b"data-archive" in dirs and b"data-live" in dirs


def test_remove_file_from_real_manifest():
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await remove(root, b"data/part-00001.csv", ps.load)
        return await save(root, ps.saver)

    listing = ps.walk(run(patch()))
    expected = {p: v for p, v in ps.expected.items() if p != "data/part-00001.csv"}
    assert listing == expected


def test_remove_last_file_prunes_directory():
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await remove(root, b"assets/css/site.css", ps.load)
        await remove(root, b"assets/img/logo.svg", ps.load)
        return await save(root, ps.saver)

    new_root = run(patch())
    listing = ps.walk(new_root)
    assert not any(p.startswith("assets/") for p in listing)
    _, dirs = run(list_directory(NodeStore(ps.load), new_root, b""))
    assert b"assets" not in dirs


def test_remove_missing_raises():
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await remove(root, b"no/such/file.txt", ps.load)

    with pytest.raises(FileNotFoundError):
        run(patch())


def test_patch_requires_loader():
    ps = PatchStore()

    async def patch():
        root = await ps.patch_root()
        await add(root, b"data/part-00002.csv", NEW_ENTRY, None, load=None)

    with pytest.raises(ValueError, match="loader"):
        run(patch())


def test_deep_add_long_path():
    ps = PatchStore()
    path = b"deeply/nested/directory/tree/with/a/reasonably/long/path/second.bin"

    async def patch():
        root = await ps.patch_root()
        await add(root, path, NEW_ENTRY, None, ps.load)
        return await save(root, ps.saver)

    listing = ps.walk(run(patch()))
    assert listing[path.decode()]["reference"] == NEW_ENTRY.hex()
    assert set(ps.expected) < set(listing)
