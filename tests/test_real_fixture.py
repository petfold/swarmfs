"""Cross-check the codec against a manifest produced by a real Bee node.

The fixture in ``fixtures/real_manifest.json`` was captured from an actual Bee
upload (see ``capture_fixture.py``); asserting against it catches wire-format
drift that a pure marshal→unmarshal round-trip cannot, since both halves of a
round-trip share our own code. Runs fully offline.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from swarmfs.mantaray import NodeStore, iter_files, list_directory, locate

FIXTURE = Path(__file__).parent / "fixtures" / "real_manifest.json"

pytestmark = pytest.mark.skipif(
    not FIXTURE.exists(),
    reason="run tests/capture_fixture.py against a live Bee node to generate the fixture",
)


def load_fixture():
    data = json.loads(FIXTURE.read_text())
    nodes = {bytes.fromhex(k): bytes.fromhex(v) for k, v in data["nodes"].items()}
    return data["root"], nodes, data["expected_files"]


def make_store(nodes):
    async def load(ref: bytes) -> bytes:
        return nodes[ref]

    return NodeStore(load)


def run(coro):
    return asyncio.run(coro)


def test_codec_parses_real_manifest():
    root, nodes, expected = load_fixture()

    async def walk():
        store = make_store(nodes)
        return {
            e.path.decode(): {"reference": e.reference.hex(), "metadata": e.metadata}
            async for e in iter_files(store, bytes.fromhex(root))
        }

    got = run(walk())
    assert got == expected


def test_real_manifest_metadata_present():
    """Real Bee attaches Content-Type + Filename to every file fork."""
    root, nodes, expected = load_fixture()
    for path, info in expected.items():
        assert info["metadata"], f"{path} lost its metadata"
        assert "Content-Type" in info["metadata"]


def test_locate_and_list_on_real_manifest():
    root, nodes, expected = load_fixture()
    store = make_store(nodes)
    root_ref = bytes.fromhex(root)

    # a directory that only exists mid-edge (data / data-archive share "data")
    files, dirs = run(list_directory(store, root_ref, b"data"))
    assert sorted(f.path.decode() for f in files) == ["part-00000.csv", "part-00001.csv"]

    # exact-file lookup resolves to a value fork with the right data reference
    loc = run(locate(store, root_ref, b"index.html"))
    assert loc is not None and loc.fork is not None and loc.fork.is_value
    child = run(store.resolve(loc.fork))
    assert child.entry.hex() == expected["index.html"]["reference"]
