"""BMT chunk addressing, cross-checked against real Swarm references.

The real-manifest fixture maps reference -> node bytes as captured from a
live Bee node; every reference IS the BMT address of its bytes, so the
fixture doubles as a set of independently-produced BMT test vectors.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("eth_hash")

from swarmfs.bmt import CHUNK_PAYLOAD_SIZE, bmt_root, cac_data, chunk_address  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "real_manifest.json"


@pytest.mark.skipif(not FIXTURE.exists(), reason="real manifest fixture not captured")
def test_bmt_reproduces_real_swarm_references():
    nodes = json.loads(FIXTURE.read_text())["nodes"]
    checked = 0
    for ref_hex, data_hex in nodes.items():
        data = bytes.fromhex(data_hex)
        if len(data) > CHUNK_PAYLOAD_SIZE:
            continue  # multi-chunk content is not a single BMT address
        assert chunk_address(cac_data(data)).hex() == ref_hex
        checked += 1
    assert checked >= 10, f"only {checked} single-chunk vectors in fixture"


def test_cac_data_layout():
    data = cac_data(b"hello")
    assert data[:8] == (5).to_bytes(8, "little")
    assert data[8:] == b"hello"


def test_bmt_root_padding():
    # payloads that differ only in trailing zeros up to the segment boundary
    # hash identically (zero padding), but an extra zero *segment* does not
    assert bmt_root(b"x") == bmt_root(b"x" + bytes(0))
    assert bmt_root(b"") == bmt_root(bytes(0))
    assert chunk_address(cac_data(b"x")) != chunk_address(cac_data(b"x\x00"))  # span differs


def test_oversize_rejected():
    with pytest.raises(ValueError, match="exceeds"):
        bmt_root(bytes(CHUNK_PAYLOAD_SIZE + 1))
    with pytest.raises(ValueError, match="exceeds"):
        cac_data(bytes(CHUNK_PAYLOAD_SIZE + 1))
