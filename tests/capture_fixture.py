"""Capture a real Bee-produced Mantaray manifest as an offline test fixture.

Uploads a small collection to a live Bee node, then walks the manifest trie
client-side (our own walker), recording every raw manifest-node chunk keyed by
its reference. The resulting JSON lets tests assert our codec parses
*Bee's own bytes* — not just bytes our marshaller round-trips.

    SWARMFS_TEST_BEE=http://localhost:1633 \\
    SWARMFS_TEST_STAMP=<batch-id> \\
    .venv/bin/python tests/capture_fixture.py

Writes tests/fixtures/real_manifest.json.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import tarfile
import urllib.request
from pathlib import Path

from swarmfs._client import SwarmClient
from swarmfs.mantaray import NodeStore, iter_files

BEE = os.environ["SWARMFS_TEST_BEE"]
STAMP = os.environ["SWARMFS_TEST_STAMP"]

# Deliberately exercises shared prefixes that split mid-edge (data / data-old),
# nested directories, and a path longer than one 30-byte prefix segment.
FILES = {
    "index.html": b"<h1>hello from a real bee node</h1>\n",
    "readme.md": b"# swarmfs real-manifest fixture\n",
    "assets/css/site.css": b"body{margin:0}\n",
    "assets/img/logo.svg": b"<svg xmlns='http://www.w3.org/2000/svg'/>\n",
    "data/part-00000.csv": b"a,b,c\n1,2,3\n",
    "data/part-00001.csv": b"a,b,c\n4,5,6\n",
    "data-archive/old.txt": b"archived\n",
    "deeply/nested/directory/tree/with/a/reasonably/long/path/file.bin": b"deep\n",
}


def upload_collection() -> str:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in FILES.items():
            ti = tarfile.TarInfo(name=name)
            ti.size = len(content)
            tar.addfile(ti, io.BytesIO(content))
    req = urllib.request.Request(
        f"{BEE}/bzz",
        data=buf.getvalue(),
        headers={
            "Content-Type": "application/x-tar",
            "Swarm-Postage-Batch-Id": STAMP,
            "Swarm-Collection": "true",
            "Swarm-Index-Document": "index.html",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["reference"]


async def capture(root: str) -> dict:
    client = SwarmClient(BEE)
    nodes: dict[str, str] = {}

    async def recording_load(ref: bytes) -> bytes:
        data = await client.bytes_get(ref.hex())
        nodes[ref.hex()] = data.hex()
        return data

    try:
        store = NodeStore(recording_load)
        entries = [e async for e in iter_files(store, bytes.fromhex(root))]
    finally:
        await client.close()

    return {
        "root": root,
        "nodes": nodes,
        "expected_files": {
            e.path.decode(): {
                "reference": e.reference.hex(),
                "metadata": e.metadata,
            }
            for e in entries
        },
    }


def main() -> None:
    root = upload_collection()
    print(f"uploaded collection, root = {root}")
    fixture = asyncio.run(capture(root))
    out = Path(__file__).parent / "fixtures" / "real_manifest.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(fixture, indent=2, sort_keys=True))
    print(f"captured {len(fixture['nodes'])} manifest nodes, "
          f"{len(fixture['expected_files'])} files -> {out}")


if __name__ == "__main__":
    main()
