"""Shared offline fixtures: build real Mantaray tries into an in-memory store
and serve them through a fake client, so the full stack (codec → walker →
filesystem) is exercised without a Bee node."""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from swarmfs.mantaray import Node, add, save

# A tree that exercises the interesting trie shapes: shared prefixes that
# split mid-edge ("data" vs "data-old"), nested dirs, >30-byte paths.
FILES = {
    "index.html": b"<h1>hello swarm</h1>",
    "assets/css/site.css": b"body { margin: 0 }",
    "assets/img/logo.png": b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
    "data/part-00000.parquet": b"PAR0" * 1024,
    "data/part-00001.parquet": b"PAR1" * 2048,
    "data-old/readme.md": b"# archived",
    "a/very/deeply/nested/directory/structure/with/a/long/path/file.bin": b"deep",
}

METADATA = {
    "index.html": {"Content-Type": "text/html; charset=utf-8", "Filename": "index.html"},
    "data/part-00000.parquet": {"Content-Type": "application/octet-stream"},
}


def build_manifest(
    files: dict[str, bytes],
    metadata: dict[str, dict[str, str]] | None = None,
    root_metadata: dict[str, str] | None = None,
) -> tuple[str, dict[bytes, bytes]]:
    """Build a trie for ``{path: content}``. Returns (root ref hex, store),
    where the store maps reference -> bytes for manifest nodes and file
    contents alike (sha256 stands in for the BMT hash; only internal
    consistency matters offline)."""
    store: dict[bytes, bytes] = {}

    def put(data: bytes) -> bytes:
        ref = hashlib.sha256(data).digest()
        store[ref] = data
        return ref

    async def saver(data: bytes) -> bytes:
        return put(data)

    async def build() -> bytes:
        root = Node()
        for path, content in files.items():
            add(root, path.encode(), put(content), (metadata or {}).get(path))
        if root_metadata:
            # bee stores manifest-level metadata (index document etc.) at "/"
            add(root, b"/", b"", root_metadata)
        return await save(root, saver)

    return asyncio.run(build()).hex(), store


class FakeClient:
    """Duck-typed SwarmClient over the in-memory store."""

    def __init__(self, store: dict[bytes, bytes]):
        self.store = store
        self.api_url = "fake://"

    async def bytes_get(self, ref: str, start=None, end=None) -> bytes:
        data = self.store.get(bytes.fromhex(ref))
        if data is None:
            raise FileNotFoundError(ref)
        if start is None and end is None:
            return data
        return data[start or 0 : end]

    async def bytes_size(self, ref: str) -> int:
        data = self.store.get(bytes.fromhex(ref))
        if data is None:
            raise FileNotFoundError(ref)
        return len(data)

    async def bzz_get(self, ref: str, path: str = "", start=None, end=None) -> bytes:
        raise FileNotFoundError(f"/bzz/{ref}/{path} (fake client has no index resolution)")

    async def bytes_iter(self, ref: str, chunk_size: int = 1 << 20):
        data = await self.bytes_get(ref)
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def close(self) -> None:
        pass


@pytest.fixture()
def manifest():
    root_hex, store = build_manifest(
        FILES, METADATA, root_metadata={"website-index-document": "index.html"}
    )
    return root_hex, store


@pytest.fixture()
def fs(manifest):
    from swarmfs import SwarmFileSystem

    root_hex, store = manifest
    fs = SwarmFileSystem(client=FakeClient(store), skip_instance_cache=True)
    return fs, root_hex
