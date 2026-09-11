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
            await add(root, path.encode(), put(content), (metadata or {}).get(path))
        if root_metadata:
            # bee stores manifest-level metadata (index document etc.) at "/"
            await add(root, b"/", b"", root_metadata)
        return await save(root, saver)

    return asyncio.run(build()).hex(), store


GOOD_STAMP = {
    "batchID": "ab" * 32,
    "usable": True,
    "batchTTL": 86400,
    "utilizationRatio": 0.25,
    "label": "test-stamp",
    "immutableFlag": True,
}


_ACT_SIDE_TABLES: dict[int, tuple[dict, dict]] = {}

# The fake node's own compressed public key: what /addresses reports and
# what ACT reads through this "node" must name as the publisher.
FAKE_PUBLIC_KEY = "03" + "ab" * 32


class FakeClient:
    """Duck-typed SwarmClient over the in-memory store.

    ACT is emulated to Bee 2.8.2's measured contract (swarmfs.act): a
    protected upload stores the content under its ordinary reference and
    returns an opaque ACT reference of the same length that maps to it;
    reading the ACT reference needs the right history *and* publisher
    (else 404), reading it bare is a 404, and reading a plain reference
    *with* ACT headers is a 404 too. Grantee lists live in a dict.
    """

    def __init__(self, store: dict[bytes, bytes], stamps: list[dict] | None = None):
        self.store = store
        self.api_url = "fake://"
        self.stamps = [GOOD_STAMP] if stamps is None else stamps
        self.uploads: list[tuple[str, int]] = []  # (stamp, nbytes) per POST /bytes|/bzz
        self.redundancies: list[int | None] = []  # redundancy level per POST /bytes|/bzz
        self.bzz_uploads: list[tuple] = []  # (filename, content_type, encrypt) per POST /bzz
        self.publisher = FAKE_PUBLIC_KEY
        # ACT reference -> (real reference, history), and grantee lists —
        # keyed by the store's identity so several FakeClients over one
        # store (two "processes" on one swarm) agree, without polluting the
        # store itself (tests assert on its keys)
        side = _ACT_SIDE_TABLES.setdefault(id(store), ({}, {}))
        self.act_map: dict[bytes, tuple[bytes, str]] = side[0]
        self.grantee_lists: dict[str, tuple[list[str], str]] = side[1]
        self.act_reads: list[tuple[str, str]] = []  # (ref, history) per ACT-headed read

    # -- ACT emulation helpers ---------------------------------------------

    @staticmethod
    def _act_check(act: bool, act_history) -> None:
        if act_history is not None and not act:
            raise ValueError("act_history given but act=False")

    def _act_wrap(self, ref: bytes, act_history: str | None) -> tuple[bytes, str]:
        history = act_history or hashlib.sha256(b"history" + ref).hexdigest()
        act_ref = hashlib.sha256(b"act" + ref + history.encode()).digest()
        if len(ref) == 64:  # encrypted content: the ACT ref is 128 hex too
            act_ref += hashlib.sha256(b"act-key" + ref).digest()
        self.act_map[act_ref] = (ref, history)
        return act_ref, history

    def _act_resolve(self, ref: str, act) -> bytes:
        rb = bytes.fromhex(ref)
        if act is None:
            if rb in self.act_map:
                raise FileNotFoundError(f"{ref} is ACT-protected (no headers)")
            return rb
        entry = self.act_map.get(rb)
        if entry is None:
            raise FileNotFoundError(f"{ref} with ACT headers is not an ACT reference")
        real, history = entry
        if act.history != history or act.publisher != self.publisher:
            raise FileNotFoundError(f"{ref}: wrong history or publisher")
        self.act_reads.append((ref, history))
        return real

    async def addresses(self) -> dict:
        return {"publicKey": self.publisher, "ethereum": "0x" + "11" * 20}

    async def grantee_create(self, grantees, stamp: str):
        from swarmfs.act import GranteeList

        keys = list(grantees)
        ref = hashlib.sha256(("grantees:" + ",".join(keys)).encode()).hexdigest() * 2
        history = hashlib.sha256(("ghist:" + ref).encode()).hexdigest()
        self.grantee_lists[ref] = (keys, history)
        self.uploads.append((stamp, len(keys)))
        return GranteeList(ref, history)

    async def grantee_get(self, reference: str) -> list[str]:
        if reference not in self.grantee_lists:
            raise FileNotFoundError(reference)
        return list(self.grantee_lists[reference][0])

    async def grantee_patch(self, reference, history, stamp, add=None, revoke=None):
        from swarmfs.act import GranteeList

        if reference not in self.grantee_lists:
            raise FileNotFoundError(reference)
        keys, hist = self.grantee_lists[reference]
        if hist != history:
            raise FileNotFoundError(f"{reference}: wrong history")
        keys = [k for k in keys if k not in (revoke or [])] + [k for k in (add or []) if k not in keys]
        ref = hashlib.sha256(("grantees:" + ",".join(keys) + reference).encode()).hexdigest() * 2
        new_hist = hashlib.sha256(("ghist:" + ref + history).encode()).hexdigest()
        self.grantee_lists[ref] = (keys, new_hist)
        self.uploads.append((stamp, len(keys)))
        return GranteeList(ref, new_hist)

    # -- reads ---------------------------------------------------------------

    async def bytes_get(self, ref: str, start=None, end=None, act=None) -> bytes:
        data = self.store.get(self._act_resolve(ref, act))
        if data is None:
            raise FileNotFoundError(ref)
        if start is None and end is None:
            return data
        return data[start or 0 : end]

    async def bytes_size(self, ref: str, act=None) -> int:
        data = self.store.get(self._act_resolve(ref, act))
        if data is None:
            raise FileNotFoundError(ref)
        return len(data)

    async def bzz_get(self, ref: str, path: str = "", start=None, end=None, act=None) -> bytes:
        raise FileNotFoundError(f"/bzz/{ref}/{path} (fake client has no index resolution)")

    async def bytes_iter(self, ref: str, chunk_size: int = 1 << 20, act=None):
        data = await self.bytes_get(ref, act=act)
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def feed_head(self, owner: str, topic: str) -> tuple[str, str] | None:
        """Emulate Bee's sequence lookup: scan indexes from 0 until a gap."""
        from swarmfs.feeds import feed_identifier, soc_address

        ob, tb = bytes.fromhex(owner), bytes.fromhex(topic)
        index = None
        i = 0
        while soc_address(feed_identifier(tb, i), ob) in self.store:
            index = i
            i += 1
        if index is None:
            return None
        return index.to_bytes(8, "big").hex(), (index + 1).to_bytes(8, "big").hex()

    async def chunk_get(self, ref: str) -> bytes:
        data = self.store.get(bytes.fromhex(ref))
        if data is None:
            raise FileNotFoundError(ref)
        return data

    async def soc_post(
        self, owner: str, identifier: str, signature: str, data: bytes, stamp: str
    ) -> str:
        """Store a single-owner chunk, verifying the signature the way Bee
        does: recover the signer from the personal-sign digest over
        keccak256(identifier + wrapped chunk address)."""
        from eth_keys import keys

        from swarmfs.bmt import chunk_address, keccak256
        from swarmfs.feeds import soc_address

        ob = bytes.fromhex(owner)
        ib = bytes.fromhex(identifier)
        sig = bytes.fromhex(signature)
        digest = keccak256(ib + chunk_address(data))
        prefixed = keccak256(b"\x19Ethereum Signed Message:\n32" + digest)
        recovered = keys.Signature(
            vrs=(
                sig[64] - 27,
                int.from_bytes(sig[:32], "big"),
                int.from_bytes(sig[32:64], "big"),
            )
        ).recover_public_key_from_msg_hash(prefixed)
        assert recovered.to_canonical_address() == ob, "SOC signature does not match owner"

        addr = soc_address(ib, ob)
        self.store[addr] = ib + sig + data  # SOC chunk data layout
        self.uploads.append((stamp, len(data)))
        return addr.hex()

    async def bytes_post(self, data, stamp: str, tag=None, pin=False,
                         redundancy=None, deferred=None, encrypt=False,
                         act=False, act_history=None):
        from swarmfs.act import ActUpload

        self._act_check(act, act_history)
        if not isinstance(data, bytes):
            data.seek(0)
            data = data.read()
        if encrypt:
            # Bee's swarm-encrypt contract: the returned reference is
            # address ‖ decryption key (64 bytes); only the full reference
            # yields the plaintext back
            ref = (hashlib.sha256(data).digest()
                   + hashlib.sha256(b"key" + data).digest())
        else:
            ref = hashlib.sha256(data).digest()
        self.store[ref] = data
        self.uploads.append((stamp, len(data)))
        self.redundancies.append(redundancy)
        if act:
            act_ref, history = self._act_wrap(ref, act_history)
            return ActUpload(act_ref.hex(), history)
        return ref.hex()

    async def bzz_post(
        self,
        data,
        stamp: str,
        filename=None,
        content_type=None,
        encrypt=False,
        pin=False,
        redundancy=None,
        act=False,
        act_history=None,
    ):
        """Emulate Bee's single-file /bzz upload: store the content and wrap
        it in a manifest with the filename as the index document."""
        from swarmfs.act import ActUpload
        from swarmfs.mantaray import Node, add, save

        self._act_check(act, act_history)
        if not isinstance(data, bytes):
            data.seek(0)
            data = data.read()
        ref = hashlib.sha256(data).digest()
        self.store[ref] = data
        self.uploads.append((stamp, len(data)))
        self.redundancies.append(redundancy)
        name = filename or "file"
        self.bzz_uploads.append((name, content_type, encrypt))
        node = Node()
        await add(
            node,
            name.encode(),
            ref,
            {"Content-Type": content_type or "application/octet-stream", "Filename": name},
        )
        await add(node, b"/", b"", {"website-index-document": name})

        async def saver(d: bytes) -> bytes:
            r = hashlib.sha256(d).digest()
            self.store[r] = d
            return r

        root = await save(node, saver)
        if act:
            act_ref, history = self._act_wrap(root, act_history)
            return ActUpload(act_ref.hex(), history)
        return root.hex()

    async def stamps_list(self) -> list[dict]:
        return self.stamps

    async def tag_create(self) -> int:
        return 1

    async def tag_get(self, uid: int) -> dict:
        return {"uid": uid}

    async def health(self) -> dict:
        return {"status": "ok", "version": "fake"}

    async def close(self) -> None:
        pass


class FakeGatewayClient(FakeClient):
    """Read-only endpoint that blocks the node-owner API, like a public
    gateway: /stamps is not available, so trust detection must fail."""

    def __init__(self, store):
        super().__init__(store)
        self.api_url = "https://gateway.example"

    async def stamps_list(self):
        raise PermissionError("403 for /stamps (gateway)")


def split_content(data: bytes, store: dict[bytes, bytes]) -> bytes:
    """Split content into a Swarm hash tree with genuine BMT addresses —
    the inverse of the verifying joiner. Returns the root reference."""
    from swarmfs.bmt import chunk_address

    def put(chunk: bytes) -> bytes:
        ref = chunk_address(chunk)
        store[ref] = chunk
        return ref

    if not data:
        return put(bytes(8))
    level: list[tuple[bytes, int]] = []
    for i in range(0, len(data), 4096):
        part = data[i : i + 4096]
        level.append((put(len(part).to_bytes(8, "little") + part), len(part)))
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 128):
            group = level[i : i + 128]
            if len(group) == 1:
                # single children are promoted, not wrapped (bee's splitter
                # does the same; the joiner treats span<=4096 as a leaf)
                nxt.append(group[0])
                continue
            span = sum(s for _, s in group)
            payload = b"".join(r for r, _ in group)
            nxt.append((put(span.to_bytes(8, "little") + payload), span))
        level = nxt
    return level[0][0]


def build_manifest_ca(
    files: dict[str, bytes],
    metadata: dict[str, dict[str, str]] | None = None,
) -> tuple[str, dict[bytes, bytes]]:
    """Like build_manifest, but everything is stored under its genuine BMT
    address (manifest nodes as single chunks, contents split into trees),
    so the verifying read path can check every fetch."""
    from swarmfs.bmt import cac_data, chunk_address

    store: dict[bytes, bytes] = {}

    async def saver(data: bytes) -> bytes:
        assert len(data) <= 4096, "manifest node exceeds one chunk (unsupported in fixture)"
        chunk = cac_data(data)
        ref = chunk_address(chunk)
        store[ref] = chunk
        return ref

    async def build() -> bytes:
        root = Node()
        for path, content in files.items():
            entry = split_content(content, store)
            await add(root, path.encode(), entry, (metadata or {}).get(path))
        return await save(root, saver)

    return asyncio.run(build()).hex(), store


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
