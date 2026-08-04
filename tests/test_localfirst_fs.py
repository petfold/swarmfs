"""L3: swarmfs's own write path over the local-first store.

The commit engine lands staged files and manifest nodes on local disk
(BMT-addressed — same refs the node returns with erasure coding off) and
journals each commit; the syncer pushes and confirms in the background;
bzzf feed updates publish only after network confirmation. Offline is the
normal mode: these tests commit against a client that cannot upload.
"""

import asyncio
import time

import pytest

pytest.importorskip("eth_hash")

from conftest import GOOD_STAMP, FakeClient  # noqa: E402

from swarmfs.commit import LocalFirstCommitEngine, StagedWrite  # noqa: E402
from swarmfs.core import SwarmFileSystem  # noqa: E402
from swarmfs.localstore import CONFIRMED, LocalStore  # noqa: E402
from swarmfs.mantaray import unmarshal  # noqa: E402
from swarmfs.splitter import content_address  # noqa: E402

WAIT = 15


class BMTFakeClient(FakeClient):
    """FakeClient whose uploads are BMT-addressed like a real node with
    erasure coding off — so the push's ref-equality assertion holds —
    plus the two endpoints the sync layer needs."""

    async def bytes_post(self, data, stamp, tag=None, pin=False,
                         redundancy=None, deferred=None):
        ref = content_address(data)
        self.store[ref] = data
        self.uploads.append((stamp, len(data)))
        return ref.hex()

    async def stewardship_get(self, ref: str) -> bool:
        return bytes.fromhex(ref) in self.store

    async def stamps_list(self) -> list:
        # GOOD_STAMP's TTL sits exactly at BeeRemote's one-day floor
        return [dict(GOOD_STAMP, batchTTL=30 * 86400)]

    async def stamp_get(self, batch_id: str) -> dict:
        return dict(GOOD_STAMP, batchID=batch_id, batchTTL=30 * 86400)


class OfflineClient:
    """A client that proves the commit path never needed the network."""

    api_url = "offline://"

    def __getattr__(self, name):
        async def refuse(*a, **kw):
            raise ConnectionError(f"offline: {name} called")
        return refuse


def staged(data: bytes) -> StagedWrite:
    return StagedWrite(data, len(data))


# -- the engine, offline -------------------------------------------------------------


def test_engine_commits_fresh_manifest_offline(tmp_path):
    local = LocalStore(str(tmp_path / "store"))
    try:
        engine = LocalFirstCommitEngine(local, client=OfflineClient())
        res = asyncio.run(engine.commit(
            None, {"a.txt": staged(b"alpha"), "b/c.txt": staged(b"beta")},
            []))
        assert local.has_root(res.new_root)
        assert res.batch == ""                        # no stamp spent
        unmarshal(local.get(res.new_root))            # a real manifest node
        by_root = dict(local.roots_below(CONFIRMED))
        state = by_root[res.new_root]
        assert set(res.written.values()) <= set(state.blobs)
        assert state.structure                        # nodes classified
        assert set(state.structure).isdisjoint(res.written.values())
    finally:
        local.close()


def test_engine_chains_lineage_offline(tmp_path):
    local = LocalStore(str(tmp_path / "store"))
    try:
        engine = LocalFirstCommitEngine(local, client=OfflineClient())
        r1 = asyncio.run(engine.commit(
            None, {"a.txt": staged(b"one")}, [])).new_root
        r2 = asyncio.run(engine.commit(
            r1, {"b.txt": staged(b"two")}, [])).new_root
        assert local.parent_of(r2) == r1              # the journal reflog
        # removing b.txt returns the manifest to r1's exact bytes
        # (mantaray prunes empty nodes): the canonical-revisit case — the
        # journal rightly refuses a duplicate event
        r3 = asyncio.run(engine.commit(r2, {}, ["b.txt"])).new_root
        assert r3 == r1
        assert local.parent_of(r3) is None            # r1's original entry
    finally:
        local.close()


def test_engine_patches_foreign_lineage_via_fallback(tmp_path):
    """A manifest that was never local (opened from a remote ref): parent
    nodes load through the node transiently and are NOT persisted — no
    forever-pinned orphans; the new root starts a fresh lineage."""
    store = {}
    client = BMTFakeClient(store)
    donor = LocalStore(str(tmp_path / "donor"))
    try:
        engine = LocalFirstCommitEngine(donor, client=OfflineClient())
        foreign = asyncio.run(engine.commit(
            None, {"a.txt": staged(b"remote data")}, [])).new_root
        for _, state in donor.roots_below(CONFIRMED):
            for ref in state.blobs:                  # "publish" donor blobs
                store[bytes.fromhex(ref)] = donor.get(ref)
    finally:
        donor.close()

    local = LocalStore(str(tmp_path / "mine"))
    try:
        engine = LocalFirstCommitEngine(local, client=client)
        res = asyncio.run(engine.commit(
            foreign, {"b.txt": staged(b"my addition")}, []))
        assert local.parent_of(res.new_root) is None  # fresh lineage
        assert not local.has_local(foreign)           # not persisted
    finally:
        local.close()


def test_engine_refuses_wrong_addressing(tmp_path):
    local = LocalStore(str(tmp_path / "s"), addressing="sha256")
    try:
        with pytest.raises(ValueError, match='addressing="swarm"'):
            LocalFirstCommitEngine(local, client=OfflineClient())
    finally:
        local.close()


# -- the filesystem ---------------------------------------------------------------------


def test_fs_requires_redundancy_off(tmp_path):
    with pytest.raises(ValueError, match="redundancy=0"):
        SwarmFileSystem(client=BMTFakeClient({}),
                        local_store=str(tmp_path / "s"),
                        skip_instance_cache=True)  # default redundancy=2


def test_fs_writes_commit_offline_then_sync(tmp_path):
    # autocommit writes (the distro fsspec's Transaction.__exit__ is
    # broken in this environment — the same known issue as test_write_fs;
    # the engine path is identical either way)
    store = {}
    fs = SwarmFileSystem(client=BMTFakeClient(store),
                         local_store=str(tmp_path / "s"),
                         redundancy=0, skip_instance_cache=True)
    fs.pipe_file("bzz://new/a.txt", b"alpha")
    fs.pipe_file(f"bzz://{fs.latest('new')}/dir/b.txt", b"beta")
    root = fs.latest("new")
    assert fs._local.has_root(root)                  # committed locally
    fs.sync(timeout=WAIT)                            # pushed + confirmed
    st = fs.sync_status()
    assert all(r == CONFIRMED for r in st.roots.values())
    assert bytes.fromhex(root) in store              # the root reached "Swarm"
    assert fs.cat(f"bzz://{root}/a.txt") == b"alpha"  # readable via the node


def test_bzzf_feed_publishes_only_after_confirmation(tmp_path):
    pytest.importorskip("eth_keys")
    from swarmfs.feedfs import SwarmFeedFileSystem

    store = {}
    client = BMTFakeClient(store)
    fs = SwarmFeedFileSystem(client=client, signer="11" * 32,
                             local_store=str(tmp_path / "s"),
                             redundancy=0, skip_instance_cache=True)
    owner = fs.signer.owner_hex
    path = f"bzzf://{owner}/notes/doc.txt"
    fs.pipe_file(path, b"first version")             # autocommit, offline-fast
    assert fs._feed_pending                          # deferred, not published
    fs.sync(timeout=WAIT)
    deadline = time.time() + WAIT                    # the publish itself runs
    while fs._feed_pending and time.time() < deadline:  # on the worker after
        time.sleep(0.05)                             # the confirmed event
    assert not fs._feed_pending
    assert fs.cat(path) == b"first version"          # resolves via the feed
    st = fs.sync_status()
    assert any(k.startswith("feed!") for k in st.remote_roots)
