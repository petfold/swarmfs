"""The sync worker's contract: roots climb the durability ladder in the
background, the journal never over-claims, and a crash at any point during
a push recovers by idempotent re-push.

Everything here drives `Syncer` against a protocol-level `FakeRemote`
(failure injection, wrong bytes, stewardship lies); the one test that
talks to a real Bee is gated on SWARMFS_TEST_BEE like the rest of the
integration layer. Timings: debounce/staleness are near-zero so rounds
fire immediately; assertions use generous wait_for timeouts, never sleeps.
"""

import hashlib
import os

import pytest

from swarmfs.localstore import (
    BlobVerificationFailed,
    CONFIRMED,
    COMMITTED,
    LocalStore,
    PUSHED,
)
from swarmfs.localsync import BeeRemote, SyncPolicy, Syncer

WAIT = 10  # generous; tests pass in milliseconds when healthy


def fast_policy(**kw):
    kw.setdefault("debounce", 0.0)
    kw.setdefault("max_staleness", 0.1)
    kw.setdefault("backoff_base", 0.02)
    kw.setdefault("backoff_max", 0.05)
    kw.setdefault("confirm_sample", 1.0)  # verify everything in tests
    return SyncPolicy(**kw)


class FakeRemote:
    """Protocol twin of BeeRemote with failure injection."""

    def __init__(self):
        self.blobs = {}
        self.pushes = 0
        self.fail_after = None    # raise after N successful pushes
        self.retrievable = True
        self.corrupt = {}         # ref -> bytes served instead

    def push_blob(self, ref, data, deferred=True):
        if self.fail_after is not None and self.pushes >= self.fail_after:
            raise ConnectionError("injected: network down")
        self.blobs[ref] = data
        self.pushes += 1

    def fetch(self, ref):
        if ref in self.corrupt:
            return self.corrupt[ref]
        return self.blobs[ref]

    def is_retrievable(self, ref):
        return self.retrievable and ref in self.blobs

    def batch_info(self):
        return "fakebatch", 1e9


@pytest.fixture
def store(tmp_path):
    s = LocalStore(str(tmp_path / "store"), addressing="sha256")
    yield s
    s.close()


def commit_blobs(store, *datas, parent=None):
    refs = [store.put(d) for d in datas]
    root = refs[0]
    store.commit_root(root, parent, refs)
    return root, refs


# -- the happy path ------------------------------------------------------------


def test_commit_reaches_network_confirmed(store):
    remote = FakeRemote()
    with Syncer(store, remote, fast_policy()):
        root, refs = commit_blobs(store, b"a" * 10, b"b" * 10)
        assert store.wait_for(root, CONFIRMED, timeout=WAIT)
        assert set(refs) <= set(remote.blobs)
        state = store.status()
        assert state.roots[root] == CONFIRMED


def test_lineage_confirms_in_order(store):
    remote = FakeRemote()
    with Syncer(store, remote, fast_policy()):
        r1, _ = commit_blobs(store, b"one")
        r2, _ = commit_blobs(store, b"two", parent=r1)
        r3, _ = commit_blobs(store, b"three", parent=r2)
        assert store.wait_for(r3, CONFIRMED, timeout=WAIT)
        # network_confirmed(r3) composes over ancestry, so this implies the
        # parent-before-child confirmation rule never tripped.
        assert store.network_confirmed(r3)


def test_sync_barrier_and_status(store):
    remote = FakeRemote()
    syncer = Syncer(store, remote, fast_policy()).start()
    try:
        for i in range(5):
            commit_blobs(store, f"blob-{i}".encode())
        syncer.sync(timeout=WAIT)
        assert all(rung == CONFIRMED
                   for rung in store.status().roots.values())
        assert syncer.last_error is None
    finally:
        syncer.stop()


def test_listener_sees_ladder_events(store):
    remote = FakeRemote()
    events = []
    store.add_listener(lambda ev: events.append(ev["ev"]))
    with Syncer(store, remote, fast_policy()) as syncer:
        commit_blobs(store, b"watched")
        syncer.sync(timeout=WAIT)
    assert events == ["committed", "pushed", "confirmed"]


# -- crash injection: idempotent re-push ------------------------------------------


def test_push_interrupted_at_every_point_recovers(tmp_path):
    """The L1 acceptance criterion: die after any number of uploaded blobs,
    reopen, re-push idempotently — the journal never over-claims, the
    remote converges to the full set."""
    datas = [f"blob-{i}".encode() * 4 for i in range(4)]
    for fail_at in range(len(datas)):
        sdir = str(tmp_path / f"s{fail_at}")
        store = LocalStore(sdir, addressing="sha256")
        remote = FakeRemote()
        remote.fail_after = fail_at
        with Syncer(store, remote, fast_policy()) as syncer:
            root, refs = commit_blobs(store, *datas)
            # the worker hits the injected failure and enters backoff
            assert not store.wait_for(root, PUSHED, timeout=0.3)
            assert store.status().roots[root] == COMMITTED  # under-claims
        store.close()

        # "restart": reopen the same store dir, heal the network
        store = LocalStore(sdir, addressing="sha256")
        remote.fail_after = None
        with Syncer(store, remote, fast_policy()) as syncer:
            syncer.sync(timeout=WAIT)
        assert set(refs) <= set(remote.blobs)
        assert store.status().roots[root] == CONFIRMED
        store.close()


def test_backoff_keeps_store_usable_then_converges(store):
    remote = FakeRemote()
    remote.fail_after = 0  # nothing gets through
    with Syncer(store, remote, fast_policy()) as syncer:
        root, _ = commit_blobs(store, b"offline work")
        assert not store.wait_for(root, PUSHED, timeout=0.3)
        assert syncer.state == "backoff"
        assert isinstance(syncer.last_error, ConnectionError)
        # local-first: the store stays fully usable while offline
        r2, _ = commit_blobs(store, b"more offline work", parent=root)
        remote.fail_after = None  # network returns
        syncer.sync(timeout=WAIT)
        assert store.network_confirmed(r2)


# -- verification ------------------------------------------------------------------


def test_confirmation_rejects_wrong_bytes(store):
    remote = FakeRemote()
    with Syncer(store, remote, fast_policy()) as syncer:
        root, refs = commit_blobs(store, b"genuine")
        remote.corrupt[refs[0]] = b"forged"
        assert not store.wait_for(root, CONFIRMED, timeout=0.5)
        assert isinstance(syncer.last_error, BlobVerificationFailed)
        assert store.status().roots[root] == PUSHED  # pushed, NOT confirmed


def test_stewardship_false_defers_confirmation(store):
    remote = FakeRemote()
    remote.retrievable = False
    with Syncer(store, remote, fast_policy()) as syncer:
        root, _ = commit_blobs(store, b"pending")
        assert not store.wait_for(root, CONFIRMED, timeout=0.3)
        remote.retrievable = True
        syncer.sync(timeout=WAIT)
        assert store.network_confirmed(root)


def test_sample_zero_trusts_node_claims(store):
    remote = FakeRemote()
    remote.corrupt["never-fetched"] = b"x"  # fetch would fail if sampled
    policy = fast_policy(confirm_sample=0)
    with Syncer(store, remote, policy) as syncer:
        assert syncer.trusting_node_claims
        root, _ = commit_blobs(store, b"trusted")
        syncer.sync(timeout=WAIT)
        assert store.network_confirmed(root)


def test_evicted_blob_heals_by_verified_refetch(tmp_path):
    store = LocalStore(str(tmp_path / "s"), addressing="sha256",
                       max_bytes=10_000)
    remote = FakeRemote()
    with Syncer(store, remote, fast_policy()) as syncer:
        root, refs = commit_blobs(store, b"value" * 20)
        syncer.sync(timeout=WAIT)
        assert store.evict(1000) > 0
        assert not store.has_local(refs[0])
        assert store.get(refs[0]) == b"value" * 20   # healed from remote
        assert store.has_local(refs[0])              # and re-stored

        store.evict(1000)
        remote.corrupt[refs[0]] = b"forged bytes"
        with pytest.raises(BlobVerificationFailed):
            store.get(refs[0])
    store.close()


def test_push_ref_equality_assertion():
    """BeeRemote refuses a node whose returned reference diverges from the
    local one (the erasure-coding address-space fork)."""
    class LyingClient:
        def bytes_post(self, data, stamp, deferred=None):
            return "ff" * 32

    remote = BeeRemote.__new__(BeeRemote)
    remote.client = LyingClient()
    remote.stamp = "aa" * 32
    ref = hashlib.sha256(b"data").hexdigest()
    with pytest.raises(BlobVerificationFailed, match="forked"):
        remote.push_blob(ref, b"data")


# -- triggers ------------------------------------------------------------------------


def test_pinned_bytes_trigger_overrides_debounce(tmp_path):
    store = LocalStore(str(tmp_path / "s"), addressing="sha256")
    remote = FakeRemote()
    policy = fast_policy(debounce=3600, max_staleness=3600,
                         pinned_bytes_limit=100)
    with Syncer(store, remote, policy):
        root, _ = commit_blobs(store, b"x" * 500)  # far over the byte limit
        assert store.wait_for(root, CONFIRMED, timeout=WAIT)
    store.close()


def test_debounce_defers_and_sync_overrides(store):
    remote = FakeRemote()
    with Syncer(store, remote, fast_policy(debounce=3600,
                                           max_staleness=3600)) as syncer:
        root, _ = commit_blobs(store, b"not yet")
        assert not store.wait_for(root, PUSHED, timeout=0.3)  # debounced
        syncer.sync(timeout=WAIT)                             # urgent wins
        assert store.network_confirmed(root)


def test_sync_timeout_names_last_error(store):
    remote = FakeRemote()
    remote.fail_after = 0
    with Syncer(store, remote, fast_policy()) as syncer:
        commit_blobs(store, b"stuck")
        with pytest.raises(TimeoutError, match="network down"):
            syncer.sync(timeout=0.4)


# -- live (gated) ---------------------------------------------------------------------

BEE = os.environ.get("SWARMFS_TEST_BEE")


@pytest.mark.skipif(not BEE, reason="set SWARMFS_TEST_BEE=<bee api url>")
def test_live_roundtrip_against_real_bee(tmp_path):
    pytest.importorskip("eth_hash")
    store = LocalStore(str(tmp_path / "live"), addressing="swarm")
    remote = BeeRemote(BEE, stamp=os.environ.get("SWARMFS_TEST_STAMP",
                                                 "auto"))
    try:
        with Syncer(store, remote, fast_policy()) as syncer:
            root, refs = commit_blobs(
                store, b"localstore live test " * 100, b"second blob")
            syncer.sync(timeout=180)
            assert store.network_confirmed(root)
            store.evict(10**9)
            assert store.get(refs[0]).startswith(b"localstore live test")
    finally:
        store.close()
        remote.close()
