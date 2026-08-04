"""The local-first store's contract, above all its one invariant:
no blob is ever evicted unless the journal proves Swarm holds it.

Everything here runs offline (L0 has no network); ``mark_pushed`` /
``mark_confirmed`` stand in for the L1 push worker. The crash-injection
tests exercise the format's recovery rule byte by byte — the design's
acceptance criterion is that any truncation leaves the journal
*under-claiming* durability, never over-claiming it.
"""

import json
import os
import random
import shutil

import pytest

from swarmfs.localstore import (
    BlobEvicted,
    BudgetExceededWarning,
    CONFIRMED,
    LocalStore,
    MemoryCacheStore,
    StoreLocked,
)


def make_store(tmp_path, name="store", **kw):
    kw.setdefault("addressing", "sha256")
    return LocalStore(str(tmp_path / name), **kw)


def blob(i, size=60):
    return f"blob-{i}-".encode().ljust(size, b"x")


# -- BytesStore contract -------------------------------------------------------


def test_round_trip_and_unknown_ref(tmp_path):
    with make_store(tmp_path) as s:
        ref = s.put(b"hello")
        assert s.get(ref) == b"hello"
        assert s.put(b"hello") == ref  # idempotent
        assert s.get_many(s.put_many([b"a", b"b"])) == {
            s.put(b"a"): b"a", s.put(b"b"): b"b"}
        with pytest.raises(KeyError):
            s.get("ab" * 32)


def test_swarm_addressing_matches_splitter(tmp_path):
    pytest.importorskip("eth_hash")
    from swarmfs.splitter import content_address

    with make_store(tmp_path, addressing="swarm") as s:
        data = b"x" * 5000  # more than one chunk
        assert s.put(data) == content_address(data).hex()


def test_format_file_refused_when_unsupported(tmp_path):
    s = make_store(tmp_path)
    s.close()
    with open(tmp_path / "store" / "format", "w") as f:
        f.write("swarmfs-localstore 99 sha256\n")
    with pytest.raises(ValueError, match="not a supported localstore"):
        make_store(tmp_path)


# -- the invariant: pinned vs evictable ----------------------------------------


def test_unconfirmed_commits_are_pinned_budget_is_soft(tmp_path):
    with make_store(tmp_path, max_bytes=100) as s:
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        with pytest.warns(BudgetExceededWarning):
            b = s.put(blob(2))  # 120 bytes total, everything pinned
        assert s.get(a) and s.get(b)  # nothing was evicted
        st = s.status()
        assert st.total_bytes == 120 and st.evictable_bytes == 0


def test_orphans_are_never_evicted(tmp_path):
    with make_store(tmp_path, max_bytes=100) as s:
        refs = []
        with pytest.warns(BudgetExceededWarning):
            for i in range(3):
                refs.append(s.put(blob(i)))
        for r in refs:  # orphans: no committed root lists them
            assert s.get(r)


def test_confirmed_blobs_become_evictable(tmp_path):
    with make_store(tmp_path, max_bytes=100) as s:
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        s.mark_pushed(a)
        s.mark_confirmed(a, batch="b1", ttl=None)
        b = s.put(blob(2))  # over budget -> a (confirmed) is evicted
        assert s.get(b)
        assert not s.has_local(a)
        with pytest.raises(BlobEvicted):
            s.get(a)
        with pytest.raises(KeyError):  # BlobEvicted IS a KeyError
            s.get(a)
        assert s.status().only_on_swarm_count == 1


def test_shared_blob_evictable_once_any_listing_root_confirmed(tmp_path):
    # A blob listed by a confirmed root is on Swarm; an unconfirmed child
    # that also references it does not re-pin it (its push needn't re-upload).
    with make_store(tmp_path, max_bytes=150) as s:
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        s.mark_confirmed(a, ttl=None)
        c = s.put(blob(2))
        s.commit_root(c, a, [c])  # child, unconfirmed; a not re-listed
        s.put(blob(3))  # 180 bytes, over by 30: a (confirmed) is evicted
        assert not s.has_local(a) and s.has_local(c)


def test_payload_evicted_before_structure(tmp_path):
    with make_store(tmp_path, max_bytes=140) as s:
        node = s.put(blob("node"))
        val = s.put(blob("val"))
        s.commit_root(node, None, [node, val], structure=[node])
        s.mark_confirmed(node, ttl=None)
        s.put(blob("new"))  # 180 total, over by 40 -> one eviction
        assert not s.has_local(val)   # payload went first
        assert s.has_local(node)      # structure survived


def test_ttl_risky_blobs_are_not_evicted(tmp_path):
    with make_store(tmp_path, max_bytes=100) as s:  # default min_evict_ttl 7d
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        s.mark_confirmed(a, batch="b1", ttl=3600)  # expires in an hour
        with pytest.warns(BudgetExceededWarning):
            s.put(blob(2))
        assert s.has_local(a)  # confirmed, but Swarm won't hold it for long


def test_named_pin_blocks_eviction_and_unpin_releases(tmp_path):
    with make_store(tmp_path, max_bytes=100) as s:
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        s.mark_confirmed(a, ttl=None)
        s.pin("hot", [a])
        with pytest.warns(BudgetExceededWarning):
            s.put(blob(2))
        assert s.has_local(a)
        s.unpin("hot")
        assert s.evict(60) == 60
        assert not s.has_local(a)


# -- the durability ladder -----------------------------------------------------


def test_confirm_requires_confirmed_parent(tmp_path):
    with make_store(tmp_path) as s:
        a = s.put(blob(1))
        b = s.put(blob(2))
        s.commit_root(a, None, [a])
        s.commit_root(b, a, [b])
        with pytest.raises(ValueError, match="must be confirmed before"):
            s.mark_confirmed(b)
        s.mark_confirmed(a)
        s.mark_confirmed(b)
        assert s.network_confirmed(b)


def test_network_confirmed_composes_over_ancestry(tmp_path):
    with make_store(tmp_path) as s:
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        assert not s.network_confirmed(a)
        s.mark_pushed(a)
        assert not s.network_confirmed(a)  # on the node is not on the network
        s.mark_confirmed(a)
        assert s.network_confirmed(a)


def test_commit_validation(tmp_path):
    with make_store(tmp_path) as s:
        a = s.put(blob(1))
        with pytest.raises(ValueError, match="not a committed root"):
            s.commit_root(a, "ff" * 32, [a])
        with pytest.raises(ValueError, match="not in the store"):
            s.commit_root(a, None, [a, "ee" * 32])
        with pytest.raises(ValueError, match="subset"):
            s.commit_root(a, None, [a], structure=["dd" * 32])
        s.commit_root(a, None, [a])
        with pytest.raises(ValueError, match="already committed"):
            s.commit_root(a, None, [a])


# -- persistence and the reflog --------------------------------------------------


def test_state_survives_reopen(tmp_path):
    s = make_store(tmp_path)
    a, b = s.put(blob(1)), s.put(blob(2))
    s.commit_root(a, None, [a])
    s.commit_root(b, a, [b], structure=[b])
    s.mark_confirmed(a, batch="b1", ttl=1e9)
    s.set_remote_root("origin", a)
    s.pin("hot", [b])
    s.close()

    with make_store(tmp_path) as s2:
        st = s2.status()
        assert st.roots == {a: CONFIRMED, b: "committed"}
        assert st.remote_roots == {"origin": a}
        assert st.pins == {"hot": 1}
        assert s2.parent_of(b) == a  # lineage = the reflog
        assert s2.network_confirmed(a) and not s2.network_confirmed(b)


def test_single_writer_lock(tmp_path):
    s = make_store(tmp_path)
    with pytest.raises(StoreLocked):
        make_store(tmp_path)
    s.close()
    make_store(tmp_path).close()


# -- crash injection: the journal may under-claim, never over-claim ---------------


def _copy_with_journal_prefix(store_dir, dest, nbytes):
    shutil.copytree(store_dir, dest)
    journal = os.path.join(dest, "journal.jsonl")
    with open(journal, "rb") as f:
        raw = f.read()
    with open(journal, "wb") as f:
        f.write(raw[:nbytes])
    os.unlink(os.path.join(dest, "lock"))


def test_torn_final_line_discarded_and_truncated(tmp_path):
    s = make_store(tmp_path)
    a = s.put(blob(1))
    s.commit_root(a, None, [a])
    s.mark_confirmed(a)
    s.close()
    journal = tmp_path / "store" / "journal.jsonl"
    raw = journal.read_bytes()
    journal.write_bytes(raw[:-9])  # tear into the confirmed event

    s2 = make_store(tmp_path)
    assert s2.status().roots[a] == "committed"  # under-claims: not confirmed
    assert s2.evict(1000) == 0                  # so the blob is pinned again
    # the torn tail was truncated on open: appending stays parseable
    s2.mark_confirmed(a)
    s2.close()
    with make_store(tmp_path) as s3:
        assert s3.status().roots[a] == CONFIRMED


def test_corrupt_midfile_line_is_reported_not_skipped(tmp_path):
    s = make_store(tmp_path)
    a = s.put(blob(1))
    s.commit_root(a, None, [a])
    s.mark_confirmed(a)
    s.close()
    journal = tmp_path / "store" / "journal.jsonl"
    lines = journal.read_bytes().splitlines(keepends=True)
    journal.write_bytes(b"garbage not json\n".join([lines[0], lines[1]]))
    with pytest.raises(ValueError, match="corrupt journal line"):
        make_store(tmp_path)


def test_every_journal_prefix_underclaims(tmp_path):
    """The acceptance criterion: truncate the journal at ANY byte and the
    recovered fold claims no more durability than the full one did."""
    s = make_store(tmp_path)
    a, b, c = (s.put(blob(i)) for i in range(3))
    s.commit_root(a, None, [a])
    s.mark_pushed(a)
    s.mark_confirmed(a, batch="b1", ttl=1e9)
    s.commit_root(b, a, [b])
    s.pin("hot", [c])
    s.set_remote_root("origin", a)
    s.mark_confirmed(b)
    s.close()
    store_dir = tmp_path / "store"
    full = len((store_dir / "journal.jsonl").read_bytes())
    full_confirmed = {a, b}

    for n in range(full + 1):
        dest = tmp_path / f"cut{n}"
        _copy_with_journal_prefix(store_dir, dest, n)
        with LocalStore(str(dest), addressing="sha256") as cut:
            st = cut.status()
            confirmed = {r for r, rung in st.roots.items()
                         if rung == CONFIRMED}
            # never over-claim: no rung above what the full journal proved,
            # and blobs of any not-yet-confirmed root are locally present
            assert confirmed <= full_confirmed
            for root, rung in st.roots.items():
                if rung != CONFIRMED:
                    assert cut.has_local(root)
        shutil.rmtree(dest)


def test_random_workload_never_evicts_unconfirmed(tmp_path):
    """Property test: under a random put/commit/confirm/evict workload with
    a hostile budget, every blob of every unconfirmed root and every orphan
    stays locally readable."""
    rng = random.Random(20260804)
    with make_store(tmp_path, max_bytes=500) as s:
        import warnings as _w
        heads = [None]
        orphans = set()
        by_root = {}
        with _w.catch_warnings():
            _w.simplefilter("ignore", BudgetExceededWarning)
            for step in range(200):
                op = rng.random()
                if op < 0.5:
                    orphans.add(s.put(blob(f"o{step}", rng.choice([30, 90]))))
                elif op < 0.75 and orphans:
                    new = [orphans.pop() for _ in range(
                        min(len(orphans), rng.randint(1, 3)))]
                    root = new[0]
                    if root not in by_root:
                        parent = rng.choice(heads)
                        s.commit_root(root, parent, new)
                        by_root[root] = (parent, new)
                        heads.append(root)
                elif op < 0.9 and by_root:
                    root = rng.choice(list(by_root))
                    parent = by_root[root][0]
                    if parent is None or s.network_confirmed(parent):
                        if s.status().roots[root] != CONFIRMED:
                            s.mark_confirmed(
                                root, ttl=rng.choice([None, 1e9]))
                else:
                    s.evict(rng.randint(0, 300))

                for root, (_, refs) in by_root.items():
                    if s.status().roots[root] != CONFIRMED:
                        for r in refs:
                            assert s.has_local(r), (step, root, r)
                for r in orphans:
                    assert s.has_local(r), (step, "orphan", r)


# -- the memory cache wrapper -----------------------------------------------------


class CountingStore:
    def __init__(self):
        self.blobs = {}
        self.gets = 0

    def put(self, data):
        import hashlib
        ref = hashlib.sha256(data).hexdigest()
        self.blobs[ref] = data
        return ref

    def get(self, ref):
        self.gets += 1
        return self.blobs[ref]


def test_memory_cache_serves_repeats_without_inner(tmp_path):
    inner = CountingStore()
    cache = MemoryCacheStore(inner, max_bytes=1000)
    ref = cache.put(b"x" * 100)
    assert cache.get(ref) == b"x" * 100
    assert inner.gets == 0  # put populated the cache


def test_memory_cache_is_bounded_lru(tmp_path):
    inner = CountingStore()
    cache = MemoryCacheStore(inner, max_bytes=250)
    r1, r2, r3 = (cache.put(bytes([i]) * 100) for i in range(3))
    assert cache._bytes <= 250            # r1 was dropped
    cache.get(r1)                          # miss -> inner
    assert inner.gets == 1
    cache.get(r3)
    assert inner.gets == 1                 # r3 still cached (recent)


def test_memory_cache_never_caches_oversized(tmp_path):
    inner = CountingStore()
    cache = MemoryCacheStore(inner, max_bytes=50)
    ref = cache.put(b"y" * 100)
    assert cache.get(ref) == b"y" * 100    # served
    assert inner.gets == 1                 # but not cached
    assert cache._bytes == 0


def test_memory_cache_get_many_mixed(tmp_path):
    inner = CountingStore()
    cache = MemoryCacheStore(inner, max_bytes=1000)
    r1 = cache.put(b"a" * 10)
    r2 = inner.put(b"b" * 10)              # only in the inner store
    out = cache.get_many([r1, r2])
    assert out == {r1: b"a" * 10, r2: b"b" * 10}
    assert inner.gets == 1


# -- durability policy -------------------------------------------------------------


def test_commit_durability_reverifies_crashed_orphan(tmp_path):
    """A pre-session orphan with torn content must not be claimed durable:
    the commit that lists it verifies, drops the bad file, and asks for a
    re-put."""
    from swarmfs.localstore import BlobVerificationFailed

    s = make_store(tmp_path)
    ref = s.put(blob(1))
    path = tmp_path / "store" / "blobs" / ref[:2] / ref
    s.close()
    path.write_bytes(b"garbage from a torn write")  # simulate the crash

    with make_store(tmp_path) as s2:
        assert s2.has_local(ref)  # scan sees the file...
        with pytest.raises(BlobVerificationFailed, match="re-put"):
            s2.commit_root(ref, None, [ref])
        assert not s2.has_local(ref)      # ...but the bad file is dropped
        assert s2.put(blob(1)) == ref     # re-put heals
        s2.commit_root(ref, None, [ref])  # and the commit lands


def test_blob_durability_mode_unchanged(tmp_path):
    with make_store(tmp_path, durability="blob") as s:
        a = s.put(blob(1))
        s.commit_root(a, None, [a])
        assert s.get(a) == blob(1)


def test_unknown_durability_rejected(tmp_path):
    with pytest.raises(ValueError, match="durability"):
        make_store(tmp_path, durability="yolo")
