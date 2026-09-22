"""bzzf:// — feed-mounted mutable filesystem, offline.

The FakeClient emulates Bee's sequence-feed lookup and verifies SOC
signatures by recovery, exactly as a real node would. Two filesystem
instances sharing one store stand in for two processes on one swarm.
"""

from __future__ import annotations

import pytest

pytest.importorskip("eth_keys")

import fsspec  # noqa: E402

from swarmfs import SwarmFeedFileSystem  # noqa: E402
from swarmfs.feeds import FeedError, FeedSigner, topic_bytes  # noqa: E402

from conftest import FakeClient  # noqa: E402

KEY = bytes(range(1, 33)).hex()
OWNER = FeedSigner(KEY).owner_hex  # derived once; the feed's address in URLs

KEY2 = bytes(range(101, 133)).hex()
OWNER2 = FeedSigner(KEY2).owner_hex


def make_fs(store, signer=KEY, **kw):
    return SwarmFeedFileSystem(
        client=FakeClient(store), signer=signer, skip_instance_cache=True, **kw
    )


def test_protocol_registered():
    assert fsspec.get_filesystem_class("bzzf") is SwarmFeedFileSystem


def test_write_then_other_instance_reads():
    """The v2 exit criterion, offline: a second mount sees committed changes."""
    store: dict = {}
    a = make_fs(store)
    a.pipe_file(f"bzzf://{OWNER}/my-data/hello.txt", b"via feed")

    b = make_fs(store, signer=None)  # a reader needs no key
    assert b.cat_file(f"bzzf://{OWNER}/my-data/hello.txt") == b"via feed"
    # listings stay in feed coordinates — the stable URL, not the root hash
    assert b.ls(f"bzzf://{OWNER}/my-data", detail=False) == [
        f"{OWNER}/my-data/hello.txt"
    ]
    assert b.find(f"bzzf://{OWNER}/my-data") == [f"{OWNER}/my-data/hello.txt"]


def test_update_cycle_two_writers():
    """Both directions: A writes, B sees it, B writes, A sees that."""
    store: dict = {}
    a = make_fs(store, feed_ttl=0)
    b = make_fs(store, feed_ttl=0)

    a.pipe_file(f"bzzf://{OWNER}/shared/state.json", b'{"v": 1}')
    assert b.cat_file(f"bzzf://{OWNER}/shared/state.json") == b'{"v": 1}'

    b.pipe_file(f"bzzf://{OWNER}/shared/state.json", b'{"v": 2}')
    assert a.cat_file(f"bzzf://{OWNER}/shared/state.json") == b'{"v": 2}'
    # older files persist across updates from either writer
    b.pipe_file(f"bzzf://{OWNER}/shared/other.txt", b"x")
    assert a.cat_file(f"bzzf://{OWNER}/shared/state.json") == b'{"v": 2}'


def test_sequence_index_advances():
    store: dict = {}
    fs = make_fs(store)
    fs.pipe_file(f"bzzf://{OWNER}/idx/a.txt", b"1")
    fs.pipe_file(f"bzzf://{OWNER}/idx/b.txt", b"2")

    import asyncio

    topic_hex = topic_bytes("idx").hex()
    head = asyncio.run(FakeClient(store).feed_head(OWNER, topic_hex))
    assert head is not None
    assert int.from_bytes(bytes.fromhex(head[0]), "big") == 1  # updates 0 and 1


def test_transaction_is_one_feed_update():
    store: dict = {}
    fs = make_fs(store)
    with fs.transaction:
        fs.pipe_file(f"bzzf://{OWNER}/tx/a.txt", b"a")
        fs.pipe_file(f"bzzf://{OWNER}/tx/b.txt", b"b")
        fs.rm_file(f"bzzf://{OWNER}/tx/a.txt")
    assert len(fs.commit_log) == 1

    import asyncio

    head = asyncio.run(FakeClient(store).feed_head(OWNER, topic_bytes("tx").hex()))
    assert int.from_bytes(bytes.fromhex(head[0]), "big") == 0  # single update

    reader = make_fs(store, signer=None)
    assert reader.cat_file(f"bzzf://{OWNER}/tx/b.txt") == b"b"
    assert not reader.exists(f"bzzf://{OWNER}/tx/a.txt")


def test_write_without_signer_fails_early():
    store: dict = {}
    fs = make_fs(store, signer=None)
    client = fs.client
    with pytest.raises(FeedError, match="requires the owner's private key"):
        fs.pipe_file(f"bzzf://{OWNER}/nope/x.txt", b"data")
    assert client.uploads == []  # nothing was uploaded


def test_wrong_signer_fails_early():
    store: dict = {}
    fs = make_fs(store, signer=KEY2)  # KEY2 does not own OWNER's feed
    with pytest.raises(FeedError, match="does not own this feed"):
        fs.pipe_file(f"bzzf://{OWNER}/nope/x.txt", b"data")
    assert fs.client.uploads == []


def test_topic_string_and_raw_hex_are_same_feed():
    store: dict = {}
    fs = make_fs(store)
    fs.pipe_file(f"bzzf://{OWNER}/my-topic/f.txt", b"data")
    raw = topic_bytes("my-topic").hex()
    reader = make_fs(store, signer=None)
    assert reader.cat_file(f"bzzf://{OWNER}/{raw}/f.txt") == b"data"


def test_empty_feed():
    store: dict = {}
    fs = make_fs(store, signer=None)
    assert fs.ls(f"bzzf://{OWNER}/never-written", detail=False) == []
    with pytest.raises(FileNotFoundError):
        fs.cat_file(f"bzzf://{OWNER}/never-written/x.txt")


def test_separate_topics_are_separate_lineages():
    store: dict = {}
    fs = make_fs(store)
    fs.pipe_file(f"bzzf://{OWNER}/one/a.txt", b"1")
    fs.pipe_file(f"bzzf://{OWNER}/two/b.txt", b"2")
    reader = make_fs(store, signer=None)
    assert reader.exists(f"bzzf://{OWNER}/one/a.txt")
    assert not reader.exists(f"bzzf://{OWNER}/two/a.txt")
    assert reader.exists(f"bzzf://{OWNER}/two/b.txt")


def test_owner_0x_prefix_accepted():
    store: dict = {}
    fs = make_fs(store)
    fs.pipe_file(f"bzzf://0x{OWNER}/pfx/x.txt", b"ok")
    reader = make_fs(store, signer=None)
    assert reader.cat_file(f"bzzf://{OWNER}/pfx/x.txt") == b"ok"


def test_bad_paths_rejected():
    store: dict = {}
    fs = make_fs(store)
    with pytest.raises(ValueError, match="ethereum address"):
        fs.ls("bzzf://nothex/topic/x")
    with pytest.raises(ValueError, match="bzzf"):
        fs.ls(f"bzzf://{OWNER}")


# -- pinned and time-travelled views (docs/distributed-writes.md §7) --------


class CountingClient(FakeClient):
    """Counts feed lookups, so a test can prove a view never issued one."""

    def __init__(self, store):
        super().__init__(store)
        self.feed_lookups = 0

    async def feed_head(self, owner, topic):
        self.feed_lookups += 1
        return await super().feed_head(owner, topic)


class _Clock:
    """Stand-in for the time module inside swarmfs.feeds (update timestamps)."""

    def __init__(self, now: int):
        self.now = now

    def time(self) -> float:
        return self.now


def _view(store, **kw):
    client = CountingClient(store)
    fs = SwarmFeedFileSystem(client=client, skip_instance_cache=True, **kw)
    return fs, client


def test_bzzf_at_root_bypasses_feed():
    store: dict = {}
    fs = make_fs(store, feed_ttl=0)
    path = f"bzzf://{OWNER}/frozen/doc.txt"
    fs.pipe_file(path, b"first")
    pinned = fs.latest(f"bzzf://{OWNER}/frozen")
    fs.pipe_file(path, b"second")
    assert fs.cat_file(path) == b"second"  # the live view moved on

    view, client = _view(store, at_root=pinned)
    assert view.cat_file(path) == b"first"  # the pinned one did not
    # the URL still reads as a feed path, and the feed was never consulted
    assert view.ls(f"bzzf://{OWNER}/frozen", detail=False) == [f"{OWNER}/frozen/doc.txt"]
    assert view.find(f"bzzf://{OWNER}/frozen") == [f"{OWNER}/frozen/doc.txt"]
    assert view.latest(f"bzzf://{OWNER}/frozen") == pinned
    assert client.feed_lookups == 0

    # 0x prefixes and uppercase are tolerated, like every other reference
    upper, _ = _view(store, at_root="0x" + pinned.upper())
    assert upper.cat_file(path) == b"first"


def test_bzzf_at_time_resolves_once(monkeypatch):
    from swarmfs import feeds as feeds_mod

    store: dict = {}
    clock = _Clock(1_000_000)
    monkeypatch.setattr(feeds_mod, "time", clock)

    fs = make_fs(store, feed_ttl=0)
    path = f"bzzf://{OWNER}/history/state.txt"
    for i in range(3):
        clock.now = 1_000_000 + i * 100
        fs.pipe_file(path, f"v{i}".encode())

    for when, expected in ((1_000_050, b"v0"), (1_000_150, b"v1"),
                           (1_000_200, b"v2"), (1_000_999, b"v2")):
        view, client = _view(store, at=when)
        assert view.cat_file(path) == expected, when
        assert client.feed_lookups == 1  # one lookup for the head, then pinned
        view.cat_file(path)
        view.ls(f"bzzf://{OWNER}/history")
        assert client.feed_lookups == 1  # …and never again

    # before the first update the feed did not exist
    early, _ = _view(store, at=999_999)
    with pytest.raises(FileNotFoundError, match="did not exist yet"):
        early.cat_file(path)

    # ISO-8601 and datetime forms address the same moment as unix seconds
    import datetime

    iso = datetime.datetime.fromtimestamp(1_000_150, datetime.timezone.utc)
    by_iso, _ = _view(store, at=iso.isoformat().replace("+00:00", "Z"))
    by_dt, _ = _view(store, at=iso)
    assert by_iso.cat_file(path) == b"v1"
    assert by_dt.cat_file(path) == b"v1"


def test_bzzf_modified_is_feed_timestamp(monkeypatch):
    import datetime

    from swarmfs import feeds as feeds_mod

    store: dict = {}
    clock = _Clock(1_700_000_000)
    monkeypatch.setattr(feeds_mod, "time", clock)

    fs = make_fs(store, feed_ttl=0)
    path = f"bzzf://{OWNER}/mtime/doc.txt"
    fs.pipe_file(path, b"one")
    published = datetime.datetime.fromtimestamp(1_700_000_000, datetime.timezone.utc)
    assert fs.modified(path) == published  # our own update

    reader = make_fs(store, signer=None, feed_ttl=0)
    assert reader.modified(path) == published  # read back from the SOC payload

    clock.now = 1_700_000_600
    fs.pipe_file(path, b"two")
    assert fs.modified(path) == datetime.datetime.fromtimestamp(
        1_700_000_600, datetime.timezone.utc)

    # a frozen root never changes, so it keeps the epoch constant
    view, _ = _view(store, at_root=fs.latest(f"bzzf://{OWNER}/mtime"))
    assert view.modified(path) == datetime.datetime.fromtimestamp(
        0, datetime.timezone.utc)

    with pytest.raises(FileNotFoundError):
        fs.modified(f"bzzf://{OWNER}/mtime/nope.txt")


def test_pinned_view_refuses_writes():
    store: dict = {}
    fs = make_fs(store)
    path = f"bzzf://{OWNER}/ro/doc.txt"
    fs.pipe_file(path, b"content")
    root = fs.latest(f"bzzf://{OWNER}/ro")

    # refused even with a signer: the pin, not the key, is what forbids it
    view = make_fs(store, at_root=root)
    for call in (
        lambda: view.pipe_file(path, b"nope"),
        lambda: view.rm_file(path),
        lambda: view.link(f"bzzf://{OWNER}/ro/linked.bin", "ab" * 32),
    ):
        with pytest.raises(FeedError, match="pinned read-only view"):
            call()
    with pytest.raises(FeedError, match="pinned read-only view"):
        with view.transaction:
            view.pipe_file(path, b"nope")

    assert view.cat_file(path) == b"content"  # reads are unaffected
    assert not view.commit_log


def test_pin_options_validated():
    store: dict = {}
    with pytest.raises(ValueError, match="not both"):
        make_fs(store, at_root="ab" * 32, at=1_000_000)
    with pytest.raises(ValueError, match="invalid at_root"):
        make_fs(store, at_root="not-a-reference")
    with pytest.raises(ValueError, match="invalid at="):
        make_fs(store, at="last tuesday")
