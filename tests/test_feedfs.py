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
