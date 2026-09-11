"""ACT-protected content (roadmap: 'pass the swarm-act-* headers through').

The FakeClient emulates Bee 2.8.2's measured ACT contract (see the
docstrings in ``swarmfs/act.py`` and ``tests/conftest.py``): a protected
upload returns an opaque ACT reference of the same length that resolves
only with the right history *and* publisher; without headers it is a 404,
and a plain reference read *with* headers is a 404 as well — which is why
the filesystem sends the headers on root references only.
"""

from __future__ import annotations

import warnings

import pytest

from conftest import FAKE_PUBLIC_KEY, FILES, FakeClient, FakeGatewayClient

from swarmfs import SwarmFileSystem, SyncSwarmClient
from swarmfs.act import Act, ActUpload, GranteeList, validate_grantee

GRANTEE = "02" + "cd" * 32


def make_fs(store, **kw):
    kw.setdefault("skip_instance_cache", True)
    return SwarmFileSystem(client=FakeClient(store), **kw)


def upload_tree(fs, tmp_path):
    d = tmp_path / "site"
    (d / "sub").mkdir(parents=True)
    (d / "a.txt").write_bytes(b"alpha " * 10)
    (d / "sub" / "b.txt").write_bytes(b"beta " * 10)
    return fs.upload(str(d))


# ------------------------------------------------------------ the header bundle

def test_act_headers_and_validation():
    act = Act("AB" * 32, "0x" + FAKE_PUBLIC_KEY.upper(), timestamp=1700000000)
    assert act.headers() == {
        "swarm-act": "true",
        "swarm-act-history-address": "ab" * 32,
        "swarm-act-publisher": FAKE_PUBLIC_KEY,
        "swarm-act-timestamp": "1700000000",
    }
    assert "swarm-act-timestamp" not in Act("ab" * 32, FAKE_PUBLIC_KEY).headers()
    with pytest.raises(ValueError, match="act_history"):
        Act("nope", FAKE_PUBLIC_KEY)
    with pytest.raises(ValueError, match="act_publisher"):
        Act("ab" * 32, "ab" * 32)  # a 64-hex address is not a public key
    assert validate_grantee("0x" + GRANTEE.upper()) == GRANTEE
    with pytest.raises(ValueError, match="grantee"):
        validate_grantee("04" + "cd" * 32)  # uncompressed prefix


# ----------------------------------------------------------------- client tier

def test_client_tier_act_contract():
    """The fake pins the measured contract; the sync facade carries it."""
    client = SyncSwarmClient(client=FakeClient({}))
    up = client.bytes_post(b"secret", "ab" * 32, act=True)
    assert isinstance(up, ActUpload) and len(up.reference) == 64 and len(up.history) == 64
    act = Act(up.history, FAKE_PUBLIC_KEY)
    assert client.bytes_get(up.reference, act=act) == b"secret"
    assert client.bytes_size(up.reference, act=act) == 6
    assert b"".join(client.bytes_iter(up.reference, act=act)) == b"secret"
    with pytest.raises(FileNotFoundError):  # bare: invisible
        client.bytes_get(up.reference)
    with pytest.raises(FileNotFoundError):  # wrong publisher
        client.bytes_get(up.reference, act=Act(up.history, GRANTEE))
    plain = client.bytes_post(b"plain", "ab" * 32)
    with pytest.raises(FileNotFoundError):  # plain ref + ACT headers
        client.bytes_get(plain, act=act)
    # history reuse: the same history comes back
    up2 = client.bytes_post(b"more", "ab" * 32, act=True, act_history=up.history)
    assert up2.history == up.history
    # encrypted + protected: 128-hex ACT reference
    up3 = client.bytes_post(b"both", "ab" * 32, act=True, encrypt=True)
    assert len(up3.reference) == 128
    with pytest.raises(ValueError, match="act=False"):
        client.bytes_post(b"x", "ab" * 32, act_history=up.history)
    # grantee endpoints
    gl = client.grantee_create([GRANTEE], "ab" * 32)
    assert isinstance(gl, GranteeList)
    assert client.grantee_get(gl.reference) == [GRANTEE]
    gl2 = client.grantee_patch(gl.reference, gl.history, "ab" * 32, add=[FAKE_PUBLIC_KEY])
    assert gl2 != gl and sorted(client.grantee_get(gl2.reference)) == sorted([GRANTEE, FAKE_PUBLIC_KEY])
    assert client.addresses()["publicKey"] == FAKE_PUBLIC_KEY


# --------------------------------------------------------------------- writes

def test_protected_directory_upload(tmp_path):
    store = {}
    fs = make_fs(store, act=True)
    assert fs.act_history is None
    root = upload_tree(fs, tmp_path)
    # act ⇒ encrypt by default: 128-hex ACT reference over encrypted content
    assert len(root) == 128
    assert fs.act_history is not None and len(fs.act_history) == 64
    assert fs.commit_log[-1].act_history == fs.act_history
    # the ACT reference is not a store key: it is opaque, not an address
    assert bytes.fromhex(root) not in store
    # ...and only the root is wrapped: every stored blob is an ordinary
    # (encrypted, 64-byte-keyed) reference
    assert all(len(k) == 64 for k in store)

    # publisher reads through the same instance: root registered by the commit
    assert fs.cat_file(f"bzz://{root}/a.txt") == b"alpha " * 10
    assert sorted(fs.find(f"bzz://{root}")) == [f"{root}/a.txt", f"{root}/sub/b.txt"]

    # a second commit continues the same history
    fs.pipe_file(f"bzz://{root}/c.txt", b"gamma")
    new = fs.latest(root)
    assert new != root and fs.act_history == fs.commit_log[0].act_history
    assert fs.cat_file(f"bzz://{new}/c.txt") == b"gamma"
    assert fs.cat_file(f"bzz://{root}/c.txt") == b"gamma"  # read-your-writes


def test_act_alone_warns_and_stays_plain(tmp_path):
    store = {}
    with pytest.warns(UserWarning, match="plaintext-addressable"):
        fs = make_fs(store, act=True, encrypt=False)
    root = upload_tree(fs, tmp_path)
    assert len(root) == 64  # ACT over plain content: 64-hex, like a plain ref
    assert fs.cat_file(f"bzz://{root}/a.txt") == b"alpha " * 10


def test_single_file_upload_is_protected(tmp_path):
    fs = make_fs({}, act=True)
    f = tmp_path / "one.txt"
    f.write_bytes(b"just one")
    ref = fs.upload(str(f))
    assert fs.act_history is not None
    client = fs.client
    assert bytes.fromhex(ref) in client.act_map


def test_transaction_is_one_protected_commit(tmp_path):
    fs = make_fs({}, act=True)
    root = upload_tree(fs, tmp_path)
    n = len(fs.client.act_map)
    with fs.transaction:
        fs.pipe_file(f"bzz://{root}/x.txt", b"x")
        fs.pipe_file(f"bzz://{root}/y.txt", b"y")
    assert len(fs.client.act_map) == n + 1  # one new wrapped root
    assert len(fs.commit_log) == 2


# ---------------------------------------------------------------------- reads

def test_reader_needs_history_and_publisher(tmp_path):
    store = {}
    pub = make_fs(store, act=True)
    root = upload_tree(pub, tmp_path)
    history = pub.act_history

    # a reader instance over the same swarm, given the history: the
    # publisher defaults to the reader's own node — here the same fake node
    reader = make_fs(store, act_history=history)
    assert reader.act is False and reader.act_mode is True
    assert reader.cat_file(f"bzz://{root}/sub/b.txt") == b"beta " * 10
    assert reader.cat_file(f"bzz://{root}/a.txt", start=6, end=11) == b"alpha"
    assert sorted(reader.ls(f"bzz://{root}", detail=False)) == [f"{root}/a.txt", f"{root}/sub"]
    info = reader.info(f"bzz://{root}/a.txt")
    assert info["size"] == 60
    assert reader.act_publisher == FAKE_PUBLIC_KEY  # resolved from /addresses
    # only the root was fetched with headers — never a child
    assert {r for r, _ in reader.client.act_reads} == {root}

    # explicit publisher works too, and a wrong one is "not found"
    ok = make_fs(store, act_history=history, act_publisher=FAKE_PUBLIC_KEY)
    assert ok.exists(f"bzz://{root}/a.txt")
    wrong = make_fs(store, act_history=history, act_publisher=GRANTEE)
    with pytest.raises(FileNotFoundError):
        wrong.ls(f"bzz://{root}")

    # no ACT configuration at all: the content is invisible
    plain = make_fs(store)
    with pytest.raises(FileNotFoundError):
        plain.ls(f"bzz://{root}")
    with pytest.raises(FileNotFoundError):
        plain.cat_file(f"bzz://{root}/a.txt")


def test_raw_reference_reads_treat_the_ref_as_a_root():
    store = {}
    client = SyncSwarmClient(client=FakeClient(store))
    up = client.bytes_post(b"raw protected blob", "ab" * 32, act=True)
    fs = make_fs(store, act_history=up.history)
    assert fs.read_reference(up.reference) == b"raw protected blob"
    assert fs.read_reference(up.reference, 4, 13) == b"protected"
    assert fs.reference_size(up.reference) == 18


def test_bzzf_feed_over_protected_roots(tmp_path):
    pytest.importorskip("eth_keys")
    from swarmfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedSigner

    key = bytes(range(1, 33)).hex()
    owner = FeedSigner(key).owner_hex
    store = {}
    writer = SwarmFeedFileSystem(client=FakeClient(store), signer=key, act=True,
                                 skip_instance_cache=True)
    writer.pipe_file(f"bzzf://{owner}/private/state.txt", b"v1")
    history = writer.act_history
    assert history
    # the feed payload is the ACT reference; a reader with the history follows it
    reader = SwarmFeedFileSystem(client=FakeClient(store), act_history=history,
                                 feed_ttl=0, skip_instance_cache=True)
    assert reader.cat_file(f"bzzf://{owner}/private/state.txt") == b"v1"
    writer.pipe_file(f"bzzf://{owner}/private/state.txt", b"v2")
    assert reader.cat_file(f"bzzf://{owner}/private/state.txt") == b"v2"
    assert writer.act_history == history  # one history across updates
    # without the history the feed resolves but the root is invisible
    blind = SwarmFeedFileSystem(client=FakeClient(store), skip_instance_cache=True)
    with pytest.raises(FileNotFoundError):
        blind.cat_file(f"bzzf://{owner}/private/state.txt")


# ------------------------------------------------------------- policy & refusals

def test_grantee_management_through_the_fs(tmp_path):
    store = {}
    fs = make_fs(store, act=True)
    assert fs.publisher_key() == FAKE_PUBLIC_KEY
    gl = fs.create_grantees([GRANTEE])
    assert fs.grantees(gl.reference) == [GRANTEE]
    # publish into the grantee list's history: readable by those keys
    pub = make_fs(store, act=True, act_history=gl.history)
    root = upload_tree(pub, tmp_path)
    assert pub.act_history == gl.history
    assert make_fs(store, act_history=gl.history).cat_file(f"bzz://{root}/a.txt") == b"alpha " * 10
    gl2 = fs.patch_grantees(gl.reference, gl.history, add=[FAKE_PUBLIC_KEY], revoke=[GRANTEE])
    assert gl2.reference != gl.reference and gl2.history != gl.history
    assert fs.grantees(gl2.reference) == [FAKE_PUBLIC_KEY]
    with pytest.raises(ValueError, match="at least one"):
        fs.create_grantees([])
    with pytest.raises(ValueError, match="nothing to add"):
        fs.patch_grantees(gl2.reference, gl2.history)
    with pytest.raises(ValueError, match="grantee"):
        fs.create_grantees(["not-a-key"])


def test_refusals(tmp_path):
    with pytest.raises(ValueError, match="verify"):
        make_fs({}, act=True, verify=True)
    with pytest.raises(ValueError, match="local_store"):
        make_fs({}, act_history="ab" * 32, local_store=str(tmp_path / "store"), redundancy=0)
    with pytest.raises(ValueError, match="act_publisher"):
        make_fs({}, act_history="ab" * 32, act_publisher="junk")
    # a gateway can never serve protected content: the node must hold a key
    gw = SwarmFileSystem(client=FakeGatewayClient({}), allow_gateway=True,
                         act_history="ab" * 32, skip_instance_cache=True)
    with pytest.raises(PermissionError, match="own node"):
        gw.ls("bzz://" + "ab" * 32)


def test_unprotected_instances_are_untouched(fs):
    """No ACT options: no registration, no headers, no behaviour change."""
    fs, root = fs
    assert fs.act_mode is False and fs.encrypt is False
    assert fs.cat_file(f"bzz://{root}/index.html") == FILES["index.html"]
    assert fs._act_roots == set()
    assert fs.client.act_reads == []
