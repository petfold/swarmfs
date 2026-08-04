"""Encrypted storage and recall (roadmap: 'Encrypted references (128-hex)
— decryption in the load path').

Storage: with ``encrypt=True`` every uploaded blob — file payloads and
manifest nodes alike — goes up with ``swarm-encrypt``, and references
become 128 hex (address + decryption key). The Mantaray codec needed no
changes: refBytesSize is data-driven, so 64-byte entries flow through
build/parse untouched. Recall: the node decrypts in the load path when
handed the full reference, so listing walks and reads work unchanged
against a trusted node; the verifying reader refuses ciphertext loudly
(it cannot traverse what it cannot decrypt), as does local-first mode
(encrypted refs are not content addresses).
"""

import hashlib
import os

import pytest

from conftest import GOOD_STAMP, FakeClient

from swarmfs import SwarmFileSystem
from swarmfs.core import CommitEngine
from swarmfs.join import VerificationError, VerifyingReader
from swarmfs.mantaray import unmarshal


# The shared FakeClient mimics Bee's swarm-encrypt contract natively
# (conftest): an encrypted upload is stored under a 64-byte reference
# (address ‖ key) and served back only for the full reference.
EncryptingFakeClient = FakeClient


def make_fs(**kw):
    store = {}
    kw.setdefault("skip_instance_cache", True)
    return SwarmFileSystem(client=EncryptingFakeClient(store), **kw), store


def upload_tree(fs, tmp_path, **upload_kw):
    d = tmp_path / "site"
    (d / "sub").mkdir(parents=True)
    (d / "a.txt").write_bytes(b"alpha " * 10)
    (d / "sub" / "b.txt").write_bytes(b"beta " * 10)
    return fs.upload(str(d), **upload_kw)


# -- storage --------------------------------------------------------------------


def test_encrypted_directory_upload_yields_128_hex_root(tmp_path):
    fs, store = make_fs()
    root = upload_tree(fs, tmp_path, encrypt=True)
    assert len(root) == 128
    # the manifest node itself is stored under a 64-byte (encrypted) ref
    # and carries 64-byte entries — refBytesSize did its job
    node = unmarshal(store[bytes.fromhex(root)])
    assert node.ref_bytes_size == 64


def test_fs_level_encrypt_applies_to_all_writes(tmp_path):
    fs, store = make_fs(encrypt=True)
    root = upload_tree(fs, tmp_path)          # no per-call flag needed
    assert len(root) == 128
    assert all(len(k) == 64 for k in store)   # every blob went up encrypted


def test_recall_listing_and_reads(tmp_path):
    fs, _ = make_fs()
    root = upload_tree(fs, tmp_path, encrypt=True)
    names = fs.ls(f"bzz://{root}", detail=False)
    assert any(n.endswith("a.txt") for n in names)
    assert fs.cat(f"bzz://{root}/a.txt") == b"alpha " * 10
    assert fs.cat(f"bzz://{root}/sub/b.txt") == b"beta " * 10
    assert fs.cat_file(f"bzz://{root}/a.txt", start=6, end=11) == b"alpha"
    assert fs.info(f"bzz://{root}/a.txt")["size"] == 60


def test_lineage_cannot_mix(tmp_path):
    fs, _ = make_fs()
    plain_root = upload_tree(fs, tmp_path)    # unencrypted lineage
    fs_enc, _ = make_fs(encrypt=True)
    fs_enc.client.store.update(fs.client.store)
    with pytest.raises(ValueError, match="cannot mix"):
        fs_enc.pipe_file(f"bzz://{plain_root}/new.txt", b"x")


# -- refusals: the honest boundaries ----------------------------------------------


def test_local_store_refuses_encrypt(tmp_path):
    with pytest.raises(ValueError, match="not content addresses"):
        SwarmFileSystem(client=EncryptingFakeClient({}),
                        local_store=str(tmp_path / "s"),
                        redundancy=0, encrypt=True,
                        skip_instance_cache=True)


def test_verifying_reader_refuses_encrypted_refs():
    reader = VerifyingReader(EncryptingFakeClient({}))
    import asyncio
    with pytest.raises(VerificationError, match="encrypted"):
        asyncio.run(reader.bytes_get("ab" * 64))


# -- live (gated): the crux — the node decrypts in the load path -------------------

BEE = os.environ.get("SWARMFS_TEST_BEE")
STAMP = os.environ.get("SWARMFS_TEST_STAMP")


@pytest.mark.skipif(not (BEE and STAMP),
                    reason="set SWARMFS_TEST_BEE and SWARMFS_TEST_STAMP")
def test_live_encrypted_roundtrip(tmp_path):
    fs = SwarmFileSystem(api_url=BEE, stamp=STAMP, skip_instance_cache=True)
    root = upload_tree(fs, tmp_path, encrypt=True)
    assert len(root) == 128
    # recall through a FRESH instance: nothing cached, every manifest node
    # and payload fetched via /bytes/<128-hex> — proving the node decrypts
    # in the load path
    reader = SwarmFileSystem(api_url=BEE, skip_instance_cache=True)
    names = reader.ls(f"bzz://{root}", detail=False)
    assert any(n.endswith("a.txt") for n in names)
    assert reader.cat(f"bzz://{root}/a.txt") == b"alpha " * 10
    assert reader.cat_file(f"bzz://{root}/sub/b.txt",
                           start=0, end=4) == b"beta"
    # size of encrypted content: HEAD /bytes 404s and the root chunk's
    # span is ciphertext (both measured on 2.8.1) — bytes_size answers
    # via a 1-byte ranged GET's Content-Range instead
    assert reader.info(f"bzz://{root}/a.txt")["size"] == 60


@pytest.mark.skipif(not (BEE and STAMP),
                    reason="set SWARMFS_TEST_BEE and SWARMFS_TEST_STAMP")
def test_live_bzzf_over_encrypted_root(tmp_path):
    """A feed can point at an encrypted root: the update carries the full
    128-hex reference, so readers of the stable URL get decryption
    transparently — settled live 2026-08-04."""
    pytest.importorskip("eth_keys")
    import time

    from swarmfs.feedfs import SwarmFeedFileSystem

    fs = SwarmFeedFileSystem(api_url=BEE, stamp=STAMP, signer="11" * 32,
                             encrypt=True, skip_instance_cache=True)
    topic = f"enc-test-{int(time.time())}"
    path = f"bzzf://{fs.signer.owner_hex}/{topic}/secret.txt"
    fs.pipe_file(path, b"encrypted behind a stable URL")

    reader = SwarmFeedFileSystem(api_url=BEE, skip_instance_cache=True)
    deadline = time.time() + 90
    data = None
    while time.time() < deadline:  # feeds are eventually consistent
        try:
            data = reader.cat(path)
            break
        except Exception:
            time.sleep(3)
    assert data == b"encrypted behind a stable URL"
