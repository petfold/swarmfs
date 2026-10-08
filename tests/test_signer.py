"""The shared secp256k1 signer: Bee's signature encoding over libsecp256k1.

The cryptography is coincurve's; what is ours is the encoding, so these
tests pin the encoding against fixed outputs. The vectors were computed
with swarm-bee 1.1.0 (`PrivateKey.sign`) and agree byte for byte with
eth-keys' pure-Python backend over 60 random keys and messages (checked
when the signer was written, 2026-10-08). Two are external anchors: key 1
has Ethereum's well-known address 0x7e5f…5bdf, and the bee-js test key
has address 0x8d37…e632.
"""

import sys

import pytest

from swarmfs import signer as S
from swarmfs.bmt import keccak256

VECTORS = [
    ("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd", b"",
     "8d3766440f0d7b949a5e32995d09619a7f86e632",
     "8dc5fe7a7fb6d021b51b4da0ea2a5a85872b44c587ebeaa5a693a78129d87ac1"
     "1c55c8cfa6e9254aab5e5e808c03f4a67261bb09d50b108daf325237c956a42b1b"),
    ("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd",
     b"hello swarm",
     "8d3766440f0d7b949a5e32995d09619a7f86e632",
     "6ca802ebab63390c1a7c6635db819297fe1181591c716e1712b1354f26668e6f"
     "5b53f09e514d1fb9df618c7283c4a5576492ac851e1fd488033425895a37814e1c"),
    ("0000000000000000000000000000000000000000000000000000000000000001",
     bytes(64),
     "7e5f4552091a69125d5dfcb7b8c2659029395bdf",
     "4cb3ecf2f817dee5f0f676740d3344d55f7ccaa7f6b84e0248980eb87114f6e3"
     "122de6fa52618fd920e6a5562634fc1d32f2f07d42749d17b8fbeb25fd0701e11c"),
    ("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364140",
     bytes(range(105)),
     "80c0dbf239224071c59dd8970ab9d542e3414ab2",
     "174264cc093ed77caf45fd2b0120c577c83ebabf07598b17dc477b3f95d3851e"
     "1c53e2855043b1cad317c160cc5919e21877f811581d5cc6492eb8d0e4c966fd1b"),
]


@pytest.fixture
def no_coincurve(monkeypatch):
    """Run as if coincurve were not installed (as under Pyodide)."""
    monkeypatch.setitem(sys.modules, "coincurve", None)


@pytest.mark.parametrize("key,data,address,signature", VECTORS)
def test_signatures_are_byte_identical_to_the_reference(key, data, address,
                                                        signature):
    pytest.importorskip("coincurve")
    signer = S.Signer(key)
    assert signer.address_hex == address
    assert signer.sign(data).hex() == signature
    assert S.Signer("0x" + key).address_hex == address  # 0x prefix accepted


@pytest.mark.parametrize("key,data,address,signature", VECTORS)
def test_recovery_with_coincurve(key, data, address, signature):
    pytest.importorskip("coincurve")
    sig = bytes.fromhex(signature)
    assert S.recover(sig, data).hex() == address
    assert S.verify(sig, data, bytes.fromhex(address))


@pytest.mark.parametrize("key,data,address,signature", VECTORS)
def test_recovery_without_coincurve(no_coincurve, key, data, address,
                                    signature):
    sig = bytes.fromhex(signature)
    assert S.recover(sig, data).hex() == address
    assert S.verify(sig, data, bytes.fromhex(address))
    assert not S.verify(sig, data + b"x", bytes.fromhex(address))


def test_signing_refuses_without_coincurve(no_coincurve):
    with pytest.raises(ImportError, match=r"swarmfs\[feeds\]"):
        S.Signer(VECTORS[0][0])


def test_both_recoveries_agree_on_random_signatures():
    pytest.importorskip("coincurve")
    import random

    rng = random.Random(11)
    for _ in range(20):
        signer = S.Signer(rng.randbytes(32))
        data = rng.randbytes(rng.randint(0, 200))
        sig = signer.sign(data)
        h = S.message_digest(keccak256(data))
        python = S._py_recover(int.from_bytes(sig[:32], "big"),
                               int.from_bytes(sig[32:64], "big"), sig[64] - 27, h)
        assert S.address_of(python) == S.recover(sig, data) == signer.address


def test_tampered_and_malformed_signatures(no_coincurve):
    key, data, address, signature = VECTORS[1]
    sig = bytearray.fromhex(signature)
    assert not S.verify(bytes(sig[:64]), data, bytes.fromhex(address))
    sig[10] ^= 1
    assert not S.verify(bytes(sig), data, bytes.fromhex(address))
    with pytest.raises(S.SignatureError):
        S.recover(bytes(64) + b"\x1b", data)  # r = s = 0
    with pytest.raises(S.SignatureError):
        S.recover(bytes.fromhex(signature)[:64] + b"\x05", data)


def test_a_private_key_must_be_32_bytes():
    pytest.importorskip("coincurve")
    with pytest.raises(ValueError):
        S.Signer("abcd")


def _soc(key: str):
    from swarmfs.bmt import cac_data, chunk_address
    from swarmfs.feeds import feed_identifier, soc_address, topic_bytes

    signer = S.Signer(key)
    identifier = feed_identifier(topic_bytes("t"), 3)
    cac = cac_data(b"\x00" * 8 + bytes(32))
    sig = signer.sign(identifier + chunk_address(cac))
    return identifier + sig + cac, signer.address, soc_address(identifier, signer.address)


def test_verify_soc_without_coincurve(monkeypatch):
    """Readers check feed updates with no compiled dependency."""
    pytest.importorskip("coincurve")  # only to sign the update
    from swarmfs.feeds import verify_soc
    from swarmfs.join import VerificationError

    data, owner, address = _soc(VECTORS[0][0])
    monkeypatch.setitem(sys.modules, "coincurve", None)
    verify_soc(data, owner, address)  # passes
    other = S.address_of(b"\x04" + bytes(64))
    with pytest.raises(VerificationError):
        verify_soc(data, other, address)


def test_the_after_hint_reaches_bee():
    import asyncio

    from swarmfs._client import SwarmClient

    seen = []

    class _Resp:
        status = 200
        headers = {"Swarm-Feed-Index": "00000000000000a0",
                   "Swarm-Feed-Index-Next": "00000000000000a1"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Session:
        closed = False

        def get(self, url, headers=None):
            seen.append(url)
            return _Resp()

    async def run():
        client = SwarmClient("http://bee")
        client._session = _Session()
        return (await client.feed_head("aa" * 20, "bb" * 32, after=159),
                await client.feed_head("aa" * 20, "bb" * 32))

    hinted, plain = asyncio.run(run())
    assert hinted == plain == ("00000000000000a0", "00000000000000a1")
    assert seen[0].endswith("?type=sequence&after=159")
    assert seen[1].endswith("?type=sequence")


# -- signing a hash as given (no prefix) ---------------------------------------
#
# eth-keys' `sign_msg_hash` over sha256(b"an offer"), as loopmarket stored
# them before it moved onto this module (2026-10-08): eth-keys writes v as
# 0/1, this module as 27/28; r and s are the same bytes.

RAW_HASH = bytes.fromhex(
    "3b3806a2e0303869f29bacb8cf52e5806f70b477d8909401cd47275785553890")
RAW_VECTORS = [
    ("01" * 32, "0x1a642f0E3c3aF545E7AcBD38b07251B3990914F1",
     "0acd72ebaadcd3dfdbb3257edee8a362fc2cab1d207b2eb17525346dc95c5a14"
     "240990587100c80397a40cde4790d5b4ce8b2ef5a84dec79cb0faf21585b4f7201",
     "031b84c5567b126440995d3ed5aaba0565d71e1834604819ff9c17f5e9d5dd078f"),
    ("02" * 32, "0x5050A4F4b3f9338C3472dcC01A87C76A144b3c9c",
     "b3d696b04f8c27485e9d1b0a3bf7ecd3428bd9c3e825e2874996802a9bf776f0"
     "52350be2ffe6a3d2376d6e3339e39ad6dd949a3793c91eeb28c267c92536ecba01",
     "024d4b6cd1361032ca9bd2aeb9d900aa4d45d9ead80ac9423374c451a7254d0766"),
]


def _v27(eth_keys_sig: str) -> bytes:
    sig = bytes.fromhex(eth_keys_sig)
    return sig[:64] + bytes([sig[64] + 27])


@pytest.mark.parametrize("key,address,signature,public", RAW_VECTORS)
def test_sign_hash_is_eth_keys_sign_msg_hash(key, address, signature, public):
    pytest.importorskip("coincurve")
    signer = S.Signer(key)
    assert signer.sign_hash(RAW_HASH) == _v27(signature)
    assert S.checksum_address(signer.address) == address


@pytest.mark.parametrize("key,address,signature,public", RAW_VECTORS)
@pytest.mark.parametrize("coincurve_present", [True, False])
def test_recover_hash_with_and_without_coincurve(monkeypatch, coincurve_present,
                                                 key, address, signature,
                                                 public):
    if coincurve_present:
        pytest.importorskip("coincurve")
    else:
        monkeypatch.setitem(sys.modules, "coincurve", None)
    for sig in (bytes.fromhex(signature), _v27(signature)):  # v 0/1 or 27/28
        assert S.checksum_address(S.recover_hash(sig, RAW_HASH)) == address
        assert S.compressed(S.recover_hash_key(sig, RAW_HASH)).hex() == public
    # the prefixed form is a different message
    assert S.recover_digest(_v27(signature), RAW_HASH) != \
        S.recover_hash(_v27(signature), RAW_HASH)


def test_a_hash_must_be_32_bytes():
    pytest.importorskip("coincurve")
    with pytest.raises(ValueError):
        S.Signer("01" * 32).sign_hash(b"short")
    with pytest.raises(ValueError):
        S.recover_hash(bytes(65), b"short")


@pytest.mark.parametrize("address", [
    # EIP-55's own examples, and key 1's address
    "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
    "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
    "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
    "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
    "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf",
])
def test_checksum_address_is_eip55(address):
    assert S.checksum_address(bytes.fromhex(address[2:])) == address


def test_compressed_matches_coincurve():
    coincurve = pytest.importorskip("coincurve")
    key = coincurve.PrivateKey(bytes.fromhex("01" * 32))
    assert S.compressed(key.public_key.format(compressed=False)) == \
        key.public_key.format(compressed=True)
