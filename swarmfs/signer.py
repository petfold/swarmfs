"""secp256k1 signatures as Bee and Ethereum use them, with no cryptography
of our own.

Signing, verifying and key recovery are libsecp256k1's (Bitcoin Core's
secp256k1 library), reached through ``coincurve`` (the ``feeds`` extra).
This module owns only the encoding around them:

- the digest Bee checks: ``keccak256("\\x19Ethereum Signed Message:\\n32" ‖
  digest32)``, where ``digest32`` is ``keccak256(data)`` — for a feed
  update, ``data`` is ``identifier ‖ wrapped-chunk address``;
- the 65-byte wire form ``r ‖ s ‖ v`` with ``v`` in ``{27, 28}``;
- an address: the last 20 bytes of ``keccak256`` of the 64-byte public key,
  written with EIP-55 mixed-case checksum by `checksum_address`.

`Signer.sign_hash` and `recover_hash` sign and recover a 32-byte hash as
given, with no prefix (eth-keys' ``sign_msg_hash``), for applications that
hash under their own domain separation, such as loopmarket's offer ids.

Signing never falls back: without coincurve it refuses, naming the pip
command, because a pure-Python signer handles the private key in code that
is neither constant-time nor libsecp256k1. Recovery — and so verification —
does fall back to pure Python when coincurve is absent: it handles no
secret, so timing does not matter, and its answers are checked against
coincurve's in the tests. That lets a reader verify feed updates and signed
records where coincurve cannot install (Pyodide).
"""

from __future__ import annotations

from .bmt import keccak256

ETH_PREFIX = b"\x19Ethereum Signed Message:\n32"


class SignatureError(ValueError):
    """A signature that is malformed or recovers no public key."""


def _coincurve():
    try:
        import coincurve
    except ImportError as e:
        raise ImportError(
            "signing needs coincurve (libsecp256k1): "
            'pip install "swarmfs[feeds]"') from e
    return coincurve


def message_digest(digest32: bytes) -> bytes:
    """The 32 bytes actually signed: the Ethereum signed-message digest of
    a 32-byte digest."""
    if len(digest32) != 32:
        raise ValueError(f"expected a 32-byte digest, got {len(digest32)}")
    return keccak256(ETH_PREFIX + digest32)


def address_of(public_key: bytes) -> bytes:
    """20-byte address of a public key (65 bytes with the 0x04 prefix, or
    the bare 64)."""
    if len(public_key) == 65 and public_key[0] == 4:
        public_key = public_key[1:]
    if len(public_key) != 64:
        raise ValueError("expected an uncompressed public key")
    return keccak256(public_key)[-20:]


def checksum_address(address: bytes) -> str:
    """``0x`` and the 20-byte address in EIP-55 mixed case: a hex letter is
    upper case where the matching nibble of the lower-case hex's keccak256
    is 8 or more."""
    if len(address) != 20:
        raise ValueError("an address is 20 bytes")
    hexed = bytes(address).hex()
    nibbles = keccak256(hexed.encode("ascii")).hex()
    return "0x" + "".join(c.upper() if int(n, 16) >= 8 else c
                          for c, n in zip(hexed, nibbles))


def compressed(public_key: bytes) -> bytes:
    """The 33-byte compressed form of an uncompressed public key."""
    if len(public_key) == 65 and public_key[0] == 4:
        public_key = public_key[1:]
    if len(public_key) != 64:
        raise ValueError("expected an uncompressed public key")
    return bytes([2 + (public_key[63] & 1)]) + public_key[:32]


def _private_bytes(private_key) -> bytes:
    if isinstance(private_key, str):
        private_key = bytes.fromhex(private_key.lower().removeprefix("0x"))
    if len(private_key) != 32:
        raise ValueError("a private key is 32 bytes (64 hex characters)")
    return bytes(private_key)


class Signer:
    """A secp256k1 private key that signs the way Bee checks."""

    def __init__(self, private_key):
        self._key = _coincurve().PrivateKey(_private_bytes(private_key))
        self.public_key = self._key.public_key.format(compressed=False)
        self.address = address_of(self.public_key)

    @property
    def address_hex(self) -> str:
        return self.address.hex()

    def sign_digest(self, digest32: bytes) -> bytes:
        """65-byte ``r ‖ s ‖ v`` over the signed-message digest of
        ``digest32``."""
        return self.sign_hash(message_digest(digest32))

    def sign_hash(self, hash32: bytes) -> bytes:
        """65-byte ``r ‖ s ‖ v`` over ``hash32`` itself, with no prefix.

        Only for a hash the caller computes under its own domain
        separation: Bee and Ethereum check `sign_digest`'s form, and the
        prefix there is what stops a signature being replayed as something
        else."""
        if len(hash32) != 32:
            raise ValueError(f"expected a 32-byte hash, got {len(hash32)}")
        sig = self._key.sign_recoverable(bytes(hash32), hasher=None)
        return sig[:64] + bytes([sig[64] + 27])

    def sign(self, data: bytes) -> bytes:
        """65-byte ``r ‖ s ‖ v`` over ``keccak256(data)`` — Bee's (and
        bee-js's) ``sign(data)``."""
        return self.sign_digest(keccak256(data))


def _split(signature: bytes):
    if len(signature) != 65:
        raise SignatureError(f"a signature is 65 bytes, got {len(signature)}")
    v = signature[64]
    recid = v - 27 if v in (27, 28) else v
    if recid not in (0, 1):
        raise SignatureError(f"unsupported recovery byte {v}")
    return signature[:32], signature[32:64], recid


def recover_hash_key(signature: bytes, hash32: bytes) -> bytes:
    """The 65-byte uncompressed public key whose key made ``signature``
    over ``hash32`` itself (no prefix). Raises ``SignatureError``."""
    if len(hash32) != 32:
        raise ValueError(f"expected a 32-byte hash, got {len(hash32)}")
    r, s, recid = _split(signature)
    try:
        coincurve = _coincurve()
    except ImportError:
        return _py_recover(int.from_bytes(r, "big"),
                           int.from_bytes(s, "big"), recid, bytes(hash32))
    try:
        return coincurve.PublicKey.from_signature_and_message(
            r + s + bytes([recid]), bytes(hash32), hasher=None
        ).format(compressed=False)
    except Exception as e:
        raise SignatureError(f"signature recovers no key: {e}") from e


def recover_hash(signature: bytes, hash32: bytes) -> bytes:
    """The 20-byte address that signed ``hash32`` itself (no prefix)."""
    return address_of(recover_hash_key(signature, hash32))


def recover_digest(signature: bytes, digest32: bytes) -> bytes:
    """The 20-byte address whose key made ``signature`` over the
    signed-message digest of ``digest32``. Raises ``SignatureError``."""
    return recover_hash(signature, message_digest(digest32))


def recover(signature: bytes, data: bytes) -> bytes:
    """``recover_digest`` over ``keccak256(data)``."""
    return recover_digest(signature, keccak256(data))


def verify(signature: bytes, data: bytes, address: bytes) -> bool:
    """True iff ``signature`` over ``data`` was made by ``address``."""
    try:
        return recover(signature, data) == bytes(address)
    except SignatureError:
        return False


# -- recovery without coincurve (public inputs only) ---------------------------

_P = 2**256 - 2**32 - 977
_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_G = (0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
      0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8)


def _add(a, b):
    if a is None:
        return b
    if b is None:
        return a
    (x1, y1), (x2, y2) = a, b
    if x1 == x2:
        if (y1 + y2) % _P == 0:
            return None
        m = 3 * x1 * x1 * pow(2 * y1, -1, _P) % _P
    else:
        m = (y2 - y1) * pow(x2 - x1, -1, _P) % _P
    x3 = (m * m - x1 - x2) % _P
    return x3, (m * (x1 - x3) - y1) % _P


def _mul(k, point):
    out = None
    while k:
        if k & 1:
            out = _add(out, point)
        point = _add(point, point)
        k >>= 1
    return out


def _py_recover(r: int, s: int, recid: int, h: bytes) -> bytes:
    if not (1 <= r < _N and 1 <= s < _N):
        raise SignatureError("signature values out of range")
    x = r
    y2 = (pow(x, 3, _P) + 7) % _P
    y = pow(y2, (_P + 1) // 4, _P)
    if y * y % _P != y2:
        raise SignatureError("signature recovers no key")
    if y % 2 != recid:
        y = _P - y
    e = int.from_bytes(h, "big") % _N
    r_inv = pow(r, -1, _N)
    q = _add(_mul(s * r_inv % _N, (x, y)), _mul((-e * r_inv) % _N, _G))
    if q is None:
        raise SignatureError("signature recovers no key")
    return b"\x04" + q[0].to_bytes(32, "big") + q[1].to_bytes(32, "big")
