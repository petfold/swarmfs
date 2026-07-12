"""Swarm feeds: a stable (owner, topic) address whose latest update points at
the current root manifest. This is what makes ``bzzf://`` mutable.

Formats mirror bee-js exactly:

- topic: 32 bytes — keccak256 of the human-readable string (or given raw as hex)
- sequence index: 8-byte big-endian, starting at 0
- feed identifier: keccak256(topic ‖ index)
- update chunk: a single-owner chunk (SOC) at keccak256(identifier ‖ owner),
  wrapping a content chunk whose payload is timestamp(8 BE) ‖ root reference
- SOC signature: ethereum personal-sign over keccak256(identifier ‖ cac address)

Reading needs no keys; writing requires the ``feeds`` extra
(``pip install swarmfs[feeds]``) and the feed owner's private key.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from ._client import SwarmClient
from .bmt import cac_data, chunk_address, keccak256

SOC_IDENTIFIER_SIZE = 32
SOC_SIGNATURE_SIZE = 65
SOC_SPAN_OFFSET = SOC_IDENTIFIER_SIZE + SOC_SIGNATURE_SIZE  # 97
SOC_PAYLOAD_OFFSET = SOC_SPAN_OFFSET + 8  # 105


class FeedError(RuntimeError):
    pass


def topic_bytes(topic: str) -> bytes:
    """Raw 64-hex topics pass through; anything else is keccak256(utf-8),
    like bee-js ``Topic.fromString``."""
    if len(topic) == 64:
        try:
            return bytes.fromhex(topic)
        except ValueError:
            pass
    return keccak256(topic.encode())


def owner_bytes(owner: str) -> bytes:
    owner = owner.lower().removeprefix("0x")
    if len(owner) != 40:
        raise ValueError(f"invalid feed owner {owner!r}: expected a 40-hex ethereum address")
    return bytes.fromhex(owner)


def feed_identifier(topic: bytes, index: int) -> bytes:
    return keccak256(topic + index.to_bytes(8, "big"))


def soc_address(identifier: bytes, owner: bytes) -> bytes:
    return keccak256(identifier + owner)


class FeedSigner:
    """Signs feed updates with the owner's private key (eth-keys)."""

    def __init__(self, private_key: str | bytes):
        try:
            from eth_keys import keys
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "feed writes require the 'feeds' extra: pip install 'swarmfs[feeds]'"
            ) from e
        if isinstance(private_key, str):
            private_key = bytes.fromhex(private_key.removeprefix("0x"))
        self._key = keys.PrivateKey(private_key)

    @property
    def owner(self) -> bytes:
        return self._key.public_key.to_canonical_address()

    @property
    def owner_hex(self) -> str:
        return self.owner.hex()

    def sign_digest(self, digest32: bytes) -> bytes:
        """Ethereum personal-message signature, r ‖ s ‖ v(27/28)."""
        prefixed = keccak256(b"\x19Ethereum Signed Message:\n32" + digest32)
        sig = self._key.sign_msg_hash(prefixed)
        return (
            sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big") + bytes([sig.v + 27])
        )


@dataclass
class FeedUpdate:
    reference: str  # hex of the root reference the feed points at
    index: int  # index of this update
    next_index: int


class FeedOps:
    """Feed lookup and update over the Bee HTTP API."""

    def __init__(self, client: SwarmClient):
        self.client = client

    async def latest(self, owner: bytes, topic: bytes) -> FeedUpdate | None:
        """Resolve the latest update, or None for a never-written feed.

        Bee's server-side sequence lookup finds the current index (returned
        in the Swarm-Feed-Index header); we then fetch the SOC chunk at that
        index ourselves and extract the reference from its payload — exact
        and independent of how the /feeds body resolves the content.
        """
        head = await self.client.feed_head(owner.hex(), topic.hex())
        if head is None:
            return None
        index_hex, next_hex = head
        index = int.from_bytes(bytes.fromhex(index_hex), "big")
        next_index = (
            int.from_bytes(bytes.fromhex(next_hex), "big") if next_hex else index + 1
        )
        soc = await self.client.chunk_get(
            soc_address(feed_identifier(topic, index), owner).hex()
        )
        reference = self._reference_from_soc(soc)
        return FeedUpdate(reference=reference, index=index, next_index=next_index)

    @staticmethod
    def _reference_from_soc(soc: bytes) -> str:
        if len(soc) < SOC_PAYLOAD_OFFSET:
            raise FeedError(f"feed update chunk too short: {len(soc)} bytes")
        payload = soc[SOC_PAYLOAD_OFFSET:]
        # bee-js "reference" format: timestamp(8) + ref(32|64); tolerate a
        # bare ref without timestamp
        if len(payload) in (40, 72):
            return payload[8:].hex()
        if len(payload) in (32, 64):
            return payload.hex()
        # "wrapped chunk" format: the update wraps the content's root chunk
        # itself; its BMT address is the reference
        return chunk_address(soc[SOC_SPAN_OFFSET:]).hex()

    async def update(
        self,
        signer: FeedSigner,
        topic: bytes,
        index: int,
        reference: str,
        stamp: str,
    ) -> None:
        """Publish ``reference`` as feed update ``index`` (bee-js format)."""
        payload = int(time.time()).to_bytes(8, "big") + bytes.fromhex(reference)
        data = cac_data(payload)
        identifier = feed_identifier(topic, index)
        digest = keccak256(identifier + chunk_address(data))
        signature = signer.sign_digest(digest)
        await self.client.soc_post(
            signer.owner_hex, identifier.hex(), signature.hex(), data, stamp
        )
