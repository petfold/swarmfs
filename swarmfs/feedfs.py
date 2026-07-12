"""SwarmFeedFileSystem: the mutable ``bzzf://`` protocol.

``bzzf://<owner>/<topic>/path`` resolves through a Swarm feed to the latest
committed root manifest, so the URL is stable while contents change. The
owner is a 40-hex ethereum address; the topic is a human-readable string
(hashed, like bee-js ``Topic.fromString``) or a raw 64-hex topic.

Reading needs no keys. Writing reuses the whole v1 commit machinery — the
feed is just another lineage whose head advances — plus one extra step after
each commit: publish a signed feed update pointing at the new root. That
needs ``signer=<owner's private key hex>`` in storage_options and the
``feeds`` extra installed.

Feeds are last-write-wins: two writers updating the same feed concurrently
will race, and the later sequence update simply wins. Feed resolution is
cached per instance for ``feed_ttl`` seconds (own commits refresh it
immediately), so other writers' updates become visible within the TTL.
"""

from __future__ import annotations

import time

from .core import SwarmFileSystem
from .feeds import FeedError, FeedOps, FeedSigner, owner_bytes, topic_bytes

_FEED_PREFIX = "feed!"


class SwarmFeedFileSystem(SwarmFileSystem):
    """Mutable, feed-mounted view of Swarm.

    Extra parameters (on top of SwarmFileSystem's):

    signer:
        The feed owner's private key (hex, 0x-prefixed or not). Required for
        writes; must match the owner in the path.
    feed_ttl:
        Seconds to cache feed resolution per instance (default 15). Lower it
        when tailing someone else's feed; own commits bypass it.
    """

    protocol = "bzzf"

    def __init__(self, *args, signer: str | None = None, feed_ttl: float = 15.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.signer = FeedSigner(signer) if signer else None
        self.feed_ttl = feed_ttl
        self._feeds = FeedOps(self.client)
        # feed key -> (next_index, cache expiry); the resolved root lives in
        # the ordinary lineage maps (_root_map/_origin) keyed by the feed key
        self._feed_state: dict[str, tuple[int, float]] = {}
        self._feed_identity: dict[str, tuple[bytes, bytes]] = {}  # key -> (owner, topic)

    # ----------------------------------------------------------- path model

    @staticmethod
    def _is_pseudo(ref: str) -> bool:
        return (
            ref == "new" or ref.startswith("new-") or ref.startswith(_FEED_PREFIX)
        )

    def _parse_feed_path(self, path: str) -> tuple[bytes, bytes, str, str]:
        owner, _, rest = path.partition("/")
        topic, _, sub = rest.partition("/")
        if not owner or not topic:
            raise ValueError(
                f"invalid bzzf path {path!r}: expected bzzf://<owner>/<topic>/<path>"
            )
        ob = owner_bytes(owner)
        tb = topic_bytes(topic)
        key = f"{_FEED_PREFIX}{ob.hex()}!{tb.hex()}"
        self._feed_identity[key] = (ob, tb)
        return ob, tb, key, sub.strip("/")

    def _subpath_of(self, path: str) -> str:
        return self._parse_feed_path(path)[3]

    def latest(self, ref: str) -> str:
        """Current head root of a feed, given ``bzzf://owner/topic[/...]``
        (or a raw root reference / feed key)."""
        ref = self._strip_protocol(ref)
        if "/" in ref:
            ref = self._parse_feed_path(ref)[2]
        return self._resolve_head(ref)

    async def _resolve_path(self, path: str) -> tuple[str, str]:
        owner, topic, key, sub = self._parse_feed_path(path)
        await self._refresh_feed(owner, topic, key)
        return self._resolve_head(key), sub

    async def _refresh_feed(self, owner: bytes, topic: bytes, key: str) -> None:
        """Look up the feed unless the cached resolution is still fresh.

        External updates are adopted by advancing the lineage head
        (last-write-wins); roots this instance itself committed are never
        rolled back by a stale lookup.
        """
        now = time.monotonic()
        state = self._feed_state.get(key)
        if state is not None and state[1] > now:
            return
        upd = await self._feeds.latest(owner, topic)
        if upd is None:
            self._feed_state[key] = (0, now + self.feed_ttl)
            return
        head = self._resolve_head(key)
        if head == key:
            # first sighting of this feed: attach the lineage
            self._root_map[key] = upd.reference
            self._origin[upd.reference] = key
        elif head != upd.reference and upd.reference not in self._origin:
            # someone else updated the feed — adopt their root as new head
            self._root_map[head] = upd.reference
            self._origin[upd.reference] = key
            self.invalidate_cache()
        self._feed_state[key] = (upd.next_index, now + self.feed_ttl)

    async def _ls(self, path, detail=True, **kwargs):
        # resolve (and possibly adopt a newer feed head — which invalidates
        # the dircache) BEFORE the cached-listing check in the base class,
        # so listing freshness honors feed_ttl like cat/info do
        stripped = self._strip_protocol(path)
        await self._resolve_path(stripped)
        return await super()._ls(stripped, detail=detail, **kwargs)

    # -------------------------------------------------------------- staging

    def _stage_write(self, ref, sub, sw):
        self._require_signer(ref)
        super()._stage_write(ref, sub, sw)

    def _stage_rm(self, ref, sub):
        self._require_signer(ref)
        super()._stage_rm(ref, sub)

    def _require_signer(self, ref: str) -> None:
        """Fail at staging time — before any upload — if this instance can't
        publish the feed update that would make the write visible."""
        key = self._origin_of(ref)
        if not key.startswith(_FEED_PREFIX):
            return
        if self.signer is None:
            raise FeedError(
                "writing to a bzzf:// feed requires the owner's private key: "
                "pass signer=<hex key> in storage_options "
                "(and install the feeds extra: pip install 'swarmfs[feeds]')"
            )
        owner, _ = self._feed_identity[key]
        if self.signer.owner != owner:
            raise FeedError(
                f"signer address 0x{self.signer.owner_hex} does not own this feed "
                f"(owner 0x{owner.hex()})"
            )

    # --------------------------------------------------------------- commit

    async def _after_commit(self, okey: str, result) -> None:
        if not okey.startswith(_FEED_PREFIX):
            return
        owner, topic = self._feed_identity[okey]
        assert self.signer is not None  # enforced at staging time
        # re-check the head index right before publishing: another writer may
        # have advanced the feed since our cached lookup (last-write-wins)
        head = await self.client.feed_head(owner.hex(), topic.hex())
        next_index = int.from_bytes(bytes.fromhex(head[0]), "big") + 1 if head else 0
        await self._feeds.update(
            self.signer, topic, next_index, result.new_root, stamp=result.batch
        )
        self._feed_state[okey] = (next_index + 1, time.monotonic() + self.feed_ttl)
