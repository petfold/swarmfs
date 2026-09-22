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

A mount can also be *pinned*: ``at_root=<reference>`` freezes the URL to one
root and ``at=<time>`` to whatever the feed pointed at then. Both are
read-only views — the stable URL keeps working, the content behind it does
not move, and no path anywhere has to be rewritten.
"""

from __future__ import annotations

import datetime
import math
import time

from fsspec.asyn import sync

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
    at_root:
        Pin every ``bzzf://`` path on this instance to one root reference:
        the feed is never looked up, so the URL reads as a frozen,
        tamper-evident snapshot. Read-only.
    at:
        Pin to the update in force at a moment: unix seconds, a
        ``datetime`` (naive values are read as UTC), or an ISO-8601 string
        such as ``"2026-09-01T12:00Z"``. Resolved once per feed, then
        behaves exactly like ``at_root``. Read-only.

    Pinned views are the reproducible-read mechanism a table or catalog
    layer needs: the paths it records stay ``bzzf://owner/topic/…`` and the
    pin is a storage option, so every fsspec consumer — dask, DuckDB,
    pyarrow — gets it with no path rewriting.
    """

    protocol = "bzzf"

    def __init__(self, *args, signer: str | None = None, feed_ttl: float = 15.0,
                 at_root: str | None = None, at=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.signer = FeedSigner(signer) if signer else None
        self.feed_ttl = feed_ttl
        if at_root is not None and at is not None:
            raise ValueError("pass at_root= or at=, not both: each pins the "
                             "view to one root")
        self.at_root = _pinned_root(at_root) if at_root is not None else None
        self.at = _parse_at(at) if at is not None else None
        self._pinned = self.at_root is not None or self.at is not None
        self._feeds = FeedOps(self.client)
        # feed key -> (next_index, cache expiry); the resolved root lives in
        # the ordinary lineage maps (_root_map/_origin) keyed by the feed key
        self._feed_state: dict[str, tuple[int, float]] = {}
        self._feed_identity: dict[str, tuple[bytes, bytes]] = {}  # key -> (owner, topic)
        # feed key -> publication time of the update it currently resolves to
        # (None when the payload format carries none) — what modified() reports
        self._feed_times: dict[str, int | None] = {}
        # local-first: feed key -> newest committed-but-unpublished root;
        # published by the confirmed-listener once the network provably
        # serves it (see _after_commit)
        self._feed_pending: dict[str, str] = {}
        if self._local is not None:
            self._local.add_listener(self._on_ladder_event)

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
        head = self._resolve_head(key)
        self._register_root(head)  # a feed's payload is a root (ACT-wrapped if protected)
        return head, sub

    async def _refresh_feed(self, owner: bytes, topic: bytes, key: str) -> None:
        """Look up the feed unless the cached resolution is still fresh.

        External updates are adopted by advancing the lineage head
        (last-write-wins); roots this instance itself committed are never
        rolled back by a stale lookup. A pinned view resolves once and then
        never looks the feed up again.
        """
        now = time.monotonic()
        state = self._feed_state.get(key)
        if state is not None and state[1] > now:
            return
        if self._pinned:
            await self._pin_feed(owner, topic, key)
            return
        await self._setup()
        upd = await self._feeds.latest(owner, topic, verify=bool(self.verify_active))
        if upd is None:
            self._bump_feed_state(key, 0)
            return
        self._feed_times[key] = upd.timestamp
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
        self._bump_feed_state(key, upd.next_index)

    async def _pin_feed(self, owner: bytes, topic: bytes, key: str) -> None:
        """Resolve a pinned view's root, once and for all."""
        await self._setup()
        if self.at_root is not None:
            root, ts = self.at_root, None
        else:
            upd = await self._feeds.at(owner, topic, self.at,
                                       verify=bool(self.verify_active))
            if upd is None:
                when = datetime.datetime.fromtimestamp(
                    self.at, datetime.timezone.utc).isoformat()
                raise FileNotFoundError(
                    f"bzzf://{owner.hex()}/… has no update at or before "
                    f"{when} — the feed did not exist yet")
            root, ts = upd.reference, upd.timestamp
        self._root_map[key] = root
        self._origin[root] = key
        self._register_root(root)
        self._feed_times[key] = ts
        # never expires: a pinned view is a fixed root, so there is nothing
        # to re-resolve (and no lookup is ever issued again)
        self._feed_state[key] = (0, math.inf)

    def _bump_feed_state(self, key: str, next_index: int) -> None:
        """Advance (never regress) the next-index counter. Feed lookups lag
        our own freshly-published updates while they propagate, so the local
        counter is the floor — without it, back-to-back commits would
        re-publish the same index and the second SOC would be dropped."""
        current = self._feed_state.get(key, (0, 0.0))[0]
        self._feed_state[key] = (max(current, next_index), time.monotonic() + self.feed_ttl)

    async def _ls(self, path, detail=True, **kwargs):
        # resolve (and possibly adopt a newer feed head — which invalidates
        # the dircache) BEFORE the cached-listing check in the base class,
        # so listing freshness honors feed_ttl like cat/info do
        stripped = self._strip_protocol(path)
        await self._resolve_path(stripped)
        return await super()._ls(stripped, detail=detail, **kwargs)

    # -------------------------------------------------------------- staging

    def _stage_write(self, ref, sub, sw):
        self._require_writable()
        self._require_signer(ref)
        super()._stage_write(ref, sub, sw)

    def _stage_rm(self, ref, sub):
        self._require_writable()
        self._require_signer(ref)
        super()._stage_rm(ref, sub)

    def _require_writable(self) -> None:
        """A pinned mount is a view of the past; a write would have nowhere
        to publish (advancing the feed would contradict the pin)."""
        if not self._pinned:
            return
        how = (f"at_root={self.at_root[:8]}…" if self.at_root is not None
               else f"at={self.at}")
        raise FeedError(
            f"this bzzf mount is a pinned read-only view ({how}): it resolves "
            "to a fixed root, so there is nothing to write into. Open the "
            "filesystem without at_root=/at= to write to the feed.")

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
        if self._syncer is not None:
            # Local-first: publication rides the durability ladder. The
            # feed must never point readers at content the network cannot
            # serve yet, so the update waits for network confirmation —
            # the journal listener below publishes the newest confirmed
            # head (and a failed publish retries on the next confirmation).
            self._feed_pending[okey] = result.new_root
            return
        await self._publish_feed(okey, result.new_root, result.batch)

    async def _publish_feed(self, okey: str, new_root: str,
                            stamp: str) -> None:
        owner, topic = self._feed_identity[okey]
        assert self.signer is not None  # enforced at staging time
        # the next index is the max of a fresh lookup (another writer may
        # have advanced the feed — last-write-wins) and our own counter
        # (lookups lag our own just-published updates while they propagate)
        head = await self.client.feed_head(owner.hex(), topic.hex())
        looked_up = int.from_bytes(bytes.fromhex(head[0]), "big") + 1 if head else 0
        local = self._feed_state.get(okey, (0, 0.0))[0]
        next_index = max(looked_up, local)
        self._feed_times[okey] = await self._feeds.update(
            self.signer, topic, next_index, new_root, stamp=stamp
        )
        self._bump_feed_state(okey, next_index + 1)

    # --------------------------------------------------------------- mtime

    def modified(self, path):
        """When the update this view resolves to was published.

        ``bzz://`` content is immutable at a reference, so the base class
        reports a constant; a feed *does* move, and consumers that cache by
        mtime (DuckDB, table readers) need to see it. Falls back to the
        epoch constant for a feed whose payload format carries no timestamp,
        and for an ``at_root=`` view — a frozen root never changes.
        """
        self.info(path)  # existence check, and resolves the feed
        key = self._parse_feed_path(self._strip_protocol(path))[2]
        ts = self._feed_times.get(key)
        return datetime.datetime.fromtimestamp(ts or 0, tz=datetime.timezone.utc)

    def _on_ladder_event(self, event: dict) -> None:
        """Journal listener (runs on the syncer's worker thread): each
        confirmed rung publishes any feed whose pending head the network
        now provably serves, then records the remote-tracking root."""
        if event.get("ev") != "confirmed":
            return
        for okey, root in list(self._feed_pending.items()):
            if not self._local.network_confirmed(root):
                continue
            stamp = self._syncer.remote.stamp  # resolves lazily; node is up
            sync(self.loop, self._publish_feed, okey, root, stamp)
            if self._feed_pending.get(okey) == root:
                del self._feed_pending[okey]
            self._local.set_remote_root(okey, root)


def _pinned_root(reference: str) -> str:
    ref = str(reference).strip().lower().removeprefix("0x")
    if len(ref) not in (64, 128) or any(c not in "0123456789abcdef" for c in ref):
        raise ValueError(
            f"invalid at_root={reference!r}: expected a 64-hex root reference "
            "(or 128 for an encrypted one) — e.g. the value fs.latest() or a "
            "CommitResult returned")
    return ref


def _parse_at(value) -> int:
    """Unix seconds from an int/float, a datetime, or an ISO-8601 string.

    Naive values are read as UTC: a pin is meant to be reproducible, and
    the local timezone of whoever runs the pipeline is not.
    """
    if isinstance(value, bool):
        raise ValueError(f"invalid at={value!r}")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime.datetime):
        dt = value
    else:
        text = str(value).strip()
        try:
            dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(
                f"invalid at={value!r}: expected unix seconds, a datetime, or "
                "an ISO-8601 string such as '2026-09-01T12:00Z'") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())
