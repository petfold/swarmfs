"""Postage stamp selection, validation, and purchase.

A commit validates its stamp *before* uploading anything, so the user gets an
actionable error up front — never a mid-write 402. Purchase is exposed as a
capability only (``StampManager.plan``/``buy``) — nothing in swarmfs ever
buys implicitly; deciding to spend the wallet's xBZZ belongs to the caller.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass

from ._client import SwarmClient
from .exceptions import BeeAPIError, StampError  # noqa: F401 — StampError's canonical home; re-exported here

BLOCK_SECS = 5  # Gnosis chain block time
PLUR_PER_BZZ = 10**16

# upload size -> batch depth. Theoretical capacity is 2**depth * 4 KB, but
# an immutable batch fails as soon as any SINGLE bucket (of 65536) fills —
# measured live: one 42 MB upload filled a depth-18 batch (4 slots per
# bucket). These tiers keep the balls-into-buckets overflow risk under ~5%
# per upload.
_DEPTH_TIERS = ((15 * 2**20, 18), (150 * 2**20, 19), (2**30, 20))


def suggest_depth(size_bytes: int) -> int:
    """Smallest batch depth that holds ``size_bytes`` with headroom."""
    for limit, depth in _DEPTH_TIERS:
        if size_bytes <= limit:
            return depth
    return 20 + math.ceil(math.log2(size_bytes / 2**30))


@dataclass
class BatchPlan:
    """A priced purchase: buy with ``StampManager.buy(amount, depth)``."""

    depth: int
    amount: int
    ttl_secs: int  # actual validity after the chain's minimum is applied
    cost_bzz: float


@dataclass
class StampInfo:
    batch_id: str
    usable: bool
    ttl: int  # seconds; -1 when the node can't estimate it
    utilization_ratio: float | None
    label: str
    immutable: bool

    @classmethod
    def from_api(cls, d: dict) -> "StampInfo":
        return cls(
            batch_id=d["batchID"],
            usable=bool(d.get("usable")),
            ttl=int(d.get("batchTTL", -1)),
            utilization_ratio=d.get("utilizationRatio"),
            label=d.get("label", ""),
            immutable=bool(d.get("immutableFlag", False)),
        )

    def problem(self, min_ttl: int) -> str | None:
        """Why this stamp can't be used right now, or None if it can."""
        if not self.usable:
            return "not usable (still syncing, or expired)"
        if 0 <= self.ttl <= min_ttl:
            return f"TTL {self.ttl}s is below the minimum {min_ttl}s"
        if self.utilization_ratio is not None and self.utilization_ratio >= 1.0:
            return "full (utilization at 100%)"
        return None


class StampManager:
    """Resolves the ``stamp`` storage option to a validated batch id.

    ``stamp`` may be an explicit batch id (64 hex chars), ``"auto"``/None to
    pick the usable batch with the longest TTL. The stamp list is fetched
    fresh per resolution so usability/TTL are current.
    """

    def __init__(self, client: SwarmClient, min_ttl: int = 60):
        self._client = client
        self.min_ttl = min_ttl

    async def resolve(self, stamp: str | None = None) -> str:
        stamps = [StampInfo.from_api(d) for d in await self._client.stamps_list()]

        if stamp and stamp != "auto":
            match = next((s for s in stamps if s.batch_id.lower() == stamp.lower()), None)
            if match is None:
                have = ", ".join(f"{s.batch_id[:8]}…({s.label or 'no label'})" for s in stamps)
                raise StampError(
                    f"postage batch {stamp!r} not found on {self._client.api_url}"
                    + (f"; batches on this node: {have}" if have else "; the node has no batches")
                )
            problem = match.problem(self.min_ttl)
            if problem:
                raise StampError(f"postage batch {stamp[:8]}… is {problem}")
            return match.batch_id

        usable = [s for s in stamps if s.problem(self.min_ttl) is None]
        if not usable:
            if not stamps:
                raise StampError(
                    f"no postage stamps on {self._client.api_url} — writing to Swarm "
                    "needs one. Buy a batch first, e.g. `swarm-cli stamp buy "
                    "--depth 20 --amount 100000000` (or POST /stamps/{amount}/{depth})."
                )
            reasons = "; ".join(
                f"{s.batch_id[:8]}…({s.label or 'no label'}): {s.problem(self.min_ttl)}"
                for s in stamps
            )
            raise StampError(f"no usable postage stamp on {self._client.api_url}: {reasons}")
        # longest remaining TTL wins; ttl == -1 (unknown) sorts last
        return max(usable, key=lambda s: s.ttl if s.ttl >= 0 else -2).batch_id

    async def plan(self, size_bytes: int, ttl_secs: int) -> BatchPlan:
        """Price a batch for ``size_bytes`` lasting ``ttl_secs``, at the
        current on-chain price.

        The node requires STRICTLY more than ``minimumValidityBlocks``
        (24 h on Gnosis) at purchase time, and the price can move between
        planning and buying (rejected live at the exact minimum) — so the
        floor is padded by an hour.
        """
        chain = await self._client.chainstate()
        price = int(chain["currentPrice"])
        floor = int(chain.get("minimumValidityBlocks", 0)) + 3600 // BLOCK_SECS
        blocks = max(math.ceil(ttl_secs / BLOCK_SECS), floor)
        depth = suggest_depth(size_bytes)
        amount = blocks * price
        return BatchPlan(
            depth=depth,
            amount=amount,
            ttl_secs=blocks * BLOCK_SECS,
            cost_bzz=amount * 2**depth / PLUR_PER_BZZ,
        )

    async def buy(self, amount: int, depth: int, *, wait_secs: int = 300) -> str:
        """Buy a batch and wait until it is usable (on-chain confirmation
        plus node sync; ~40 s live). Returns the batch id.

        Spends the node wallet's xBZZ — callers decide, this only executes.
        """
        try:
            batch_id = await self._client.stamp_buy(amount, depth)
        except BeeAPIError as e:
            if "insufficient amount" in e.detail:
                hint = ("the on-chain price moved between planning and "
                        "buying — retry, or ask for a longer validity")
            else:
                hint = (f"the node's wallet may lack xBZZ or xDAI for gas "
                        f"(check {self._client.api_url}/wallet)")
            raise StampError(f"buying the batch failed: {e} — {hint}") from None

        # from here on the money is spent: every failure path must carry
        # the batch id, or a confirmed batch would be orphaned
        deadline = time.monotonic() + wait_secs
        while time.monotonic() < deadline:
            try:
                if (await self._client.stamp_get(batch_id)).get("usable"):
                    return batch_id
            except (BeeAPIError, FileNotFoundError) as e:
                # 400/404 while the purchase tx confirms: not known yet
                if isinstance(e, BeeAPIError) and e.status != 400:
                    raise StampError(
                        f"batch {batch_id} was bought (tx submitted) but "
                        f"polling its status failed: {e} — check "
                        f"{self._client.api_url}/stamps/{batch_id} and use "
                        "it once usable"
                    ) from None
            await asyncio.sleep(3)
        raise StampError(
            f"batch {batch_id} was bought but is still not usable after "
            f"{wait_secs}s — it may just need longer; check "
            f"{self._client.api_url}/stamps/{batch_id} and use it once usable"
        )
