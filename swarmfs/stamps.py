"""Postage stamp selection and validation.

A commit validates its stamp *before* uploading anything, so the user gets an
actionable error up front — never a mid-write 402.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._client import SwarmClient
from .exceptions import StampError  # noqa: F401 — canonical home; re-exported here


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
