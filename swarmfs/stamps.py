"""Postage stamp selection, validation, purchase, and renewal.

A commit validates its stamp *before* uploading anything, so the user gets an
actionable error up front — never a mid-write 402. Spending is exposed as a
capability only (``StampManager.plan``/``buy``, ``plan_topup``/``topup``,
``plan_dilute``/``dilute``) — nothing in swarmfs ever spends implicitly;
deciding to spend the wallet's xBZZ belongs to the caller. Every ``plan_*``
method is a pure question ("what would this cost?"); only the verbs move
money.

The batch lifecycle after purchase, since it is easy to get wrong:

* **Topping up adds time**, it does not restart it: the new remaining TTL is
  the old remaining plus what ``added_amount`` buys. Batches hold a per-chunk
  balance that drains at the chainstate's ``currentPrice`` per block, so a
  quoted TTL is an estimate that shortens if the price rises.
* **Diluting adds capacity** (more chunk slots) for gas only, but spreads the
  same balance over twice the chunks per depth step, roughly halving the
  remaining TTL each step — so on a nearly-full immutable batch, dilute
  *before* topping up or you pay for time you immediately throw away.
* **Neither can resurrect an expired batch**: the node drops expired batches,
  and a topup against one fails. Renew while it is alive.
* **A batch's ``amount`` is cumulative, not remaining** (learned live, after
  it broke an assertion here): it counts from the creation block, so
  ``amount / currentPrice * 5`` is the batch's *total* lifetime and the
  elapsed part is already spent. Remaining life is the node's ``batchTTL``.
  Only a topup's *added* amount converts straight to added time.
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


def ttl_to_amount(ttl_secs: int, price: int) -> int:
    """Per-chunk ``amount`` buying ``ttl_secs`` of validity at ``price``
    (the chainstate's ``currentPrice``, charged per chunk per block)."""
    if price <= 0:
        raise ValueError(f"currentPrice must be positive, got {price!r}")
    return math.ceil(ttl_secs / BLOCK_SECS) * price


def amount_to_ttl(amount: int, price: int) -> int:
    """Seconds of validity a per-chunk ``amount`` buys at ``price`` — the
    inverse of :func:`ttl_to_amount`.

    Correct for an *added* amount (what a topup buys). It is NOT the
    remaining life of an existing batch: a batch's ``amount`` field is
    cumulative since creation, so it implies lifetime measured from the
    creation block, and the elapsed part is already spent. Measured live:
    ``amount`` 32954342400 at price 68657 implies 27.78 days, the batch was
    3.79 days old, and the node reported 24.0 days left (within 0.06%). For
    remaining life, read the node's ``batchTTL`` (``StampInfo.ttl``).
    """
    if price <= 0:
        raise ValueError(f"currentPrice must be positive, got {price!r}")
    return int(amount / price * BLOCK_SECS)


def batch_cost_bzz(amount: int, depth: int) -> float:
    """Cost in xBZZ of a per-chunk ``amount`` over a whole batch. Postage
    is paid for all ``2**depth`` chunk slots, used or not — which is why a
    deep batch costs the same whether it holds one file or a million."""
    return amount * 2**depth / PLUR_PER_BZZ


@dataclass
class BatchPlan:
    """A priced purchase: buy with ``StampManager.buy(amount, depth)``."""

    depth: int
    amount: int
    ttl_secs: int  # actual validity after the chain's minimum is applied
    cost_bzz: float


@dataclass
class TopupPlan:
    """A priced extension: apply with ``StampManager.topup(batch_id,
    added_amount)``. ``total_ttl_secs`` is what the batch ends up with —
    topping up adds to the remaining life rather than replacing it."""

    batch_id: str
    depth: int
    added_amount: int
    added_ttl_secs: int
    total_ttl_secs: int
    cost_bzz: float
    warning: str | None = None


@dataclass
class DilutePlan:
    """A depth increase: apply with ``StampManager.dilute(batch_id,
    to_depth)``. Costs gas only — the price is paid in TTL, which is
    roughly halved per depth step."""

    batch_id: str
    from_depth: int
    to_depth: int
    ttl_before_secs: int
    ttl_after_secs: int
    warning: str | None = None


@dataclass
class StampInfo:
    batch_id: str
    usable: bool
    ttl: int  # seconds; -1 when the node can't estimate it
    utilization_ratio: float | None
    label: str
    immutable: bool
    depth: int = 0
    amount: int = 0  # per chunk, in plur — CUMULATIVE since block_number
    bucket_depth: int = 16
    utilization: int | None = None  # chunks in the fullest bucket
    block_number: int = 0  # creation block; amount is measured from here

    @classmethod
    def from_api(cls, d: dict) -> "StampInfo":
        return cls(
            batch_id=d["batchID"],
            usable=bool(d.get("usable")),
            ttl=int(d.get("batchTTL", -1)),
            utilization_ratio=d.get("utilizationRatio"),
            label=d.get("label", ""),
            immutable=bool(d.get("immutableFlag", False)),
            depth=int(d.get("depth", 0)),
            amount=int(d.get("amount") or 0),
            bucket_depth=int(d.get("bucketDepth", 16)),
            utilization=d.get("utilization"),
            block_number=int(d.get("blockNumber", 0)),
        )

    @property
    def bucket_capacity(self) -> int:
        """Chunks one bucket holds. An immutable batch dies as soon as any
        SINGLE bucket fills, so this — not ``2**depth`` — bounds an upload."""
        return 2 ** max(self.depth - self.bucket_depth, 0)

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
            cost_bzz=batch_cost_bzz(amount, depth),
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

    # -- inspection ---------------------------------------------------------

    async def list_batches(self) -> list[StampInfo]:
        """Every batch the node knows about, parsed.

        For expiry monitoring, pair with ``StampInfo.problem(min_ttl)``:
        passing the warning threshold you care about (a week, say) turns
        "still usable" into "needs renewing" without new API surface.
        """
        return [StampInfo.from_api(d) for d in await self._client.stamps_list()]

    async def get_batch(self, batch_id: str) -> StampInfo:
        """One batch's current state, parsed."""
        return StampInfo.from_api(await self._client.stamp_get(batch_id))

    async def balance_bzz(self) -> float:
        """The node wallet's spendable xBZZ — what purchases draw on."""
        return int((await self._client.wallet())["bzzBalance"]) / PLUR_PER_BZZ

    # -- renewal ------------------------------------------------------------

    async def plan_topup(
        self,
        batch_id: str,
        *,
        ttl_secs: int | None = None,
        total_ttl_secs: int | None = None,
        budget_bzz: float | None = None,
    ) -> TopupPlan:
        """Price an extension of ``batch_id``, at the current on-chain price.

        Exactly one target, matching the three questions publishers actually
        ask: ``ttl_secs`` (extend BY this long), ``total_ttl_secs`` (extend
        TO this total remaining life), or ``budget_bzz`` (spend at most this
        much and take whatever time it buys).

        Buys nothing and spends nothing — pass the result's ``added_amount``
        to :meth:`topup`. Check ``warning``: it flags the dilute-first trap
        and an unknown TTL.
        """
        targets = (ttl_secs, total_ttl_secs, budget_bzz)
        if sum(t is not None for t in targets) != 1:
            raise ValueError(
                "plan_topup needs exactly one of ttl_secs= (extend by), "
                "total_ttl_secs= (extend to), or budget_bzz= (spend at most)"
            )
        info = await self.get_batch(batch_id)
        price = int((await self._client.chainstate())["currentPrice"])
        remaining = max(info.ttl, 0)

        if budget_bzz is not None:
            if budget_bzz <= 0:
                raise ValueError(f"budget_bzz must be positive, got {budget_bzz!r}")
            added = int(budget_bzz * PLUR_PER_BZZ) // 2**info.depth
            if added <= 0:
                raise ValueError(
                    f"{budget_bzz} xBZZ buys nothing at depth {info.depth} "
                    f"({2**info.depth} chunk slots, each paid for separately) — "
                    f"the minimum meaningful budget is "
                    f"{batch_cost_bzz(1, info.depth):.6f} xBZZ"
                )
        else:
            want = ttl_secs if ttl_secs is not None else total_ttl_secs - remaining
            if want <= 0:
                raise ValueError(
                    f"batch {batch_id[:8]}… already has {remaining}s left, which "
                    f"is at or past the requested total of {total_ttl_secs}s — "
                    "nothing to buy"
                    if total_ttl_secs is not None
                    else f"ttl_secs must be positive, got {ttl_secs!r}"
                )
            added = ttl_to_amount(want, price)

        warning = None
        if info.immutable and (info.utilization_ratio or 0) >= 0.8:
            warning = (
                f"batch is immutable and {info.utilization_ratio:.0%} through its "
                f"bucket capacity ({info.bucket_capacity} chunks per bucket): "
                "dilute FIRST, since dilution halves the remaining TTL per depth "
                "step and would discard part of what this topup buys"
            )
        elif info.ttl < 0:
            warning = (
                "the node reports no TTL estimate for this batch, so "
                "total_ttl_secs counts only what this topup adds"
            )

        return TopupPlan(
            batch_id=info.batch_id,
            depth=info.depth,
            added_amount=added,
            added_ttl_secs=amount_to_ttl(added, price),
            total_ttl_secs=remaining + amount_to_ttl(added, price),
            cost_bzz=batch_cost_bzz(added, info.depth),
            warning=warning,
        )

    async def topup(
        self,
        batch_id: str,
        added_amount: int,
        *,
        wait_secs: int = 300,
        check_balance: bool = True,
    ) -> StampInfo:
        """Extend ``batch_id`` by ``added_amount`` (per chunk) and wait until
        the node has applied it. Returns the batch's new state.

        Spends the node wallet's xBZZ — callers decide, this only executes.
        ``check_balance`` refuses up front when the wallet cannot cover the
        cost (fail early, in the spirit of validating a stamp before an
        upload) rather than letting the transaction fail on chain.
        """
        if added_amount <= 0:
            raise ValueError(f"added_amount must be positive, got {added_amount!r}")
        before = await self.get_batch(batch_id)
        cost = batch_cost_bzz(added_amount, before.depth)
        if check_balance:
            balance = await self.balance_bzz()
            if cost > balance:
                raise StampError(
                    f"topping up batch {batch_id[:8]}… by {added_amount} costs "
                    f"{cost:.4f} xBZZ (per-chunk amount over {2**before.depth} "
                    f"slots at depth {before.depth}) but the node wallet holds "
                    f"{balance:.4f} xBZZ — fund "
                    f"{self._client.api_url}/wallet, or ask for less time "
                    "(topups stack, so a smaller one now loses nothing)"
                )

        try:
            tx = await self._client.stamp_topup(batch_id, added_amount)
        except BeeAPIError as e:
            if e.status == 404 or "not exist" in e.detail or "cannot topup" in e.detail:
                hint = (
                    "the node does not know this batch — check the id, and note "
                    "that an EXPIRED batch is dropped and cannot be revived by a "
                    "topup (its content is already unpaid-for and awaiting "
                    "garbage collection)"
                )
            else:
                hint = (
                    f"the node's wallet may lack xBZZ or xDAI for gas (check "
                    f"{self._client.api_url}/wallet)"
                )
            raise StampError(f"topping up batch {batch_id} failed: {e} — {hint}") from None

        # from here on the money is spent: every failure path must carry the
        # batch id and the tx, or a paid-for topup becomes unverifiable
        return await self._await_applied(
            batch_id,
            lambda info: info.amount > before.amount,
            what=f"topup (+{added_amount}, {cost:.4f} xBZZ)",
            tx=tx,
            wait_secs=wait_secs,
        )

    async def plan_dilute(self, batch_id: str, to_depth: int) -> DilutePlan:
        """Price a depth increase in the currency it actually costs: TTL.

        Dilution only ever raises depth (Bee cannot shrink a batch), spends
        no xBZZ beyond gas, and roughly halves the remaining life per step.
        """
        info = await self.get_batch(batch_id)
        if to_depth <= info.depth:
            raise StampError(
                f"batch {batch_id[:8]}… is already at depth {info.depth}; dilution "
                f"only increases depth (asked for {to_depth}). Capacity cannot be "
                "given back once bought."
            )
        before = max(info.ttl, 0)
        after = int(before / 2 ** (to_depth - info.depth))
        warning = None
        if 0 <= after <= self.min_ttl:
            warning = (
                f"diluting to depth {to_depth} would leave only {after}s of life "
                f"(from {before}s) — top up in the same session, or the extra "
                "capacity arrives on a batch that is about to expire"
            )
        return DilutePlan(
            batch_id=info.batch_id,
            from_depth=info.depth,
            to_depth=to_depth,
            ttl_before_secs=before,
            ttl_after_secs=after,
            warning=warning,
        )

    async def dilute(
        self, batch_id: str, to_depth: int, *, wait_secs: int = 300
    ) -> StampInfo:
        """Raise ``batch_id`` to ``to_depth`` and wait until the node has
        applied it. Returns the batch's new state.

        Costs gas and — because the same balance now covers twice the chunks
        per step — roughly half the remaining TTL per step. Plan it with
        :meth:`plan_dilute` first; callers decide, this only executes.
        """
        before = await self.get_batch(batch_id)
        if to_depth <= before.depth:
            raise StampError(
                f"batch {batch_id[:8]}… is already at depth {before.depth}; "
                f"dilution only increases depth (asked for {to_depth})"
            )
        try:
            tx = await self._client.stamp_dilute(batch_id, to_depth)
        except BeeAPIError as e:
            raise StampError(
                f"diluting batch {batch_id} to depth {to_depth} failed: {e} — the "
                f"node may not know the batch, or its wallet may lack xDAI for gas "
                f"(check {self._client.api_url}/wallet)"
            ) from None

        return await self._await_applied(
            batch_id,
            lambda info: info.depth >= to_depth,
            what=f"dilution to depth {to_depth}",
            tx=tx,
            wait_secs=wait_secs,
        )

    async def _await_applied(
        self,
        batch_id: str,
        done,
        *,
        what: str,
        tx: str,
        wait_secs: int,
    ) -> StampInfo:
        """Poll ``batch_id`` until ``done(info)``, after a submitted topup or
        dilution.

        Bee returns the transaction hash before it indexes the chain event, so
        an immediate read still shows the OLD amount/depth — trusting it looks
        exactly like a silently failed topup (measured live: the wallet had
        already been debited while ``/stamps`` still reported the old amount).
        The transaction is already paid for, so every failure path names both
        the batch and the tx.
        """
        deadline = time.monotonic() + wait_secs
        latest: StampInfo | None = None
        while time.monotonic() < deadline:
            try:
                latest = await self.get_batch(batch_id)
                if done(latest):
                    return latest
            except (BeeAPIError, FileNotFoundError) as e:
                # 400/404 can occur transiently while the tx confirms
                if isinstance(e, BeeAPIError) and e.status != 400:
                    raise StampError(
                        f"{what} of batch {batch_id} was submitted (tx {tx}) but "
                        f"polling its status failed: {e} — the transaction may "
                        f"still land; check {self._client.api_url}/stamps/{batch_id}"
                    ) from None
            await asyncio.sleep(3)
        seen = (
            f" (it still reads amount={latest.amount}, depth={latest.depth})"
            if latest is not None
            else ""
        )
        raise StampError(
            f"{what} of batch {batch_id} was submitted (tx {tx}) but the node has "
            f"not applied it after {wait_secs}s{seen} — the transaction may still "
            f"be confirming; check {self._client.api_url}/stamps/{batch_id} before "
            "paying again"
        )
