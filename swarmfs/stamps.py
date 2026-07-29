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
* **Never derive remaining life from ``amount``** (learned live, twice, after
  it broke assertions here). It counts from the creation block, so
  ``amount / currentPrice * 5`` is *total* lifetime with the elapsed part
  already spent; and it is the local issuer's own bookkeeping, which bee
  increments in memory on a topup without persisting — it was seen to revert
  to the creation value while the topups it had recorded remained in effect.
  ``batchTTL`` is the field that tracks the chain. Only a topup's *added*
  amount converts straight to added time.
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

CHUNK_SIZE = 4096
BUCKET_DEPTH = 16  # bee: pkg/postage/stamp.go — BucketDepth is a constant
BUCKETS = 1 << BUCKET_DEPTH
MIN_DEPTH = BUCKET_DEPTH + 1
"""Shallowest batch bee will sell. Verified live against Bee 2.8.1:
``POST /stamps/1/16`` is rejected at parameter validation with
``{"field": "depth", "error": "want min:17"}``. (swarm-bee's ``MIN_DEPTH``
says 16, which the node does not accept.)"""
DEFAULT_RISK = 0.01
"""Default bucket-overflow risk :func:`suggest_depth` will accept.

The consequence of losing that bet is a failed upload (HTTP 402 "batch is
overissued"), recoverable by diluting one depth and retrying — not a lost
batch. 1% trades that annoyance against the doubled cost of a deeper batch.
Pass ``risk=`` to choose differently, or size exactly with
:func:`depth_for_addresses` when the chunk addresses are known.
"""

BRANCHES = 128  # refs per intermediate chunk (bee: swarm.BmtBranches)
ENC_BRANCHES = BRANCHES // 2  # encrypted refs are 64 bytes, so half as many

# Bee's appendix-F erasure tables, verbatim from pkg/file/redundancy/level.go
# (mediumEt/strongEt/insaneEt/paranoidEt and their enc* counterparts), as
# {level: (shards, parities)} descending. Level 0 (NONE) has no table.
_ET: dict[int, tuple[tuple[int, ...], tuple[int, ...]]] = {
    1: ((95, 69, 47, 29, 15, 6, 2, 1), (9, 8, 7, 6, 5, 4, 3, 2)),
    2: ((105, 96, 87, 78, 70, 62, 54, 47, 40, 33, 27, 21, 16, 11, 7, 4, 2, 1),
        (21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4)),
    3: ((93, 88, 83, 78, 74, 69, 64, 60, 55, 51, 46, 42, 38, 34, 30, 27, 23, 20,
         17, 14, 11, 9, 6, 4, 3, 2, 1),
        (31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14,
         13, 12, 11, 10, 9, 8, 7, 6, 5)),
    4: ((37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20,
         19, 18),
        (89, 87, 86, 84, 83, 81, 80, 78, 76, 75, 73, 71, 70, 68, 66, 65, 63, 61,
         59, 58)),
}
_ENC_ET: dict[int, tuple[tuple[int, ...], tuple[int, ...]]] = {
    1: ((47, 34, 23, 14, 7, 3, 1), (9, 8, 7, 6, 5, 4, 3)),
    2: ((52, 48, 43, 39, 35, 31, 27, 23, 20, 16, 13, 10, 8, 5, 3, 2, 1),
        (21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5)),
    3: ((46, 44, 41, 39, 37, 34, 32, 30, 27, 25, 23, 21, 19, 17, 15, 13, 11, 10,
         8, 7, 5, 4, 3, 2, 1),
        (31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14,
         13, 12, 11, 10, 9, 8, 6)),
    4: ((18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1),
        (87, 84, 81, 78, 75, 71, 68, 65, 61, 58, 55, 52, 48, 45, 42, 39, 35, 32)),
}
_REPLICA_COUNTS = (0, 2, 4, 8, 16)  # bee: replicaCounts — dispersed root replicas


def _parities(table, level: int, shards: int) -> int:
    for s, p in zip(*table[level]):
        if shards >= s:
            return p
    return 0


def _group_shape(redundancy: int, encrypted: bool) -> tuple[int, int]:
    """``(data chunks, parity chunks)`` per erasure group, mirroring bee's
    ``redundancy.New()`` — note it takes ``maxParity`` from the *plain* table
    even for encrypted uploads (pkg/file/redundancy/redundancy.go:47-55)."""
    if redundancy not in range(5):
        raise ValueError(f"redundancy must be 0-4, got {redundancy!r}")
    if redundancy == 0:
        return (ENC_BRANCHES if encrypted else BRANCHES), 0
    if encrypted:
        return (BRANCHES - _parities(_ENC_ET, redundancy, ENC_BRANCHES)) // 2, \
            _parities(_ET, redundancy, ENC_BRANCHES)
    return BRANCHES - _parities(_ET, redundancy, BRANCHES), \
        _parities(_ET, redundancy, BRANCHES)


def stamped_chunks(data_chunks: int, *, redundancy: int = 2,
                   encrypted: bool = False) -> int:
    """How many chunks bee actually stamps for ``data_chunks`` leaves.

    A batch is filled by *stamped* chunks, not by payload bytes: every tree
    level adds intermediates, erasure coding adds parity to each level, and
    the root gets dispersed replicas. Levels ≥ 1 inflate the count by 7.6%
    (MEDIUM) to 228% (PARANOID) plain, and more when encrypted — measured
    from bee's own tables, which is why sizing must know both flags.
    """
    if data_chunks <= 0:
        return 0
    shards, parity = _group_shape(redundancy, encrypted)
    total, level = 0, data_chunks
    while level > 1:
        groups = math.ceil(level / shards)
        total += level + groups * parity
        level = groups
    return total + 1 + _REPLICA_COUNTS[redundancy]  # + root, + its replicas


def overflow_risk(chunks: int, depth: int, *, loads: dict[int, int] | None = None) -> float:
    """P(some bucket overflows) when ``chunks`` chunks with unpredictable
    addresses are stamped on a batch of ``depth``.

    ``loads`` optionally gives the bucket occupancy already present, as
    ``{load: how many buckets have it}`` — from :func:`bucket_histogram` or
    ``GET /stamps/{id}/buckets`` — so the estimate only has to be
    probabilistic about the chunks whose addresses are genuinely unknown.

    Poisson model over ``2**16`` buckets, each holding ``2**(depth-16)``.
    Validated against a live batch: predicted 139.1/8.5/0.4 buckets with
    ≥3/≥4/≥5 chunks, observed 126/6/0.
    """
    capacity = 1 << max(depth - BUCKET_DEPTH, 0)
    counts = dict(loads) if loads else {0: BUCKETS}
    if any(load > capacity for load in counts):
        return 1.0  # already past capacity — an invalid state, not a risk
    if chunks <= 0:
        return 0.0
    # Buckets with no room left are handled exactly: any chunk landing in one
    # overflows, so the approximation must not smear that away (independence
    # alone would report only 1-1/e for a batch whose every bucket is full).
    full = sum(n for load, n in counts.items() if load >= capacity)
    if full >= BUCKETS:
        return 1.0
    survive = (1.0 - full / BUCKETS) ** chunks
    # ...and the rest with the usual independent-buckets Poisson approximation
    lam = chunks / (BUCKETS - full)
    for load, n_buckets in counts.items():
        if load >= capacity:
            continue
        room = capacity - load
        cdf = sum(math.exp(-lam) * lam**k / math.factorial(k) for k in range(room + 1))
        survive *= min(cdf, 1.0) ** n_buckets
    return 1.0 - survive


def suggest_depth(size_bytes: int, *, redundancy: int = 2, encrypted: bool = False,
                  risk: float = DEFAULT_RISK) -> int:
    """Smallest batch depth whose buckets hold ``size_bytes`` at ``risk``.

    An ESTIMATE, for when the data has not been split yet: it knows how many
    chunks bee will stamp (:func:`stamped_chunks`, so ``redundancy`` and
    ``encrypted`` matter) but not *where* they land, since a chunk's bucket
    is the first 16 bits of its content address.

    Prefer :func:`depth_for_addresses` when the addresses are known —
    ``split()`` computes them offline, and for a plain upload that gives the
    exact answer at zero risk. Measured on a live batch: this estimate put
    depth 18 at 34% overflow risk for content whose true histogram fit it
    exactly, with a maxed-out bucket to spare.
    """
    if size_bytes < 0:
        raise ValueError(f"size_bytes must not be negative, got {size_bytes!r}")
    if not 0.0 < risk < 1.0:
        raise ValueError(f"risk must be a probability in (0, 1), got {risk!r}")
    chunks = stamped_chunks(math.ceil(size_bytes / CHUNK_SIZE),
                            redundancy=redundancy, encrypted=encrypted)
    depth = MIN_DEPTH
    while overflow_risk(chunks, depth) > risk:
        depth += 1
    return depth


def bucket_histogram(addresses) -> dict[int, int]:
    """``{load: how many buckets hold that many}`` for chunk ``addresses``.

    Addresses may be 32-byte values or hex strings; the bucket is the first
    ``BUCKET_DEPTH`` bits, exactly as bee's ``toBucket`` computes it. Buckets
    holding nothing are included, so the counts sum to ``2**16``.
    """
    counts: dict[int, int] = {}
    for addr in addresses:
        if isinstance(addr, str):
            addr = bytes.fromhex(addr)
        if len(addr) < 4:
            raise ValueError(f"chunk address too short: {addr.hex()}")
        bucket = int.from_bytes(addr[:4], "big") >> (32 - BUCKET_DEPTH)
        counts[bucket] = counts.get(bucket, 0) + 1
    loads: dict[int, int] = {}
    for load in counts.values():
        loads[load] = loads.get(load, 0) + 1
    loads[0] = BUCKETS - len(counts)
    return loads


def depth_for_addresses(addresses, *, extra_chunks: int = 0,
                        risk: float = DEFAULT_RISK) -> int:
    """Smallest depth that fits chunks whose addresses are already known.

    With ``extra_chunks=0`` this is EXACT and carries no risk: the deepest
    bucket decides, so the answer is ``16 + ceil(log2(max load))``. Pass the
    whole tree from ``split(data)[1]`` for a plain (``redundancy=0``) upload.

    When erasure coding is on, the parity chunks — and the intermediates,
    whose fan-out changes — are generated by the node, so their addresses
    cannot be known offline. Pass the leaf addresses and set
    ``extra_chunks`` to how many unpredictable ones will be stamped, e.g.
    ``stamped_chunks(n, redundancy=2) - n``; the known part stays exact and
    only the remainder is modelled.
    """
    loads = bucket_histogram(addresses)
    max_load = max(loads, default=0)
    depth = MIN_DEPTH
    while (1 << (depth - BUCKET_DEPTH)) < max_load:
        depth += 1
    while overflow_risk(extra_chunks, depth, loads=loads) > risk:
        depth += 1
    return depth


def ttl_to_amount(ttl_secs: int, price: int) -> int:
    """Per-chunk ``amount`` buying ``ttl_secs`` of validity at ``price``
    (the chainstate's ``currentPrice``, charged per chunk per block)."""
    if price <= 0:
        raise ValueError(f"currentPrice must be positive, got {price!r}")
    return math.ceil(ttl_secs / BLOCK_SECS) * price


def amount_to_ttl(amount: int, price: int) -> int:
    """Seconds of validity a per-chunk ``amount`` buys at ``price`` — the
    inverse of :func:`ttl_to_amount`.

    Correct for an *added* amount (what a topup buys). It is NOT a way to
    learn an existing batch's remaining life — read ``batchTTL``
    (``StampInfo.ttl``) for that. Two live findings, in order of discovery:

    1. A batch's ``amount`` describes lifetime from its *creation block*, so
       the elapsed part is already spent: 32954342400 at price 68657 implies
       27.78 days on a batch 3.79 days old whose reported TTL was 24.0 days
       (the three agree within 0.06%).
    2. It is also not a reliable ledger. ``/stamps`` reports the local stamp
       *issuer's* ``BatchAmount``, which bee's ``HandleTopUp`` increments in
       memory (``pkg/postage/service.go:186``) without persisting; hours after
       two confirmed topups the field had reverted to the creation value while
       ``batchTTL`` still reflected both. Trust ``batchTTL``.
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
    """A priced purchase: buy with ``StampManager.buy(amount, depth)``.

    ``redundancy``/``encrypted`` record the upload shape the depth assumed —
    uploading with a *higher* redundancy level than planned for means more
    stamped chunks than the depth was sized for.
    """

    depth: int
    amount: int
    ttl_secs: int  # actual validity after the chain's minimum is applied
    cost_bzz: float
    redundancy: int = 2
    encrypted: bool = False


@dataclass
class BucketStats:
    """A batch's true bucket occupancy, from ``GET /stamps/{id}/buckets``.

    ``max_load`` is what actually bounds further uploads: a chunk hashing
    into a bucket already at ``capacity`` is refused on an immutable batch.
    """

    depth: int
    bucket_depth: int
    capacity: int  # chunks per bucket = 2**(depth - bucket_depth)
    chunks: int  # total stamped
    max_load: int  # fullest bucket
    loads: dict[int, int]  # {load: how many buckets}

    @property
    def headroom(self) -> int:
        """Free slots in the fullest bucket — 0 means the next chunk that
        hashes there is refused (402 "batch is overissued")."""
        return max(self.capacity - self.max_load, 0)

    def risk_for(self, chunks: int) -> float:
        """P(overflow) if ``chunks`` more chunks were stamped on this batch."""
        return overflow_risk(chunks, self.depth, loads=self.loads)

    @classmethod
    def from_api(cls, d: dict) -> "BucketStats":
        buckets = d.get("buckets") or []
        loads: dict[int, int] = {}
        total = 0
        for b in buckets:
            load = int(b.get("collisions", 0))
            loads[load] = loads.get(load, 0) + 1
            total += load
        depth = int(d.get("depth", 0))
        bucket_depth = int(d.get("bucketDepth", BUCKET_DEPTH))
        return cls(
            depth=depth,
            bucket_depth=bucket_depth,
            capacity=int(d.get("bucketUpperBound", 1 << max(depth - bucket_depth, 0))),
            chunks=total,
            max_load=max(loads, default=0),
            loads=loads,
        )


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
        """Chunks one bucket holds — ``2**(depth - bucket_depth)``.

        This, not ``2**depth``, is what bounds an upload: a chunk hashing
        into a full bucket is refused on an immutable batch (HTTP 402,
        "batch is overissued"). The batch survives and keeps paying for what
        it already stamped; diluting one depth doubles every bucket and lets
        the upload through. ``utilization`` is the fullest bucket's count.
        """
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

    async def plan(self, size_bytes: int, ttl_secs: int, *, redundancy: int = 2,
                   encrypted: bool = False, risk: float = DEFAULT_RISK,
                   depth: int | None = None) -> BatchPlan:
        """Price a batch for ``size_bytes`` lasting ``ttl_secs``, at the
        current on-chain price.

        The node requires STRICTLY more than ``minimumValidityBlocks``
        (24 h on Gnosis) at purchase time, and the price can move between
        planning and buying (rejected live at the exact minimum) — so the
        floor is padded by an hour.

        Depth comes from :func:`suggest_depth`, so it depends on how the data
        will be uploaded: ``redundancy`` (swarmfs writes level 2 by default)
        and ``encrypted`` both add stamped chunks. Pass ``depth=`` to override
        with an exact figure from :func:`depth_for_addresses`.
        """
        chain = await self._client.chainstate()
        price = int(chain["currentPrice"])
        floor = int(chain.get("minimumValidityBlocks", 0)) + 3600 // BLOCK_SECS
        blocks = max(math.ceil(ttl_secs / BLOCK_SECS), floor)
        if depth is None:
            depth = suggest_depth(size_bytes, redundancy=redundancy,
                                  encrypted=encrypted, risk=risk)
        amount = blocks * price
        return BatchPlan(
            depth=depth,
            amount=amount,
            ttl_secs=blocks * BLOCK_SECS,
            cost_bzz=batch_cost_bzz(amount, depth),
            redundancy=redundancy,
            encrypted=encrypted,
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

    async def buckets(self, batch_id: str) -> BucketStats:
        """The batch's true bucket occupancy — what actually bounds another
        upload. Use ``BucketStats.risk_for(n)`` to size the next upload
        against reality instead of estimating from bytes."""
        return BucketStats.from_api(await self._client.stamp_buckets(batch_id))

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

        Detection watches ``amount`` and ``batchTTL`` (see below); a topup so
        small that it moves neither — under ~1 s of extra life — cannot be
        confirmed this way and will raise after ``wait_secs`` even though the
        transaction landed.
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
        # batch id and the tx, or a paid-for topup becomes unverifiable.
        #
        # Watch BOTH signals. `amount` is the local stamp issuer's bookkeeping
        # (bee's HandleTopUp adds to it in memory — service.go:186 — and it
        # reverts to the creation value when the issuer reloads, observed live
        # hours after two successful topups), while `batchTTL` is derived from
        # the batchstore and tracks the chain. Either moving means the node has
        # applied the topup; requiring `amount` alone would eventually hang on
        # a paid-for extension.
        return await self._await_applied(
            batch_id,
            lambda info: info.amount > before.amount or info.ttl > before.ttl,
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
