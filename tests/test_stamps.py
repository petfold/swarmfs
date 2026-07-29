"""StampManager: selection, fail-early validation, purchase, renewal."""

from __future__ import annotations

import asyncio

import pytest

from swarmfs.stamps import StampError, StampManager

from conftest import GOOD_STAMP


class StampsOnlyClient:
    api_url = "fake://"

    def __init__(self, stamps):
        self._stamps = stamps

    async def stamps_list(self):
        return self._stamps


def resolve(stamps, stamp=None, **kwargs):
    mgr = StampManager(StampsOnlyClient(stamps), **kwargs)
    return asyncio.run(mgr.resolve(stamp))


def test_auto_picks_longest_ttl():
    a = dict(GOOD_STAMP, batchID="aa" * 32, batchTTL=100)
    b = dict(GOOD_STAMP, batchID="bb" * 32, batchTTL=99999)
    c = dict(GOOD_STAMP, batchID="cc" * 32, batchTTL=5000)
    assert resolve([a, b, c]) == "bb" * 32
    assert resolve([a, b, c], stamp="auto") == "bb" * 32


def test_auto_skips_unusable_and_full():
    syncing = dict(GOOD_STAMP, batchID="aa" * 32, usable=False, batchTTL=99999)
    full = dict(GOOD_STAMP, batchID="bb" * 32, utilizationRatio=1.0, batchTTL=99999)
    ok = dict(GOOD_STAMP, batchID="cc" * 32, batchTTL=100)
    assert resolve([syncing, full, ok]) == "cc" * 32


def test_no_stamps_is_actionable():
    with pytest.raises(StampError, match="swarm-cli stamp buy"):
        resolve([])


def test_all_unusable_lists_reasons():
    syncing = dict(GOOD_STAMP, batchID="aa" * 32, usable=False)
    expiring = dict(GOOD_STAMP, batchID="bb" * 32, batchTTL=5)
    with pytest.raises(StampError) as e:
        resolve([syncing, expiring], min_ttl=60)
    msg = str(e.value)
    assert "not usable" in msg and "below the minimum" in msg


def test_explicit_stamp_found_and_validated():
    ok = dict(GOOD_STAMP, batchID="ab" * 32)
    assert resolve([ok], stamp="AB" * 32) == "ab" * 32  # case-insensitive
    with pytest.raises(StampError, match="not found"):
        resolve([ok], stamp="ff" * 32)


def test_unknown_ttl_is_usable_but_last_choice():
    unknown = dict(GOOD_STAMP, batchID="aa" * 32, batchTTL=-1)
    known = dict(GOOD_STAMP, batchID="bb" * 32, batchTTL=100)
    assert resolve([unknown, known]) == "bb" * 32
    assert resolve([unknown]) == "aa" * 32


# ---------------------------------------------------------------------------
# purchase: plan() and buy()
# ---------------------------------------------------------------------------

from swarmfs.exceptions import BeeAPIError
from swarmfs.stamps import (
    MIN_DEPTH,
    BatchPlan,
    bucket_histogram,
    depth_for_addresses,
    overflow_risk,
    stamped_chunks,
    suggest_depth,
)

MB = 2**20


class BuyClient(StampsOnlyClient):
    """StampsOnlyClient plus the purchase surface."""

    def __init__(self, stamps=(), chain=None, poll=(), buy_error=None):
        super().__init__(list(stamps))
        self._chain = chain or {"currentPrice": "1000", "minimumValidityBlocks": 17280}
        self._poll = iter(poll)
        self._buy_error = buy_error
        self.bought = []

    async def chainstate(self):
        return self._chain

    async def stamp_buy(self, amount, depth):
        if self._buy_error:
            raise self._buy_error
        self.bought.append((amount, depth))
        return "ab" * 32

    async def stamp_get(self, batch_id):
        step = next(self._poll)
        if isinstance(step, Exception):
            raise step
        return step


def test_suggest_depth_is_monotonic_and_respects_the_minimum():
    sizes = [1, 6, 15, 42, 100, 600, 1024, 4096]
    depths = [suggest_depth(mb * MB) for mb in sizes]
    assert depths == sorted(depths), depths
    # bee refuses anything shallower than 17 (verified live: "want min:17")
    assert suggest_depth(1) == MIN_DEPTH == 17
    assert suggest_depth(0) == MIN_DEPTH


def test_suggest_depth_prevents_the_historical_failure():
    """A 42 MB upload filled a depth-18 batch live. At swarmfs's default
    redundancy the model puts that at ~13%, so it must pick 19."""
    assert suggest_depth(42 * MB, redundancy=2) == 19
    assert overflow_risk(stamped_chunks(42 * MB // 4096, redundancy=2), 18) > 0.1
    assert overflow_risk(stamped_chunks(42 * MB // 4096, redundancy=2), 19) < 0.001


def test_suggest_depth_tightens_with_redundancy_and_encryption():
    # PARANOID stamps 3.4x the chunks, so it needs more room for the same data
    assert suggest_depth(15 * MB, redundancy=0) == 18
    assert suggest_depth(15 * MB, redundancy=4) == 19
    # ... and the old redundancy-blind tiers would have said 18 for both,
    # which runs ~11% overflow risk at PARANOID
    assert overflow_risk(stamped_chunks(15 * MB // 4096, redundancy=4), 18) > 0.1
    # encryption adds chunks too (up to 1.66x at PARANOID), so it can only
    # ever push the depth up, never down
    for mb in (1, 15, 42, 150, 600):
        for lv in range(5):
            plain = suggest_depth(mb * MB, redundancy=lv)
            enc = suggest_depth(mb * MB, redundancy=lv, encrypted=True)
            assert enc >= plain, (mb, lv, plain, enc)
    # and at PARANOID the 1.7x gap is big enough to change the depth outright
    # for many sizes — the claim that encryption is negligible was wrong
    crossings = [mb for mb in range(1, 2000, 3)
                 if suggest_depth(mb * MB, redundancy=4, encrypted=True)
                 > suggest_depth(mb * MB, redundancy=4)]
    assert crossings, "encryption should change the suggested depth somewhere"


def test_suggest_depth_risk_is_a_dial():
    size = 42 * MB
    assert suggest_depth(size, risk=0.5) < suggest_depth(size, risk=0.01)
    # a tighter target can never suggest a shallower batch
    depths = [suggest_depth(size, risk=r) for r in (0.5, 0.1, 0.01, 1e-6)]
    assert depths == sorted(depths)
    for bad in (0, 1, -0.1, 1.5):
        with pytest.raises(ValueError, match="probability"):
            suggest_depth(size, risk=bad)


def test_stamped_chunks_matches_bees_group_shapes():
    # NONE: 1000 leaves + 8 intermediates + 1 root, no parity, no replicas
    assert stamped_chunks(1000, redundancy=0) == 1009
    # STRONG groups 107 data + 21 parity (bee's strongEt at 128 shards):
    # 1000 + ceil(1000/107)*21 = 1210, then 10 intermediates + 21 parity,
    # + root + 4 dispersed replicas
    assert stamped_chunks(1000, redundancy=2) == 1000 + 10 * 21 + 10 + 21 + 1 + 4
    assert stamped_chunks(0) == 0
    factors = [stamped_chunks(10_000, redundancy=lv) for lv in range(5)]
    assert factors == sorted(factors)  # more redundancy, more chunks
    # encryption raises every level (the correction: not ~1%, up to 1.66x)
    for lv in range(1, 5):
        plain = stamped_chunks(10_000, redundancy=lv)
        enc = stamped_chunks(10_000, redundancy=lv, encrypted=True)
        assert enc > plain, lv
    assert stamped_chunks(10_000, redundancy=4, encrypted=True) / \
        stamped_chunks(10_000, redundancy=4) == pytest.approx(1.7, abs=0.1)
    with pytest.raises(ValueError, match="redundancy must be 0-4"):
        stamped_chunks(10, redundancy=5)


def test_plan_pads_the_chain_minimum():
    mgr = StampManager(BuyClient())
    floor = 17280 + 720  # minimumValidityBlocks + 1h price-drift pad
    plan = asyncio.run(mgr.plan(10 * MB, ttl_secs=3600))
    assert plan == BatchPlan(
        depth=18, amount=floor * 1000, ttl_secs=floor * 5,
        cost_bzz=floor * 1000 * 2**18 / 10**16, redundancy=2, encrypted=False,
    )
    week = 7 * 86400
    plan = asyncio.run(mgr.plan(10 * MB, ttl_secs=week))
    assert plan.amount == (week // 5) * 1000


def test_plan_records_the_upload_shape_it_sized_for():
    mgr = StampManager(BuyClient())
    paranoid = asyncio.run(mgr.plan(15 * MB, 86400, redundancy=4))
    assert (paranoid.depth, paranoid.redundancy) == (19, 4)
    # cost follows the depth it chose, not the payload size
    plain = asyncio.run(mgr.plan(15 * MB, 86400, redundancy=0))
    assert plain.depth == 18 and paranoid.cost_bzz == 2 * plain.cost_bzz
    # an exact depth (from depth_for_addresses) overrides the estimate
    exact = asyncio.run(mgr.plan(15 * MB, 86400, depth=17))
    assert exact.depth == 17


def test_buy_polls_through_the_confirmation_window(monkeypatch):
    async def no_sleep(_):
        pass

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    client = BuyClient(poll=(
        BeeAPIError(400, "fake://stamps/ab", "batch not found"),  # tx confirming
        FileNotFoundError("fake://stamps/ab"),
        {"usable": False},
        {"usable": True},
    ))
    assert asyncio.run(StampManager(client).buy(1000, 18)) == "ab" * 32
    assert client.bought == [(1000, 18)]


def test_buy_failure_paths_carry_the_batch_id(monkeypatch):
    async def no_sleep(_):
        pass

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    # a non-400 polling failure after purchase must name the bought batch
    client = BuyClient(poll=(BeeAPIError(500, "fake://stamps/ab", "boom"),))
    with pytest.raises(StampError, match=f"batch {'ab' * 32} was bought"):
        asyncio.run(StampManager(client).buy(1000, 18))

    # timeout too
    ticks = iter(range(0, 100_000, 500))
    import swarmfs.stamps as stamps_mod

    monkeypatch.setattr(stamps_mod.time, "monotonic", lambda: next(ticks))
    client = BuyClient(poll=iter(lambda: {"usable": False}, None))
    with pytest.raises(StampError, match="still not usable"):
        asyncio.run(StampManager(client).buy(1000, 18, wait_secs=300))


def test_buy_maps_rejections_to_actionable_hints():
    client = BuyClient(buy_error=BeeAPIError(
        400, "fake://stamps", "insufficient amount for 24h minimum validity"))
    with pytest.raises(StampError, match="price moved"):
        asyncio.run(StampManager(client).buy(1, 18))

    client = BuyClient(buy_error=BeeAPIError(500, "fake://stamps", "wallet empty"))
    with pytest.raises(StampError, match="xBZZ or xDAI"):
        asyncio.run(StampManager(client).buy(1, 18))


# ---------------------------------------------------------------------------
# exact sizing from known chunk addresses, and the real bucket histogram
#
# LIVE_LOADS is the demo batch's actual occupancy, measured 2026-07-29 via
# GET /stamps/<id>/buckets: 16279 chunks over 65536 buckets, fullest bucket
# 4 (matching the summary utilization=4, utilizationRatio=0.5 at depth 19).
# ---------------------------------------------------------------------------

LIVE_LOADS = {0: 51070, 1: 12785, 2: 1555, 3: 120, 4: 6}
LIVE_CHUNKS = sum(load * n for load, n in LIVE_LOADS.items())


def _addresses_with_loads(loads: dict[int, int]) -> list[bytes]:
    """Synthesize chunk addresses realizing a given bucket histogram."""
    out, bucket = [], 0
    for load, n_buckets in sorted(loads.items()):
        if load == 0:
            bucket += n_buckets
            continue
        for _ in range(n_buckets):
            prefix = (bucket << (32 - 16)).to_bytes(4, "big")
            out += [prefix + bytes(28) for _ in range(load)]
            bucket += 1
    return out


def test_live_histogram_is_consistent_and_matches_the_summary():
    assert LIVE_CHUNKS == 16279
    assert sum(LIVE_LOADS.values()) == 65536
    assert max(LIVE_LOADS) == 4  # == the batch's reported utilization
    # utilizationRatio 0.5 at depth 19: 4 of 2**(19-16)
    assert max(LIVE_LOADS) / 2 ** (19 - 16) == 0.5


def test_overflow_risk_reproduces_the_live_measurement():
    """The estimate said depth 18 was a coin-flip-ish gamble for content whose
    true histogram fit depth 18 exactly — the case for exact sizing."""
    assert overflow_risk(LIVE_CHUNKS, 18) == pytest.approx(0.34, abs=0.02)
    assert overflow_risk(LIVE_CHUNKS, 19) < 1e-3
    # and the model's per-bucket predictions matched the observed histogram
    # (139.1/8.5/0.4 expected buckets with >=3/>=4/>=5; observed 126/6/0)
    assert overflow_risk(LIVE_CHUNKS, 17) > 0.99  # cap 2, hopeless


def test_overflow_risk_accounts_for_existing_occupancy():
    # the same batch, viewed at its real depth: adding a little is safe...
    assert overflow_risk(100, 19, loads=LIVE_LOADS) < 0.01
    # ...adding a lot is not, and it is riskier than starting from empty
    assert overflow_risk(20_000, 19, loads=LIVE_LOADS) > overflow_risk(20_000, 19)
    # a bucket already at capacity means the next chunk can always collide
    assert overflow_risk(1, 18, loads={4: 65536}) == 1.0
    assert overflow_risk(0, 19, loads=LIVE_LOADS) == 0.0  # nothing to add


def test_bucket_histogram_matches_bees_toBucket():
    # bucket = the first 16 bits of the address, big-endian
    addr = bytes([0xAB, 0xCD]) + bytes(30)
    assert bucket_histogram([addr]) == {1: 1, 0: 65535}
    assert bucket_histogram([addr, addr.hex()]) == {2: 1, 0: 65535}  # hex accepted
    # two addresses differing below the 16-bit prefix share a bucket
    other = bytes([0xAB, 0xCD, 0xFF]) + bytes(29)
    assert bucket_histogram([addr, other]) == {2: 1, 0: 65535}
    # ...and differing inside it do not
    apart = bytes([0xAB, 0xCE]) + bytes(30)
    assert bucket_histogram([addr, apart]) == {1: 2, 0: 65534}
    assert bucket_histogram([]) == {0: 65536}
    with pytest.raises(ValueError, match="too short"):
        bucket_histogram([b"\x01\x02"])


def test_depth_for_addresses_is_exact_when_nothing_is_unpredictable():
    """With every address known, the deepest bucket decides — no probability
    involved. This is the redundancy=0 path: split() gives the whole tree."""
    for max_load, want_depth in ((1, 17), (2, 17), (3, 18), (4, 18), (5, 19), (9, 20)):
        addrs = _addresses_with_loads({max_load: 1, 1: 10})
        assert depth_for_addresses(addrs) == want_depth, max_load
        # exact means exactly that: zero risk at the depth it returns
        assert overflow_risk(0, want_depth, loads=bucket_histogram(addrs)) == 0.0
    # the live content: max load 4 fits depth 18, which the estimate feared
    assert depth_for_addresses(_addresses_with_loads(LIVE_LOADS)) == 18
    assert suggest_depth(LIVE_CHUNKS * 4096, redundancy=0) == 19  # the estimate


def test_depth_for_addresses_models_only_the_unknown_part():
    addrs = _addresses_with_loads({4: 6, 3: 120, 2: 1555, 1: 12785})
    exact = depth_for_addresses(addrs)
    # parity chunks the node will generate have unpredictable addresses, so
    # they can only push the depth up
    n = len(addrs)
    extra = stamped_chunks(n, redundancy=2) - n
    assert depth_for_addresses(addrs, extra_chunks=extra) >= exact
    # ...and asking for a stricter risk can only push it up further
    loose = depth_for_addresses(addrs, extra_chunks=extra, risk=0.5)
    strict = depth_for_addresses(addrs, extra_chunks=extra, risk=1e-9)
    assert loose <= strict
    assert depth_for_addresses([]) == MIN_DEPTH


# ---------------------------------------------------------------------------
# renewal: plan_topup()/topup() and plan_dilute()/dilute()
#
# The numbers are the ones measured live on 2026-07-29 against Bee 2.8.1,
# topping up the searchable-blog demo's batch: depth 19, amount
# 32954342400, batchTTL 2073723, currentPrice 68657, wallet 1.3207 xBZZ.
# A 1 xBZZ topup (addedAmount 19073486328) took it to amount 52027828728 /
# batchTTL 3462314. Keeping those in the suite means the arithmetic is
# pinned to observed node behaviour, not to a re-derivation of it.
# ---------------------------------------------------------------------------

from swarmfs.stamps import (  # noqa: E402
    DilutePlan,
    StampInfo,
    TopupPlan,
    amount_to_ttl,
    batch_cost_bzz,
    ttl_to_amount,
)

LIVE_PRICE = 68657
LIVE_BATCH = {
    "batchID": "c9" * 32,
    "usable": True,
    "batchTTL": 2073723,
    "utilization": 4,
    "utilizationRatio": 0.5,
    "label": "",
    "depth": 19,
    "bucketDepth": 16,
    "amount": "32954342400",  # the API returns this as a string
    "blockNumber": 47381172,
    "immutableFlag": True,
}
LIVE_TOPUP_AMOUNT = 19073486328  # exactly 1 xBZZ at depth 19
LIVE_WALLET_PLUR = 13207006405427200
TX = "0xce216a813e0863a9607ade3b73344dd545122de1ec0667d38a6eb27c2e7c335c"


class RenewClient(StampsOnlyClient):
    """StampsOnlyClient plus the renewal surface.

    ``states`` feeds successive ``stamp_get`` results (an Exception is
    raised instead of returned), so a test can reproduce the node's
    indexing lag: the batch reads unchanged for a while after the
    transaction is submitted.
    """

    def __init__(self, batch=None, price=LIVE_PRICE, bzz_plur=LIVE_WALLET_PLUR,
                 states=None, tx_error=None, buckets=None):
        batch = LIVE_BATCH if batch is None else batch
        super().__init__([batch])
        self._batch = batch
        self._price = price
        self._bzz = bzz_plur
        self._states = iter(states) if states is not None else None
        self._tx_error = tx_error
        self._buckets = buckets
        self.topups: list[tuple[str, int]] = []
        self.dilutions: list[tuple[str, int]] = []

    async def stamp_buckets(self, batch_id):
        return self._buckets or {"depth": 19, "bucketDepth": 16, "buckets": []}

    async def chainstate(self):
        return {"currentPrice": str(self._price), "minimumValidityBlocks": 17280}

    async def wallet(self):
        return {"bzzBalance": str(self._bzz), "nativeTokenBalance": "99999"}

    async def stamp_get(self, batch_id):
        if self._states is None:
            return self._batch
        step = next(self._states)
        if isinstance(step, Exception):
            raise step
        return step

    async def stamp_topup(self, batch_id, added_amount):
        if self._tx_error:
            raise self._tx_error
        self.topups.append((batch_id, added_amount))
        return TX

    async def stamp_dilute(self, batch_id, depth):
        if self._tx_error:
            raise self._tx_error
        self.dilutions.append((batch_id, depth))
        return TX


@pytest.fixture
def no_sleep(monkeypatch):
    async def _sleep(_):
        pass

    monkeypatch.setattr(asyncio, "sleep", _sleep)


def run(coro):
    return asyncio.run(coro)


# -- the pure arithmetic ----------------------------------------------------


def test_amount_and_ttl_conversions_round_trip():
    assert amount_to_ttl(LIVE_TOPUP_AMOUNT, LIVE_PRICE) == 1389041  # 16.08 days
    assert batch_cost_bzz(LIVE_TOPUP_AMOUNT, 19) == pytest.approx(1.0, rel=1e-9)
    for ttl in (3600, 86400, 60 * 86400):
        assert amount_to_ttl(ttl_to_amount(ttl, LIVE_PRICE), LIVE_PRICE) == pytest.approx(
            ttl, rel=1e-4
        )


def test_amount_describes_lifetime_from_creation_not_what_remains():
    """Learned live, after it broke an integration assertion: ``amount``
    counts from the creation block, so it implies TOTAL lifetime. Remaining
    life is the node's ``batchTTL`` — never re-derive it from ``amount``.
    """
    age_secs = (47446655 - 47381172) * 5  # chainstate block - blockNumber
    total = amount_to_ttl(32954342400, LIVE_PRICE)
    assert total / 86400 == pytest.approx(27.78, rel=1e-3)  # NOT the 24.0 left
    assert total - age_secs == pytest.approx(2073723, rel=1e-3)  # == batchTTL


def test_amount_is_not_a_reliable_ledger_of_topups():
    """Second live finding: /stamps reports the local issuer's BatchAmount,
    which bee's HandleTopUp increments in memory without persisting. Hours
    after two confirmed topups it had reverted to the creation value
    (32954342400) while batchTTL still showed both (40.1 days) — so a
    topup must never be confirmed by watching `amount` alone.
    """
    reverted = dict(LIVE_BATCH, amount="32954342400", batchTTL=3466143)
    info = run(StampManager(RenewClient(reverted)).get_batch("c9" * 32))
    assert info.amount == 32954342400  # as if never topped up
    assert info.ttl == 3466143  # but ~40.1 days remain, from two topups
    # the amount-derived lifetime is now far SHORTER than the real TTL,
    # which is exactly why it cannot be used as a source of truth
    assert amount_to_ttl(info.amount, LIVE_PRICE) < info.ttl


def test_conversions_reject_a_nonsensical_price():
    for price in (0, -1):
        with pytest.raises(ValueError, match="currentPrice"):
            ttl_to_amount(3600, price)
        with pytest.raises(ValueError, match="currentPrice"):
            amount_to_ttl(1000, price)


def test_cost_scales_with_the_whole_batch_not_the_bytes_used():
    # postage pays for every slot: one depth step doubles the cost
    assert batch_cost_bzz(1000, 20) == 2 * batch_cost_bzz(1000, 19)


# -- inspection ------------------------------------------------------------


def test_get_batch_parses_the_renewal_fields():
    info = run(StampManager(RenewClient()).get_batch("c9" * 32))
    assert info == StampInfo(
        batch_id="c9" * 32, usable=True, ttl=2073723, utilization_ratio=0.5,
        label="", immutable=True, depth=19, amount=32954342400, bucket_depth=16,
        utilization=4, block_number=47381172,
    )
    assert info.amount == 32954342400  # string in the API, int here
    assert info.bucket_capacity == 8  # 2**(19-16): what an immutable upload may fill


def test_list_batches_supports_expiry_monitoring():
    fresh = dict(LIVE_BATCH, batchID="aa" * 32, batchTTL=40 * 86400)
    stale = dict(LIVE_BATCH, batchID="bb" * 32, batchTTL=3 * 86400)
    mgr = StampManager(StampsOnlyClient([fresh, stale]))
    batches = run(mgr.list_batches())
    week = 7 * 86400
    assert [b.problem(week) is None for b in batches] == [True, False]
    assert "below the minimum" in batches[1].problem(week)


def test_balance_bzz():
    assert run(StampManager(RenewClient()).balance_bzz()) == pytest.approx(1.3207, rel=1e-4)


def test_buckets_reports_the_real_headroom():
    """GET /stamps/{id}/buckets is the ground truth the summary ratio only
    approximates — and what a publisher needs before adding to a batch."""
    from swarmfs.stamps import BucketStats

    api = {
        "depth": 19, "bucketDepth": 16, "bucketUpperBound": 8,
        "buckets": [{"bucketID": i, "collisions": load}
                    for load, n in LIVE_LOADS.items() for i in range(n)],
    }
    stats = run(StampManager(RenewClient(buckets=api)).buckets("c9" * 32))
    assert stats == BucketStats(
        depth=19, bucket_depth=16, capacity=8, chunks=LIVE_CHUNKS,
        max_load=4, loads=LIVE_LOADS,
    )
    assert stats.headroom == 4  # 8 - the fullest bucket's 4
    # sizing the next upload against reality rather than against bytes
    assert stats.risk_for(100) < 0.01
    assert stats.risk_for(200_000) > 0.9
    assert stats.risk_for(0) == 0.0

    # a batch whose fullest bucket is at capacity: no headroom, and the next
    # chunk hashing there is refused (402) — but the batch is not destroyed
    full = dict(api, depth=18, bucketUpperBound=4)
    stats = BucketStats.from_api(full)
    assert stats.headroom == 0 and stats.risk_for(1) > 0
    assert stats.max_load == stats.capacity


# -- plan_topup ------------------------------------------------------------


def test_plan_topup_by_budget_reproduces_the_live_topup():
    plan = run(StampManager(RenewClient()).plan_topup("c9" * 32, budget_bzz=1.0))
    assert plan.added_amount == LIVE_TOPUP_AMOUNT
    assert plan.cost_bzz == pytest.approx(1.0, rel=1e-9)
    assert plan.added_ttl_secs / 86400 == pytest.approx(16.08, rel=1e-3)
    # live: batchTTL 3462314 a few minutes later (the batch drains meanwhile)
    assert plan.total_ttl_secs == pytest.approx(3462314, abs=1000)
    assert plan.warning is None
    assert plan == TopupPlan(
        batch_id="c9" * 32, depth=19, added_amount=LIVE_TOPUP_AMOUNT,
        added_ttl_secs=1389041, total_ttl_secs=2073723 + 1389041,
        cost_bzz=plan.cost_bzz, warning=None,
    )


def test_plan_topup_by_ttl_reproduces_the_live_six_hour_topup():
    """Second live data point (same batch, a few hours later): +6h quoted
    296779680 / 0.015560 xBZZ at price 68699, and the applied amount delta
    equalled it exactly. Note the price had drifted from 68657 that morning
    — quoted durations are estimates against a moving price.
    """
    later = dict(LIVE_BATCH, amount="52027828728", batchTTL=3457374)
    mgr = StampManager(RenewClient(later, price=68699))
    plan = run(mgr.plan_topup("c9" * 32, ttl_secs=6 * 3600))
    assert plan.added_amount == 296779680
    assert plan.cost_bzz == pytest.approx(0.015560, rel=1e-4)
    assert plan.added_ttl_secs == pytest.approx(6 * 3600, rel=1e-4)
    assert plan.total_ttl_secs == pytest.approx(3478974, abs=2)


def test_plan_topup_by_and_to_are_different_questions():
    mgr = StampManager(RenewClient())
    by_60d = run(mgr.plan_topup("c9" * 32, ttl_secs=60 * 86400))
    to_60d = run(mgr.plan_topup("c9" * 32, total_ttl_secs=60 * 86400))
    assert by_60d.cost_bzz == pytest.approx(3.732, rel=1e-3)
    assert to_60d.cost_bzz == pytest.approx(2.239, rel=1e-3)
    # "extend to" only buys the shortfall, so it must be cheaper by the
    # 24 days the batch already has
    assert by_60d.added_ttl_secs - to_60d.added_ttl_secs == pytest.approx(2073723, rel=1e-3)
    assert to_60d.total_ttl_secs == pytest.approx(60 * 86400, rel=1e-4)


def test_plan_topup_needs_exactly_one_target():
    mgr = StampManager(RenewClient())
    with pytest.raises(ValueError, match="exactly one"):
        run(mgr.plan_topup("c9" * 32))
    with pytest.raises(ValueError, match="exactly one"):
        run(mgr.plan_topup("c9" * 32, ttl_secs=3600, budget_bzz=1.0))


def test_plan_topup_rejects_targets_that_buy_nothing():
    mgr = StampManager(RenewClient())
    with pytest.raises(ValueError, match="already has"):
        run(mgr.plan_topup("c9" * 32, total_ttl_secs=86400))  # less than remains
    with pytest.raises(ValueError, match="must be positive"):
        run(mgr.plan_topup("c9" * 32, ttl_secs=0))
    with pytest.raises(ValueError, match="must be positive"):
        run(mgr.plan_topup("c9" * 32, budget_bzz=0))
    with pytest.raises(ValueError, match="buys nothing"):
        run(mgr.plan_topup("c9" * 32, budget_bzz=1e-12))


def test_plan_topup_warns_to_dilute_first_on_a_full_immutable_batch():
    nearly_full = dict(LIVE_BATCH, utilizationRatio=0.875, utilization=7)
    plan = run(StampManager(RenewClient(nearly_full)).plan_topup(
        "c9" * 32, ttl_secs=86400))
    assert "dilute FIRST" in plan.warning and "88%" in plan.warning
    # a mutable batch has no bucket-overflow death, so no warning
    mutable = dict(nearly_full, immutableFlag=False)
    plan = run(StampManager(RenewClient(mutable)).plan_topup("c9" * 32, ttl_secs=86400))
    assert plan.warning is None


def test_plan_topup_flags_an_unknown_ttl():
    unknown = dict(LIVE_BATCH, batchTTL=-1, utilizationRatio=0.1)
    plan = run(StampManager(RenewClient(unknown)).plan_topup("c9" * 32, ttl_secs=86400))
    assert "no TTL estimate" in plan.warning
    assert plan.total_ttl_secs == plan.added_ttl_secs  # nothing to add to


# -- topup ----------------------------------------------------------------


def test_topup_waits_for_the_node_to_index_the_chain_event(no_sleep):
    after = dict(LIVE_BATCH, amount="52027828728", batchTTL=3462314)
    client = RenewClient(states=[
        LIVE_BATCH,  # the pre-flight read
        LIVE_BATCH,  # tx submitted, node has not indexed it yet
        LIVE_BATCH,  # ... still not
        after,       # applied
    ])
    info = run(StampManager(client).topup("c9" * 32, LIVE_TOPUP_AMOUNT))
    assert client.topups == [("c9" * 32, LIVE_TOPUP_AMOUNT)]
    assert info.amount == 52027828728
    assert info.ttl == 3462314
    # the delta is exactly what was paid for — the additive property
    assert info.amount - 32954342400 == LIVE_TOPUP_AMOUNT


def test_topup_is_confirmed_by_ttl_when_amount_does_not_move(no_sleep):
    """Because `amount` is unreliable local bookkeeping, a TTL jump alone
    must count as "applied" — otherwise a paid-for topup hangs to timeout."""
    ttl_only = dict(LIVE_BATCH, batchTTL=2073723 + 21600)  # +6h, amount unchanged
    client = RenewClient(states=[LIVE_BATCH, LIVE_BATCH, ttl_only])
    info = run(StampManager(client).topup("c9" * 32, 296779680))
    assert info.ttl == 2073723 + 21600
    assert info.amount == 32954342400  # never moved, and that is fine


def test_topup_refuses_before_spending_when_the_wallet_is_short():
    # the live wallet (1.3207 xBZZ) could not fund a 2-month extension
    client = RenewClient()
    plan = run(StampManager(client).plan_topup("c9" * 32, total_ttl_secs=60 * 86400))
    with pytest.raises(StampError, match="but the node wallet holds") as e:
        run(StampManager(client).topup("c9" * 32, plan.added_amount))
    assert "2.239" in str(e.value) and "1.3207" in str(e.value)
    assert client.topups == []  # nothing was submitted
    # ...and the check is skippable for callers who know better
    client = RenewClient(states=[LIVE_BATCH, dict(LIVE_BATCH, amount="99999999999999")])
    run(StampManager(client).topup("c9" * 32, plan.added_amount, check_balance=False))
    assert client.topups == [("c9" * 32, plan.added_amount)]


def test_topup_rejects_a_nonpositive_amount():
    client = RenewClient()
    with pytest.raises(ValueError, match="must be positive"):
        run(StampManager(client).topup("c9" * 32, 0))
    assert client.topups == []


def test_topup_explains_that_an_expired_batch_cannot_be_revived():
    client = RenewClient(tx_error=BeeAPIError(
        500, "fake://stamps/topup", "cannot topup batch"))
    with pytest.raises(StampError, match="cannot be revived") as e:
        run(StampManager(client).topup("c9" * 32, LIVE_TOPUP_AMOUNT))
    assert "garbage collection" in str(e.value)


def test_topup_maps_other_rejections_to_the_wallet_hint():
    client = RenewClient(tx_error=BeeAPIError(500, "fake://stamps/topup", "boom"))
    with pytest.raises(StampError, match="xBZZ or xDAI"):
        run(StampManager(client).topup("c9" * 32, LIVE_TOPUP_AMOUNT))


def test_topup_failure_paths_carry_the_batch_and_the_tx(no_sleep, monkeypatch):
    # a non-400 polling failure after the money is spent
    client = RenewClient(states=[
        LIVE_BATCH, BeeAPIError(500, "fake://stamps/c9", "boom")])
    with pytest.raises(StampError) as e:
        run(StampManager(client).topup("c9" * 32, LIVE_TOPUP_AMOUNT))
    assert "c9" * 32 in str(e.value) and TX in str(e.value)
    assert "polling its status failed" in str(e.value)

    # a transient 400 while the tx confirms is tolerated, not fatal
    after = dict(LIVE_BATCH, amount="52027828728")
    client = RenewClient(states=[
        LIVE_BATCH, BeeAPIError(400, "fake://stamps/c9", "not found"),
        FileNotFoundError("fake://stamps/c9"), after])
    assert run(StampManager(client).topup("c9" * 32, LIVE_TOPUP_AMOUNT)).amount == 52027828728

    # a timeout must still name the batch, the tx, and what the node reads
    import swarmfs.stamps as stamps_mod

    # 100s per clock read: a few polls happen, then the deadline passes
    ticks = iter(range(0, 100_000, 100))
    monkeypatch.setattr(stamps_mod.time, "monotonic", lambda: next(ticks))
    client = RenewClient(states=iter(lambda: LIVE_BATCH, None))
    with pytest.raises(StampError) as e:
        run(StampManager(client).topup("c9" * 32, LIVE_TOPUP_AMOUNT, wait_secs=300))
    msg = str(e.value)
    assert TX in msg and "not applied it after 300s" in msg
    assert "still reads amount=32954342400" in msg and "before paying again" in msg


# -- dilution -------------------------------------------------------------


def test_plan_dilute_prices_the_step_in_ttl():
    plan = run(StampManager(RenewClient()).plan_dilute("c9" * 32, 21))
    assert plan == DilutePlan(
        batch_id="c9" * 32, from_depth=19, to_depth=21,
        ttl_before_secs=2073723, ttl_after_secs=518430,  # halved per step
        warning=None,
    )
    one_step = run(StampManager(RenewClient()).plan_dilute("c9" * 32, 20))
    assert one_step.ttl_after_secs == 2073723 // 2


def test_plan_dilute_warns_when_it_would_leave_a_dying_batch():
    nearly_gone = dict(LIVE_BATCH, batchTTL=100)
    plan = run(StampManager(RenewClient(nearly_gone)).plan_dilute("c9" * 32, 21))
    assert plan.ttl_after_secs == 25
    assert "top up in the same session" in plan.warning


def test_dilution_only_ever_increases_depth():
    mgr = StampManager(RenewClient())
    for depth in (18, 19):
        with pytest.raises(StampError, match="only increases depth"):
            run(mgr.plan_dilute("c9" * 32, depth))
        with pytest.raises(StampError, match="only increases depth"):
            run(mgr.dilute("c9" * 32, depth))


def test_dilute_waits_for_the_new_depth(no_sleep):
    client = RenewClient(states=[
        LIVE_BATCH,                                    # pre-flight
        LIVE_BATCH,                                    # not indexed yet
        dict(LIVE_BATCH, depth=20, batchTTL=1036861),  # applied
    ])
    info = run(StampManager(client).dilute("c9" * 32, 20))
    assert client.dilutions == [("c9" * 32, 20)]
    assert info.depth == 20 and info.bucket_capacity == 16


def test_dilute_failures_name_the_batch_and_the_gas_hint():
    client = RenewClient(tx_error=BeeAPIError(500, "fake://stamps/dilute", "boom"))
    with pytest.raises(StampError, match="xDAI for gas") as e:
        run(StampManager(client).dilute("c9" * 32, 20))
    assert "c9" * 32 in str(e.value)
