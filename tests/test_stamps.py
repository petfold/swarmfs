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
from swarmfs.stamps import BatchPlan, suggest_depth

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


def test_suggest_depth_bucket_overflow_tiers():
    assert suggest_depth(1 * MB) == 18
    assert suggest_depth(15 * MB) == 18
    assert suggest_depth(16 * MB) == 19  # a 42 MB upload filled depth 18 live
    assert suggest_depth(150 * MB) == 19
    assert suggest_depth(1024 * MB) == 20
    assert suggest_depth(2048 * MB) == 21


def test_plan_pads_the_chain_minimum():
    mgr = StampManager(BuyClient())
    floor = 17280 + 720  # minimumValidityBlocks + 1h price-drift pad
    plan = asyncio.run(mgr.plan(10 * MB, ttl_secs=3600))
    assert plan == BatchPlan(
        depth=18, amount=floor * 1000, ttl_secs=floor * 5,
        cost_bzz=floor * 1000 * 2**18 / 10**16,
    )
    week = 7 * 86400
    plan = asyncio.run(mgr.plan(10 * MB, ttl_secs=week))
    assert plan.amount == (week // 5) * 1000


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
                 states=None, tx_error=None):
        batch = LIVE_BATCH if batch is None else batch
        super().__init__([batch])
        self._batch = batch
        self._price = price
        self._bzz = bzz_plur
        self._states = iter(states) if states is not None else None
        self._tx_error = tx_error
        self.topups: list[tuple[str, int]] = []
        self.dilutions: list[tuple[str, int]] = []

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


def test_a_batchs_amount_is_cumulative_not_remaining():
    """Learned live, after it broke an integration assertion: ``amount``
    counts from the creation block, so it implies TOTAL lifetime. Remaining
    life is the node's ``batchTTL`` — never re-derive it from ``amount``.
    """
    age_secs = (47446655 - 47381172) * 5  # chainstate block - blockNumber
    total = amount_to_ttl(32954342400, LIVE_PRICE)
    assert total / 86400 == pytest.approx(27.78, rel=1e-3)  # NOT the 24.0 left
    assert total - age_secs == pytest.approx(2073723, rel=1e-3)  # == batchTTL


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
