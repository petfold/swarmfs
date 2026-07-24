"""StampManager: selection and fail-early validation."""

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
