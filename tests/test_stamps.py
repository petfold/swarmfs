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
