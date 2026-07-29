"""The exception taxonomy: what each Bee HTTP status raises, and that the
hierarchy keeps every existing `except OSError` / `except PermissionError`
seam working."""

from __future__ import annotations

import asyncio

import pytest

from swarmfs import BeeAPIError, BeePermissionError, StampError, SwarmClient, SwarmError


class Resp:
    def __init__(self, status: int, text: str = ""):
        self.status = status
        self._text = text

    async def text(self) -> str:
        return self._text


def _raise(status: int, text: str = ""):
    client = SwarmClient("http://bee.example")
    asyncio.run(client._raise_for_status(Resp(status, text), "http://bee.example/bzz"))


def test_status_mapping():
    _raise(200)  # no error
    with pytest.raises(FileNotFoundError):
        _raise(404)
    with pytest.raises(StampError, match="402.*swarm-cli stamp buy"):
        _raise(402, "batch not usable")
    with pytest.raises(BeePermissionError) as ei:
        _raise(403, "forbidden")
    assert ei.value.status == 403
    with pytest.raises(BeeAPIError) as ei:
        _raise(500, "boom")
    assert ei.value.status == 500 and ei.value.detail == "boom"


def test_bucket_full_402_explains_the_recovery():
    """Bee returns 402 "batch is overissued" when a chunk hashes into a full
    bucket (ErrBucketFull). That is recoverable and does not lose the batch,
    so the error must say so rather than read like a dead stamp."""
    with pytest.raises(StampError) as e:
        _raise(402, "batch is overissued")
    msg = str(e.value)
    assert "Nothing already stored is lost" in msg
    assert "diluting one depth" in msg and "retrying" in msg
    assert "top up" in msg  # dilution halves the remaining TTL
    assert "buckets" in msg  # where to see the real headroom
    # the generic 402 (no stamp accepted) keeps its own advice
    with pytest.raises(StampError, match="swarm-cli stamp buy"):
        _raise(402, "batch not usable")


def test_hierarchy():
    # gateway detection and generic IO handling catch these as before
    assert issubclass(BeePermissionError, PermissionError)
    assert issubclass(BeeAPIError, SwarmError) and issubclass(SwarmError, OSError)
    # stamp failures are one type whether caught locally or as a node 402,
    # importable from its old home too
    from swarmfs.stamps import StampError as FromStamps

    assert FromStamps is StampError
    assert issubclass(StampError, OSError)
