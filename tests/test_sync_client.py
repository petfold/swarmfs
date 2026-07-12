"""SyncSwarmClient: the blocking facade over the async client — offline,
against the fake client (the facade drives coroutines on fsspec's shared
background loop, so a duck-typed async client exercises it fully)."""

from __future__ import annotations

import pytest

from swarmfs import SwarmClient, SyncSwarmClient

from conftest import GOOD_STAMP, FakeClient

STAMP = GOOD_STAMP["batchID"]


def test_blocking_roundtrip():
    with SyncSwarmClient(client=FakeClient({})) as client:
        assert client.health()["status"] == "ok"
        assert client.stamps_list()[0]["usable"] is True

        ref = client.bytes_post(b"hello swarm", STAMP)
        assert client.bytes_get(ref) == b"hello swarm"
        assert client.bytes_get(ref, 6, 11) == b"swarm"
        assert client.bytes_size(ref) == 11
        assert b"".join(client.bytes_iter(ref, chunk_size=4)) == b"hello swarm"

        # single-file upload wraps in a manifest, like the async client
        mref = client.bzz_post(b"file body", STAMP, filename="f.txt")
        assert len(mref) == 64


def test_facade_mirrors_async_surface():
    """Every SwarmClient coroutine has a blocking twin with its docs."""
    import inspect

    for name, member in inspect.getmembers(SwarmClient):
        if name.startswith("_") or not inspect.iscoroutinefunction(member):
            continue
        twin = getattr(SyncSwarmClient, name, None)
        assert twin is not None, f"SyncSwarmClient is missing {name}"
        assert not inspect.iscoroutinefunction(twin)
        assert name in (twin.__doc__ or ""), f"{name} twin lacks a docstring"
    # the async generator too
    assert not inspect.isasyncgenfunction(SyncSwarmClient.bytes_iter)


def test_errors_propagate():
    client = SyncSwarmClient(client=FakeClient({}))
    with pytest.raises(FileNotFoundError):
        client.bytes_get("ab" * 32)


def test_endpoint_resolution(monkeypatch):
    monkeypatch.setenv("BEE_API_URL", "http://bee.example:1633")
    assert SyncSwarmClient().api_url == "http://bee.example:1633"
    assert SyncSwarmClient(api_url="http://other:1633").api_url == "http://other:1633"
