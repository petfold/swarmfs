"""swarmfs exception taxonomy.

Everything swarmfs raises about the node or the network derives from
``SwarmError``, which is an ``OSError`` — so generic fsspec/IO error handling
(and our own ``except OSError`` seams) keeps working. Plain lookup misses
stay builtin ``FileNotFoundError``; fsspec semantics depend on that.
"""

from __future__ import annotations


class SwarmError(OSError):
    """Base class for errors talking to a Bee node or the Swarm network."""


class BeeAPIError(SwarmError):
    """An HTTP error response from the Bee API, with the status attached."""

    def __init__(self, status: int, url: str, detail: str = ""):
        self.status = status
        self.url = url
        self.detail = detail
        super().__init__(
            f"Bee API {status} for {url}" + (f": {detail}" if detail else "")
        )


class BeePermissionError(BeeAPIError, PermissionError):
    """401/403 — the endpoint refuses the operation (typical of gateways
    blocking the node-owner API)."""


class StampError(SwarmError):
    """No usable postage stamp for a write — from local validation before a
    commit, or a 402 Payment Required from the node."""
