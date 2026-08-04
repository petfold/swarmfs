"""swarmfs — an fsspec backend for Ethereum Swarm (``bzz://``)."""

from fsspec import register_implementation

from ._client import SwarmClient, SyncSwarmClient
from .core import SwarmFile, SwarmFileSystem
from .exceptions import BeeAPIError, BeePermissionError, StampError, SwarmError
from .feedfs import SwarmFeedFileSystem

__version__ = "0.6.0"
__all__ = [
    "SwarmFileSystem",
    "SwarmFeedFileSystem",
    "SwarmFile",
    "SwarmClient",
    "SyncSwarmClient",
    "SwarmError",
    "BeeAPIError",
    "BeePermissionError",
    "StampError",
    "split",
    "content_address",
    "__version__",
]


def __getattr__(name):
    # The splitter needs keccak256 (the `feeds` extra); import it on use so a
    # plain install stays importable. The module is `splitter`, not `split`, so
    # importing it can never shadow the `split` function it exports (and
    # `from . import split` here would recurse into this very hook).
    if name in ("split", "content_address"):
        import importlib

        return getattr(importlib.import_module("swarmfs.splitter"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# The pyproject entry points cover pip installs; registering on import too
# makes editable/dev usage work without re-resolving entry points.
register_implementation("bzz", SwarmFileSystem, clobber=True)
register_implementation("bzzf", SwarmFeedFileSystem, clobber=True)
