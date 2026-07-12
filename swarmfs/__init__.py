"""swarmfs — an fsspec backend for Ethereum Swarm (``bzz://``)."""

from fsspec import register_implementation

from ._client import SwarmClient, SyncSwarmClient
from .core import SwarmFile, SwarmFileSystem
from .exceptions import BeeAPIError, BeePermissionError, StampError, SwarmError
from .feedfs import SwarmFeedFileSystem

__version__ = "0.1.0.dev0"
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
    "__version__",
]

# The pyproject entry points cover pip installs; registering on import too
# makes editable/dev usage work without re-resolving entry points.
register_implementation("bzz", SwarmFileSystem, clobber=True)
register_implementation("bzzf", SwarmFeedFileSystem, clobber=True)
