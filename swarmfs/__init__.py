"""swarmfs — an fsspec backend for Ethereum Swarm (``bzz://``)."""

from fsspec import register_implementation

from .core import SwarmFile, SwarmFileSystem

__version__ = "0.1.0.dev0"
__all__ = ["SwarmFileSystem", "SwarmFile", "__version__"]

# The pyproject entry point covers pip installs; registering on import too
# makes editable/dev usage work without re-resolving entry points.
register_implementation("bzz", SwarmFileSystem, clobber=True)
