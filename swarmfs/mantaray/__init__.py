"""Pure-Python Mantaray trie codec (parse, build, walk).

Independently useful — candidate for extraction as a standalone
``mantaray-py`` package once the API settles.
"""

from .build import add, save
from .node import (
    NT_EDGE,
    NT_VALUE,
    NT_WITH_METADATA,
    NT_WITH_PATH_SEPARATOR,
    Fork,
    MantarayFormatError,
    Node,
    marshal,
    unmarshal,
)
from .walk import FileEntry, Location, NodeStore, iter_files, list_directory, locate

__all__ = [
    "NT_EDGE",
    "NT_VALUE",
    "NT_WITH_METADATA",
    "NT_WITH_PATH_SEPARATOR",
    "Fork",
    "MantarayFormatError",
    "Node",
    "marshal",
    "unmarshal",
    "add",
    "save",
    "FileEntry",
    "Location",
    "NodeStore",
    "iter_files",
    "list_directory",
    "locate",
]
