"""Building Mantaray tries in memory (a direct port of bee's ``Node.Add``).

v0 needs this for offline test fixtures; it is also the foundation of the v1
write path. Patching an already-persisted trie (load-on-demand during ``add``)
is deliberately not implemented yet — that lands with the v1 commit engine.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from .node import (
    NT_EDGE,
    NT_VALUE,
    NT_WITH_METADATA,
    NT_WITH_PATH_SEPARATOR,
    PREFIX_MAX_SIZE,
    Fork,
    Node,
    marshal,
)

Saver = Callable[[bytes], Awaitable[bytes]]


def _common(a: bytes, b: bytes) -> bytes:
    i = 0
    while i < len(a) and i < len(b) and a[i] == b[i]:
        i += 1
    return a[:i]


def _update_path_separator(node: Node, path: bytes) -> None:
    # bee counts a separator only at index > 0
    if path.find(b"/") > 0:
        node.node_type |= NT_WITH_PATH_SEPARATOR
    else:
        node.node_type &= ~NT_WITH_PATH_SEPARATOR


def _new_child(parent: Node) -> Node:
    return Node(ref_bytes_size=parent.ref_bytes_size, obfuscation_key=parent.obfuscation_key)


def add(node: Node, path: bytes, entry: bytes, metadata: dict[str, str] | None = None) -> None:
    """Insert ``entry`` (a swarm reference, or b"" for metadata-only) at ``path``."""
    if node.ref_bytes_size == 0:
        if len(entry) > 256:
            raise ValueError(f"node entry size > 256: {len(entry)}")
        if entry:
            node.ref_bytes_size = len(entry)
    elif entry and len(entry) != node.ref_bytes_size:
        raise ValueError(f"invalid entry size: {len(entry)}, expected {node.ref_bytes_size}")

    if not path:
        node.entry = entry
        node.node_type |= NT_VALUE
        if metadata:
            node.metadata = metadata
            node.node_type |= NT_WITH_METADATA
        node.ref = None
        return

    f = node.forks.get(path[0])
    if f is None:
        nn = _new_child(node)
        if len(path) > PREFIX_MAX_SIZE:
            prefix, rest = path[:PREFIX_MAX_SIZE], path[PREFIX_MAX_SIZE:]
            add(nn, rest, entry, metadata)
            _update_path_separator(nn, prefix)
            node.forks[path[0]] = Fork(prefix=prefix, node=nn)
            node.node_type |= NT_EDGE
            return
        nn.entry = entry
        if metadata:
            nn.metadata = metadata
            nn.node_type |= NT_WITH_METADATA
        nn.node_type |= NT_VALUE
        _update_path_separator(nn, path)
        node.forks[path[0]] = Fork(prefix=path, node=nn)
        node.node_type |= NT_EDGE
        return

    if f.node is None:
        raise NotImplementedError(
            "patching a persisted trie is not supported yet (v1 commit engine)"
        )

    c = _common(f.prefix, path)
    rest = f.prefix[len(c) :]
    nn = f.node
    if rest:
        # split: move the current node under a new intermediate node
        nn = _new_child(node)
        _update_path_separator(f.node, rest)
        nn.forks[rest[0]] = Fork(prefix=rest, node=f.node)
        nn.node_type |= NT_EDGE
        if len(path) == len(c):
            nn.node_type |= NT_VALUE
    _update_path_separator(nn, path)
    add(nn, path[len(c) :], entry, metadata)
    node.forks[path[0]] = Fork(prefix=c, node=nn)
    node.node_type |= NT_EDGE


async def save(node: Node, saver: Saver) -> bytes:
    """Persist a built trie depth-first; returns the root reference.

    ``saver`` stores one serialized node and returns its swarm reference
    (e.g. a ``/bytes`` upload in v1, or a dict insert in tests).
    """
    for b in sorted(node.forks):
        f = node.forks[b]
        if f.node is not None:
            await save(f.node, saver)
            assert f.node.ref is not None
            f.ref = f.node.ref
            f.node_type = f.node.node_type
            f.metadata = f.node.metadata
    node.ref = await saver(marshal(node))
    return node.ref
