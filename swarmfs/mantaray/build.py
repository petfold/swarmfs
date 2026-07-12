"""Building and patching Mantaray tries (async port of bee's ``Node.Add``/``Remove``).

Fresh tries are built entirely in memory. Patching a *persisted* trie resolves
nodes on demand along the mutated path only — and ``save`` re-serializes only
nodes that were materialized — so changing one file in a large collection
re-uploads O(path depth) nodes, not the whole trie.

Copy-on-write rules:

- nodes are resolved for writing with a fresh ``unmarshal`` (never through the
  shared read cache, whose nodes must stay immutable);
- the fork record seeds the child's ``node_type``/``metadata`` — a node's own
  type is not persisted in its serialization, only its parent's fork knows it;
- ``save`` recurses only into forks with an in-memory child, then re-syncs the
  fork record from the child.
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
    unmarshal,
)

Saver = Callable[[bytes], Awaitable[bytes]]
Loader = Callable[[bytes], Awaitable[bytes]]


def _common(a: bytes, b: bytes) -> bytes:
    i = 0
    while i < len(a) and i < len(b) and a[i] == b[i]:
        i += 1
    return a[:i]


def _sep_flag(node_type: int, path: bytes) -> int:
    # bee counts a separator only at index > 0
    if path.find(b"/") > 0:
        return node_type | NT_WITH_PATH_SEPARATOR
    return node_type & ~NT_WITH_PATH_SEPARATOR


def _update_sep(node: Node, path: bytes) -> None:
    node.node_type = _sep_flag(node.node_type, path)


async def _resolve_for_write(fork: Fork, load: Loader | None) -> Node:
    """Materialize a fork's child as a private, mutable node."""
    if fork.node is None:
        if load is None:
            raise ValueError("patching a persisted trie requires a loader")
        node = unmarshal(await load(fork.ref))
        node.node_type = fork.node_type
        node.metadata = dict(fork.metadata) if fork.metadata else None
        node.ref = None
        fork.node = node
    return fork.node


async def add(
    node: Node,
    path: bytes,
    entry: bytes,
    metadata: dict[str, str] | None = None,
    load: Loader | None = None,
) -> None:
    """Insert ``entry`` (a swarm reference, or b"" for metadata-only) at ``path``.

    ``load`` fetches serialized nodes by reference; required only when the
    insertion path crosses forks that exist by reference (patching).
    """
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

    node.ref = None
    f = node.forks.get(path[0])
    if f is None:
        nn = Node(ref_bytes_size=node.ref_bytes_size, obfuscation_key=node.obfuscation_key)
        if len(path) > PREFIX_MAX_SIZE:
            prefix, rest = path[:PREFIX_MAX_SIZE], path[PREFIX_MAX_SIZE:]
            await add(nn, rest, entry, metadata, load)
            _update_sep(nn, prefix)
            node.forks[path[0]] = Fork(prefix=prefix, node=nn)
        else:
            nn.entry = entry
            if metadata:
                nn.metadata = metadata
                nn.node_type |= NT_WITH_METADATA
            nn.node_type |= NT_VALUE
            _update_sep(nn, path)
            node.forks[path[0]] = Fork(prefix=path, node=nn)
        node.node_type |= NT_EDGE
        return

    c = _common(f.prefix, path)
    rest = f.prefix[len(c) :]
    if rest:
        # split the edge: move the existing child under a new intermediate
        # node — by reference, no load needed
        nn = Node(ref_bytes_size=node.ref_bytes_size, obfuscation_key=node.obfuscation_key)
        moved = Fork(
            node_type=_sep_flag(f.node_type, rest),
            prefix=rest,
            ref=f.ref,
            metadata=f.metadata,
            node=f.node,
        )
        if moved.node is not None:
            _update_sep(moved.node, rest)
        nn.forks[rest[0]] = moved
        nn.node_type |= NT_EDGE
        if len(path) == len(c):
            nn.node_type |= NT_VALUE
    else:
        nn = await _resolve_for_write(f, load)
    _update_sep(nn, path)
    await add(nn, path[len(c) :], entry, metadata, load)
    node.forks[path[0]] = Fork(prefix=c, node=nn)
    node.node_type |= NT_EDGE


async def remove(node: Node, path: bytes, load: Loader | None = None) -> None:
    """Remove the entry at ``path`` (with any subtree below it).

    Deviation from bee: intermediate nodes left with no forks, no entry and no
    metadata are pruned, so removing a directory's last file removes the
    directory — the semantics fsspec users expect from implicit directories.

    Raises FileNotFoundError if the path does not exist.
    """
    if not path:
        raise ValueError("empty path")
    f = node.forks.get(path[0])
    if f is None or not path.startswith(f.prefix):
        raise FileNotFoundError(path.decode("utf-8", "surrogateescape"))
    node.ref = None
    rest = path[len(f.prefix) :]
    if not rest:
        del node.forks[path[0]]
        return
    child = await _resolve_for_write(f, load)
    await remove(child, rest, load)
    if not child.forks and not child.has_entry and not child.metadata:
        del node.forks[path[0]]


async def save(node: Node, saver: Saver) -> bytes:
    """Persist a trie depth-first; returns the root reference.

    Only forks with an in-memory child (freshly built, or materialized by a
    patch) are re-serialized; everything else is written by its existing
    reference — this is what keeps single-file changes cheap.
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
