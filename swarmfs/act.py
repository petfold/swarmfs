"""ACT — Swarm's Access Control Trie: who may read a reference.

The model, as Bee 2.8.x actually implements it (every point below was
measured live against a node; see CLAUDE.md "ACT-protected content"):

- An upload with ``Swarm-Act: true`` returns an **ACT reference**: the real
  reference *encrypted* with an access key. It is the same length as the
  reference it hides (64 hex plain, 128 hex when the upload was also
  ``Swarm-Encrypt``), so it is indistinguishable from an ordinary
  reference — only the caller knows it is protected.
- Reading it needs three headers: ``Swarm-Act: true``,
  ``Swarm-Act-History-Address`` (the publisher's *history*, the chain of
  access-key grants) and ``Swarm-Act-Publisher`` (the publisher's
  compressed public key — mandatory, even for the publisher). Without them
  the node answers 404; the content is invisible, not forbidden.
  Decryption is done by the node with *its own* key, so reading protected
  content only works through a node whose key is the publisher's or a
  grantee's — never through a gateway.
- **Only the root is wrapped.** The manifest node behind an ACT reference
  carries ordinary child references, and those resolve *without* ACT
  headers (with them: 404). Worse, plain ACT leaves the content itself
  plaintext-addressable: the underlying reference is even exposed as the
  ``ETag`` of a protected read, and any node storing the chunks sees the
  bytes. ACT is access control over the *entry point*; confidentiality is
  ``Swarm-Encrypt``. swarmfs therefore encrypts by default whenever it
  protects (``act=True`` implies ``encrypt=True``).
- A **history** is created by the first protected upload (returned in the
  ``Swarm-Act-History-Address`` response header) or by creating a grantee
  list; later uploads pass it to be readable by the same set of grantees.
  Losing the history means losing access — swarmfs keeps it on the
  filesystem instance (``fs.act_history``) and in every ``CommitResult``;
  persisting it is the caller's job, and the docs say so.
- **Grantees** are compressed secp256k1 public keys (66 hex). The list
  lives at its own reference; ``POST /grantee`` creates one (returning a
  list reference and a history), ``PATCH /grantee/{ref}`` adds/revokes
  (returning *new* references — both advance), ``GET /grantee/{ref}``
  lists. Bee refuses two updates within the same second.

This module is the policy tier: the header bundle (`Act`), the read-side
wrapper that applies it to roots only (`ActReader`), and grantee
management with the usual stamp resolution (`ActManager`). The raw
endpoints live in ``_client.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable, NamedTuple

_HEX = re.compile(r"^[0-9a-fA-F]+$")


class ActUpload(NamedTuple):
    """What a protected upload returns: the ACT reference and the history
    that unlocks it (new, or the one that was passed in)."""

    reference: str
    history: str


class GranteeList(NamedTuple):
    """A grantee list's reference and the history that carries its keys.
    Both change on every patch."""

    reference: str
    history: str


@dataclass(frozen=True)
class Act:
    """The read-side ACT context: which history, whose key, and (optionally)
    as of when. ``headers()`` is what goes on the request."""

    history: str
    publisher: str
    timestamp: int | None = None

    def __post_init__(self):
        h = self.history.lower().removeprefix("0x")
        p = self.publisher.lower().removeprefix("0x")
        if len(h) != 64 or not _HEX.match(h):
            raise ValueError(
                f"act_history must be a 64-hex reference, got {self.history!r}")
        if len(p) != 66 or not _HEX.match(p) or p[:2] not in ("02", "03"):
            raise ValueError(
                "act_publisher must be a compressed secp256k1 public key "
                f"(66 hex starting 02/03 — GET /addresses → publicKey), got {self.publisher!r}")
        object.__setattr__(self, "history", h)
        object.__setattr__(self, "publisher", p)

    def headers(self) -> dict[str, str]:
        h = {
            "swarm-act": "true",
            "swarm-act-history-address": self.history,
            "swarm-act-publisher": self.publisher,
        }
        if self.timestamp is not None:
            h["swarm-act-timestamp"] = str(int(self.timestamp))
        return h


def validate_grantee(key: str) -> str:
    k = key.lower().removeprefix("0x")
    if len(k) != 66 or not _HEX.match(k) or k[:2] not in ("02", "03"):
        raise ValueError(
            "a grantee is a compressed secp256k1 public key (66 hex starting "
            f"02/03), got {key!r}")
    return k


class ActReader:
    """Reader wrapper that sends the ACT headers on **root** references and
    nothing else.

    ``roots`` is the set of references the filesystem has seen as roots
    (URL roots, feed heads, commit results, raw-reference reads); child
    references inside a manifest never appear there, and must not carry
    the headers (a plain reference read with ACT headers is a 404).
    ``act_of`` is consulted per call because the history can change — the
    first protected commit on an instance creates it.
    """

    def __init__(self, inner, act_of: Callable[[], Act | None], roots: set[str]):
        self.inner = inner
        self._act_of = act_of
        self.roots = roots

    def _act(self, ref: str) -> Act | None:
        return self._act_of() if ref.lower() in self.roots else None

    async def bytes_get(self, ref: str, start=None, end=None) -> bytes:
        act = self._act(ref)
        if act is None:
            return await self.inner.bytes_get(ref, start, end)
        return await self.inner.bytes_get(ref, start, end, act=act)

    async def bytes_size(self, ref: str) -> int | None:
        act = self._act(ref)
        if act is None:
            return await self.inner.bytes_size(ref)
        return await self.inner.bytes_size(ref, act=act)

    async def bytes_iter(self, ref: str, chunk_size: int = 1 << 20):
        act = self._act(ref)
        it = (self.inner.bytes_iter(ref, chunk_size) if act is None
              else self.inner.bytes_iter(ref, chunk_size, act=act))
        async for chunk in it:
            yield chunk

    def __getattr__(self, name):
        return getattr(self.inner, name)


class ActManager:
    """Grantee management over a client, with swarmfs's stamp policy.

    Like `StampManager`, questions spend nothing and only the verbs
    (`create_grantees`, `patch_grantees`) need a stamp — resolved the same
    way commits resolve theirs (explicit batch id, or ``"auto"``).
    """

    def __init__(self, client, stamps, stamp: str | None = None):
        self.client = client
        self.stamps = stamps
        self.stamp = stamp

    async def publisher(self) -> str:
        """This node's compressed public key — the ``Swarm-Act-Publisher``
        readers of content *this node* protects must send."""
        return (await self.client.addresses())["publicKey"]

    async def grantees(self, reference: str) -> list[str]:
        """The keys on a grantee list (a pure question; no stamp)."""
        return await self.client.grantee_get(reference)

    async def create_grantees(self, keys: Iterable[str]) -> GranteeList:
        """Create a grantee list; the returned history is what protected
        uploads pass (``act_history``) to be readable by these keys."""
        keys = [validate_grantee(k) for k in keys]
        if not keys:
            raise ValueError("a grantee list needs at least one public key")
        batch = await self.stamps.resolve(self.stamp)
        return await self.client.grantee_create(keys, batch)

    async def patch_grantees(
        self, reference: str, history: str, add: Iterable[str] = (),
        revoke: Iterable[str] = (),
    ) -> GranteeList:
        """Add and/or revoke keys. Returns the **new** list reference and
        history — both advance; keep using the new ones. Bee refuses two
        patches within one second."""
        add = [validate_grantee(k) for k in add]
        revoke = [validate_grantee(k) for k in revoke]
        if not add and not revoke:
            raise ValueError("nothing to add or revoke")
        batch = await self.stamps.resolve(self.stamp)
        return await self.client.grantee_patch(reference, history, batch,
                                               add=add, revoke=revoke)
