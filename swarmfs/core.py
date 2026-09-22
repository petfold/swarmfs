"""SwarmFileSystem: fsspec AsyncFileSystem over the Bee HTTP API.

Paths look like ``bzz://<64-or-128-hex-reference>/<path-inside-manifest>``;
the reference plays the role of a bucket. ``bzz://new/...`` (or ``new-<any>``)
addresses a fresh, not-yet-committed manifest.

Writes are copy-on-write: they are staged on the filesystem instance and
committed — each commit uploads the changed data, patches the manifest trie
client-side, and yields a *new* root reference (the old root is untouched;
every commit is a snapshot). Outside a transaction every write operation
commits immediately; inside ``with fs.transaction:`` everything is committed
together on exit. The instance remembers old→new root mappings, so reads
through the original URL keep seeing the latest committed state
(read-your-writes); ``fs.latest(ref)`` returns the current head and
``fs.commit_log`` the full history.
"""

from __future__ import annotations

import asyncio
import datetime
import mimetypes
import os
import posixpath
import shutil
import warnings
import weakref

from fsspec.asyn import AsyncFileSystem, sync
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.transaction import Transaction
from fsspec.utils import stringify_path

from ._client import DEFAULT_API_URL, SwarmClient, SyncSwarmClient
from .act import Act, ActManager, ActReader, ActUpload
from ._listing import ListingBackend, detect_listing_backend
from .commit import (CommitEngine, CommitResult, Staged, StagedLink,
                     StagedWrite, check_link_refsize)
from .exceptions import StampError
from .stamps import StampManager


def _validate_ref(ref: str) -> None:
    if len(ref) not in (64, 128) or any(c not in "0123456789abcdefABCDEF" for c in ref):
        raise ValueError(
            f"invalid swarm reference {ref!r}: expected 64 hex chars "
            "(or 128 for encrypted references), or 'new' for a fresh manifest. "
            "ENS names are not supported yet. To upload new content and get "
            "its reference back, use fs.upload(local_path)."
        )


def _link_reference(reference: str) -> str:
    """Normalize and validate a reference handed to ``fs.link``."""
    ref = stringify_path(reference).strip().lower().removeprefix("0x")
    if len(ref) not in (64, 128) or any(c not in "0123456789abcdef" for c in ref):
        raise ValueError(
            f"invalid swarm reference {reference!r} to link: expected 64 hex "
            "chars (or 128 for an encrypted reference) — the value "
            "fs.put_blob() and SwarmClient.bytes_post() return."
        )
    return ref


def _read_all(data) -> bytes:
    data.seek(0)
    return data.read()


class SwarmTransaction(Transaction):
    """Defers all staged writes to a single commit per manifest lineage."""

    def complete(self, commit=True):
        fs = self.fs
        while self.files:
            f = self.files.popleft()
            if commit:
                f.commit()
            else:
                f.discard()
        if commit:
            sync(fs.loop, fs._commit_all)
        else:
            fs.discard_staged()
        fs._intrans = False
        fs._transaction = None
        # Deliberately NOT `self.fs = None`: fsspec < 2024.3.0's
        # Transaction.__exit__ touches self.fs unconditionally after
        # complete() (the `if self.fs:` guard arrived in 2024.3.0), so
        # clearing it here crashed every transaction exit for users on
        # the nine months of fsspec releases our floor claims to support.
        # Leaving it set is harmless on both generations: old __exit__
        # re-clears two already-cleared attributes, new __exit__ clears
        # fs itself.


class LocalFirstReader:
    """The read side of local-first mode: refs the local store holds (or
    can heal by verified re-fetch) are served from disk — which is what
    makes offline read-your-writes work — and everything foreign
    delegates to the node's reader unchanged, preserving range-granular
    remote reads. Local bytes need no re-verification: locally written
    blobs were addressed by us, healed blobs are hash-checked on the way
    in. Ranges on local blobs are slices (the whole blob is on disk)."""

    def __init__(self, local, inner):
        self.local = local
        self.inner = inner

    def _try_local(self, ref: str) -> bytes | None:
        try:
            return self.local.get(ref)  # heals evicted refs via the syncer
        except KeyError:
            # unknown (foreign) ref — or evicted with the network down, in
            # which case the inner reader fails with the honest network
            # error anyway
            return None

    async def bytes_get(self, ref: str, start=None, end=None) -> bytes:
        data = await asyncio.to_thread(self._try_local, ref)
        if data is None:
            return await self.inner.bytes_get(ref, start, end)
        if start is None and end is None:
            return data
        if start is not None and end is not None and end <= start:
            return b""
        return data[start or 0: end]

    async def bytes_size(self, ref: str) -> int | None:
        size = self.local.local_size(ref)
        if size is not None:
            return size
        return await self.inner.bytes_size(ref)

    async def bytes_iter(self, ref: str, chunk_size: int = 1 << 20):
        data = await asyncio.to_thread(self._try_local, ref)
        if data is None:
            async for chunk in self.inner.bytes_iter(ref, chunk_size):
                yield chunk
            return
        for i in range(0, len(data), chunk_size):
            yield data[i: i + chunk_size]

    def __getattr__(self, name):
        return getattr(self.inner, name)


class SwarmFileSystem(AsyncFileSystem):
    """Read/write access to Swarm content via a Bee node.

    Parameters
    ----------
    api_url:
        Bee API endpoint. Resolution order: this argument, then ``$BEE_API_URL``,
        then ``http://localhost:1633``. A local light node is the recommended
        setup; public gateways work for reads but are discouraged (unverified
        trust in the gateway).
    block_size:
        Default block size for opened files (readahead / block caching).
    timeout:
        Total per-request timeout in seconds.
    headers:
        Extra HTTP headers sent with every request.
    stamp:
        Postage batch for writes: a batch id (64 hex), or ``"auto"``/None to
        pick the usable batch with the longest TTL at commit time.
    pin:
        Ask the node to pin uploaded content locally.
    redundancy:
        Erasure-coding level 0–4 for uploads (Bee's ``swarm-redundancy-level``):
        parity chunks are added so content survives missing chunks. Defaults
        to 2 ("strong"); pass 0 to disable, or None for the node's default.
    encrypt:
        (default None = ``act``, i.e. off unless ACT-protecting.)
        Encrypt every upload (files AND manifest nodes) node-side; commits
        and uploads then return 128-hex references — address plus
        decryption key. Whoever holds the full reference can read (the
        node decrypts in the load path); everyone else stores noise. A
        lineage is either encrypted or not — patching across the boundary
        is refused. Incompatible with ``local_store`` (encrypted refs are
        not content addresses) and with ``verify`` (the verifying reader
        cannot traverse ciphertext; it refuses 128-hex refs loudly).
    allow_gateway:
        Explicitly permit using an endpoint that is not your own node.
        Endpoints where the node-owner API (``/stamps``) is unreachable are
        treated as gateways and refused unless this is set — run a light
        node instead if you can.
    verify:
        Client-side chunk verification (BMT-hash every fetched chunk against
        its reference). Default: on for gateways, off for your own node.
    act, act_history, act_publisher, act_timestamp:
        Access control (Swarm ACT; see ``swarmfs.act``). ``act=True``
        protects every commit and upload: the new root goes up with
        ``swarm-act`` and comes back as an ACT reference readable only by
        the publisher's and the grantees' nodes. The first protected
        commit creates a *history* (``fs.act_history``, also on each
        ``CommitResult``) — persist it, it is what unlocks the content;
        pass it back as ``act_history`` to keep publishing into the same
        one (or pass the history of a grantee list). Reading protected
        content needs ``act_history`` plus ``act_publisher`` (the
        publisher's compressed public key; defaults to this node's own —
        right when you are the publisher). With either set, **every root
        reference on this instance is treated as protected**; child
        references never are. ``act_timestamp`` reads as of a moment in
        the history. ACT alone leaves content plaintext-addressable, so
        ``act=True`` turns ``encrypt`` on unless explicitly ``False``.
        Only through your own node (the node decrypts with its key);
        incompatible with ``verify`` and ``local_store``.
    client:
        Injection seam for a pre-built ``SwarmClient`` (used by tests).
    index:
        Maintain a root index (``.swarmfs/index.json``) on every commit: one
        file listing every entry's path, data reference, size and metadata,
        so a reader answers ``ls``/``find``/``info`` from a single fetch
        instead of one round trip per trie node (2,224 of them for a
        2,000-file dataset). Off by default because it **changes the root**:
        an indexed manifest no longer has the same reference as a plain bee
        upload of the same tree. Worth it for datasets big enough to feel
        the walk; pointless for a handful of files. Reading uses an index
        whenever one is present, whatever this is set to — and a commit
        with it off *drops* an index it finds, so a stale one can never
        answer with yesterday's content.
    local_store:
        Local-first mode (docs/localstore-design.md): path to a store
        directory (or a ready ``LocalStore``). Commits land on local disk
        instantly — offline is the normal mode, no stamp is needed at
        commit time — and a background syncer pushes them to Swarm and
        confirms peer-to-peer; ``fs.sync()`` is the certainty barrier and
        ``fs.sync_status()`` the ladder view. bzzf feed updates publish
        only after confirmation. Requires ``redundancy=0`` (erasure
        coding would fork the node's references from the local BMT
        address space). Reads still go to the node — local-first covers
        the write path.
    """

    protocol = "bzz"
    root_marker = ""
    transaction_type = SwarmTransaction

    def __init__(
        self,
        api_url: str | None = None,
        block_size: int | None = None,
        timeout: float = 120,
        headers: dict[str, str] | None = None,
        stamp: str | None = None,
        pin: bool = False,
        redundancy: int | None = 2,
        encrypt: bool | None = None,
        allow_gateway: bool = False,
        verify: bool | None = None,
        client: SwarmClient | None = None,
        local_store: str | None = None,
        act: bool = False,
        act_history: str | None = None,
        act_publisher: str | None = None,
        act_timestamp: int | None = None,
        index: bool = False,
        asynchronous: bool = False,
        loop=None,
        **storage_options,
    ):
        super().__init__(asynchronous=asynchronous, loop=loop, **storage_options)
        # an injected client's endpoint wins over the env/default so trust
        # detection judges the endpoint actually in use
        self.api_url = api_url or (
            client.api_url if client is not None else None
        ) or os.environ.get("BEE_API_URL", DEFAULT_API_URL)
        self.client = client or SwarmClient(self.api_url, timeout=timeout, headers=headers)
        self.block_size = block_size or 2**20
        self.stamp = stamp
        self.pin = pin
        if redundancy is not None and redundancy not in range(5):
            raise ValueError(f"redundancy must be 0-4, got {redundancy!r}")
        self.redundancy = redundancy
        # ACT (swarmfs.act): act=True protects writes; act_history (+
        # publisher) unlocks reads. Either makes this an "ACT instance":
        # every root reference it sees is protected, children never are.
        self.act = act
        self.act_history = (act_history.lower().removeprefix("0x")
                            if act_history else None)
        self.act_publisher = act_publisher
        self.act_timestamp = act_timestamp
        self.act_mode = act or self.act_history is not None
        self._act_roots: set[str] = set()
        if self.act_history is not None and act_publisher is not None:
            Act(self.act_history, act_publisher)  # validate early
        if encrypt is None:
            # ACT wraps only the root reference; the content itself stays
            # plaintext-addressable (its reference is even the ETag of a
            # protected read). Confidentiality is swarm-encrypt, so protect
            # ⇒ encrypt unless the caller explicitly declines.
            encrypt = act
        elif act and not encrypt:
            warnings.warn(
                "act=True with encrypt=False: ACT hides the root reference "
                "but the content stays plaintext-addressable (any node "
                "storing the chunks can read it, and the underlying "
                "reference is exposed to authorized readers) — pass "
                "encrypt=True for confidentiality", stacklevel=2)
        self.encrypt = encrypt
        if self.act_mode and verify:
            raise ValueError(
                "verify=True cannot be combined with ACT: an ACT reference is "
                "an encrypted reference, not a content address, so the root "
                "cannot be checked against it")
        self.allow_gateway = allow_gateway
        self.verify = verify
        self.verify_active: bool | None = None  # resolved by _setup
        self._reader = None  # client, or a VerifyingReader over it
        self._setup_done = False
        self._backend: ListingBackend | None = None
        # Local-first mode (L3): commits land in a store directory and a
        # background syncer pushes/confirms; the network leaves the write
        # path entirely. See docs/localstore-design.md.
        self._local = None
        self._syncer = None
        if local_store is not None:
            if self.act_mode:
                raise ValueError(
                    "local_store cannot be combined with ACT: an ACT reference "
                    "is not a content address, and the node — not this "
                    "process — holds the key that resolves it")
            if encrypt:
                raise ValueError(
                    "local_store cannot be combined with encrypt=True: "
                    "encrypted references are not content addresses (the "
                    "decryption key rides in the reference), so the local "
                    "store's BMT address space and journal cannot hold "
                    "them")
            if redundancy not in (None, 0):
                raise ValueError(
                    "local_store requires redundancy=0: erasure coding "
                    "changes the node's references, forking them from the "
                    "local store's BMT address space")
            from .commit import LocalFirstCommitEngine
            from .localstore import LocalStore
            from .localsync import BeeRemote, Syncer

            self._local = (local_store if not isinstance(local_store, str)
                           else LocalStore(local_store, addressing="swarm"))
            remote = BeeRemote(client=SyncSwarmClient(client=self.client),
                               stamp=stamp or "auto")
            self._syncer = Syncer(self._local, remote).start()
            self._engine = LocalFirstCommitEngine(self._local, self.client,
                                                  index=index)
            weakref.finalize(self, _stop_local_first,
                             self._syncer, self._local)
        else:
            self._engine = CommitEngine(
                self.client, StampManager(self.client), pin=pin,
                redundancy=redundancy, encrypt=encrypt, act=act,
                act_publisher=act_publisher, index=index,
            )
        # staging, keyed by the *origin* root of each manifest lineage
        self._staged: dict[str, dict[str, Staged]] = {}
        self._staged_rm: dict[str, set[str]] = {}
        self._root_map: dict[str, str] = {}  # committed root -> its successor
        self._origin: dict[str, str] = {}  # any root in a lineage -> origin
        self._commit_lock = asyncio.Lock()
        self.commit_log: list[CommitResult] = []
        weakref.finalize(self, self._close_client, self.loop, self.client)

    # -- local-first surface (present when local_store= was given) -----------

    def sync(self, timeout: float | None = None) -> None:
        """Block until every local-first commit is network-confirmed — the
        certainty barrier ('my data is really out there')."""
        if self._syncer is None:
            raise RuntimeError("no local_store configured: writes go "
                               "straight to the node, nothing to sync")
        self._syncer.sync(timeout)

    def sync_status(self):
        """The local-first store's ladder view (swarmfs `StoreStatus`):
        pinned vs evictable bytes, per-root rungs, batch expiries."""
        if self._local is None:
            raise RuntimeError("no local_store configured")
        return self._local.status()

    @staticmethod
    def _close_client(loop, client: SwarmClient) -> None:
        if loop is not None and loop.is_running():
            try:
                sync(loop, client.close, timeout=0.1)
            except (TimeoutError, FSTimeoutError, NotImplementedError, RuntimeError):
                pass

    # ----------------------------------------------------------- path model

    @classmethod
    def _strip_protocol(cls, path) -> str:
        path = stringify_path(path)
        for prefix in (f"{cls.protocol}://", f"{cls.protocol}:"):
            if path.startswith(prefix):
                path = path[len(prefix) :]
                break
        return path.strip("/")

    @staticmethod
    def _is_pseudo(ref: str) -> bool:
        return ref == "new" or ref.startswith("new-")

    def _resolve_head(self, ref: str) -> str:
        # a commit of identical content yields an identical root (content
        # addressing), so guard against identity/cyclic entries
        while ref in self._root_map and self._root_map[ref] != ref:
            ref = self._root_map[ref]
        return ref

    def latest(self, ref: str) -> str:
        """Follow committed root mappings to the current head of a lineage."""
        return self._resolve_head(self._strip_protocol(ref).partition("/")[0])

    def _split_ref(self, path: str) -> tuple[str, str]:
        """Split into (resolved root reference, subpath)."""
        ref, _, sub = path.partition("/")
        if not ref:
            raise ValueError(
                "empty swarm reference: Swarm is content-addressed, so a write "
                "destination does not exist until the network returns its "
                "reference. Use fs.upload(local_path) to upload a file or "
                "directory and get the new reference back, or write to "
                "bzz://new/<path> and read fs.latest('new') afterwards."
            )
        if not self._is_pseudo(ref):
            _validate_ref(ref)
        head = self._resolve_head(ref)
        self._register_root(ref)
        self._register_root(head)
        return head, sub.strip("/")

    async def _resolve_path(self, path: str) -> tuple[str, str]:
        """Async seam over _split_ref — bzzf:// overrides this with a feed
        lookup, which needs I/O."""
        return self._split_ref(path)

    def _subpath_of(self, path: str) -> str:
        """The within-manifest part of a stripped path (syntactic only)."""
        return path.partition("/")[2].strip("/")

    def _origin_of(self, ref: str) -> str:
        return self._origin.get(ref, ref)

    # ------------------------------------------------------------------ ACT

    def _register_root(self, ref: str) -> None:
        """Remember ``ref`` as a *root* on an ACT instance: roots get the ACT
        headers, the child references inside them never do."""
        if self.act_mode and ref and not self._is_pseudo(ref):
            self._act_roots.add(ref.lower())

    def _current_act(self) -> Act | None:
        if not self.act_mode or self.act_history is None or self.act_publisher is None:
            return None
        return Act(self.act_history, self.act_publisher, self.act_timestamp)

    def _act_for(self, ref: str) -> Act | None:
        return self._current_act() if ref.lower() in self._act_roots else None

    def _adopt_act(self, history: str | None, root: str | None) -> None:
        """After a protected write: keep the history (the first commit
        creates it) and register the new root."""
        if history:
            self.act_history = history.lower()
        if root:
            self._register_root(root)

    def _act_manager(self) -> ActManager:
        return ActManager(self.client, StampManager(self.client), self.stamp)

    async def _publisher_key(self) -> str:
        await self._setup()
        return await self._act_manager().publisher()

    def publisher_key(self) -> str:
        """This node's compressed public key — what readers of content this
        node protects pass as ``act_publisher`` (and what another publisher
        adds to a grantee list to let this node read)."""
        return sync(self.loop, self._publisher_key)

    async def _create_grantees(self, keys):
        await self._setup()
        return await self._act_manager().create_grantees(keys)

    def create_grantees(self, keys):
        """Create an ACT grantee list from compressed public keys; returns
        ``GranteeList(reference, history)``. Publish with
        ``act_history=<that history>`` and those keys' nodes can read.
        Spends a stamp (resolved like commits: ``stamp`` or auto)."""
        return sync(self.loop, self._create_grantees, keys)

    async def _grantees(self, reference: str):
        await self._setup()
        return await self._act_manager().grantees(reference)

    def grantees(self, reference: str) -> list[str]:
        """The public keys on a grantee list (free: a pure question)."""
        return sync(self.loop, self._grantees, reference)

    async def _patch_grantees(self, reference, history, add, revoke):
        await self._setup()
        return await self._act_manager().patch_grantees(reference, history, add, revoke)

    def patch_grantees(self, reference: str, history: str, add=(), revoke=()):
        """Add/revoke grantee keys; returns the **new** ``GranteeList`` —
        reference and history both advance, keep the new ones. Bee refuses
        two patches within one second."""
        return sync(self.loop, self._patch_grantees, reference, history, add, revoke)

    def _overlay(self, ref: str) -> tuple[dict[str, Staged], set[str]]:
        okey = self._origin_of(ref)
        return self._staged.get(okey, {}), self._staged_rm.get(okey, set())

    async def _setup(self) -> None:
        """First-contact checks, once per instance: reachability (with a
        useful error), gateway detection, and verification mode."""
        if self._setup_done:
            return
        import aiohttp
        from urllib.parse import urlsplit

        try:
            await self.client.health()
        except (aiohttp.ClientConnectionError, ConnectionError) as e:
            if self._local is not None:
                # Local-first: an unreachable endpoint is *your node,
                # currently offline* — that's the normal mode, not an
                # error. Serve everything the local store holds; foreign
                # refs fail at fetch time with the plain network error.
                # (No gateway-detection risk: the endpoint is fixed by
                # configuration and local-first pushes need the node-owner
                # API anyway.) Verification stays off unless forced —
                # there is nothing remote to distrust while offline.
                self.trusted = True
                self.verify_active = bool(self.verify)
                reader = (self.client if not self.verify_active
                          else self._verifying_reader())
                self._reader = LocalFirstReader(self._local, reader)
                self._setup_done = True
                return
            raise ConnectionError(
                f"cannot reach a Bee node at {self.api_url} ({e}). swarmfs expects "
                "a node you run yourself — a local light node is quick to set up: "
                "https://docs.ethswarm.org/docs/bee/installation/quick-start. "
                "If your node runs elsewhere, pass api_url=... or set BEE_API_URL."
            ) from e
        except OSError:
            pass  # endpoint reachable but blocks /health (some gateways)

        host = urlsplit(self.api_url).hostname
        if host in ("localhost", "127.0.0.1", "::1"):
            trusted = True
        else:
            try:
                await self.client.stamps_list()
                trusted = True
            except OSError:
                trusted = False  # node-owner API blocked: a gateway
        if not trusted and not self.allow_gateway:
            raise PermissionError(
                f"{self.api_url} looks like a public gateway (the node-owner API "
                "is not accessible). swarmfs encourages running your own light "
                "node: https://docs.ethswarm.org/docs/bee/installation/quick-start. "
                "To read through this gateway anyway, pass allow_gateway=True "
                "(chunk verification is then enabled by default)."
            )
        if self.act_mode:
            if not trusted:
                raise PermissionError(
                    f"{self.api_url} is not your own node, and ACT-protected "
                    "content can only be read through a node holding the "
                    "publisher's or a grantee's key — the node decrypts, not "
                    "this process. Point api_url at your light node.")
            if self.act_publisher is None:
                # the publisher defaults to *this* node — right whenever the
                # reader is the publisher; grantees pass the publisher's key
                self.act_publisher = (await self.client.addresses())["publicKey"]
                if self.act_history is not None:
                    Act(self.act_history, self.act_publisher)  # validate
            if isinstance(self._engine, CommitEngine):
                self._engine.act_publisher = self.act_publisher
        self.trusted = trusted
        self.verify_active = self.verify if self.verify is not None else not trusted
        if self.act_mode:
            self.verify_active = False  # refused at construction if forced
        self._reader = (self._verifying_reader() if self.verify_active
                        else self.client)
        if self.act_mode:
            self._reader = ActReader(self._reader, self._current_act, self._act_roots)
        if self._local is not None:
            # Local-first reads: refs the store holds are served from
            # disk (offline read-your-writes); foreign refs go to the
            # node as before.
            self._reader = LocalFirstReader(self._local, self._reader)
        self._setup_done = True

    def _verifying_reader(self):
        from .join import VerifyingReader

        return VerifyingReader(self.client)

    async def _get_reader(self):
        await self._setup()
        return self._reader

    async def _read_reference(self, ref: str, start=None, end=None) -> bytes:
        return await (await self._get_reader()).bytes_get(ref, start, end)

    def read_reference(self, ref: str, start=None, end=None) -> bytes:
        """Raw-reference read: the bytes behind `ref` (optionally the
        `[start, end)` range), routed through the same reader as path
        reads — so verification policy applies, and in local-first mode
        locally held refs are served from disk. The supported public form
        of the raw-ref primitive (grown for ontodag-fs, which was
        reaching into `_read_reference`); use `cat`/`open` for paths
        inside manifests."""
        self._register_root(ref)  # a raw reference handed in is a root
        return sync(self.loop, self._read_reference, ref, start, end)

    async def _reference_size(self, ref: str) -> int | None:
        return await (await self._get_reader()).bytes_size(ref)

    def reference_size(self, ref: str) -> int | None:
        """Size in bytes of the content behind a raw `ref`, through the
        same reader (local-first answers from the store without reading
        the blob; otherwise a header-only request)."""
        self._register_root(ref)
        return sync(self.loop, self._reference_size, ref)

    async def _get_backend(self) -> ListingBackend:
        if self._backend is None:
            await self._setup()
            self._backend = await detect_listing_backend(self._reader)
        return self._backend

    def modified(self, path):
        """A fixed timestamp, for consumers (e.g. DuckDB's fsspec bridge)
        that require ``modified()`` not to raise.

        ``bzz://`` content is content-addressed and immutable at a fixed
        reference, so there is no real last-modified time to report; this
        checks the path exists (like ``info``) and returns the epoch.
        ``bzzf://`` mounts inherit this — it does not reflect a feed's most
        recent update.
        """
        self.info(path)
        return datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            self.dircache.pop(path, None)
        super().invalidate_cache(path)

    # -------------------------------------------------------------- staging

    def _guess_metadata(
        self, sub: str, content_type: str | None = None, metadata: dict | None = None
    ) -> dict[str, str]:
        if metadata is not None:
            return metadata
        ct = content_type or mimetypes.guess_type(sub)[0] or "application/octet-stream"
        return {"Content-Type": ct, "Filename": posixpath.basename(sub)}

    @staticmethod
    def _staged_info(name: str, sw: Staged) -> dict:
        """The ``info``/``ls`` view of a staged entry. A link carries its
        reference (so sizes can be filled from the node and reads go
        straight to it); a write carries its buffered size."""
        meta = sw.metadata or {}
        out = {
            "name": name,
            "type": "file",
            "size": sw.size,
            "staged": True,
            "mimetype": meta.get("Content-Type"),
            "metadata": meta,
        }
        if isinstance(sw, StagedLink):
            out["reference"] = sw.reference
        return out

    def _stage_write(self, ref: str, sub: str, sw: Staged) -> None:
        okey = self._origin_of(ref)
        self._staged.setdefault(okey, {})[sub] = sw
        self._staged_rm.get(okey, set()).discard(sub)
        self.invalidate_cache()

    def _stage_rm(self, ref: str, sub: str) -> None:
        okey = self._origin_of(ref)
        self._staged.get(okey, {}).pop(sub, None)
        self._staged_rm.setdefault(okey, set()).add(sub)
        self.invalidate_cache()

    def _unstage(self, ref: str, sub: str) -> None:
        okey = self._origin_of(ref)
        self._staged.get(okey, {}).pop(sub, None)
        self._staged_rm.get(okey, set()).discard(sub)
        self.invalidate_cache()

    async def _stage_path(self, path: str, sw: Staged, commit: bool) -> None:
        """Resolve, stage, optionally commit — used by SwarmFile writes,
        where resolution must happen lazily (feeds resolve asynchronously)."""
        ref, sub = await self._resolve_path(path)
        if not sub:
            raise IsADirectoryError(path)
        self._stage_write(ref, sub, sw)
        if commit:
            await self._commit_root(ref)

    async def _unstage_path(self, path: str) -> None:
        ref, sub = await self._resolve_path(path)
        self._unstage(ref, sub)

    def discard_staged(self) -> None:
        """Drop everything staged and uncommitted, on every lineage."""
        for writes in self._staged.values():
            for sw in writes.values():
                sw.close()
        self._staged.clear()
        self._staged_rm.clear()
        self.invalidate_cache()

    async def _commit_root(self, ref: str) -> str | None:
        """Commit staged operations for the lineage containing ``ref``.

        Returns the new root reference, or None if nothing was staged.
        Serialized under a lock so concurrent writers (e.g. zarr chunk
        uploads) extend one lineage instead of forking it.
        """
        okey = self._origin_of(ref)
        async with self._commit_lock:
            writes = self._staged.pop(okey, {})
            removes = self._staged_rm.pop(okey, set())
            if not writes and not removes:
                return None
            head = self.latest(okey)
            real_root = None if self._is_pseudo(head) else head
            try:
                res = await self._engine.commit(
                    real_root, writes, removes, stamp=self.stamp,
                    **({"act_history": self.act_history} if self.act else {}))
            except BaseException:
                # a failed commit (e.g. no usable stamp) must not lose staged data
                restored = self._staged.setdefault(okey, {})
                for k, v in writes.items():
                    restored.setdefault(k, v)
                self._staged_rm.setdefault(okey, set()).update(removes)
                raise
            if res.new_root != head:
                self._root_map[head] = res.new_root
                self._origin[res.new_root] = okey
            self._adopt_act(res.act_history, res.new_root)
            self.commit_log.append(res)
            await self._after_commit(okey, res)
        self.invalidate_cache()
        return res.new_root

    async def _after_commit(self, okey: str, result: CommitResult) -> None:
        """Hook run (under the commit lock) after each successful commit —
        bzzf:// publishes the feed update here."""

    async def _commit_all(self) -> dict[str, str | None]:
        results = {}
        for okey in set(self._staged) | set(self._staged_rm):
            results[okey] = await self._commit_root(okey)
        return results

    def commit_all(self) -> dict[str, str | None]:
        """Commit everything staged; returns {origin root: new root}."""
        return sync(self.loop, self._commit_all)

    # ------------------------------------------------------------------ info

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        ref, sub = await self._resolve_path(path)
        staged, removed = self._overlay(ref)
        if not sub:
            if not self._is_pseudo(ref):
                backend = await self._get_backend()
                st = await backend.stat(ref, "")
                if st is None:
                    raise FileNotFoundError(path)
            return {"name": path, "type": "directory", "size": 0}
        if sub in staged:
            out = self._staged_info(path, staged[sub])
            if out["size"] is None:
                # a link without an advisory size: ask the node for the span
                await self._fill_sizes([out])
            return out
        if any(s.startswith(sub + "/") for s in staged):
            return {"name": path, "type": "directory", "size": 0}
        if sub in removed or self._is_pseudo(ref):
            raise FileNotFoundError(path)
        backend = await self._get_backend()
        st = await backend.stat(ref, sub)
        if st is None:
            raise FileNotFoundError(path)
        if st.kind == "file":
            assert st.reference is not None
            meta = st.metadata or {}
            return {
                "name": path,
                "type": "file",
                # an index records the size; a trie entry does not, so ask
                "size": (st.size if st.size is not None else
                         await (await self._get_reader()).bytes_size(st.reference.hex())),
                "reference": st.reference.hex(),
                "mimetype": meta.get("Content-Type"),
                "metadata": meta,
            }
        return {"name": path, "type": "directory", "size": 0}

    # -------------------------------------------------------------------- ls

    async def _fill_sizes(self, entries: list[dict]) -> None:
        sem = asyncio.Semaphore(16)

        async def one(e: dict) -> None:
            async with sem:
                try:
                    e["size"] = await (await self._get_reader()).bytes_size(e["reference"])
                except OSError:
                    e["size"] = None

        await asyncio.gather(
            *(
                one(e)
                for e in entries
                if e["type"] == "file" and e["size"] is None and e.get("reference")
            )
        )

    async def _ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if path not in self.dircache:
            ref, sub = await self._resolve_path(path)
            staged, removed = self._overlay(ref)
            by_name: dict[str, dict] = {}
            is_dir = self._is_pseudo(ref)  # pseudo roots are directories-in-progress
            if not self._is_pseudo(ref):
                backend = await self._get_backend()
                res = await backend.list_dir(ref, sub)
                if res is not None:
                    is_dir = True
                    files, dirs = res
                    base = f"{path}/" if path else ""
                    for d in dirs:
                        by_name[f"{base}{d}"] = {
                            "name": f"{base}{d}",
                            "type": "directory",
                            "size": 0,
                        }
                    # a name can be both a file and a directory in a Mantaray
                    # trie; the file entry wins, matching _info
                    for f in files:
                        meta = f.metadata or {}
                        name = f"{base}{f.path.decode('utf-8', 'surrogateescape')}"
                        by_name[name] = {
                            "name": name,
                            "type": "file",
                            "size": f.size,
                            "reference": f.reference.hex(),
                            "mimetype": meta.get("Content-Type"),
                            "metadata": meta,
                        }
            # overlay staged writes and removals
            prefix = f"{sub}/" if sub else ""
            base = f"{path}/" if path else ""
            for s in staged:
                if prefix and not s.startswith(prefix):
                    continue
                rel = s[len(prefix) :]
                is_dir = True
                if "/" in rel:
                    d = rel.split("/", 1)[0]
                    by_name.setdefault(
                        f"{base}{d}", {"name": f"{base}{d}", "type": "directory", "size": 0}
                    )
                else:
                    by_name[f"{base}{rel}"] = self._staged_info(
                        f"{base}{rel}", staged[s])
            for r in removed:
                if (not prefix or r.startswith(prefix)) and "/" not in r[len(prefix) :]:
                    by_name.pop(f"{base}{r[len(prefix):]}", None)
            if not is_dir and not by_name:
                # not a directory — a file (ls of a file lists itself) or
                # nonexistent (_info raises FileNotFoundError)
                self.dircache[path] = [await self._info(path)]
            else:
                entries = [by_name[name] for name in sorted(by_name)]
                await self._fill_sizes(entries)
                self.dircache[path] = entries
        entries = self.dircache[path]
        if detail:
            return entries
        return [e["name"] for e in entries]

    # ------------------------------------------------------------------ find

    async def _find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        path = self._strip_protocol(path)
        ref, sub = await self._resolve_path(path)
        staged, removed = self._overlay(ref)

        def depth_ok(rel: str) -> bool:
            return maxdepth is None or rel.count("/") + 1 <= maxdepth

        base = f"{path}/"
        out: dict[str, dict] = {}
        if not self._is_pseudo(ref):
            backend = await self._get_backend()
            st = await backend.stat(ref, sub)
            if st is None and not staged and not removed:
                raise FileNotFoundError(path)
            if st is not None and st.kind == "file":
                if sub not in removed:
                    out[path] = await self._info(path)
            elif st is not None:
                prefix = f"{sub}/" if sub else ""
                async for e in backend.iter_files(ref, prefix):
                    rel = e.path.decode("utf-8", "surrogateescape")
                    if not depth_ok(rel):
                        continue
                    meta = e.metadata or {}
                    name = base + rel
                    out[name] = {
                        "name": name,
                        "type": "file",
                        "size": e.size,
                        "reference": e.reference.hex(),
                        "mimetype": meta.get("Content-Type"),
                        "metadata": meta,
                    }
        # overlay
        prefix = f"{sub}/" if sub else ""
        for s, sw in staged.items():
            if prefix and not s.startswith(prefix):
                continue
            rel = s[len(prefix) :] if prefix else s
            if not rel or not depth_ok(rel):
                continue
            name = base + rel if rel != sub or prefix else path
            out[name] = self._staged_info(name, sw)
        for r in removed:
            if not prefix or r.startswith(prefix):
                out.pop(base + (r[len(prefix) :] if prefix else r), None)
        if detail:
            await self._fill_sizes(list(out.values()))
        if withdirs:
            dirs: set[str] = set()
            for name in list(out):
                parent = name.rsplit("/", 1)[0]
                while len(parent) > len(path):
                    dirs.add(parent)
                    parent = parent.rsplit("/", 1)[0]
            for d in dirs:
                out[d] = {"name": d, "type": "directory", "size": 0}
        names = sorted(out)
        if detail:
            return {name: out[name] for name in names}
        return names

    # ------------------------------------------------------------------ read

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        ref, sub = await self._resolve_path(path)
        staged, removed = self._overlay(ref)
        if sub in staged:
            sw = staged[sub]
            if isinstance(sw, StagedLink):
                # nothing buffered locally: the content is already on Swarm
                return await (await self._get_reader()).bytes_get(
                    sw.reference, start, end)
            data = sw.payload()
            return data[start or 0 : end if end is not None else len(data)]
        if sub in removed:
            raise FileNotFoundError(path)
        if self._is_pseudo(ref):
            raise FileNotFoundError(path)
        if not sub:
            # bare reference: let Bee resolve the manifest's index document
            await self._setup()
            if self.verify_active:
                from .join import VerificationError

                raise VerificationError(
                    "bare-reference reads resolve server-side (/bzz) and cannot "
                    "be verified — address the file by its explicit path"
                )
            return await self.client.bzz_get(ref, "", start, end, act=self._act_for(ref))
        backend = await self._get_backend()
        st = await backend.stat(ref, sub)
        if st is None:
            raise FileNotFoundError(path)
        if st.kind != "file":
            raise IsADirectoryError(path)
        assert st.reference is not None
        return await (await self._get_reader()).bytes_get(st.reference.hex(), start, end)

    async def _get_file(self, rpath, lpath, **kwargs):
        if await self._isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
            return
        info = await self._info(rpath)
        if info.get("staged"):
            data = await self._cat_file(rpath)
            with open(lpath, "wb") as f:
                f.write(data)
            return
        with open(lpath, "wb") as f:
            async for chunk in (await self._get_reader()).bytes_iter(info["reference"]):
                f.write(chunk)

    # ----------------------------------------------------------------- write

    async def _pipe_file(self, path, value, content_type=None, metadata=None, **kwargs):
        path = self._strip_protocol(path)
        ref, sub = await self._resolve_path(path)
        if not sub:
            raise IsADirectoryError("cannot write the manifest root; give a file path")
        data = bytes(value)
        sw = StagedWrite(
            data=data, size=len(data), metadata=self._guess_metadata(sub, content_type, metadata)
        )
        self._stage_write(ref, sub, sw)
        if not self._intrans:
            await self._commit_root(ref)

    async def _put_file(self, lpath, rpath, content_type=None, **kwargs):
        if os.path.isdir(lpath):
            return
        path = self._strip_protocol(rpath)
        ref, sub = await self._resolve_path(path)
        if not sub:
            raise IsADirectoryError("cannot write the manifest root; give a file path")
        spool = StagedWrite.spooled()
        with open(lpath, "rb") as f:
            shutil.copyfileobj(f, spool)
        sw = StagedWrite(
            data=spool, size=spool.tell(), metadata=self._guess_metadata(sub, content_type)
        )
        self._stage_write(ref, sub, sw)
        if not self._intrans:
            await self._commit_root(ref)

    async def _rm_file(self, path, **kwargs):
        path = self._strip_protocol(path)
        ref, sub = await self._resolve_path(path)
        if not sub:
            raise IsADirectoryError("cannot remove the manifest root")
        staged, removed = self._overlay(ref)
        exists_remote = False
        if not self._is_pseudo(ref):
            backend = await self._get_backend()
            st = await backend.stat(ref, sub)
            if st is not None and st.kind == "directory":
                return  # directories are implicit; they vanish with their files
            exists_remote = st is not None and sub not in removed
        if sub in staged:
            self._unstage(ref, sub)
        elif not exists_remote:
            raise FileNotFoundError(path)
        if exists_remote:
            self._stage_rm(ref, sub)
            if not self._intrans:
                await self._commit_root(ref)

    async def _cp_file(self, path1, path2, **kwargs):
        info = await self._info(path1)
        if info["type"] != "file":
            raise IsADirectoryError(path1)
        data = await self._cat_file(path1)
        meta = info.get("metadata") or {}
        await self._pipe_file(path2, data, content_type=meta.get("Content-Type"))

    async def _mkdir(self, path, create_parents=True, **kwargs):
        pass  # directories are implicit in Mantaray manifests

    async def _makedirs(self, path, exist_ok=False):
        pass

    # ------------------------------------------- distributed writes

    async def _put_blob(self, data, stamp: str | None = None) -> str:
        await self._setup()
        if self.act_mode:
            raise ValueError(
                "put_blob is not available on an ACT instance: a bare blob is "
                "not a root to wrap, and this instance treats every root "
                "reference as protected — a plain reference read back through "
                "it would 404. Protect the manifest root instead (the commit "
                "wraps it) and link plain references into it."
            )
        if self._local is not None:
            payload = data if isinstance(data, bytes) else _read_all(data)
            ref = await asyncio.to_thread(self._local.put, payload)
            if not self._local.has_root(ref):
                # a root of its own (one blob): the usual push/confirm ladder
                # then applies, and fs.sync() is the barrier before the
                # reference is handed to whoever will link it
                try:
                    await asyncio.to_thread(
                        self._local.commit_root, ref, None, [ref])
                except ValueError:
                    # identical content is the same reference: another
                    # put_blob of these bytes journaled it while we waited
                    if not self._local.has_root(ref):
                        raise
            return ref
        if not isinstance(data, bytes):
            data.seek(0)
        batch = await self._engine.stamps.resolve(stamp or self.stamp)
        return await self.client.bytes_post(
            data, batch, pin=self.pin, redundancy=self.redundancy,
            encrypt=self.encrypt,
        )

    def put_blob(self, data, stamp: str | None = None) -> str:
        """Upload one payload and return its **data reference** — no
        manifest, no lineage, no staging.

        The worker-side half of a distributed write
        (docs/distributed-writes.md): each process uploads its own blobs
        through its own node, hands the references to whichever process
        owns the manifest, and that one ``link``s them into a single
        commit. One ``POST /bytes``; the stamp is resolved first (fail
        early), and the instance's ``pin``/``redundancy``/``encrypt``
        policy applies — an ``encrypt=True`` instance returns a 128-hex
        reference, as ``upload`` does.

        ``data`` is bytes or a binary file-like object. Always immediate:
        transactions do not defer it (there is nothing to commit).

        There is no ``content_type`` here — ``POST /bytes`` stores raw
        chunks and Swarm keeps no metadata for them. Content type and
        filename are *manifest* metadata: they are set when the reference
        is linked (guessed from the path, or passed as ``metadata=``).

        In local-first mode the blob lands in the local store and is
        journaled as a root of its own, so it is pushed and confirmed like
        any commit — call ``fs.sync()`` before handing the reference on,
        or the manifest that links it will name content the network does
        not yet hold.
        """
        return sync(self.loop, self._put_blob, data, stamp)

    async def _resolve_stamp(self, stamp: str | None = None) -> str:
        await self._setup()
        if self._local is not None:
            # local-first: commits spend nothing, the push owns postage —
            # so the honest answer is the batch the syncer pushes with
            batch = self._syncer.remote.stamp if self._syncer else None
            if batch:
                return batch
            raise StampError(
                "this local-first instance has no postage batch resolved yet: "
                "commits are offline and the push spends the stamp, so there "
                "is nothing to report until the syncer has run")
        return await self._engine.stamps.resolve(stamp or self.stamp)

    def resolve_stamp(self, stamp: str | None = None) -> str:
        """The postage batch id this instance would spend on a write, picked
        and validated *now* — ``"auto"`` resolved to a concrete batch.

        A write validates its stamp anyway; this exposes the answer, which
        matters when something else has to record it. A dataset assembled
        across several nodes, for instance, lives only as long as the
        shortest-lived batch that stamped a part of it, and only the
        uploading process can say which batch that was (see
        ``swarmfs.dask``). In local-first mode it reports the batch the
        syncer pushes with, since the commit itself spends nothing.
        """
        return sync(self.loop, self._resolve_stamp, stamp)

    async def _link(self, path, reference, size=None, metadata=None) -> None:
        path = self._strip_protocol(path)
        ref, sub = await self._resolve_path(path)
        if not sub:
            raise IsADirectoryError("cannot link the manifest root; give a file path")
        reference = _link_reference(reference)
        check_link_refsize(reference, self.encrypt, sub)
        self._stage_write(ref, sub, StagedLink(
            reference, size, self._guess_metadata(sub, None, metadata)))
        if not self._intrans:
            await self._commit_root(ref)

    def link(self, path, reference: str, *, size: int | None = None,
             metadata: dict[str, str] | None = None) -> None:
        """Stage a manifest entry at ``path`` pointing at an existing
        ``reference`` — the driver-side half of a distributed write.

        Staging, lineage and transaction rules are exactly those of a
        written file: ``bzz://new/…`` starts a fresh manifest, an existing
        root extends that lineage, ``bzzf://`` advances the feed, and
        ``with fs.transaction:`` collects any number of links into one
        commit. The commit has nothing to upload for a link — the content
        is already on Swarm — so it only patches the trie.

        ``size`` is advisory: it lets ``info()``/``ls()`` answer before the
        content is ever read; without it the size is read from the node
        like any other entry. ``metadata`` defaults to the same bee-style
        ``Content-Type``/``Filename`` a written file gets, guessed from
        ``path``.

        The reference is **not** fetched here: linking costs no round trip
        and deliberately does not prove the content is retrievable yet
        (the uploader may still be syncing). Whoever produced the
        reference owns its residency on the network.
        """
        return sync(self.loop, self._link, path, reference, size, metadata)

    # --------------------------------------------- one-shot upload / download

    async def _upload(
        self,
        lpath: str,
        content_type: str | None = None,
        encrypt: bool = False,
        redundancy: int | None = None,
    ) -> str:
        await self._setup()
        lpath = os.path.expanduser(stringify_path(lpath))
        red = self.redundancy if redundancy is None else redundancy
        if red is not None and red not in range(5):
            raise ValueError(f"redundancy must be 0-4, got {red!r}")

        if os.path.isdir(lpath):
            writes: dict[str, StagedWrite] = {}
            for dirpath, _, files in os.walk(lpath):
                for fname in files:
                    full = os.path.join(dirpath, fname)
                    rel = os.path.relpath(full, lpath).replace(os.sep, "/")
                    spool = StagedWrite.spooled()
                    with open(full, "rb") as f:
                        shutil.copyfileobj(f, spool)
                    writes[rel] = StagedWrite(
                        data=spool, size=spool.tell(), metadata=self._guess_metadata(rel)
                    )
            if not writes:
                raise FileNotFoundError(f"{lpath} is an empty directory; nothing to upload")
            engine = self._engine
            enc = encrypt or self.encrypt
            if red != engine.redundancy or enc != engine.encrypt:
                engine = CommitEngine(self.client, engine.stamps, pin=self.pin,
                                      redundancy=red, encrypt=enc, act=self.act,
                                      act_publisher=self.act_publisher)
            res = await engine.commit(
                None, writes, [], stamp=self.stamp,
                **({"act_history": self.act_history} if self.act else {}))
            self._adopt_act(res.act_history, res.new_root)
            self.commit_log.append(res)
            return res.new_root

        # single file: one direct POST /bzz — no manifest construction, no
        # transaction machinery; Bee wraps the file and returns the reference
        batch = await self._engine.stamps.resolve(self.stamp)
        ct = content_type or mimetypes.guess_type(lpath)[0] or "application/octet-stream"
        with open(lpath, "rb") as f:
            res = await self.client.bzz_post(
                f,
                batch,
                filename=os.path.basename(lpath),
                content_type=ct,
                encrypt=encrypt or self.encrypt,
                pin=self.pin,
                redundancy=red,
                act=self.act,
                act_history=self.act_history if self.act else None,
            )
        if isinstance(res, ActUpload):
            self._adopt_act(res.history, res.reference)
            return res.reference
        return res

    def upload(
        self,
        lpath,
        rpath=None,
        recursive: bool = False,
        content_type: str | None = None,
        encrypt: bool = False,
        redundancy: int | None = None,
        **kwargs,
    ) -> str | None:
        """Upload a local file or directory to Swarm; returns the reference.

        The one-liner: ``ref = fs.upload("photo.jpg")``. The postage stamp is
        validated first (fail early), then a single file goes up as one direct
        ``POST /bzz`` and a directory through the commit engine as a fresh
        manifest; either way the new content's reference comes back as the
        return value — on Swarm the destination address is the *result* of a
        write, not its input. Always immediate: transactions don't defer it.

        ``content_type`` overrides the filename-based guess (single file
        only); ``encrypt`` asks Bee to encrypt (single file only; the returned
        128-hex reference includes the decryption key); ``redundancy``
        overrides the instance's erasure-coding level.

        With ``rpath`` given this is fsspec's generic ``upload`` (an alias of
        ``put``, targeting an existing manifest path) and returns None.
        """
        if rpath is not None:
            return self.put(lpath, rpath, recursive=recursive, **kwargs)
        return sync(
            self.loop,
            self._upload,
            lpath,
            content_type=content_type,
            encrypt=encrypt,
            redundancy=redundancy,
        )

    def download(self, rpath, lpath, recursive: bool = False, **kwargs):
        """Download ``bzz://<reference>/<path>`` to a local file — an alias of
        ``get`` (pass ``recursive=True`` for a whole directory). Reads need no
        stamp; with verification active every chunk is BMT-checked."""
        return self.get(rpath, lpath, recursive=recursive, **kwargs)

    # ------------------------------------------------------------------ open

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs,
    ):
        if mode not in ("rb", "wb"):
            raise NotImplementedError(f"mode {mode!r} not supported (only rb/wb)")
        return SwarmFile(
            self,
            path,
            mode=mode,
            block_size=block_size or self.block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )


class SwarmFile(AbstractBufferedFile):
    """File handle: ranged reads against ``/bytes``; buffered, staged writes.

    Reading resolves the path to its data reference once (at open), so every
    ``_fetch_range`` is a direct range request — what makes Parquet predicate
    pushdown and zarr chunk reads viable. Writing buffers to a spooled temp
    file and stages it on close (committing immediately unless inside a
    transaction).
    """

    def __init__(
        self,
        fs: SwarmFileSystem,
        path: str,
        mode: str = "rb",
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        **kwargs,
    ):
        self._content_type = content_type
        self._metadata = metadata
        super().__init__(fs, path, mode=mode, **kwargs)
        if mode == "rb":
            self.reference: str | None = (self.details or {}).get("reference")
            if self.size is None:
                raise OSError(
                    f"could not determine size of {path}; the Bee endpoint at "
                    f"{fs.api_url} answered neither HEAD /bytes nor GET /chunks"
                )
        else:
            self._stripped = fs._strip_protocol(path)
            if not fs._subpath_of(self._stripped):
                raise IsADirectoryError(path)
            self._spool = None

    def _fetch_range(self, start: int, end: int) -> bytes:
        if self.reference:
            return sync(self.fs.loop, self.fs._read_reference, self.reference, start, end)
        return sync(self.fs.loop, self.fs._cat_file, self.path, start, end)

    def _initiate_upload(self):
        self._spool = StagedWrite.spooled()

    def _upload_chunk(self, final=False):
        self.buffer.seek(0)
        shutil.copyfileobj(self.buffer, self._spool)
        if final:
            sw = StagedWrite(
                data=self._spool,
                size=self._spool.tell(),
                metadata=self.fs._guess_metadata(
                    self.fs._subpath_of(self._stripped), self._content_type, self._metadata
                ),
            )
            sync(
                self.fs.loop,
                self.fs._stage_path,
                self._stripped,
                sw,
                self.autocommit and not self.fs._intrans,
            )
        return True

    def commit(self):
        pass  # the lineage-wide commit happens in SwarmTransaction.complete

    def discard(self):
        sync(self.fs.loop, self.fs._unstage_path, self._stripped)


def _stop_local_first(syncer, local) -> None:
    try:
        syncer.stop()
    finally:
        local.close()
