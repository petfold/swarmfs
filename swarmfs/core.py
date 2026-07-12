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
import mimetypes
import os
import posixpath
import shutil
import weakref

from fsspec.asyn import AsyncFileSystem, sync
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.transaction import Transaction
from fsspec.utils import stringify_path

from ._client import DEFAULT_API_URL, SwarmClient
from ._listing import ListingBackend, detect_listing_backend
from .commit import CommitEngine, CommitResult, StagedWrite
from .stamps import StampManager


def _validate_ref(ref: str) -> None:
    if len(ref) not in (64, 128) or any(c not in "0123456789abcdefABCDEF" for c in ref):
        raise ValueError(
            f"invalid swarm reference {ref!r}: expected 64 hex chars "
            "(or 128 for encrypted references), or 'new' for a fresh manifest. "
            "ENS names are not supported yet. To upload new content and get "
            "its reference back, use fs.upload(local_path)."
        )


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
        self.fs = None


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
    allow_gateway:
        Explicitly permit using an endpoint that is not your own node.
        Endpoints where the node-owner API (``/stamps``) is unreachable are
        treated as gateways and refused unless this is set — run a light
        node instead if you can.
    verify:
        Client-side chunk verification (BMT-hash every fetched chunk against
        its reference). Default: on for gateways, off for your own node.
    client:
        Injection seam for a pre-built ``SwarmClient`` (used by tests).
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
        allow_gateway: bool = False,
        verify: bool | None = None,
        client: SwarmClient | None = None,
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
        self.allow_gateway = allow_gateway
        self.verify = verify
        self.verify_active: bool | None = None  # resolved by _setup
        self._reader = None  # client, or a VerifyingReader over it
        self._setup_done = False
        self._backend: ListingBackend | None = None
        self._engine = CommitEngine(
            self.client, StampManager(self.client), pin=pin, redundancy=redundancy
        )
        # staging, keyed by the *origin* root of each manifest lineage
        self._staged: dict[str, dict[str, StagedWrite]] = {}
        self._staged_rm: dict[str, set[str]] = {}
        self._root_map: dict[str, str] = {}  # committed root -> its successor
        self._origin: dict[str, str] = {}  # any root in a lineage -> origin
        self._commit_lock = asyncio.Lock()
        self.commit_log: list[CommitResult] = []
        weakref.finalize(self, self._close_client, self.loop, self.client)

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
        return self._resolve_head(ref), sub.strip("/")

    async def _resolve_path(self, path: str) -> tuple[str, str]:
        """Async seam over _split_ref — bzzf:// overrides this with a feed
        lookup, which needs I/O."""
        return self._split_ref(path)

    def _subpath_of(self, path: str) -> str:
        """The within-manifest part of a stripped path (syntactic only)."""
        return path.partition("/")[2].strip("/")

    def _origin_of(self, ref: str) -> str:
        return self._origin.get(ref, ref)

    def _overlay(self, ref: str) -> tuple[dict[str, StagedWrite], set[str]]:
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
        except aiohttp.ClientConnectionError as e:
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
        self.trusted = trusted
        self.verify_active = self.verify if self.verify is not None else not trusted
        if self.verify_active:
            from .join import VerifyingReader

            self._reader = VerifyingReader(self.client)
        else:
            self._reader = self.client
        self._setup_done = True

    async def _get_reader(self):
        await self._setup()
        return self._reader

    async def _read_reference(self, ref: str, start=None, end=None) -> bytes:
        return await (await self._get_reader()).bytes_get(ref, start, end)

    async def _get_backend(self) -> ListingBackend:
        if self._backend is None:
            await self._setup()
            self._backend = await detect_listing_backend(self._reader)
        return self._backend

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

    def _stage_write(self, ref: str, sub: str, sw: StagedWrite) -> None:
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

    async def _stage_path(self, path: str, sw: StagedWrite, commit: bool) -> None:
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
                res = await self._engine.commit(real_root, writes, removes, stamp=self.stamp)
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
            sw = staged[sub]
            meta = sw.metadata or {}
            return {
                "name": path,
                "type": "file",
                "size": sw.size,
                "staged": True,
                "mimetype": meta.get("Content-Type"),
                "metadata": meta,
            }
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
                "size": await (await self._get_reader()).bytes_size(st.reference.hex()),
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
                            "size": None,
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
                    sw = staged[s]
                    meta = sw.metadata or {}
                    by_name[f"{base}{rel}"] = {
                        "name": f"{base}{rel}",
                        "type": "file",
                        "size": sw.size,
                        "staged": True,
                        "mimetype": meta.get("Content-Type"),
                        "metadata": meta,
                    }
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
                        "size": None,
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
            meta = sw.metadata or {}
            name = base + rel if rel != sub or prefix else path
            out[name] = {
                "name": name,
                "type": "file",
                "size": sw.size,
                "staged": True,
                "mimetype": meta.get("Content-Type"),
                "metadata": meta,
            }
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
            data = staged[sub].payload()
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
            return await self.client.bzz_get(ref, "", start, end)
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
            if encrypt:
                raise NotImplementedError(
                    "encrypt=True is only supported for single-file uploads; "
                    "directory manifests are built client-side, unencrypted"
                )
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
            if red != engine.redundancy:
                engine = CommitEngine(self.client, engine.stamps, pin=self.pin, redundancy=red)
            res = await engine.commit(None, writes, [], stamp=self.stamp)
            self.commit_log.append(res)
            return res.new_root

        # single file: one direct POST /bzz — no manifest construction, no
        # transaction machinery; Bee wraps the file and returns the reference
        batch = await self._engine.stamps.resolve(self.stamp)
        ct = content_type or mimetypes.guess_type(lpath)[0] or "application/octet-stream"
        with open(lpath, "rb") as f:
            return await self.client.bzz_post(
                f,
                batch,
                filename=os.path.basename(lpath),
                content_type=ct,
                encrypt=encrypt,
                pin=self.pin,
                redundancy=red,
            )

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
