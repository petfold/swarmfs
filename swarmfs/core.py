"""SwarmFileSystem: fsspec AsyncFileSystem over the Bee HTTP API.

Paths look like ``bzz://<64-or-128-hex-reference>/<path-inside-manifest>``;
the reference plays the role of a bucket. v0 is read-only — writes arrive
with the v1 transactional commit engine.
"""

from __future__ import annotations

import asyncio
import os
import weakref

from fsspec.asyn import AsyncFileSystem, sync
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import stringify_path

from ._client import DEFAULT_API_URL, SwarmClient
from ._listing import ListingBackend, detect_listing_backend

_WRITES_MSG = "swarmfs is read-only for now; writes arrive with the v1 commit engine"


def _validate_ref(ref: str) -> None:
    if len(ref) not in (64, 128) or any(c not in "0123456789abcdefABCDEF" for c in ref):
        raise ValueError(
            f"invalid swarm reference {ref!r}: expected 64 hex chars "
            "(or 128 for encrypted references). ENS names are not supported yet."
        )


class SwarmFileSystem(AsyncFileSystem):
    """Read access to Swarm content via a Bee node or public gateway.

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
    client:
        Injection seam for a pre-built ``SwarmClient`` (used by tests).
    """

    protocol = "bzz"
    root_marker = ""

    def __init__(
        self,
        api_url: str | None = None,
        block_size: int | None = None,
        timeout: float = 120,
        headers: dict[str, str] | None = None,
        client: SwarmClient | None = None,
        asynchronous: bool = False,
        loop=None,
        **storage_options,
    ):
        super().__init__(asynchronous=asynchronous, loop=loop, **storage_options)
        self.api_url = api_url or os.environ.get("BEE_API_URL", DEFAULT_API_URL)
        self.client = client or SwarmClient(self.api_url, timeout=timeout, headers=headers)
        self.block_size = block_size or 2**20
        self._backend: ListingBackend | None = None
        weakref.finalize(self, self._close_client, self.loop, self.client)

    @staticmethod
    def _close_client(loop, client: SwarmClient) -> None:
        if loop is not None and loop.is_running():
            try:
                sync(loop, client.close, timeout=0.1)
            except (TimeoutError, FSTimeoutError, NotImplementedError, RuntimeError):
                pass

    @classmethod
    def _strip_protocol(cls, path) -> str:
        path = stringify_path(path)
        for prefix in (f"{cls.protocol}://", f"{cls.protocol}:"):
            if path.startswith(prefix):
                path = path[len(prefix) :]
                break
        return path.strip("/")

    @staticmethod
    def _split_ref(path: str) -> tuple[str, str]:
        ref, _, sub = path.partition("/")
        _validate_ref(ref)
        return ref, sub.strip("/")

    async def _get_backend(self) -> ListingBackend:
        if self._backend is None:
            self._backend = await detect_listing_backend(self.client)
        return self._backend

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            self.dircache.pop(path, None)
        super().invalidate_cache(path)

    # ------------------------------------------------------------------ info

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        ref, sub = self._split_ref(path)
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
                "size": await self.client.bytes_size(st.reference.hex()),
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
                    e["size"] = await self.client.bytes_size(e["reference"])
                except OSError:
                    e["size"] = None

        await asyncio.gather(*(one(e) for e in entries if e["type"] == "file"))

    async def _ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if path not in self.dircache:
            ref, sub = self._split_ref(path)
            backend = await self._get_backend()
            res = await backend.list_dir(ref, sub)
            if res is None:
                # not a directory — a file (fsspec: ls of a file lists itself)
                # or nonexistent (_info raises FileNotFoundError)
                self.dircache[path] = [await self._info(path)]
            else:
                files, dirs = res
                base = f"{path}/" if path else ""
                # a name can be both a file and a directory in a Mantaray trie;
                # keep the file entry (file wins, matching _info)
                by_name = {
                    f"{base}{d}": {"name": f"{base}{d}", "type": "directory", "size": 0}
                    for d in dirs
                }
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
        ref, sub = self._split_ref(path)
        backend = await self._get_backend()

        base = f"{path}/"
        out: dict[str, dict] = {}
        st = await backend.stat(ref, sub)
        if st is None:
            raise FileNotFoundError(path)
        if st.kind == "file":
            out[path] = await self._info(path)
        else:
            prefix = f"{sub}/" if sub else ""
            async for e in backend.iter_files(ref, prefix):
                rel = e.path.decode("utf-8", "surrogateescape")
                if maxdepth is not None and rel.count("/") + 1 > maxdepth:
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
            if detail:
                await self._fill_sizes(list(out.values()))
            if withdirs:
                dirs: set[str] = set()
                for name in out:
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
        ref, sub = self._split_ref(path)
        if not sub:
            # bare reference: let Bee resolve the manifest's index document
            return await self.client.bzz_get(ref, "", start, end)
        backend = await self._get_backend()
        st = await backend.stat(ref, sub)
        if st is None:
            raise FileNotFoundError(path)
        if st.kind != "file":
            raise IsADirectoryError(path)
        assert st.reference is not None
        return await self.client.bytes_get(st.reference.hex(), start, end)

    async def _get_file(self, rpath, lpath, **kwargs):
        if await self._isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
            return
        info = await self._info(rpath)
        with open(lpath, "wb") as f:
            async for chunk in self.client.bytes_iter(info["reference"]):
                f.write(chunk)

    # ----------------------------------------------------------------- write

    async def _pipe_file(self, path, value, **kwargs):
        raise NotImplementedError(_WRITES_MSG)

    async def _put_file(self, lpath, rpath, **kwargs):
        raise NotImplementedError(_WRITES_MSG)

    async def _rm_file(self, path, **kwargs):
        raise NotImplementedError(_WRITES_MSG)

    async def _mkdir(self, path, **kwargs):
        raise NotImplementedError(_WRITES_MSG)

    async def _makedirs(self, path, exist_ok=False):
        raise NotImplementedError(_WRITES_MSG)

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
        if mode != "rb":
            raise NotImplementedError(_WRITES_MSG)
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
    """Read-only file handle with range-based fetching.

    The path is resolved to its data reference once (at open, via ``info``);
    every ``_fetch_range`` is then a direct ``/bytes`` range request, which is
    what makes Parquet predicate pushdown and zarr chunk reads viable.
    """

    def __init__(self, fs: SwarmFileSystem, path: str, mode: str = "rb", **kwargs):
        super().__init__(fs, path, mode=mode, **kwargs)
        self.reference: str | None = (self.details or {}).get("reference")
        if self.size is None:
            raise OSError(
                f"could not determine size of {path}; the Bee endpoint at "
                f"{fs.api_url} answered neither HEAD /bytes nor GET /chunks"
            )

    def _fetch_range(self, start: int, end: int) -> bytes:
        if self.reference:
            return sync(self.fs.loop, self.fs.client.bytes_get, self.reference, start, end)
        return sync(self.fs.loop, self.fs._cat_file, self.path, start, end)

    def _initiate_upload(self):
        raise NotImplementedError(_WRITES_MSG)

    def _upload_chunk(self, final=False):
        raise NotImplementedError(_WRITES_MSG)
