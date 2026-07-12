"""Thin async wrapper over the Bee HTTP API endpoints swarmfs needs."""

from __future__ import annotations

import aiohttp

DEFAULT_API_URL = "http://localhost:1633"


class SwarmClient:
    """One instance per filesystem; the aiohttp session is created lazily on
    the filesystem's event loop and closed via the filesystem's finalizer."""

    def __init__(
        self,
        api_url: str = DEFAULT_API_URL,
        timeout: float = 120,
        headers: dict[str, str] | None = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.headers = headers or {}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers=self.headers,
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _range_header(start: int | None, end: int | None) -> dict[str, str]:
        """fsspec's half-open [start, end) -> inclusive HTTP Range."""
        if start is None and end is None:
            return {}
        start = start or 0
        if end is None:
            return {"Range": f"bytes={start}-"}
        return {"Range": f"bytes={start}-{end - 1}"}

    async def _raise_for_status(self, resp: aiohttp.ClientResponse, what: str) -> None:
        if resp.status < 400:
            return
        detail = ""
        try:
            detail = (await resp.text())[:200]
        except Exception:
            pass
        if resp.status == 404:
            raise FileNotFoundError(what)
        if resp.status in (401, 402, 403):
            raise PermissionError(f"Bee API {resp.status} for {what}: {detail}")
        raise OSError(f"Bee API {resp.status} for {what}: {detail}")

    async def bytes_get(
        self, ref: str, start: int | None = None, end: int | None = None
    ) -> bytes:
        """GET /bytes/{ref}, optionally a byte range (end exclusive)."""
        if start is not None and end is not None and end <= start:
            return b""
        url = f"{self.api_url}/bytes/{ref}"
        headers = self._range_header(start, end)
        session = await self._get_session()
        async with session.get(url, headers=headers) as resp:
            if resp.status == 416:  # range beyond EOF
                return b""
            await self._raise_for_status(resp, url)
            data = await resp.read()
        if headers and resp.status == 200:
            # server ignored the Range header; slice locally
            data = data[start or 0 : end]
        return data

    @staticmethod
    def _decode_span(span: bytes) -> int:
        # bee encodes the erasure-coding redundancy level in the span's most
        # significant byte (pkg/file/redundancy/span.go): span[7] > 128 means
        # the top byte is `level | 0x80` and the real length is span[:7].
        if span[7] > 128:
            span = span[:7] + b"\x00"
        return int.from_bytes(span, "little")

    async def bytes_size(self, ref: str) -> int | None:
        """Size of the data at ``ref`` without downloading it.

        Reads the root chunk's 8-byte span via /chunks (the span of a root
        chunk is the total content length). Falls back to HEAD /bytes — but
        only as a fallback: Bee (≤2.8.x at least) puts the *raw* span,
        redundancy bits included, in that Content-Length header, which can
        come out negative and make HTTP clients reject the response outright.
        Returns None if neither works (e.g. a restrictive gateway).
        """
        session = await self._get_session()
        try:
            async with session.get(f"{self.api_url}/chunks/{ref}") as resp:
                if resp.status < 400:
                    chunk = await resp.read()
                    if len(chunk) >= 8:
                        return self._decode_span(chunk[:8])
        except (aiohttp.ClientError, OSError):
            pass
        url = f"{self.api_url}/bytes/{ref}"
        try:
            async with session.head(url) as resp:
                await self._raise_for_status(resp, url)
                length = int(resp.headers.get("Content-Length", -1))
                if length >= 0:
                    return length
        except FileNotFoundError:
            raise
        except (aiohttp.ClientError, OSError, ValueError):
            pass
        return None

    async def bzz_get(
        self, ref: str, path: str = "", start: int | None = None, end: int | None = None
    ) -> bytes:
        """GET /bzz/{ref}/{path} — server-side path resolution (follows the
        manifest's index document when path is empty)."""
        if start is not None and end is not None and end <= start:
            return b""
        url = f"{self.api_url}/bzz/{ref}/{path}"
        headers = self._range_header(start, end)
        session = await self._get_session()
        async with session.get(url, headers=headers) as resp:
            if resp.status == 416:
                return b""
            await self._raise_for_status(resp, url)
            data = await resp.read()
        if headers and resp.status == 200:
            data = data[start or 0 : end]
        return data

    async def bytes_iter(self, ref: str, chunk_size: int = 1 << 20):
        """Stream /bytes/{ref} in chunks (for downloads to local files)."""
        url = f"{self.api_url}/bytes/{ref}"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            async for chunk in resp.content.iter_chunked(chunk_size):
                yield chunk

    # ------------------------------------------------------------ write side

    async def bytes_post(
        self,
        data: bytes,
        stamp: str,
        tag: int | None = None,
        pin: bool = False,
        redundancy: int | None = None,
    ) -> str:
        """POST /bytes — upload a blob, returns its reference (hex).

        ``redundancy`` is Bee's erasure-coding level (0–4): parity chunks are
        added to multi-chunk trees so content survives missing chunks.
        """
        url = f"{self.api_url}/bytes"
        headers = {
            "swarm-postage-batch-id": stamp,
            "content-type": "application/octet-stream",
        }
        if tag is not None:
            headers["swarm-tag"] = str(tag)
        if pin:
            headers["swarm-pin"] = "true"
        if redundancy is not None:
            headers["swarm-redundancy-level"] = str(redundancy)
        session = await self._get_session()
        async with session.post(url, data=data, headers=headers) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json())["reference"]

    async def stamps_list(self) -> list[dict]:
        """GET /stamps — the node's postage batches."""
        url = f"{self.api_url}/stamps"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json()).get("stamps") or []

    async def tag_create(self) -> int:
        """POST /tags — a tag uid for tracking upload progress."""
        url = f"{self.api_url}/tags"
        session = await self._get_session()
        async with session.post(url, json={}) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json())["uid"]

    async def tag_get(self, uid: int) -> dict:
        url = f"{self.api_url}/tags/{uid}"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.json()

    # ------------------------------------------------------------ feeds/SOC

    async def feed_head(self, owner: str, topic: str) -> tuple[str, str] | None:
        """Current (index, next index) of a sequence feed, as hex strings from
        the Swarm-Feed-Index headers; None if the feed has no updates yet.

        Sends Swarm-Only-Root-Chunk so Bee doesn't stream the resolved
        content — only the headers matter here.
        """
        url = f"{self.api_url}/feeds/{owner}/{topic}?type=sequence"
        session = await self._get_session()
        async with session.get(url, headers={"Swarm-Only-Root-Chunk": "true"}) as resp:
            if resp.status == 404:
                return None
            await self._raise_for_status(resp, url)
            index = resp.headers.get("Swarm-Feed-Index")
            next_index = resp.headers.get("Swarm-Feed-Index-Next", "")
            if not index:
                return None
            return index, next_index

    async def chunk_get(self, ref: str) -> bytes:
        """GET /chunks/{ref} — one raw chunk (span+payload; SOCs include
        identifier and signature)."""
        url = f"{self.api_url}/chunks/{ref}"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.read()

    async def soc_post(
        self, owner: str, identifier: str, signature: str, data: bytes, stamp: str
    ) -> str:
        """POST /soc/{owner}/{identifier}?sig=… — upload a single-owner chunk
        (the node verifies the signature). Body is the wrapped chunk data."""
        url = f"{self.api_url}/soc/{owner}/{identifier}?sig={signature}"
        headers = {
            "swarm-postage-batch-id": stamp,
            "content-type": "application/octet-stream",
        }
        session = await self._get_session()
        async with session.post(url, data=data, headers=headers) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json())["reference"]

    async def health(self) -> dict:
        url = f"{self.api_url}/health"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.json()
