"""Thin async wrapper over the Bee HTTP API endpoints swarmfs needs,
plus its blocking twin for code that doesn't use asyncio."""

from __future__ import annotations

import functools
import os
import weakref

import aiohttp
from fsspec.asyn import get_loop, sync

from .exceptions import BeeAPIError, BeePermissionError, StampError

DEFAULT_API_URL = "http://localhost:1633"


class SwarmClient:
    """Async wrapper over the Bee HTTP API — the middle tier of swarmfs's API.

    Use this when you want direct programmatic calls against Bee (upload a
    blob, fetch bytes, post a feed update) without filesystem semantics or
    the fsspec machinery. ``SwarmFileSystem`` builds everything on top of it,
    one instance per filesystem; the aiohttp session is created lazily on
    the calling event loop and closed via ``close()`` (the filesystem does
    this from its finalizer).

    The endpoint resolves as: explicit ``api_url`` → the ``BEE_API_URL``
    environment variable → ``http://localhost:1633``.
    """

    def __init__(
        self,
        api_url: str | None = None,
        timeout: float = 120,
        headers: dict[str, str] | None = None,
    ):
        api_url = api_url or os.environ.get("BEE_API_URL") or DEFAULT_API_URL
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
        if resp.status == 402:
            if "overissued" in detail:
                # bee's ErrBucketFull: a chunk hashed into a bucket already at
                # 2**(depth-bucket_depth). The batch is NOT lost — what it has
                # stamped stays paid for — and diluting preserves the bucket
                # counters, so +1 depth doubles every bucket and the same
                # upload then succeeds with the same root (addressing is
                # deterministic). See stamps.BucketStats for the true occupancy.
                raise StampError(
                    f"Bee API 402 for {what}: {detail} — the batch is full in "
                    "at least one bucket, so this chunk cannot be stamped. "
                    "Nothing already stored is lost. Recover by diluting one "
                    "depth (PATCH /stamps/dilute/{batch}/{depth+1}, which "
                    "doubles every bucket's capacity) and retrying — then top "
                    "up, since dilution halves the remaining TTL. "
                    "GET /stamps/{batch}/buckets shows the real headroom."
                )
            raise StampError(
                f"Bee API 402 (payment required) for {what}: {detail} — the "
                "endpoint did not accept the postage stamp; check your batches "
                "with GET /stamps, or buy one (`swarm-cli stamp buy`)"
            )
        if resp.status in (401, 403):
            raise BeePermissionError(resp.status, what, detail)
        raise BeeAPIError(resp.status, what, detail)

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
        if len(ref) == 128:
            # Encrypted reference: the root chunk's span bytes are
            # ciphertext (meaningless client-side) and HEAD /bytes 404s on
            # encrypted refs (measured on Bee 2.8.1) — but a 1-byte ranged
            # GET decrypts and Content-Range carries the plaintext total.
            url = f"{self.api_url}/bytes/{ref}"
            headers = {"Range": "bytes=0-0"}
            async with session.get(url, headers=headers) as resp:
                await self._raise_for_status(resp, url)
                await resp.read()
                content_range = resp.headers.get("Content-Range", "")
                if "/" in content_range:
                    try:
                        return int(content_range.rsplit("/", 1)[1])
                    except ValueError:
                        pass
            return None
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
        deferred: bool | None = None,
        encrypt: bool = False,
    ) -> str:
        """POST /bytes — upload a blob, returns its reference (hex).

        ``redundancy`` is Bee's erasure-coding level (0–4): parity chunks are
        added to multi-chunk trees so content survives missing chunks.
        ``deferred`` sets ``swarm-deferred-upload``: True stores on the node
        and syncs in the background, False pushes straight to the network
        before returning (slower, but the 201 then means the network has
        it); None leaves the node's default. With ``encrypt`` the node
        encrypts chunk-by-chunk and the returned reference is 128 hex —
        address plus decryption key; whoever holds the full reference can
        read, everyone else stores noise.
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
        if deferred is not None:
            headers["swarm-deferred-upload"] = "true" if deferred else "false"
        if encrypt:
            headers["swarm-encrypt"] = "true"
        session = await self._get_session()
        async with session.post(url, data=data, headers=headers) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json())["reference"]

    async def bzz_post(
        self,
        data,
        stamp: str,
        filename: str | None = None,
        content_type: str | None = None,
        encrypt: bool = False,
        pin: bool = False,
        redundancy: int | None = None,
    ) -> str:
        """POST /bzz — upload a single file, returns its reference (hex).

        Bee wraps the file in a manifest with the filename as its index
        document, so both ``/bzz/{ref}/`` and ``/bzz/{ref}/{filename}``
        resolve to it. ``data`` may be bytes or a (binary) file object,
        which aiohttp streams. With ``encrypt`` the returned reference is
        128 hex chars (reference + decryption key).
        """
        url = f"{self.api_url}/bzz"
        params = {"name": filename} if filename else {}
        headers = {
            "swarm-postage-batch-id": stamp,
            "content-type": content_type or "application/octet-stream",
        }
        if encrypt:
            headers["swarm-encrypt"] = "true"
        if pin:
            headers["swarm-pin"] = "true"
        if redundancy is not None:
            headers["swarm-redundancy-level"] = str(redundancy)
        session = await self._get_session()
        async with session.post(url, data=data, params=params, headers=headers) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json())["reference"]

    async def stamps_list(self) -> list[dict]:
        """GET /stamps — the node's postage batches."""
        url = f"{self.api_url}/stamps"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json()).get("stamps") or []

    async def stamp_get(self, batch_id: str) -> dict:
        """GET /stamps/{id} — one batch's state (400/404s while a fresh
        purchase's transaction is still confirming)."""
        url = f"{self.api_url}/stamps/{batch_id}"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.json()

    async def stamp_buy(self, amount: int, depth: int) -> str:
        """POST /stamps/{amount}/{depth} — buy a postage batch with the
        node wallet's xBZZ. Returns the batch id as soon as the purchase
        transaction is submitted (NOT yet usable — poll ``stamp_get``)."""
        url = f"{self.api_url}/stamps/{amount}/{depth}"
        session = await self._get_session()
        async with session.post(url) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json())["batchID"]

    async def stamp_buckets(self, batch_id: str) -> dict:
        """GET /stamps/{id}/buckets — the batch's per-bucket occupancy.

        Returns ``depth``/``bucketDepth``/``bucketUpperBound`` plus a
        ``buckets`` list of all 65536 counters. This is the ground truth for
        how much a batch can still take: an upload fails when a chunk hashes
        into a bucket already at the upper bound, which the summary
        ``utilizationRatio`` only approximates. The response is ~2 MB.
        """
        url = f"{self.api_url}/stamps/{batch_id}/buckets"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.json()

    async def stamp_topup(self, batch_id: str, added_amount: int) -> str:
        """PATCH /stamps/topup/{id}/{amount} — extend a batch's life with
        the node wallet's xBZZ. ``added_amount`` is per chunk and ADDS to
        the batch's remaining balance (it does not restart it). Returns the
        transaction hash as soon as the tx is submitted; the batch's own
        ``amount`` only changes once the node indexes the chain event, so
        poll ``stamp_get`` rather than trusting an immediate read."""
        url = f"{self.api_url}/stamps/topup/{batch_id}/{added_amount}"
        session = await self._get_session()
        async with session.patch(url) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json()).get("txHash", "")

    async def stamp_dilute(self, batch_id: str, depth: int) -> str:
        """PATCH /stamps/dilute/{id}/{depth} — raise a batch's depth so it
        holds more chunks. Costs gas only, but the same balance now covers
        twice the chunks per depth step, so the remaining TTL is roughly
        halved each step. Returns the transaction hash; poll ``stamp_get``
        for the new depth."""
        url = f"{self.api_url}/stamps/dilute/{batch_id}/{depth}"
        session = await self._get_session()
        async with session.patch(url) as resp:
            await self._raise_for_status(resp, url)
            return (await resp.json()).get("txHash", "")

    async def chainstate(self) -> dict:
        """GET /chainstate — currentPrice, minimumValidityBlocks, etc."""
        url = f"{self.api_url}/chainstate"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.json()

    async def wallet(self) -> dict:
        """GET /wallet — the node wallet's xBZZ (``bzzBalance``, in plur)
        and xDAI (``nativeTokenBalance``, for gas)."""
        url = f"{self.api_url}/wallet"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return await resp.json()

    async def stewardship_get(self, ref: str) -> bool:
        """GET /stewardship/{ref} — the node's claim that the reference is
        retrievable from the network. A claim, not a proof: for the
        trust-tiered confirmation policy see swarmfs.localsync."""
        url = f"{self.api_url}/stewardship/{ref}"
        session = await self._get_session()
        async with session.get(url) as resp:
            await self._raise_for_status(resp, url)
            return bool((await resp.json()).get("isRetrievable"))

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


# ------------------------------------------------------------- sync facade


def _close_quietly(loop, client: SwarmClient) -> None:
    if loop is not None and loop.is_running():
        try:
            sync(loop, client.close, timeout=0.1)
        except Exception:
            pass  # interpreter shutdown; the daemon loop dies with it


class SyncSwarmClient:
    """Blocking twin of ``SwarmClient``, for code that doesn't use asyncio.

    Same endpoint resolution and the same methods (generated below, one per
    ``SwarmClient`` coroutine, same signatures); each call runs on fsspec's
    shared background event loop and blocks for the result. From async code
    use ``SwarmClient`` directly — fsspec's ``sync`` raises rather than
    deadlocks if called on the loop's own thread.

    Usable as a context manager (``with SyncSwarmClient() as client:``);
    otherwise the HTTP session is closed by a finalizer.
    """

    def __init__(
        self,
        api_url: str | None = None,
        timeout: float = 120,
        headers: dict[str, str] | None = None,
        client: SwarmClient | None = None,
    ):
        self._client = client or SwarmClient(api_url, timeout=timeout, headers=headers)
        self.api_url = self._client.api_url
        self.loop = get_loop()
        weakref.finalize(self, _close_quietly, self.loop, self._client)

    def __enter__(self) -> "SyncSwarmClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        """Blocking ``SwarmClient.close`` — close the HTTP session."""
        sync(self.loop, self._client.close)

    def bytes_iter(self, ref: str, chunk_size: int = 1 << 20):
        """Blocking iterator over ``SwarmClient.bytes_iter`` chunks."""
        ait = self._client.bytes_iter(ref, chunk_size).__aiter__()
        while True:
            try:
                yield sync(self.loop, ait.__anext__)
            except StopAsyncIteration:
                return


def _sync_method(name: str):
    coro = getattr(SwarmClient, name)

    @functools.wraps(coro)
    def method(self, *args, **kwargs):
        return sync(self.loop, getattr(self._client, name), *args, **kwargs)

    method.__doc__ = f"Blocking ``SwarmClient.{name}``. {coro.__doc__ or ''}"
    return method


for _name in (
    "health",
    "bytes_get",
    "bytes_size",
    "bzz_get",
    "bytes_post",
    "bzz_post",
    "stamps_list",
    "stamp_get",
    "stamp_buy",
    "stamp_buckets",
    "stamp_topup",
    "stamp_dilute",
    "chainstate",
    "wallet",
    "stewardship_get",
    "tag_create",
    "tag_get",
    "feed_head",
    "chunk_get",
    "soc_post",
):
    setattr(SyncSwarmClient, _name, _sync_method(_name))
del _name
