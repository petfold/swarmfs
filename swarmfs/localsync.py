"""The network half of the local-first store: push worker + confirmation.

L1 of the design (``docs/localstore-design.md``): a `Syncer` watches a
`LocalStore` and climbs its roots up the durability ladder against a
`BeeRemote` — *committed → pushed (on-node) → network-confirmed* — entirely
in the background. `commit_root` stays local-fast; certainty is on demand
(`Syncer.sync()`, `LocalStore.wait_for`); every ladder event is appended to
the journal only after the fact it records is true (the lag rule), so a
crash at any point recovers by re-pushing idempotently — content-addressed
re-uploads are deduped by the node, and re-stamping the same chunk on the
same batch costs no bucket slot.

Trust tiering (design doc, *Verification and trust*):

- the push response is only ever a node claim, so it promotes a root to
  *pushed* and no further;
- *confirmed* — the rung that permits eviction — is p2p-native: the
  stewardship check IS a network check (verified in the Bee source:
  ``steward.IsRetrievable`` bypasses the local store and retrieves every
  chunk through the retrieval protocol from proximity-selected peers —
  ``pkg/steward/steward.go``, ``pkg/retrieval/retrieval.go``), plus a
  sample of the root's blobs fetched back and hashed against their refs
  (integrity). `confirm_sample=0` skips the hash sample and leans on
  stewardship alone.
- every upload asserts the node returned the locally computed reference
  (free: the ref is the blob's filename) — the tripwire for the
  erasure-coding address-space fork.
- a blob the network cannot retrieve is *repaired*, not just waited for:
  the node's push-sync can count a chunk delivered on a "shallow receipt"
  from a peer too far from the chunk's neighbourhood to keep it, and then
  never retries it (seen twice on Gnosis mainnet: 29,602 shallow receipts
  in 2026-09, 36,589 of 372,313 pushes on 2026-10-07, both with the
  pusher reporting everything synced). When a sampled blob is not
  retrievable, every blob of that root is checked, each missing one is
  pushed again from the local copy as a direct (non-deferred) upload, and
  the next round checks again. Only missing blobs are resent.

Push triggers are the WAL-checkpoint trio (design doc, *Auto-push policy*):
debounce, max staleness, pinned-bytes threshold; `sync()` and budget
pressure fire immediately. Offline, the worker backs off exponentially and
keeps the store fully usable — that is the point of local-first.
"""

from __future__ import annotations

import math
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

from fsspec.asyn import sync as _run_sync

from ._client import SyncSwarmClient
from .localstore import (
    BlobVerificationFailed,
    COMMITTED,
    CONFIRMED,
    LocalStore,
    PUSHED,
)
from .stamps import StampManager


#: The smallest sample `confirm_sample` takes of a root's blobs. A lost
#: blob slips past a sample of k with probability (1 - p)**k when a
#: fraction p of the root was lost; at the shallow-receipt rate seen twice
#: on a light node (about one in ten) that is 90% for one blob and 19% for
#: sixteen. A root this small or smaller is checked whole.
MIN_CONFIRM_SAMPLE = 16

#: How many confirmation checks run at once (`SyncPolicy.check_concurrency`).
#: Measured 2026-10-07 against a Bee 2.8.2 light node behind NAT with ~140
#: peers: a stewardship check takes about 1.2 s, and throughput grows almost
#: linearly to 16 at once (12 checks/s) and still well to 32 (20/s), then
#: flattens (64: 23/s, 128: 26-33/s) while each check waits 2-4 s. Reads of
#: chunks the node must fetch behave alike: about 4/s per request in flight
#: up to 32. The best number depends on the node more than on the machine
#: running this (about 1 ms of client CPU per request);
#: `scripts/concurrency_sweep.py` measures it for yours.
CHECK_CONCURRENCY = 32

#: How many uploads run at once (`SyncPolicy.push_concurrency`): the
#: worker's pushes and its repairs. Measured 2026-10-08, same node: deferred
#: uploads (the worker's kind: the node stores, then pushes on its own) go
#: 249/s one at a time, 609/s at 4, 869/s at 16, 914/s at 32 and 896/s at
#: 64, where the node's local work has levelled off; direct uploads (repairs: the request
#: returns once the network has the chunk, ~300 ms) go 2.6/s one at a time,
#: 34/s at 16, 64/s at 32 and 85/s at 64. 32 serves both.
PUSH_CONCURRENCY = 32


@dataclass
class SyncPolicy:
    """When the worker pushes, and how confirmation verifies.

    The three triggers each bound a different risk — debounce coalesces
    bursts (request overhead), `max_staleness` bounds how long any commit
    exists on one disk, `pinned_bytes_limit` bounds the size of a possible
    loss and relieves the budget (None: a quarter of the store's budget
    when one is set, else disabled). `confirm_sample` is the fraction of a
    root's blobs retrieve-and-verified before it is confirmed (when > 0,
    never fewer than `MIN_CONFIRM_SAMPLE` blobs, or the whole root if it
    is smaller); 0 trusts the node's stewardship claim alone — a
    deliberate weakening, reported by `Syncer.trusting_node_claims`.
    `check_concurrency` is how many of those network checks (fetches and
    stewardship calls) run at once: each takes up to a second on a light
    node, almost all of it waiting, so one at a time made confirming a
    large root take most of an hour. `push_concurrency` is the same for
    uploads, the worker's pushes and its repairs.
    """
    debounce: float = 10.0
    max_staleness: float = 300.0
    pinned_bytes_limit: Optional[int] = None
    confirm_sample: float = 0.25
    direct_upload: bool = False
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    check_concurrency: int = CHECK_CONCURRENCY
    push_concurrency: int = PUSH_CONCURRENCY


class BeeRemote:
    """The Swarm side of a sync, over the client tier.

    Thin by design: upload one blob (asserting the returned reference),
    fetch one blob, ask stewardship, report the batch's TTL. Everything
    policy-shaped lives in `Syncer`; everything endpoint-shaped in
    `SwarmClient`.
    """

    def __init__(self, api_url: Optional[str] = None,
                 stamp: Optional[str] = "auto",
                 client: Optional[SyncSwarmClient] = None,
                 min_batch_ttl: int = 86400):
        self.client = client or SyncSwarmClient(api_url)
        self.min_batch_ttl = min_batch_ttl
        # stamp=None -> read-only remote: no stamp, fetch/stewardship only.
        # This is the *witness* shape — see Syncer(witness=…) — and needs
        # no trust: every fetched byte is hashed against its ref by the
        # caller. "auto" resolves LAZILY, on first use: a local-first
        # store must be constructible offline, and postage is the push's
        # concern, not the constructor's (a resolution failure surfaces
        # in the worker's backoff/last_error, retried when the node is
        # back). An explicit batch id is used as-is — the node rejects a
        # bad one loudly at push time.
        self._stamp_arg = stamp
        self._stamp_resolved = None if stamp == "auto" else stamp

    @property
    def stamp(self) -> Optional[str]:
        if self._stamp_arg is None:
            return None
        if self._stamp_resolved is None:
            self._stamp_resolved = _run_sync(
                self.client.loop,
                StampManager(self.client._client,
                             self.min_batch_ttl).resolve, self._stamp_arg)
        return self._stamp_resolved

    @stamp.setter
    def stamp(self, value: Optional[str]) -> None:
        self._stamp_arg = value
        self._stamp_resolved = None if value == "auto" else value

    def push_blob(self, ref: str, data: bytes,
                  deferred: bool = True) -> None:
        stamp = self.stamp
        if stamp is None:
            raise RuntimeError(
                "this BeeRemote is read-only (stamp=None) — a witness "
                "verifies, it does not upload")
        got = self.client.bytes_post(data, stamp, deferred=deferred)
        if got != ref:
            raise BlobVerificationFailed(
                f"the node returned reference {got[:16]}… for a blob "
                f"locally addressed {ref[:16]}… — the address spaces have "
                "forked. Most likely the node applied erasure coding "
                "(parity chunks change every intermediate reference); "
                "localstore's swarm addressing requires redundancy off "
                "for this store's uploads.")

    def fetch(self, ref: str) -> bytes:
        # Verification happens at the LocalStore seam (verify_fetch) and in
        # the Syncer's confirmation pass — one place each, not everywhere.
        return self.client.bytes_get(ref)

    def is_retrievable(self, ref: str) -> bool:
        return self.client.stewardship_get(ref)

    def batch_info(self) -> tuple[str, Optional[float]]:
        ttl = self.client.stamp_get(self.stamp).get("batchTTL", -1)
        return self.stamp, (float(ttl) if ttl and ttl >= 0 else None)

    def close(self) -> None:
        self.client.close()


class Syncer:
    """Background pusher for one `LocalStore` against one remote.

    Wires itself in on construction: registers a journal listener (commits
    wake the loop) and installs itself as the store's fetcher (evicted
    blobs heal by verified re-fetch). `start()` spawns the daemon thread;
    `sync()` is the blocking certainty barrier; `state`/`last_error` are
    the polling surface beyond `store.status()`.
    """

    def __init__(self, store: LocalStore, remote,
                 policy: Optional[SyncPolicy] = None, witness=None):
        self.store = store
        self.remote = remote
        #: Optional independent endpoint (`BeeRemote(url, stamp=None)`,
        #: read-only) that confirmation's verify fetches go through instead
        #: of the uploading node. NOT needed for network proof — same-node
        #: stewardship already asks the network peer-to-peer (see the
        #: module docstring) — this guards the narrower scenario of the
        #: uploading node itself lying or compromised. Prefer a second
        #: node you run over a gateway (a centralized witness is a
        #: liveness dependency); either way it is untrusted by
        #: construction — every fetched byte is hashed against its ref, so
        #: a bad witness can only delay confirmation, never lose data.
        self.witness = witness
        self.policy = policy or SyncPolicy()
        if self.policy.pinned_bytes_limit is None and store.max_bytes:
            self.policy.pinned_bytes_limit = store.max_bytes // 4
        self.state = "idle"
        self.last_error: Optional[Exception] = None
        #: Blobs pushed again because the network could not retrieve
        #: them after their first push: {ref: times}. A blob listed here
        #: was lost in delivery, not on this disk.
        self.repaired: dict = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._urgent = False
        self._backoff = 0.0
        self._thread: Optional[threading.Thread] = None
        store.add_listener(self._on_event)
        if store.fetcher is None:
            store.fetcher = self.remote.fetch

    @property
    def trusting_node_claims(self) -> bool:
        """True when `confirm_sample == 0`: eviction safety rests on the
        node's stewardship claims instead of retrieve-and-verify."""
        return self.policy.confirm_sample == 0

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> "Syncer":
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._loop, name="localstore-syncer", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

    def __enter__(self) -> "Syncer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # -- the barrier -----------------------------------------------------------

    def sync(self, timeout: Optional[float] = None) -> None:
        """Block until every root is network-confirmed (the fsync of the
        durability ladder). Raises TimeoutError — naming the last sync
        error, if any — when `timeout` passes first."""
        self._urgent = True
        self._wake.set()
        if not self.store.wait_for(None, CONFIRMED, timeout):
            detail = f" (last sync error: {self.last_error!r})" \
                if self.last_error else ""
            raise TimeoutError(
                f"sync did not complete within {timeout}s{detail}")

    # -- the worker -----------------------------------------------------------

    def _on_event(self, event: dict) -> None:
        if event.get("ev") == "committed":
            self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            delay = self._due(time.time())
            if delay is None or delay > 0:
                self._wake.wait(timeout=delay)
                self._wake.clear()
                continue
            try:
                self.state = "syncing"
                self._push_round()
                self._confirm_round()
                self.last_error = None
                self._backoff = 0.0
                self.state = "idle"
            except Exception as e:  # keep the worker alive; report, retry
                self.last_error = e
                self._backoff = min(
                    max(self._backoff * 2, self.policy.backoff_base),
                    self.policy.backoff_max)
                self.state = "backoff"
                self._wake.wait(timeout=self._backoff)
                self._wake.clear()

    def _due(self, now: float) -> Optional[float]:
        """Seconds until the next push is due: 0 = now, None = nothing to
        do (wait for a commit). The three-trigger policy lives here."""
        stats = self.store.sync_stats()
        if stats["unconfirmed"] == 0:
            self._urgent = False
            return None
        if self._urgent:
            return 0.0
        limit = self.policy.pinned_bytes_limit
        if limit and stats["pinned_bytes"] > limit:
            return 0.0
        oldest = stats["oldest_unpushed_ts"]
        if oldest is None:
            return 0.0  # everything pushed; confirmation is still owed
        due_at = min(stats["last_commit_ts"] + self.policy.debounce,
                     oldest + self.policy.max_staleness)
        return max(0.0, due_at - now)

    def _push_round(self) -> None:
        deferred = not self.policy.direct_upload
        for root, state in self.store.roots_below(PUSHED):
            if self._stop.is_set():
                return
            self._each(lambda ref: self.remote.push_blob(
                ref, self.store.get(ref), deferred=deferred),
                state.blobs, self.policy.push_concurrency)
            if self._stop.is_set():
                return  # some blobs were skipped: not pushed yet
            self.store.mark_pushed(root)  # after the fact: the lag rule

    def _confirm_round(self) -> None:
        for root, state in self.store.roots_below(CONFIRMED):
            if self._stop.is_set():
                return
            if state.rung == COMMITTED:
                continue  # push failed mid-round; next round retries it
            sample = self._sample(state.blobs)
            fetch_via = self.witness or self.remote
            self._each(lambda ref: self._verify(fetch_via, root, ref), sample)
            retrievable = self._each(self.remote.is_retrievable, sample)
            if self._stop.is_set():
                return
            if not all(retrievable):
                self._repair(root, state.blobs)
                if self._stop.is_set():
                    return
            batch, ttl = self.remote.batch_info()
            self.store.mark_confirmed(root, batch=batch, ttl=ttl)

    def _repair(self, root: str, blobs: list) -> None:
        """A sampled blob of `root` is missing from the network: check every
        blob of the root, push each missing one again directly from the
        local copy (pinned until confirmed), and leave the root to be
        checked on a later round. Raises, so the worker backs off first."""
        retrievable = self._each(self.remote.is_retrievable, blobs)
        if self._stop.is_set():
            return
        missing = [ref for ref, ok in zip(blobs, retrievable) if not ok]
        if not missing:
            return                  # a passing blip: the whole root is there
        def repush(ref):
            self.remote.push_blob(ref, self.store.get(ref), deferred=False)
            return ref
        for ref in self._each(repush, missing, self.policy.push_concurrency):
            if ref is not None:
                self.repaired[ref] = self.repaired.get(ref, 0) + 1
        raise RuntimeError(
            f"{len(missing)} of {len(blobs)} blobs of root {root[:8]}… were "
            "not retrievable from the network; pushed again directly, "
            "checking again later")

    def _verify(self, fetch_via, root: str, ref: str) -> None:
        data = fetch_via.fetch(ref)
        if self.store.address(data) != ref:
            raise BlobVerificationFailed(
                f"retrieve-and-verify failed for {ref[:16]}… of "
                f"root {root[:8]}…: fetched bytes do not hash to "
                "the reference")

    def _each(self, fn, refs: list, limit: Optional[int] = None) -> list:
        """``fn(ref)`` for every ref, `limit` (default
        ``policy.check_concurrency``) at a time, results in order (None
        where the worker was stopping). The remote's calls block —
        SyncSwarmClient runs each on fsspec's event loop — so threads are
        what overlap them. An exception in any call is raised here, after
        the others finish."""
        def guarded(ref):
            return None if self._stop.is_set() else fn(ref)
        if limit is None:
            limit = self.policy.check_concurrency
        workers = max(1, min(limit, len(refs)))
        if workers == 1:
            return [guarded(ref) for ref in refs]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(guarded, refs))

    def _sample(self, blobs: list) -> list:
        frac = self.policy.confirm_sample
        if not blobs or frac <= 0:
            return []
        k = min(len(blobs), max(MIN_CONFIRM_SAMPLE,
                                math.ceil(frac * len(blobs))))
        return random.sample(blobs, k)
