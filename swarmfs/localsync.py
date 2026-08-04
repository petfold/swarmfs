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
- *confirmed* — the rung that permits eviction — requires
  retrieve-and-verify: a sample of the root's blobs fetched back and hashed
  against their refs, plus the node's stewardship claim. `confirm_sample=0`
  is the explicit opt-out that trusts stewardship alone.
- every upload asserts the node returned the locally computed reference
  (free: the ref is the blob's filename) — the tripwire for the
  erasure-coding address-space fork.

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


@dataclass
class SyncPolicy:
    """When the worker pushes, and how confirmation verifies.

    The three triggers each bound a different risk — debounce coalesces
    bursts (request overhead), `max_staleness` bounds how long any commit
    exists on one disk, `pinned_bytes_limit` bounds the size of a possible
    loss and relieves the budget (None: a quarter of the store's budget
    when one is set, else disabled). `confirm_sample` is the fraction of a
    root's blobs retrieve-and-verified before it is confirmed (at least
    one when > 0); 0 trusts the node's stewardship claim alone — a
    deliberate weakening, reported by `Syncer.trusting_node_claims`.
    """
    debounce: float = 10.0
    max_staleness: float = 300.0
    pinned_bytes_limit: Optional[int] = None
    confirm_sample: float = 0.25
    direct_upload: bool = False
    backoff_base: float = 1.0
    backoff_max: float = 60.0


class BeeRemote:
    """The Swarm side of a sync, over the client tier.

    Thin by design: upload one blob (asserting the returned reference),
    fetch one blob, ask stewardship, report the batch's TTL. Everything
    policy-shaped lives in `Syncer`; everything endpoint-shaped in
    `SwarmClient`.
    """

    def __init__(self, api_url: Optional[str] = None, stamp: str = "auto",
                 client: Optional[SyncSwarmClient] = None,
                 min_batch_ttl: int = 86400):
        self.client = client or SyncSwarmClient(api_url)
        self.stamp = _run_sync(
            self.client.loop,
            StampManager(self.client._client, min_batch_ttl).resolve, stamp)

    def push_blob(self, ref: str, data: bytes,
                  deferred: bool = True) -> None:
        got = self.client.bytes_post(data, self.stamp, deferred=deferred)
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
                 policy: Optional[SyncPolicy] = None):
        self.store = store
        self.remote = remote
        self.policy = policy or SyncPolicy()
        if self.policy.pinned_bytes_limit is None and store.max_bytes:
            self.policy.pinned_bytes_limit = store.max_bytes // 4
        self.state = "idle"
        self.last_error: Optional[Exception] = None
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
        for root, state in self.store.roots_below(PUSHED):
            if self._stop.is_set():
                return
            for ref in state.blobs:
                self.remote.push_blob(
                    ref, self.store.get(ref),
                    deferred=not self.policy.direct_upload)
            self.store.mark_pushed(root)  # after the fact: the lag rule

    def _confirm_round(self) -> None:
        for root, state in self.store.roots_below(CONFIRMED):
            if self._stop.is_set():
                return
            if state.rung == COMMITTED:
                continue  # push failed mid-round; next round retries it
            sample = self._sample(state.blobs)
            for ref in sample:
                data = self.remote.fetch(ref)
                if self.store.address(data) != ref:
                    raise BlobVerificationFailed(
                        f"retrieve-and-verify failed for {ref[:16]}… of "
                        f"root {root[:8]}…: fetched bytes do not hash to "
                        "the reference")
                if not self.remote.is_retrievable(ref):
                    raise RuntimeError(
                        f"node reports {ref[:16]}… not yet retrievable "
                        f"from the network; retrying root {root[:8]}… "
                        "later")
            batch, ttl = self.remote.batch_info()
            self.store.mark_confirmed(root, batch=batch, ttl=ttl)

    def _sample(self, blobs: list) -> list:
        frac = self.policy.confirm_sample
        if not blobs or frac <= 0:
            return []
        k = min(len(blobs), max(1, math.ceil(frac * len(blobs))))
        return random.sample(blobs, k)
