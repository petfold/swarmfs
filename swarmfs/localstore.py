"""Local-first blob store: memory, local disk and Swarm cooperating.

This is the L0 (offline) layer of the local-first design — see
``docs/localstore-design.md`` for rationale and ``docs/localstore-format.md``
for the normative on-disk format this module implements. The network half
(push worker, tag/stewardship confirmation) is L1; until it lands, the
``mark_pushed``/``mark_confirmed`` verbs are how a caller (or a test)
advances roots up the durability ladder.

The one invariant everything here serves: **a blob is deleted locally only
when the journal proves Swarm holds it** (a ``confirmed`` root lists it).
Blobs reachable only from unconfirmed roots are pinned — no budget pressure
evicts them; the budget is *soft* for pinned data, by decision: losing the
ability to save work is worse than exceeding a quota, so the store warns
(``BudgetExceededWarning``) and carries on.

Bookkeeping follows the lag rule: journal events are appended *after* the
fact they record is true, so any crash leaves the journal under-claiming
durability and recovery is always the safe, idempotent direction.

No fsspec, no aiohttp: this module's only intra-package dependency is the
BMT splitter (for ``addressing="swarm"``, the default, which makes a local
blob's name equal what ``POST /bytes`` will return for it — build offline,
publish later, nothing re-addressed).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import time
import warnings
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

FORMAT_MAGIC = "swarmfs-localstore"
FORMAT_VERSION = 1
ADDRESSINGS = ("swarm", "sha256")

#: Default safety margin for TTL-aware eviction: a blob whose covering batch
#: expires within this window is not evicted (Swarm would soon be allowed to
#: drop the only remaining copy). Callers running a confirmation refresh loop
#: (L1) can lower it; ``0`` disables the check.
DEFAULT_MIN_EVICT_TTL = 7 * 24 * 3600

# Durability rungs, ordered. "network-confirmed" for a *tree* additionally
# requires every ancestor to be CONFIRMED — see LocalStore.network_confirmed.
COMMITTED, PUSHED, CONFIRMED = "committed", "pushed", "confirmed"
_RUNG_ORDER = {COMMITTED: 0, PUSHED: 1, CONFIRMED: 2}


class BlobEvicted(KeyError):
    """The ref is known to this store (a committed root lists it) but the
    blob file has been evicted; the bytes live on Swarm. Distinct from a
    plain ``KeyError`` (never heard of that ref) so callers can say
    "exists, on Swarm, and you are offline" instead of "not found" —
    still a ``KeyError`` so generic BytesStore consumers keep working."""


class StoreLocked(RuntimeError):
    """Another process holds this store's writer lock (format v1 is
    single-writer; concurrent readers of blob files are always safe)."""


class BudgetExceededWarning(UserWarning):
    """The byte budget is exceeded and everything over it is pinned
    (unpushed) data. The limit is soft by design — push to convert pinned
    data to evictable and relieve the pressure."""


@dataclass
class RootState:
    """Fold of the journal for one root."""
    parent: Optional[str]
    blobs: List[str]
    structure: Set[str] = field(default_factory=set)
    rung: str = COMMITTED
    batch: Optional[str] = None
    ttl: Optional[float] = None
    confirmed_ts: Optional[float] = None


@dataclass
class StoreStatus:
    """What ``LocalStore.status()`` reports. ``only_on_swarm_count`` is the
    number of journal-known blobs with no local file — the portion for which
    Swarm is truly authoritative and stamp expiry means loss."""
    blob_count: int
    total_bytes: int
    pinned_bytes: int
    evictable_bytes: int
    max_bytes: Optional[int]
    only_on_swarm_count: int
    roots: Dict[str, str]          # root -> rung
    remote_roots: Dict[str, str]   # remote name -> root
    pins: Dict[str, int]           # pin name -> ref count


class LocalStore:
    """A local-first blob store over one on-disk store directory.

    Implements the BytesStore contract (``put``/``get``/``put_many``/
    ``get_many``) plus the local-first surface: ``commit_root`` records a
    new root and its new blobs in the journal; ``mark_pushed`` /
    ``mark_confirmed`` climb the durability ladder; ``pin``/``unpin`` hold
    working-set blobs; ``status`` reports where everything stands.

    ``max_bytes`` is the byte budget (None = unbounded) and
    ``min_free_bytes`` a filesystem free-space floor; breaching either
    triggers eviction of *evictable* blobs only (payload before structure,
    LRU within each class, TTL-risky blobs skipped). Pinned data is never
    evicted; if it alone exceeds the budget the store warns and proceeds.
    """

    def __init__(self, path: str, addressing: str = "swarm",
                 max_bytes: Optional[int] = None,
                 min_free_bytes: Optional[int] = None,
                 min_evict_ttl: float = DEFAULT_MIN_EVICT_TTL):
        if addressing not in ADDRESSINGS:
            raise ValueError(f"unknown addressing {addressing!r}; "
                             f"one of {ADDRESSINGS}")
        self.path = os.path.abspath(os.path.expanduser(path))
        self.max_bytes = max_bytes
        self.min_free_bytes = min_free_bytes
        self.min_evict_ttl = min_evict_ttl
        self._blob_dir = os.path.join(self.path, "blobs")
        os.makedirs(self._blob_dir, exist_ok=True)
        self._init_format(addressing)
        self.addressing = addressing = self._read_format()
        if addressing == "swarm":
            try:
                from .splitter import content_address
            except ImportError as e:
                raise ImportError(
                    'addressing="swarm" needs the BMT splitter: '
                    'pip install "swarmfs[feeds]" (or open the store with '
                    'addressing="sha256", losing Swarm address-space '
                    "compatibility)") from e
            self._address = lambda data: content_address(data).hex()
        else:
            self._address = lambda data: hashlib.sha256(data).hexdigest()

        self._lock_fd = os.open(os.path.join(self.path, "lock"),
                                os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self._lock_fd)
            self._lock_fd = None
            raise StoreLocked(
                f"{self.path} is open in another process (single writer "
                "per store in format v1)")

        # Fold of the journal (authoritative) …
        self._roots: Dict[str, RootState] = {}
        self._remote_roots: Dict[str, str] = {}
        self._pins: Dict[str, Set[str]] = {}
        self._blob_roots: Dict[str, List[str]] = {}  # ref -> listing roots
        self._journal_path = os.path.join(self.path, "journal.jsonl")
        self._replay_journal()
        self._journal_fd = open(self._journal_path, "a", encoding="utf-8")
        # … plus the rebuildable local view (sizes of files actually present;
        # recency comes from the files' own mtimes at eviction time).
        self._local: Dict[str, int] = {}
        self._scan_blobs()
        self._over_budget_warned = False

    # -- store directory ----------------------------------------------------

    def _init_format(self, addressing: str) -> None:
        fpath = os.path.join(self.path, "format")
        if not os.path.exists(fpath):
            tmp = fpath + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(f"{FORMAT_MAGIC} {FORMAT_VERSION} {addressing}\n")
                f.flush()
                os.fsync(f.fileno())
            os.rename(tmp, fpath)

    def _read_format(self) -> str:
        with open(os.path.join(self.path, "format"), encoding="utf-8") as f:
            fields = f.read().split()
        if (len(fields) != 3 or fields[0] != FORMAT_MAGIC
                or fields[1] != str(FORMAT_VERSION)
                or fields[2] not in ADDRESSINGS):
            raise ValueError(
                f"{self.path} is not a supported localstore "
                f"(format line: {' '.join(fields)!r}; this implementation "
                f"supports {FORMAT_MAGIC} {FORMAT_VERSION} {ADDRESSINGS})")
        return fields[2]

    def _blob_path(self, ref: str) -> str:
        return os.path.join(self._blob_dir, ref[:2], ref)

    def _scan_blobs(self) -> None:
        self._local.clear()
        with os.scandir(self._blob_dir) as fans:
            for fan in fans:
                if not fan.is_dir():
                    continue
                with os.scandir(fan.path) as entries:
                    for e in entries:
                        if e.is_file() and not e.name.endswith(".tmp"):
                            self._local[e.name] = e.stat().st_size

    # -- journal --------------------------------------------------------------

    def _replay_journal(self) -> None:
        if not os.path.exists(self._journal_path):
            return
        with open(self._journal_path, "rb") as f:
            lines = f.readlines()
        valid_bytes = 0
        for i, raw in enumerate(lines):
            event = None
            if raw.endswith(b"\n"):  # an unterminated line is torn by definition
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    event = None
            if event is None:
                if i == len(lines) - 1:
                    break  # torn final write — discard, everything before holds
                raise ValueError(
                    f"{self._journal_path}:{i + 1}: corrupt journal line "
                    "before the end of the file (a torn write can only be "
                    "the last line)")
            self._apply(event)
            valid_bytes += len(raw)
        if valid_bytes < sum(len(raw) for raw in lines):
            # Cut the torn tail off now (we hold the writer lock), or the
            # next append would concatenate onto the fragment and turn a
            # recoverable torn line into a corrupt mid-file one.
            os.truncate(self._journal_path, valid_bytes)

    def _append(self, event: dict) -> None:
        """Append one event — call only AFTER the fact it records is true
        (the lag rule: the journal may under-claim, never over-claim)."""
        event["ts"] = time.time()
        self._journal_fd.write(json.dumps(event, separators=(",", ":")) + "\n")
        self._journal_fd.flush()
        os.fsync(self._journal_fd.fileno())
        self._apply(event)

    def _apply(self, event: dict) -> None:
        ev = event.get("ev")
        if ev == "committed":
            root = event["root"]
            self._roots[root] = RootState(
                parent=event.get("parent"),
                blobs=list(event.get("blobs", [])),
                structure=set(event.get("structure", [])),
            )
            for ref in self._roots[root].blobs:
                self._blob_roots.setdefault(ref, []).append(root)
        elif ev in ("pushed", "confirmed"):
            state = self._roots.get(event["root"])
            if state is None:
                return  # unknown root: tolerate (compacted-away lineage)
            rung = PUSHED if ev == "pushed" else CONFIRMED
            if _RUNG_ORDER[rung] > _RUNG_ORDER[state.rung]:
                state.rung = rung
            if ev == "confirmed":
                state.batch = event.get("batch")
                state.ttl = event.get("ttl")
                state.confirmed_ts = event.get("ts")
        elif ev == "remote-root":
            self._remote_roots[event["remote"]] = event["root"]
        elif ev == "pin":
            self._pins[event["name"]] = set(event.get("refs", []))
        elif ev == "unpin":
            self._pins.pop(event["name"], None)
        elif ev == "snapshot":
            self._load_snapshot(event.get("state", {}))
        # unknown ev: ignore (forward compatibility; preserved on compaction
        # is a compaction-time concern — compaction is not implemented yet).

    def _load_snapshot(self, state: dict) -> None:
        self._roots = {
            root: RootState(parent=s.get("parent"),
                            blobs=list(s.get("blobs", [])),
                            structure=set(s.get("structure", [])),
                            rung=s.get("rung", COMMITTED),
                            batch=s.get("batch"), ttl=s.get("ttl"),
                            confirmed_ts=s.get("confirmed_ts"))
            for root, s in state.get("roots", {}).items()
        }
        self._remote_roots = dict(state.get("remote_roots", {}))
        self._pins = {n: set(r) for n, r in state.get("pins", {}).items()}
        self._blob_roots = {}
        for root, s in self._roots.items():
            for ref in s.blobs:
                self._blob_roots.setdefault(ref, []).append(root)

    # -- BytesStore contract --------------------------------------------------

    def put(self, data: bytes) -> str:
        """Store a blob, return its ref. Blobs not yet listed by any
        committed root are orphans and never evicted (they may be a commit
        in progress); classify structure vs payload at ``commit_root``."""
        ref = self._address(data)
        path = self._blob_path(ref)
        if ref not in self._local:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.rename(tmp, path)
            self._local[ref] = len(data)
            self._enforce_budget()
        return ref

    def get(self, ref: str) -> bytes:
        path = self._blob_path(ref)
        try:
            with open(path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            if ref in self._blob_roots:
                raise BlobEvicted(
                    f"{ref} was evicted locally; the bytes are on Swarm "
                    "(fetch requires the network / the L1 layer)") from None
            raise KeyError(ref) from None
        os.utime(path)  # recency signal for LRU eviction
        return data

    def put_many(self, datas: Iterable[bytes]) -> List[str]:
        return [self.put(d) for d in datas]

    def get_many(self, refs: Iterable[str]) -> Dict[str, bytes]:
        return {r: self.get(r) for r in refs}

    def has_local(self, ref: str) -> bool:
        """Present on local disk right now (False for evicted/unknown)."""
        return ref in self._local

    # -- local-first surface ----------------------------------------------------

    def commit_root(self, root: str, parent: Optional[str],
                    blobs: Iterable[str],
                    structure: Iterable[str] = ()) -> None:
        """Record a new root: `blobs` are the blobs NEW in this commit (the
        application's commit machinery knows them exactly; blobs shared with
        `parent` are not repeated), `structure` the subset that is index
        structure rather than payload. All of them are pinned until the root
        is confirmed."""
        blobs = list(blobs)
        structure = set(structure)
        if root in self._roots:
            raise ValueError(f"root {root[:8]}… already committed")
        if parent is not None and parent not in self._roots:
            raise ValueError(f"parent {parent[:8]}… is not a committed root")
        if not structure <= set(blobs):
            raise ValueError("structure refs must be a subset of blobs")
        missing = [b for b in blobs if b not in self._local]
        if missing:
            raise ValueError(
                f"cannot commit {root[:8]}…: {len(missing)} listed blob(s) "
                f"not in the store (first: {missing[0][:16]}…)")
        self._append({
            "ev": "committed", "root": root, "parent": parent,
            "blobs": blobs, "structure": sorted(structure),
            "bytes": sum(self._local[b] for b in blobs),
        })

    def mark_pushed(self, root: str) -> None:
        """Record that a push of `root`'s blobs was accepted by a Bee node.
        Call AFTER the push succeeded (the lag rule)."""
        if root not in self._roots:
            raise ValueError(f"unknown root {root[:8]}…")
        self._append({"ev": "pushed", "root": root})

    def mark_confirmed(self, root: str, batch: Optional[str] = None,
                       ttl: Optional[float] = None) -> None:
        """Record that `root`'s blobs were verified retrievable from the
        network, covered by postage `batch` with `ttl` seconds remaining.
        Call AFTER the verification passed (the lag rule). The parent must
        be confirmed first, so "network-confirmed" composes over the tree.
        This is what flips this root's blobs from pinned to evictable."""
        state = self._roots.get(root)
        if state is None:
            raise ValueError(f"unknown root {root[:8]}…")
        if state.parent is not None:
            parent = self._roots.get(state.parent)
            if parent is None or parent.rung != CONFIRMED:
                raise ValueError(
                    f"parent {state.parent[:8]}… must be confirmed before "
                    f"{root[:8]}… (confirmation composes over ancestry)")
        self._append({"ev": "confirmed", "root": root,
                      "batch": batch, "ttl": ttl})
        self._enforce_budget()  # newly evictable blobs may relieve pressure

    def network_confirmed(self, root: str) -> bool:
        """True iff `root` and every ancestor are confirmed — the whole tree
        is retrievable from the network."""
        while root is not None:
            state = self._roots.get(root)
            if state is None or state.rung != CONFIRMED:
                return False
            root = state.parent
        return True

    def set_remote_root(self, remote: str, root: str) -> None:
        """Record that `remote` (a feed, a node) points at `root` — the
        remote-tracking ref. Call AFTER the remote actually moved."""
        self._append({"ev": "remote-root", "remote": remote, "root": root})

    def remote_root(self, remote: str) -> Optional[str]:
        return self._remote_roots.get(remote)

    def parent_of(self, root: str) -> Optional[str]:
        state = self._roots.get(root)
        if state is None:
            raise KeyError(root)
        return state.parent

    def pin(self, name: str, refs: Iterable[str]) -> None:
        """Hold the listed blobs against eviction under `name` (working-set
        control: the *application* computes the refs, e.g. by walking a
        key-prefix subtree — this layer never interprets blobs). A repeated
        name replaces the earlier list."""
        self._append({"ev": "pin", "name": name, "refs": sorted(set(refs))})

    def unpin(self, name: str) -> None:
        self._append({"ev": "unpin", "name": name})

    # -- pinned / evictable / eviction -----------------------------------------

    def _evictable(self, ref: str, now: float) -> bool:
        """The invariant lives here: True only if the journal proves Swarm
        holds this blob (some listing root is confirmed), no named pin holds
        it, and its covering batch is not about to expire."""
        roots = self._blob_roots.get(ref)
        if not roots:
            return False  # orphan (possibly a commit in progress): pinned
        if any(ref in pinned for pinned in self._pins.values()):
            return False
        for root in roots:
            state = self._roots[root]
            if state.rung != CONFIRMED:
                continue
            if (self.min_evict_ttl and state.ttl is not None
                    and state.confirmed_ts is not None
                    and state.confirmed_ts + state.ttl - now
                        < self.min_evict_ttl):
                continue  # Swarm holds it, but not for much longer: keep
            return True
        return False

    def _enforce_budget(self) -> None:
        over = self._bytes_over_budget()
        if over <= 0:
            self._over_budget_warned = False
            return
        freed = self.evict(over)
        if freed < over and not self._over_budget_warned:
            self._over_budget_warned = True
            warnings.warn(
                f"store at {self.path} exceeds its budget by "
                f"{over - freed} bytes and everything over it is pinned "
                "(unpushed) data; the limit is soft by design — push and "
                "confirm to make blobs evictable", BudgetExceededWarning,
                stacklevel=3)

    def _bytes_over_budget(self) -> int:
        over = 0
        if self.max_bytes is not None:
            over = max(over, sum(self._local.values()) - self.max_bytes)
        if self.min_free_bytes is not None:
            free = shutil.disk_usage(self.path).free
            over = max(over, self.min_free_bytes - free)
        return over

    def evict(self, nbytes: int) -> int:
        """Evict up to `nbytes` of evictable blobs; return bytes actually
        freed. Payload blobs go before structure blobs (structure is small,
        touched by every operation, and keeps diff/merge local even when
        values are remote), LRU by file mtime within each class."""
        now = time.time()
        structure_refs = set()
        for state in self._roots.values():
            structure_refs |= state.structure
        candidates = [r for r in self._local if self._evictable(r, now)]
        def mtime(ref: str) -> float:
            try:
                return os.stat(self._blob_path(ref)).st_mtime
            except FileNotFoundError:
                return 0.0
        candidates.sort(key=lambda r: (r in structure_refs, mtime(r)))
        freed = 0
        for ref in candidates:
            if freed >= nbytes:
                break
            try:
                os.unlink(self._blob_path(ref))
            except FileNotFoundError:
                pass
            freed += self._local.pop(ref, 0)
        return freed

    # -- status -----------------------------------------------------------------

    def status(self) -> StoreStatus:
        now = time.time()
        pinned = evictable = 0
        for ref, size in self._local.items():
            if self._evictable(ref, now):
                evictable += size
            else:
                pinned += size
        return StoreStatus(
            blob_count=len(self._local),
            total_bytes=pinned + evictable,
            pinned_bytes=pinned,
            evictable_bytes=evictable,
            max_bytes=self.max_bytes,
            only_on_swarm_count=sum(
                1 for ref in self._blob_roots if ref not in self._local),
            roots={r: s.rung for r, s in self._roots.items()},
            remote_roots=dict(self._remote_roots),
            pins={n: len(refs) for n, refs in self._pins.items()},
        )

    # -- lifecycle ----------------------------------------------------------------

    def close(self) -> None:
        if getattr(self, "_journal_fd", None) is not None:
            self._journal_fd.close()
            self._journal_fd = None
        if getattr(self, "_lock_fd", None) is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = None

    def __enter__(self) -> "LocalStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class MemoryCacheStore:
    """A byte-budgeted in-memory LRU cache wrapped around any BytesStore.

    Transparent acceleration only — it holds no durability state and may
    drop anything (the *inner* store is authoritative), which is exactly
    the cache/replica distinction: LocalStore is a replica, this is not.
    Safe because blobs are immutable and content-addressed: a cached entry
    can never go stale. Blobs larger than the whole budget are served but
    never cached."""

    def __init__(self, inner, max_bytes: int = 64 * 1024 * 1024):
        self.inner = inner
        self.max_bytes = max_bytes
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()
        self._bytes = 0

    def _remember(self, ref: str, data: bytes) -> None:
        if len(data) > self.max_bytes:
            return
        if ref in self._cache:
            self._bytes -= len(self._cache.pop(ref))
        self._cache[ref] = data
        self._bytes += len(data)
        while self._bytes > self.max_bytes:
            _, dropped = self._cache.popitem(last=False)
            self._bytes -= len(dropped)

    def put(self, data: bytes) -> str:
        ref = self.inner.put(data)
        self._remember(ref, data)
        return ref

    def get(self, ref: str) -> bytes:
        data = self._cache.get(ref)
        if data is not None:
            self._cache.move_to_end(ref)
            return data
        data = self.inner.get(ref)
        self._remember(ref, data)
        return data

    def put_many(self, datas: Iterable[bytes]) -> List[str]:
        datas = list(datas)
        put_many = getattr(self.inner, "put_many", None)
        refs = (put_many(datas) if put_many
                else [self.inner.put(d) for d in datas])
        for ref, data in zip(refs, datas):
            self._remember(ref, data)
        return refs

    def get_many(self, refs: Iterable[str]) -> Dict[str, bytes]:
        refs = list(refs)
        out = {}
        missing = []
        for ref in refs:
            data = self._cache.get(ref)
            if data is not None:
                self._cache.move_to_end(ref)
                out[ref] = data
            else:
                missing.append(ref)
        if missing:
            get_many = getattr(self.inner, "get_many", None)
            fetched = (get_many(missing) if get_many
                       else {r: self.inner.get(r) for r in missing})
            for ref, data in fetched.items():
                self._remember(ref, data)
            out.update(fetched)
        return {r: out[r] for r in refs}
