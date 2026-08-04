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
import threading
import time
import warnings
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

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


class BlobVerificationFailed(RuntimeError):
    """Bytes fetched back for a known ref do not hash to that ref — the
    endpoint served wrong content. Never masked as a KeyError: the blob
    exists, someone lied about its bytes."""


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
    committed_ts: Optional[float] = None


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
    #: batch id -> earliest estimated expiry (unix ts) among the roots it
    #: covers. THE number to watch once local is partial: expired batch +
    #: evicted blob = permanent loss, so surface it prominently.
    batch_expiries: Dict[str, float] = field(default_factory=dict)


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
                 min_evict_ttl: float = DEFAULT_MIN_EVICT_TTL,
                 fetcher: Optional[Callable[[str], bytes]] = None,
                 verify_fetch: bool = True,
                 durability: str = "commit"):
        #: fsync policy — "commit" (default): blobs are written without
        #: fsync and `commit_root` flushes the listed blobs (and their
        #: directories) in one barrier before the journal event, so a
        #: many-small-blob commit pays one batch of fsyncs instead of one
        #: per put; "blob": every put fsyncs immediately (paranoid).
        #: Either way the journal event only ever follows the barrier —
        #: "committed" always means durable on local disk.
        if durability not in ("commit", "blob"):
            raise ValueError('durability must be "commit" or "blob"')
        self.durability = durability
        self._session_written: Set[str] = set()  # written this session
        self._session_synced: Set[str] = set()   # …and fsynced
        self._session_listed: Set[str] = set()   # …and listed by an event
        #: Optional network heal: called with a ref when `get` finds the blob
        #: evicted; must return the bytes (a Syncer wires this to its remote).
        self.fetcher = fetcher
        #: Verify healed bytes hash to their ref before serving/re-storing
        #: (the verified-re-fetch requirement; disable only for a trusted
        #: node, mirroring swarmfs's own `verify` semantics).
        self.verify_fetch = verify_fetch
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

        # One writer *process* (the flock), but within it the app thread and
        # a sync worker both mutate the journal/fold — hence the mutex. The
        # condition backs wait_for; listeners get events outside the lock.
        self._mutex = threading.RLock()
        self._cond = threading.Condition(self._mutex)
        self._listeners: List[Callable[[dict], None]] = []
        # Fold of the journal (authoritative) …
        self._latest_root: Optional[str] = None
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
        with self._mutex:
            event["ts"] = time.time()
            self._journal_fd.write(
                json.dumps(event, separators=(",", ":")) + "\n")
            self._journal_fd.flush()
            os.fsync(self._journal_fd.fileno())
            self._apply(event)
            self._cond.notify_all()
            listeners = list(self._listeners)
        # Exception-isolated, called on the mutating thread with the lock
        # released; a listener must stay quick and non-blocking (set a flag,
        # wake a worker) — heavy work belongs on the listener's own thread.
        for fn in listeners:
            try:
                fn(event)
            except Exception:
                warnings.warn(
                    f"localstore listener {fn!r} raised; ignored",
                    RuntimeWarning, stacklevel=2)

    def add_listener(self, fn: Callable[[dict], None]) -> None:
        """Push notifications: `fn(event)` after every journal append —
        `committed`/`pushed`/`confirmed`/… — after the fact it records is
        true (notifications inherit the lag rule). Cross-process consumers
        should tail `journal.jsonl` instead."""
        with self._mutex:
            self._listeners.append(fn)

    def remove_listener(self, fn: Callable[[dict], None]) -> None:
        with self._mutex:
            self._listeners.remove(fn)

    def _apply(self, event: dict) -> None:
        ev = event.get("ev")
        if ev == "committed":
            root = event["root"]
            self._roots[root] = RootState(
                parent=event.get("parent"),
                blobs=list(event.get("blobs", [])),
                structure=set(event.get("structure", [])),
                committed_ts=event.get("ts"),
            )
            for ref in self._roots[root].blobs:
                self._blob_roots.setdefault(ref, []).append(root)
                self._session_listed.add(ref)
            self._latest_root = root
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
        elif ev == "rebased":
            root = event["root"]
            prev = self._roots.get(root)
            self._roots = {root: RootState(
                parent=None,
                blobs=list(event.get("blobs", [])),
                structure=set(event.get("structure", [])),
                rung=prev.rung if prev else COMMITTED,
                batch=prev.batch if prev else None,
                ttl=prev.ttl if prev else None,
                confirmed_ts=prev.confirmed_ts if prev else None,
                committed_ts=(prev.committed_ts if prev
                              else event.get("ts")),
            )}
            self._blob_roots = {}
            for ref in self._roots[root].blobs:
                self._blob_roots.setdefault(ref, []).append(root)
                self._session_listed.add(ref)
            self._latest_root = root
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
        self._latest_root = state.get("latest_root") or (
            next(reversed(self._roots)) if self._roots else None)
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
        with self._mutex:
            if ref not in self._local:
                self._write_blob(ref, data)
                self._enforce_budget()
        return ref

    def _write_blob(self, ref: str, data: bytes) -> None:
        path = self._blob_path(ref)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            if self.durability == "blob":
                os.fsync(f.fileno())
        os.rename(tmp, path)
        self._local[ref] = len(data)
        self._session_written.add(ref)
        if self.durability == "blob":
            self._session_synced.add(ref)

    def _sync_blobs(self, refs: List[str]) -> None:
        """The commit-boundary barrier: make the listed blob files (and the
        directory entries that name them) durable before the journal event
        claims them. One batch of fsyncs per commit, not one per put."""
        dirs = set()
        for ref in refs:
            if ref in self._session_synced:
                continue
            if ref in self._blob_roots:
                # Already listed by a committed event, so already made
                # durable by that event's barrier — shared blobs and
                # rebase lists skip the redundant fsync.
                self._session_synced.add(ref)
                continue
            path = self._blob_path(ref)
            if ref not in self._session_written and \
                    ref not in self._blob_roots:
                # A pre-session orphan (a put from a session that crashed
                # before any commit): its file may be torn — an unflushed
                # write followed by the rename can survive a crash as a
                # correctly named file with garbage content. Verify before
                # this commit claims it durable; on mismatch drop it so
                # the caller can re-put.
                with open(path, "rb") as f:
                    data = f.read()
                if self._address(data) != ref:
                    os.unlink(path)
                    self._local.pop(ref, None)
                    raise BlobVerificationFailed(
                        f"blob {ref[:16]}… on disk does not hash to its "
                        "name (torn write from a crashed session?); the "
                        "file was dropped — re-put the data and retry the "
                        "commit")
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            dirs.add(os.path.dirname(path))
            self._session_synced.add(ref)
        for d in dirs | ({self._blob_dir} if dirs else set()):
            fd = os.open(d, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def get(self, ref: str) -> bytes:
        path = self._blob_path(ref)
        try:
            with open(path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            if ref not in self._blob_roots:
                raise KeyError(ref) from None
            if self.fetcher is None:
                raise BlobEvicted(
                    f"{ref} was evicted locally; the bytes are on Swarm "
                    "(attach a fetcher/Syncer, or reconnect)") from None
            return self._heal(ref)
        os.utime(path)  # recency signal for LRU eviction
        return data

    def _heal(self, ref: str) -> bytes:
        """Verified re-fetch of an evicted blob: pull it back through
        `fetcher`, check it hashes to its ref (unless `verify_fetch` is
        off — trusted node), re-store it locally, serve it."""
        data = self.fetcher(ref)
        if self.verify_fetch and self._address(data) != ref:
            raise BlobVerificationFailed(
                f"re-fetched bytes for {ref[:16]}… do not hash to that "
                "reference — the endpoint served wrong content")
        with self._mutex:
            if ref not in self._local:
                self._write_blob(ref, data)
                self._enforce_budget()
        return data

    def address(self, data: bytes) -> str:
        """This store's ref for `data` without storing it (the addressing
        scheme is fixed per store) — the primitive behind every
        verification in the sync layer."""
        return self._address(data)

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
        with self._mutex:
            if root in self._roots:
                raise ValueError(f"root {root[:8]}… already committed")
            if parent is not None and parent not in self._roots:
                raise ValueError(
                    f"parent {parent[:8]}… is not a committed root")
            if not structure <= set(blobs):
                raise ValueError("structure refs must be a subset of blobs")
            missing = [b for b in blobs if b not in self._local]
            if missing:
                raise ValueError(
                    f"cannot commit {root[:8]}…: {len(missing)} listed "
                    f"blob(s) not in the store (first: {missing[0][:16]}…)")
            self._sync_blobs(blobs)  # durable BEFORE the event claims them
            self._append({
                "ev": "committed", "root": root, "parent": parent,
                "blobs": blobs, "structure": sorted(structure),
                "bytes": sum(self._local[b] for b in blobs),
            })

    def mark_pushed(self, root: str) -> None:
        """Record that a push of `root`'s blobs was accepted by a Bee node.
        Call AFTER the push succeeded (the lag rule)."""
        with self._mutex:
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
        with self._mutex:
            state = self._roots.get(root)
            if state is None:
                raise ValueError(f"unknown root {root[:8]}…")
            if state.parent is not None:
                parent = self._roots.get(state.parent)
                if parent is None or parent.rung != CONFIRMED:
                    raise ValueError(
                        f"parent {state.parent[:8]}… must be confirmed "
                        f"before {root[:8]}… (confirmation composes over "
                        "ancestry)")
            self._append({"ev": "confirmed", "root": root,
                          "batch": batch, "ttl": ttl})
            self._enforce_budget()  # newly evictable blobs relieve pressure

    def network_confirmed(self, root: str) -> bool:
        """True iff `root` and every ancestor are confirmed — the whole tree
        is retrievable from the network."""
        with self._mutex:
            while root is not None:
                state = self._roots.get(root)
                if state is None or state.rung != CONFIRMED:
                    return False
                root = state.parent
            return True

    def set_remote_root(self, remote: str, root: str) -> None:
        """Record that `remote` (a feed, a node) points at `root` — the
        remote-tracking ref. Call AFTER the remote actually moved."""
        with self._mutex:
            self._append({"ev": "remote-root", "remote": remote,
                          "root": root})

    def remote_root(self, remote: str) -> Optional[str]:
        with self._mutex:
            return self._remote_roots.get(remote)

    def parent_of(self, root: str) -> Optional[str]:
        with self._mutex:
            state = self._roots.get(root)
            if state is None:
                raise KeyError(root)
            return state.parent

    def has_root(self, root: str) -> bool:
        with self._mutex:
            return root in self._roots

    def latest_root(self) -> Optional[str]:
        """The most recently committed root — what an application should
        open at (the journal is the pointer: lineage lives here, not in a
        separate ref file)."""
        with self._mutex:
            return self._latest_root

    def wait_for(self, root: Optional[str] = None, rung: str = CONFIRMED,
                 timeout: Optional[float] = None) -> bool:
        """Block until `root` reaches `rung` (for CONFIRMED: it and every
        ancestor — the network-confirmed sense), or until *every* root has
        when `root` is None. Returns False on timeout. The certainty
        barrier: `wait_for(root)` after a commit is "my work is safe"."""
        target = _RUNG_ORDER[rung]

        def ready() -> bool:
            if root is None:
                return all(self._reached(r, target) for r in self._roots)
            return self._reached(root, target)

        with self._cond:
            return self._cond.wait_for(ready, timeout)

    def _reached(self, root: str, target: int) -> bool:
        state = self._roots.get(root)
        if state is None or _RUNG_ORDER[state.rung] < target:
            return False
        if target == _RUNG_ORDER[CONFIRMED]:
            return self.network_confirmed(root)
        return True

    # -- sync-worker accessors ---------------------------------------------------

    def roots_below(self, rung: str) -> List[Tuple[str, RootState]]:
        """Roots not yet at `rung`, parents before children — the work list
        for a push/confirm round (topological order makes the
        parent-confirmed-first rule automatic)."""
        with self._mutex:
            def depth(r: str) -> int:
                d = 0
                while True:
                    parent = self._roots[r].parent
                    if parent is None or parent not in self._roots:
                        return d
                    r, d = parent, d + 1
            target = _RUNG_ORDER[rung]
            pending = [(root, state) for root, state in self._roots.items()
                       if _RUNG_ORDER[state.rung] < target]
            pending.sort(key=lambda item: depth(item[0]))
            return pending

    def sync_stats(self) -> dict:
        """The numbers the auto-push triggers read, in one locked pass:
        `last_commit_ts`, `oldest_unpushed_ts`, `pinned_bytes`, and
        `unconfirmed` (count of roots below CONFIRMED)."""
        with self._mutex:
            now = time.time()
            commit_ts = [s.committed_ts for s in self._roots.values()
                         if s.committed_ts is not None]
            unpushed_ts = [s.committed_ts for s in self._roots.values()
                           if s.rung == COMMITTED
                           and s.committed_ts is not None]
            pinned = sum(size for ref, size in self._local.items()
                         if not self._evictable(ref, now))
            return {
                "last_commit_ts": max(commit_ts) if commit_ts else None,
                "oldest_unpushed_ts": (min(unpushed_ts) if unpushed_ts
                                       else None),
                "pinned_bytes": pinned,
                "unconfirmed": sum(1 for s in self._roots.values()
                                   if s.rung != CONFIRMED),
            }

    def rebase_root(self, root: str, blobs: Iterable[str],
                    structure: Iterable[str] = ()) -> None:
        """Collapse the journal's lineage onto `root`, whose `blobs` list
        must be its FULL reachable set — the *application* computes it,
        because this layer is blob-blind: a blob the tip still references
        may be listed only in an ancestor's event (the same fact that made
        push-latest-only impossible for the worker at L1; squash is the
        app-assisted version). Every other root leaves the fold; their
        exclusive blobs become orphans (`gc_orphans` deletes them).
        Durability facts for `root` — rung, batch, TTL — survive: they are
        still true. Each listed blob must be locally present or listed by
        a confirmed root (on Swarm); otherwise dropping the old events
        would lose it, and this refuses instead."""
        blobs = list(blobs)
        structure = set(structure)
        with self._mutex:
            if root not in self._roots:
                raise ValueError(f"unknown root {root[:8]}…")
            if not structure <= set(blobs):
                raise ValueError("structure refs must be a subset of blobs")
            for b in blobs:
                if b in self._local:
                    continue
                listing = self._blob_roots.get(b, [])
                if not any(self._roots[r].rung == CONFIRMED
                           for r in listing):
                    raise ValueError(
                        f"blob {b[:16]}… is neither local nor on Swarm — "
                        "rebasing would lose it")
            self._sync_blobs([b for b in blobs if b in self._local])
            self._append({
                "ev": "rebased", "root": root, "blobs": blobs,
                "structure": sorted(structure),
                "bytes": sum(self._local.get(b, 0) for b in blobs),
            })

    def gc_orphans(self) -> Tuple[int, int]:
        """Delete local blob files that no committed root lists and no
        named pin holds — the debris a rebase leaves (dropped history's
        exclusive blobs) and torn leftovers of crashed sessions. Blobs
        written *this* session but not yet committed are spared: they may
        be a commit in progress. (A blob committed and later dropped by a
        rebase is fair game — "ever listed by an event" is what separates
        staging from debris.) Returns ``(count, bytes_freed)``."""
        with self._mutex:
            pinned = set()
            for refs in self._pins.values():
                pinned |= refs
            staging = self._session_written - self._session_listed
            victims = [r for r in self._local
                       if r not in self._blob_roots and r not in pinned
                       and r not in staging]
            freed = 0
            for ref in victims:
                try:
                    os.unlink(self._blob_path(ref))
                except FileNotFoundError:
                    pass
                freed += self._local.pop(ref, 0)
            return len(victims), freed

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
        with self._mutex:
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
        with self._mutex:
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
                batch_expiries=self._batch_expiries(),
            )

    def _batch_expiries(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for state in self._roots.values():
            if (state.batch and state.ttl is not None
                    and state.confirmed_ts is not None):
                expiry = state.confirmed_ts + state.ttl
                if state.batch not in out or expiry < out[state.batch]:
                    out[state.batch] = expiry
        return out

    def scrub(self) -> dict:
        """Bitrot check: re-hash every local blob against its name (the
        format mandates a mismatching file be treated as absent — this
        makes that real). A corrupt *evictable* blob is dropped and heals
        by verified re-fetch on its next read; a corrupt *pinned* blob is
        the only copy going bad — the scan completes, then raises
        `BlobVerificationFailed` naming every such ref. On-demand and
        O(store): run it from a cron, never a hot path. Returns
        ``{"scanned": n, "dropped": [refs]}`` when all is well."""
        with self._mutex:
            refs = list(self._local)
        dropped, corrupt_pinned = [], []
        now = time.time()
        for ref in refs:
            try:
                with open(self._blob_path(ref), "rb") as f:
                    data = f.read()
            except FileNotFoundError:
                continue  # evicted mid-scan
            if self._address(data) == ref:
                continue
            with self._mutex:
                if self._evictable(ref, now):
                    try:
                        os.unlink(self._blob_path(ref))
                    except FileNotFoundError:
                        pass
                    self._local.pop(ref, None)
                    dropped.append(ref)
                else:
                    corrupt_pinned.append(ref)
        if corrupt_pinned:
            shown = ", ".join(r[:16] + "…" for r in corrupt_pinned[:5])
            raise BlobVerificationFailed(
                f"{len(corrupt_pinned)} pinned blob(s) are corrupt on disk "
                f"and exist nowhere else ({shown}) — restore from a backup "
                "or accept the loss; evictable corruption was healed "
                f"({len(dropped)} dropped for re-fetch)")
        return {"scanned": len(refs), "dropped": dropped}

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
