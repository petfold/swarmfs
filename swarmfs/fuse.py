"""Mount a Swarm reference or feed as a local directory, read-only, via FUSE.

This is fsspec's generic FUSE wrapper (``fsspec.fuse.FUSEr``, built on
`fusepy <https://github.com/fusepy/fusepy>`_) over the existing backend —
not a second implementation of anything. What swarmfs adds is policy and
polish:

- **Read-only, enforced twice.** The kernel mounts with the ``ro`` flag, so
  writes fail with EROFS before they reach Python, and every mutating
  operation here raises EROFS as well. A ``bzz://`` reference is immutable
  by construction, and a ``bzzf://`` feed is a *view* that follows the
  feed's updates (``feed_ttl``) — neither is a place to type into by
  default. ``rw=True`` (``swarmfs mount --rw``) opts in: each saved file is
  one commit — see ``WritableSwarmFUSEr``.
- **Attributes that make sense for content-addressed data.** Files are
  ``0444``, directories ``0555``, owned by the mounting user; timestamps are
  the mount time — a constant, so nothing downstream sees content "change"
  between two ``stat`` calls the way fsspec's default (``time.time()`` on
  every call) makes it look.
- **Errors mapped to errno**, always. fusepy turns an ``OSError`` into its
  ``errno``, but ``SwarmError`` and friends carry none, and fsspec's wrapper
  lets everything but ``FileNotFoundError`` escape — so every operation is
  guarded: not-found → ENOENT, node/network trouble → EIO, and the exception
  is logged rather than lost.
- **Kernel page cache for immutable content.** A ``bzz://`` mount passes
  ``kernel_cache``: the same path can never hold different bytes, so cached
  pages are correct forever. Feed mounts do not (their content moves).
- **Fail in the terminal, not in the mount.** The reference is resolved
  (``fs.info``) *before* mounting, so a bad reference, an unreachable node,
  or a refused gateway raises a normal exception with swarmfs's usual
  message instead of producing a directory where every command says
  "Input/output error".

Requires the ``fuse`` extra (``pip install "swarmfs[fuse]"``) and a
system libfuse **2** (``libfuse2``/``libfuse2t64`` on Debian/Ubuntu,
macFUSE on macOS) — fusepy loads ``libfuse.so.2``, not libfuse3.
"""

from __future__ import annotations

import errno
import functools
import logging
import os
import posixpath
import re
import shutil
import stat
import tempfile
import threading
import time

from fsspec.core import url_to_fs

from .core import SwarmFileSystem
from .feedfs import SwarmFeedFileSystem

logger = logging.getLogger("swarmfs.fuse")

_BARE_REF = re.compile(r"^(?:[0-9a-fA-F]{64}|[0-9a-fA-F]{128})(?:/.*)?$")

INSTALL_HINT = (
    "the FUSE mount needs fusepy and a system libfuse 2: "
    "pip install \"swarmfs[fuse]\", plus `apt install libfuse2` "
    "(`libfuse2t64` on Ubuntu 24.04+) or macFUSE on macOS"
)


def _import_fusepy():
    """Import fusepy, turning its two failure modes into actionable errors.

    ``import fuse`` raises ImportError when fusepy is not installed and an
    ``EnvironmentError`` ("Unable to find libfuse") when the Python package
    is present but the C library is not.
    """
    try:
        import fuse
    except ImportError as e:
        raise ImportError(f"fusepy is not installed — {INSTALL_HINT}") from e
    except OSError as e:  # fusepy: EnvironmentError('Unable to find libfuse')
        raise OSError(f"libfuse not found ({e}) — {INSTALL_HINT}") from e
    return fuse


def normalize_url(url: str) -> str:
    """Accept a bare 64/128-hex reference (optionally ``/subpath``) as
    shorthand for ``bzz://<ref>``; pass every other URL through untouched
    (``bzz://``, ``bzzf://``, or an fsspec chain like
    ``simplecache::bzz://…``)."""
    url = url.strip()
    if _BARE_REF.match(url):
        return f"bzz://{url}"
    return url


def swarm_filesystem_of(fs) -> SwarmFileSystem:
    """The ``SwarmFileSystem`` inside ``fs`` — ``fs`` itself, or the target
    of a caching/chained wrapper (fsspec wrappers expose it as ``.fs``).
    Raises ValueError when there is none: this mounts Swarm, not memory."""
    seen = 0
    while fs is not None and seen < 8:
        if isinstance(fs, SwarmFileSystem):
            return fs
        fs = getattr(fs, "fs", None)
        seen += 1
    raise ValueError(
        "not a Swarm URL: expected bzz://<ref>/…, bzzf://<owner>/<topic>/…, "
        "a bare 64/128-hex reference, or an fsspec chain ending in one "
        "(simplecache::bzz://…)")


@functools.lru_cache(maxsize=None)
def _ops_class():
    """Build the FUSE operations class lazily: importing ``fsspec.fuse``
    imports fusepy at module level, and this module must import cleanly
    on machines without libfuse (the CLI's argument parsing, for one)."""
    fuse = _import_fusepy()
    from fsspec.fuse import FUSEr

    FuseOSError = fuse.FuseOSError

    def guarded(method):
        """Map exceptions to errno; fusepy would otherwise report EINVAL
        for anything that is not an OSError with a positive errno, and a
        ``SwarmError`` has ``errno=None`` (which its handler cannot even
        compare)."""

        @functools.wraps(method)
        def wrapper(self, path, *args, **kwargs):
            try:
                return method(self, path, *args, **kwargs)
            except FuseOSError:
                raise
            except FileNotFoundError as e:
                raise FuseOSError(errno.ENOENT) from e
            except IsADirectoryError as e:
                raise FuseOSError(errno.EISDIR) from e
            except NotADirectoryError as e:
                raise FuseOSError(errno.ENOTDIR) from e
            except PermissionError as e:
                logger.warning("%s %s: %s", method.__name__, path, e)
                raise FuseOSError(errno.EACCES) from e
            except NotImplementedError as e:
                logger.warning("%s %s: %s", method.__name__, path, e)
                raise FuseOSError(errno.EOPNOTSUPP) from e
            except Exception as e:  # SwarmError, aiohttp, anything
                logger.error("%s %s: %s", method.__name__, path, e)
                raise FuseOSError(errno.EIO) from e

        return wrapper

    def read_only(op):
        def refuse(self, *args, **kwargs):
            raise FuseOSError(errno.EROFS)

        refuse.__name__ = op
        return refuse

    class SwarmFUSEr(FUSEr):
        """fsspec's FUSEr, read-only, with content-addressing-appropriate
        attributes and errno mapping. See the module docstring."""

        use_ns = True  # fusepy: nanosecond timestamps (silences its warning)

        def __init__(self, fs, path, ready_file=False):
            super().__init__(fs, path, ready_file=ready_file)
            self._mount_time_ns = time.time_ns()
            self._uid = os.getuid()
            self._gid = os.getgid()

        def _full(self, path: str) -> str:
            return "".join([self.root, path.lstrip("/")]).rstrip("/")

        def _is_ready_file(self, path: str) -> bool:
            return bool(self._ready_file) and path in ("/.fuse_ready", ".fuse_ready")

        def _attrs(self, is_dir: bool, size: int) -> dict:
            if is_dir:
                mode, nlink = stat.S_IFDIR | 0o555, 2
            else:
                mode, nlink = stat.S_IFREG | 0o444, 1
            return {
                "st_mode": mode,
                "st_nlink": nlink,
                "st_size": size,
                "st_uid": self._uid,
                "st_gid": self._gid,
                "st_blksize": getattr(self.fs, "block_size", None) or 2**20,
                "st_atime": self._mount_time_ns,
                "st_mtime": self._mount_time_ns,
                "st_ctime": self._mount_time_ns,
            }

        @guarded
        def getattr(self, path, fh=None):
            if self._is_ready_file(path):
                return self._attrs(False, 5)
            info = self.fs.info(self._full(path))
            if info["type"] != "file":
                return self._attrs(True, 0)
            size = info.get("size")
            if size is None:  # a listing could not size it; be honest, not zero
                raise OSError(f"could not determine the size of {path}")
            return self._attrs(False, size)

        @guarded
        def readdir(self, path, fh):
            return super().readdir(path, fh)

        @guarded
        def open(self, path, flags):
            if flags & os.O_ACCMODE != os.O_RDONLY:
                raise FuseOSError(errno.EROFS)
            if self._is_ready_file(path):
                fh = self.counter
                self.counter += 1
                return fh
            return super().open(path, flags)

        @guarded
        def read(self, path, size, offset, fh):
            if self._is_ready_file(path):
                return b"ready"[offset : offset + size]
            return super().read(path, size, offset, fh)

        @guarded
        def release(self, path, fh):
            return super().release(path, fh)

        def statfs(self, path):
            # Nothing meaningful to report about "free space" on Swarm; the
            # block sizes keep df/rsync arithmetic sane, f_namemax matches
            # what Mantaray paths comfortably hold.
            return {
                "f_bsize": 4096,
                "f_frsize": 4096,
                "f_blocks": 0,
                "f_bfree": 0,
                "f_bavail": 0,
                "f_files": 0,
                "f_ffree": 0,
                "f_namemax": 255,
            }

        def utimens(self, path, times=None):
            raise FuseOSError(errno.EROFS)

    # fsspec's FUSEr implements these over fs.open("wb")/touch/rm; a Swarm
    # mount is read-only (see module docstring), so refuse them all — the
    # kernel's `ro` flag already does, this is the second lock.
    for op in ("create", "write", "truncate", "mkdir", "rmdir", "unlink",
               "rename", "chmod", "chown", "link", "symlink", "mknod",
               "setxattr", "removexattr"):
        setattr(SwarmFUSEr, op, read_only(op))

    class WritableSwarmFUSEr(SwarmFUSEr):
        """The writable mount (``rw=True``): every write is a commit.

        Content addressing gives an object its identity only once it is
        complete, so writes are *buffered per open file* (a spooled temp
        file, 16 MiB in memory then disk) and become one ``fs.open(path,
        "wb")`` write — one commit, one new root — when the last handle is
        released. fsspec's own FUSEr writes straight into a write-mode
        buffered file, which cannot ``seek()``; this replaces that path.

        Two things the kernel expects that a Mantaray manifest cannot
        provide are answered from the mounter's own tables: a file that has
        been created but not yet released (``getattr`` right after
        ``create``), and an empty directory (``mkdir`` — manifests have no
        empty directories, they are implicit in paths). Both vanish into the
        real filesystem the moment content lands. ``unlink`` is ``fs.rm``,
        ``rename`` is ``fs.mv`` inside ``fs.transaction`` (one commit),
        ``chmod``/``chown``/``utimens`` are accepted and ignored (a content
        address has no mode or mtime), so ``cp -p``, ``rsync`` and editors'
        save dances complete. Reads of an in-flight file come from its
        buffer. Everything else is the read-only mounter unchanged.
        """

        def __init__(self, fs, path, ready_file=False):
            super().__init__(fs, path, ready_file=ready_file)
            self._pending: dict[str, tempfile.SpooledTemporaryFile] = {}  # full path -> buffer
            self._dirty: set[str] = set()
            self._handles: dict[int, str] = {}  # fh -> full path of a pending buffer
            self._dirs: set[str] = set()  # mkdir'd, still empty (phantom) directories

        # -- helpers ----------------------------------------------------------

        def _attrs(self, is_dir: bool, size: int) -> dict:
            a = super()._attrs(is_dir, size)
            a["st_mode"] = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
            return a

        def _parent_exists(self, full: str) -> bool:
            parent = posixpath.dirname(full.rstrip("/"))
            if parent in self._dirs or parent == self.root.rstrip("/"):
                return True
            if any(p.startswith(parent + "/") for p in list(self._pending) + list(self._dirs)):
                return True
            try:
                return self.fs.isdir(parent)
            except OSError:
                return False

        def _new_handle(self, full: str) -> int:
            fh = self.counter
            self.counter += 1
            self._handles[fh] = full
            return fh

        def _materialize(self, full: str) -> tempfile.SpooledTemporaryFile:
            """Buffer for `full`: the pending one, or the existing content."""
            buf = self._pending.get(full)
            if buf is None:
                buf = tempfile.SpooledTemporaryFile(max_size=16 * 2**20)
                try:
                    buf.write(self.fs.cat_file(full))
                except FileNotFoundError:
                    pass
                self._pending[full] = buf
            return buf

        def _commit(self, full: str) -> None:
            buf = self._pending.pop(full)
            dirty = full in self._dirty
            self._dirty.discard(full)
            try:
                if not dirty:
                    return
                buf.seek(0)
                try:
                    with self.fs.open(full, "wb") as out:
                        shutil.copyfileobj(buf, out)
                except BaseException:
                    # a refused commit must not lose the buffer: the file
                    # stays pending (and dirty), so a retry can succeed
                    buf.seek(0)
                    self._pending[full] = buf
                    self._dirty.add(full)
                    raise
                # the directory is real now
                d = posixpath.dirname(full)
                while d and d in self._dirs:
                    self._dirs.discard(d)
                    d = posixpath.dirname(d)
                logger.info("committed %s", full)
            finally:
                if full not in self._pending:
                    buf.close()

        # -- attributes & listing -------------------------------------------------

        @guarded
        def getattr(self, path, fh=None):
            full = self._full(path)
            if full in self._pending:
                buf = self._pending[full]
                buf.seek(0, os.SEEK_END)
                return self._attrs(False, buf.tell())
            if full in self._dirs:
                return self._attrs(True, 0)
            try:
                return super().getattr(path, fh)
            except FuseOSError as e:
                if e.errno == errno.ENOENT and any(
                        p.startswith(full + "/") for p in list(self._pending) + list(self._dirs)):
                    return self._attrs(True, 0)  # implied by something in flight
                raise

        @guarded
        def readdir(self, path, fh):
            full = self._full(path)
            try:
                entries = super().readdir(path, fh)
            except FuseOSError as e:
                if e.errno != errno.ENOENT:
                    raise
                entries = [".", ".."]
            prefix = (full + "/") if full else "/"
            extra = set()
            for p in list(self._pending) + list(self._dirs):
                if p.startswith(prefix):
                    extra.add(p[len(prefix):].split("/", 1)[0])
            return list(dict.fromkeys(entries + sorted(extra)))

        # -- files ------------------------------------------------------------------

        @guarded
        def create(self, path, mode, fi=None):
            full = self._full(path)
            if not self._parent_exists(full):
                raise FuseOSError(errno.ENOENT)
            buf = tempfile.SpooledTemporaryFile(max_size=16 * 2**20)
            old = self._pending.get(full)
            if old is not None:
                old.close()
            self._pending[full] = buf
            self._dirty.add(full)  # even an untouched new file is a (empty) write
            return self._new_handle(full)

        @guarded
        def open(self, path, flags):
            full = self._full(path)
            accmode = flags & os.O_ACCMODE
            if accmode == os.O_RDONLY:
                if full in self._pending:
                    return self._new_handle(full)
                return super().open(path, flags)
            if flags & os.O_TRUNC:
                buf = tempfile.SpooledTemporaryFile(max_size=16 * 2**20)
                old = self._pending.get(full)
                if old is not None:
                    old.close()
                self._pending[full] = buf
                self._dirty.add(full)
            else:
                self._materialize(full)
            return self._new_handle(full)

        @guarded
        def read(self, path, size, offset, fh):
            full = self._handles.get(fh)
            if full is not None and full in self._pending:
                buf = self._pending[full]
                buf.seek(offset)
                return buf.read(size)
            return super().read(path, size, offset, fh)

        @guarded
        def write(self, path, data, offset, fh):
            full = self._handles.get(fh) or self._full(path)
            buf = self._materialize(full)
            buf.seek(offset)
            buf.write(data)
            self._dirty.add(full)
            return len(data)

        @guarded
        def truncate(self, path, length, fh=None):
            full = self._full(path)
            buf = self._materialize(full)
            buf.truncate(length)
            if length:
                buf.seek(0, os.SEEK_END)
                if buf.tell() < length:  # extend with zeros, like POSIX
                    buf.write(b"\0" * (length - buf.tell()))
            self._dirty.add(full)
            if fh is None and not any(h == full for h in self._handles.values()):
                self._commit(full)  # truncate(2) on a closed file: its own write
            return 0

        @guarded
        def flush(self, path, fh):
            # The kernel calls flush synchronously on close(2) and returns
            # its error to the caller; release comes later, asynchronously.
            # Committing here means `cp` and editors learn about a refused
            # commit (no stamp, a refused classification) as an error from
            # close, and that the file is really committed when close
            # returns — which is what a shell user assumes.
            full = self._handles.get(fh)
            if full is not None and full in self._dirty:
                self._commit(full)
            return 0

        @guarded
        def fsync(self, path, datasync, fh):
            return self.flush(path, fh)

        @guarded
        def release(self, path, fh):
            full = self._handles.pop(fh, None)
            if full is None:
                return super().release(path, fh)
            if full in self._pending and full not in self._handles.values():
                self._commit(full)  # anything left un-flushed
            return 0

        @guarded
        def unlink(self, path):
            full = self._full(path)
            if full in self._pending:
                self._pending.pop(full).close()
                self._dirty.discard(full)
                self._handles = {h: p for h, p in self._handles.items() if p != full}
                try:
                    self.fs.info(full)
                except FileNotFoundError:
                    return 0  # never committed: nothing more to do
            self.fs.rm(full)
            return 0

        @guarded
        def rename(self, old, new):
            src, dst = self._full(old), self._full(new)
            if src in self._pending:  # in flight: just rebind the buffer
                self._pending[dst] = self._pending.pop(src)
                if src in self._dirty:
                    self._dirty.discard(src)
                    self._dirty.add(dst)
                self._handles = {h: (dst if p == src else p) for h, p in self._handles.items()}
                return 0
            if src in self._dirs:
                self._dirs.discard(src)
                self._dirs.add(dst)
                return 0
            if self.fs.isdir(src) and not self.fs.isfile(src):
                # directories are implicit: move every file under the prefix
                # (fsspec's generic recursive mv trips over that), one commit
                files = self.fs.find(src)
                with self.fs.transaction:
                    for f in files:
                        rel = f[len(self.fs._strip_protocol(src)):].lstrip("/")
                        self.fs.cp_file(f, posixpath.join(dst, rel))
                        self.fs.rm_file(f)
                # phantom children (mkdir'd, still empty) move along
                for d in [d for d in self._dirs if d.startswith(src + "/")]:
                    self._dirs.discard(d)
                    self._dirs.add(dst + d[len(src):])
                return 0
            with self.fs.transaction:
                self.fs.mv(src, dst)
            return 0

        # -- directories --------------------------------------------------------------

        @guarded
        def mkdir(self, path, mode):
            full = self._full(path)
            if not self._parent_exists(full):
                raise FuseOSError(errno.ENOENT)
            try:
                if self.fs.exists(full):
                    raise FuseOSError(errno.EEXIST)
            except OSError:
                pass
            self.fs.mkdir(full)  # a filesystem that refuses (lattice edits) refuses here
            self._dirs.add(full)
            return 0

        @guarded
        def rmdir(self, path):
            full = self._full(path)
            if full in self._dirs:
                if any(p.startswith(full + "/") for p in list(self._pending) + list(self._dirs)):
                    raise FuseOSError(errno.ENOTEMPTY)
                self._dirs.discard(full)
                return 0
            try:
                if self.fs.ls(full, detail=False):
                    raise FuseOSError(errno.ENOTEMPTY)
            except FileNotFoundError:
                raise FuseOSError(errno.ENOENT) from None
            return 0  # an empty manifest directory does not exist to begin with

        # -- metadata a content address does not have --------------------------------

        def chmod(self, path, mode):
            return 0

        def chown(self, path, uid, gid):
            return 0

        def utimens(self, path, times=None):
            return 0

    return SwarmFUSEr, WritableSwarmFUSEr


def mount(
    url: str,
    mountpoint: str,
    *,
    foreground: bool = True,
    threads: bool = False,
    ready_file: bool = False,
    allow_other: bool = False,
    fs=None,
    fsname: str | None = None,
    rw: bool = False,
    **storage_options,
):
    """Mount ``url`` at ``mountpoint`` — read-only by default, writable
    with ``rw=True``.

    Parameters
    ----------
    url:
        ``bzz://<ref>[/path]``, ``bzzf://<owner>/<topic>[/path]``, a bare
        64/128-hex reference, or an fsspec chain such as
        ``simplecache::bzz://<ref>`` (local caching for free).
    mountpoint:
        An existing directory.
    foreground:
        Block until the filesystem is unmounted (``fusermount -u`` or
        Ctrl-C). With ``False`` the FUSE loop runs in a daemon thread and
        that ``Thread`` is returned — the shape tests use.
    threads:
        Let FUSE dispatch operations concurrently. Off by default, as
        fsspec recommends; the async plumbing is thread-safe, the gain is
        modest for one reader.
    ready_file:
        Expose a synthetic ``.fuse_ready`` file once the mount is serving
        (readiness probe for scripts/tests; not listed by ``readdir``).
    allow_other:
        Let users other than the mounting one access the mount (needs
        ``user_allow_other`` in ``/etc/fuse.conf``).
    fs:
        A pre-built filesystem to mount instead of resolving ``url``;
        ``url`` then only supplies the path inside it. **Any** fsspec
        filesystem is accepted here, not only Swarm ones — this is how
        ontodag-fs mounts its lattice view with the same read-only
        policy, errno mapping and attributes (its own writes are refused
        with EROFS instead of fsspec's bare EINVAL). ``kernel_cache`` is
        only enabled when a plain ``bzz://`` filesystem is found inside.
    fsname:
        What ``mount``/``df`` show as the source (default: ``url`` when it
        has no commas or spaces).
    rw:
        Writable mount: each file written is committed when its last handle
        closes (one commit per file — the autocommit semantics of the
        filesystem underneath), ``rm``/``mv``/``mkdir`` map to the
        filesystem's verbs, and ``chmod``/``utimens`` are accepted and
        ignored. On a ``bzz://`` mount every commit yields a new root; the
        mount keeps showing the latest (read-your-writes) and the final
        root is logged and returned via ``fs.latest(...)`` — this function
        logs it at unmount. On ``bzzf://`` each commit publishes the feed.
        A Swarm filesystem is checked for a usable postage stamp *before*
        mounting. Not ``kernel_cache``d (content moves).
    **storage_options:
        Passed to ``url_to_fs`` — ``api_url``, ``allow_gateway``,
        ``verify``, ``feed_ttl``… For a chained URL, key them by protocol
        the fsspec way: ``bzz={"api_url": ...}``.

    Raises
    ------
    ValueError
        ``url`` does not resolve to a Swarm filesystem.
    FileNotFoundError / NotADirectoryError
        the mountpoint is missing or not a directory, or the reference has
        no such path.
    ImportError / OSError
        fusepy or libfuse is missing (message says what to install).
    """
    url = normalize_url(url)
    if fs is None:
        fs, path = url_to_fs(url, **storage_options)
        inner = swarm_filesystem_of(fs)
    else:
        path = fs._strip_protocol(url) or getattr(fs, "root_marker", "")
        try:
            inner = swarm_filesystem_of(fs)
        except ValueError:
            inner = None  # a foreign fsspec filesystem: same policy, no kernel_cache

    if not os.path.isdir(mountpoint):
        if os.path.exists(mountpoint):
            raise NotADirectoryError(f"mountpoint is not a directory: {mountpoint}")
        raise FileNotFoundError(
            f"mountpoint does not exist: {mountpoint} (create it first — an "
            "empty directory)")

    # Resolve before mounting: a bad reference or an unreachable node
    # should fail here, with swarmfs's own message, not as EIO in a mount.
    info = fs.info(path)
    if info["type"] == "file":
        raise NotADirectoryError(
            f"{url} is a file; FUSE mounts a directory — mount its parent "
            "and read the file inside")

    if rw and inner is not None and getattr(inner, "_local", None) is None:
        # fail early, the swarmfs way: a mount that cannot commit should not
        # come up and then refuse every save with EACCES
        from fsspec.asyn import sync

        from .stamps import StampManager

        sync(inner.loop, StampManager(inner.client).resolve, inner.stamp)

    fuse = _import_fusepy()
    ro_cls, rw_cls = _ops_class()
    ops = (rw_cls if rw else ro_cls)(fs, path, ready_file=ready_file)

    options: dict = {"subtype": "swarmfs"}
    if not rw:
        options["ro"] = True
    name = fsname if fsname is not None else url
    if name and not re.search(r"[,\s]", name):
        options["fsname"] = name  # what `mount` and `df` display as the source
    if inner is not None and not rw and not isinstance(inner, SwarmFeedFileSystem):
        # content at a fixed bzz:// path can never change: cached pages are
        # correct forever. A feed's content moves, so no kernel_cache there;
        # nor for a foreign filesystem, whose paths may change meaning, nor
        # for a writable mount.
        options["kernel_cache"] = True
    if allow_other:
        options["allow_other"] = True

    logger.info("mounting %s at %s (%s)", url, mountpoint,
                ", ".join(f"{k}={v}" for k, v in options.items()))

    def run():
        try:
            fuse.FUSE(ops, mountpoint, foreground=True, nothreads=not threads, **options)
        finally:
            if rw and inner is not None and not isinstance(inner, SwarmFeedFileSystem):
                heads = {}
                for res in getattr(inner, "commit_log", []):
                    origin = inner._origin.get(res.new_root, res.old_root or res.new_root)
                    heads[origin] = inner.latest(res.new_root)
                for origin, head in heads.items():
                    logger.warning("unmounted: bzz://%s is now bzz://%s", origin, head)
                    print(f"bzz://{head}", flush=True)

    if not foreground:
        th = threading.Thread(target=run, name=f"swarmfs-fuse {mountpoint}", daemon=True)
        th.start()
        return th
    try:
        run()
    except KeyboardInterrupt:  # pragma: no cover - libfuse normally handles SIGINT
        pass
    return None
