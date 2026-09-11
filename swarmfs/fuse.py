"""Mount a Swarm reference or feed as a local directory, read-only, via FUSE.

This is fsspec's generic FUSE wrapper (``fsspec.fuse.FUSEr``, built on
`fusepy <https://github.com/fusepy/fusepy>`_) over the existing backend —
not a second implementation of anything. What swarmfs adds is policy and
polish:

- **Read-only, enforced twice.** The kernel mounts with the ``ro`` flag, so
  writes fail with EROFS before they reach Python, and every mutating
  operation here raises EROFS as well. A ``bzz://`` reference is immutable
  by construction, and a ``bzzf://`` feed is a *view* that follows the
  feed's updates (``feed_ttl``) — neither is a place to type into. Writing
  through a mount (each ``release`` a commit) is a possible follow-up, not
  a thing this module half-does.
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
import re
import stat
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

    return SwarmFUSEr


def mount(
    url: str,
    mountpoint: str,
    *,
    foreground: bool = True,
    threads: bool = False,
    ready_file: bool = False,
    allow_other: bool = False,
    fs=None,
    **storage_options,
):
    """Mount ``url`` at ``mountpoint``, read-only.

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
        A pre-built filesystem to mount instead of resolving ``url``
        (tests inject a fake-node instance); ``url`` then only supplies
        the path.
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
    else:
        path = fs._strip_protocol(url)
    inner = swarm_filesystem_of(fs)

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

    fuse = _import_fusepy()
    ops_cls = _ops_class()
    ops = ops_cls(fs, path, ready_file=ready_file)

    options: dict = {"ro": True, "subtype": "swarmfs"}
    if not re.search(r"[,\s]", url):
        options["fsname"] = url  # what `mount` and `df` display as the source
    if not isinstance(inner, SwarmFeedFileSystem):
        # content at a fixed bzz:// path can never change: cached pages are
        # correct forever. A feed's content moves, so no kernel_cache there.
        options["kernel_cache"] = True
    if allow_other:
        options["allow_other"] = True

    logger.info("mounting %s at %s (%s)", url, mountpoint,
                ", ".join(f"{k}={v}" for k, v in options.items()))

    def run():
        fuse.FUSE(ops, mountpoint, foreground=True, nothreads=not threads, **options)

    if not foreground:
        th = threading.Thread(target=run, name=f"swarmfs-fuse {mountpoint}", daemon=True)
        th.start()
        return th
    try:
        run()
    except KeyboardInterrupt:  # pragma: no cover - libfuse normally handles SIGINT
        pass
    return None
