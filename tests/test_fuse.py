"""The standalone FUSE mount (``swarmfs.fuse`` / ``swarmfs mount``).

Two layers: URL/option handling and the CLI run everywhere; the mount
itself (marker ``fuse``) needs fusepy + libfuse 2 + ``/dev/fuse`` +
``fusermount`` and skips — naming the missing piece — where it cannot run.
It mounts the offline fake-node filesystem from conftest and reads it back
through the kernel, so the whole chain kernel → fusepy → FUSEr → fsspec
sync wrappers → Mantaray walk is exercised without a Bee node.
"""

from __future__ import annotations

import errno
import os
import shutil
import stat
import subprocess
import threading
import time

import pytest

from swarmfs import SwarmFileSystem
from swarmfs.cli import _storage_options, build_parser, main
from swarmfs.fuse import normalize_url, swarm_filesystem_of

from conftest import FILES, FakeClient

REF = "ab" * 32


# ---------------------------------------------------------------- no FUSE needed

def test_normalize_url_accepts_bare_references():
    assert normalize_url(REF) == f"bzz://{REF}"
    assert normalize_url(f"{REF}/data/sub") == f"bzz://{REF}/data/sub"
    assert normalize_url("cd" * 64) == f"bzz://{'cd' * 64}"  # encrypted ref
    assert normalize_url(f"bzz://{REF}") == f"bzz://{REF}"
    assert normalize_url(f"bzzf://{'11' * 20}/topic") == f"bzzf://{'11' * 20}/topic"
    assert normalize_url(f"simplecache::bzz://{REF}") == f"simplecache::bzz://{REF}"
    assert normalize_url("memory://x") == "memory://x"  # not ours; left alone


def test_swarm_filesystem_of_unwraps_chains(fs):
    import fsspec

    fs, root = fs
    assert swarm_filesystem_of(fs) is fs
    cached = fsspec.filesystem("simplecache", fs=fs)
    assert swarm_filesystem_of(cached) is fs
    with pytest.raises(ValueError, match="not a Swarm URL"):
        swarm_filesystem_of(fsspec.filesystem("memory"))


def test_mount_refuses_non_swarm_url_before_touching_fuse(tmp_path):
    from swarmfs.fuse import mount

    with pytest.raises(ValueError, match="not a Swarm URL"):
        mount("memory://whatever", str(tmp_path))


def test_mount_checks_mountpoint_and_reference_first(fs, tmp_path):
    """Setup errors surface in the terminal, before anything is mounted."""
    from swarmfs.fuse import mount

    fs, root = fs
    with pytest.raises(FileNotFoundError, match="mountpoint does not exist"):
        mount(f"bzz://{root}", str(tmp_path / "missing"), fs=fs)
    (tmp_path / "file").write_text("x")
    with pytest.raises(NotADirectoryError, match="not a directory"):
        mount(f"bzz://{root}", str(tmp_path / "file"), fs=fs)
    with pytest.raises(FileNotFoundError):
        mount(f"bzz://{root}/nope", str(tmp_path), fs=fs)
    with pytest.raises(NotADirectoryError, match="is a file"):
        mount(f"bzz://{root}/index.html", str(tmp_path), fs=fs)


def test_cli_parses_mount_options():
    args = build_parser().parse_args([
        "mount", REF, "/mnt/x", "--api-url", "http://bee:1633", "--allow-gateway",
        "--no-verify", "--timeout", "30", "-o", "block_size=4096", "-o", "pin=true",
    ])
    assert args.command == "mount"
    assert _storage_options(args) == {
        "api_url": "http://bee:1633", "allow_gateway": True, "verify": False,
        "timeout": 30.0, "block_size": 4096, "pin": True,
    }


def test_cli_keys_options_by_protocol_for_chained_urls():
    args = build_parser().parse_args([
        "mount", f"simplecache::bzz://{REF}", "/mnt/x", "--api-url", "http://bee:1633",
        "-o", "simplecache-cache_storage=/tmp/c", "-o", "feed_ttl=5",
    ])
    assert _storage_options(args) == {
        "bzz": {"api_url": "http://bee:1633", "feed_ttl": 5},
        "simplecache": {"cache_storage": "/tmp/c"},
    }
    args = build_parser().parse_args(["mount", f"bzzf://{'11' * 20}/t", "/mnt", "--feed-ttl", "2"])
    assert _storage_options(args) == {"feed_ttl": 2.0}


def test_cli_entry_points(capsys):
    assert main([]) == 2  # help, no command
    assert "mount" in capsys.readouterr().out
    with pytest.raises(SystemExit) as e:
        main(["--version"])
    assert e.value.code == 0
    # a setup error is a message and exit 1, not a traceback
    assert main(["mount", "memory://x", "/nonexistent-mountpoint"]) == 1
    assert "not a Swarm URL" in capsys.readouterr().err


# ------------------------------------------------------------------ the mount

def _fuse_unavailable() -> str | None:
    try:
        import fuse  # noqa: F401
    except ImportError:
        return "fusepy not installed (pip install 'swarmfs[fuse]')"
    except OSError as e:  # fusepy: EnvironmentError('Unable to find libfuse')
        return f"libfuse 2 not available: {e}"
    if not os.path.exists("/dev/fuse"):
        return "/dev/fuse missing (container without FUSE?)"
    if not os.access("/dev/fuse", os.R_OK | os.W_OK):
        return "/dev/fuse not accessible to this user"
    if not (shutil.which("fusermount") or shutil.which("fusermount3")):
        return "fusermount not on PATH"
    return None


def _unmount(mountpoint: str) -> None:
    for cmd in (["fusermount", "-u"], ["fusermount3", "-u"], ["umount"]):
        if shutil.which(cmd[0]):
            r = subprocess.run([*cmd, mountpoint], capture_output=True, text=True)
            if r.returncode == 0:
                return
    raise RuntimeError(f"could not unmount {mountpoint}")


@pytest.mark.fuse
def test_mount_serves_the_manifest_read_only(fs, tmp_path):
    reason = _fuse_unavailable()
    if reason:
        pytest.skip(reason)
    from swarmfs.fuse import mount

    fs, root = fs
    mp = tmp_path / "mnt"
    mp.mkdir()
    th = mount(f"bzz://{root}", str(mp), fs=fs, foreground=False, ready_file=True)
    assert isinstance(th, threading.Thread)
    try:
        deadline = time.monotonic() + 15
        while not os.path.exists(mp / ".fuse_ready"):
            if not th.is_alive():
                pytest.fail("FUSE thread died before the mount became ready")
            if time.monotonic() > deadline:
                pytest.fail("mount did not become ready within 15 s")
            time.sleep(0.05)

        # directory listing (the ready file is a probe, not an entry)
        assert sorted(os.listdir(mp)) == ["a", "assets", "data", "data-old", "index.html"]
        assert sorted(os.listdir(mp / "data")) == ["part-00000.parquet", "part-00001.parquet"]

        # contents, through the kernel: whole file, nested, deep path, offsets
        assert (mp / "index.html").read_bytes() == FILES["index.html"]
        assert (mp / "assets/css/site.css").read_bytes() == FILES["assets/css/site.css"]
        deep = "a/very/deeply/nested/directory/structure/with/a/long/path/file.bin"
        assert (mp / deep).read_bytes() == FILES[deep]
        with open(mp / "data/part-00001.parquet", "rb") as f:
            f.seek(4000)
            assert f.read(10) == FILES["data/part-00001.parquet"][4000:4010]

        # attributes: real sizes, read-only modes, owned by us, stable times
        st = os.stat(mp / "data/part-00000.parquet")
        assert stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o444
        assert st.st_size == len(FILES["data/part-00000.parquet"])
        assert st.st_uid == os.getuid()
        dst = os.stat(mp / "assets")
        assert stat.S_ISDIR(dst.st_mode) and stat.S_IMODE(dst.st_mode) == 0o555
        assert os.stat(mp / "index.html").st_mtime == st.st_mtime

        # errno mapping
        with pytest.raises(FileNotFoundError):
            os.stat(mp / "does-not-exist")
        with pytest.raises(NotADirectoryError):
            os.listdir(mp / "index.html")

        # read-only, whichever lock the kernel reaches first
        for attempt in (
            lambda: open(mp / "new.txt", "wb"),
            lambda: open(mp / "index.html", "ab"),
            lambda: os.mkdir(mp / "newdir"),
            lambda: os.unlink(mp / "index.html"),
            lambda: os.rename(mp / "index.html", mp / "x"),
        ):
            with pytest.raises(OSError) as e:
                attempt()
            assert e.value.errno in (errno.EROFS, errno.EACCES, errno.EPERM), e.value
    finally:
        _unmount(str(mp))
        th.join(timeout=10)
    assert not th.is_alive(), "FUSE loop did not exit after unmount"
    assert os.listdir(mp) == []  # the mountpoint is an ordinary empty dir again


@pytest.mark.fuse
def test_mount_root_can_be_a_subdirectory(fs, tmp_path):
    reason = _fuse_unavailable()
    if reason:
        pytest.skip(reason)
    from swarmfs.fuse import mount

    fs, root = fs
    mp = tmp_path / "mnt"
    mp.mkdir()
    th = mount(f"{root}/data", str(mp), fs=fs, foreground=False, ready_file=True)
    try:
        deadline = time.monotonic() + 15
        while not os.path.exists(mp / ".fuse_ready"):
            assert th.is_alive(), "FUSE thread died"
            assert time.monotonic() < deadline, "mount not ready"
            time.sleep(0.05)
        assert sorted(os.listdir(mp)) == ["part-00000.parquet", "part-00001.parquet"]
        assert (mp / "part-00000.parquet").read_bytes() == FILES["data/part-00000.parquet"]
    finally:
        _unmount(str(mp))
        th.join(timeout=10)


@pytest.mark.fuse
def test_any_fsspec_filesystem_mounts_through_fs(tmp_path):
    """``fs=`` accepts a foreign fsspec filesystem: same read-only policy,
    attributes and errno mapping (ontodag-fs mounts its view this way)."""
    reason = _fuse_unavailable()
    if reason:
        pytest.skip(reason)
    import fsspec

    from swarmfs.fuse import mount

    mem = fsspec.filesystem("memory")
    mem.pipe_file("/mounted/a.txt", b"alpha")
    mem.pipe_file("/mounted/sub/b.txt", b"beta")
    mp = tmp_path / "mnt"
    mp.mkdir()
    th = mount("memory:///mounted", str(mp), fs=mem, fsname="memory-view",
               foreground=False, ready_file=True)
    try:
        deadline = time.monotonic() + 15
        while not os.path.exists(mp / ".fuse_ready"):
            assert th.is_alive(), "FUSE thread died"
            assert time.monotonic() < deadline, "mount not ready"
            time.sleep(0.05)
        assert sorted(os.listdir(mp)) == ["a.txt", "sub"]
        assert (mp / "sub/b.txt").read_bytes() == b"beta"
        assert stat.S_IMODE(os.stat(mp / "a.txt").st_mode) == 0o444
        with pytest.raises(OSError) as e:
            open(mp / "new.txt", "wb")
        assert e.value.errno in (errno.EROFS, errno.EACCES, errno.EPERM)
        with pytest.raises(FileNotFoundError):
            os.stat(mp / "nope")
    finally:
        _unmount(str(mp))
        th.join(timeout=10)


@pytest.mark.fuse
def test_bzzf_mount_is_a_live_view_of_the_feed(manifest, tmp_path):
    """A feed mount stays in feed coordinates and follows updates: the
    reader instance re-resolves the feed after ``feed_ttl``, and the mount
    passes no ``kernel_cache``, so a second read sees the new content."""
    reason = _fuse_unavailable()
    if reason:
        pytest.skip(reason)
    pytest.importorskip("eth_keys")
    from swarmfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedSigner
    from swarmfs.fuse import mount

    from conftest import FakeClient

    key = bytes(range(1, 33)).hex()
    owner = FeedSigner(key).owner_hex
    _, store = manifest
    writer = SwarmFeedFileSystem(client=FakeClient(store), signer=key,
                                 skip_instance_cache=True)
    writer.pipe_file(f"bzzf://{owner}/mounted/state.txt", b"v1")
    reader = SwarmFeedFileSystem(client=FakeClient(store), feed_ttl=0.2,
                                 skip_instance_cache=True)  # no key: readers need none

    mp = tmp_path / "mnt"
    mp.mkdir()
    th = mount(f"bzzf://{owner}/mounted", str(mp), fs=reader, foreground=False,
               ready_file=True)
    try:
        deadline = time.monotonic() + 15
        while not os.path.exists(mp / ".fuse_ready"):
            assert th.is_alive(), "FUSE thread died"
            assert time.monotonic() < deadline, "mount not ready"
            time.sleep(0.05)
        assert os.listdir(mp) == ["state.txt"]
        assert (mp / "state.txt").read_bytes() == b"v1"

        writer.pipe_file(f"bzzf://{owner}/mounted/state.txt", b"v2")
        # feed_ttl (0.2 s) plus the kernel's default 1 s attribute cache
        time.sleep(1.5)
        assert (mp / "state.txt").read_bytes() == b"v2"
    finally:
        _unmount(str(mp))
        th.join(timeout=10)


# ------------------------------------------------------------- writable mount

def _wait_ready(mp, th):
    deadline = time.monotonic() + 15
    while not os.path.exists(mp / ".fuse_ready"):
        assert th.is_alive(), "FUSE thread died before the mount was ready"
        assert time.monotonic() < deadline, "mount did not become ready"
        time.sleep(0.05)


@pytest.mark.fuse
def test_rw_mount_commits_on_release(fs, tmp_path):
    """Every saved file is one commit; the mount shows the new state; the
    lineage head advances (read-your-writes through the root map)."""
    reason = _fuse_unavailable()
    if reason:
        pytest.skip(reason)
    from swarmfs.fuse import mount

    fs, root = fs
    mp = tmp_path / "mnt"
    mp.mkdir()
    th = mount(f"bzz://{root}", str(mp), fs=fs, foreground=False, ready_file=True, rw=True)
    try:
        _wait_ready(mp, th)
        assert stat.S_IMODE(os.stat(mp / "index.html").st_mode) == 0o644

        # create + write + close: one commit, visible through the mount and the fs
        (mp / "data" / "new.txt").write_bytes(b"fresh")
        assert (mp / "data" / "new.txt").read_bytes() == b"fresh"
        assert len(fs.commit_log) == 1
        head = fs.latest(root)
        assert head != root
        assert fs.cat_file(f"bzz://{head}/data/new.txt") == b"fresh"
        assert fs.cat_file(f"bzz://{root}/data/new.txt") == b"fresh"  # read-your-writes

        # getattr right after create, before release (the kernel does this)
        with open(mp / "data" / "partial.txt", "wb") as f:
            f.write(b"half")
            f.flush()
            assert os.stat(mp / "data" / "partial.txt").st_size == 4
            assert "partial.txt" in os.listdir(mp / "data")
        assert len(fs.commit_log) == 2

        # overwrite (O_TRUNC) and append
        (mp / "index.html").write_bytes(b"<h1>rewritten</h1>")
        with open(mp / "index.html", "ab") as f:
            f.write(b"<!-- more -->")
        assert (mp / "index.html").read_bytes() == b"<h1>rewritten</h1><!-- more -->"
        assert fs.cat_file(f"bzz://{root}/index.html") == b"<h1>rewritten</h1><!-- more -->"

        # mkdir (implicit in manifests): phantom until content lands
        os.mkdir(mp / "newdir")
        assert os.path.isdir(mp / "newdir") and os.listdir(mp / "newdir") == []
        os.mkdir(mp / "newdir" / "sub")
        with pytest.raises(OSError) as e:
            os.rmdir(mp / "newdir")
        assert e.value.errno == errno.ENOTEMPTY
        os.rmdir(mp / "newdir" / "sub")
        (mp / "newdir" / "inside.txt").write_bytes(b"in")
        assert sorted(os.listdir(mp / "newdir")) == ["inside.txt"]
        assert fs.exists(f"bzz://{root}/newdir/inside.txt")

        # unlink and rename go through the filesystem's verbs
        os.unlink(mp / "data-old" / "readme.md")
        assert not fs.exists(f"bzz://{root}/data-old/readme.md")
        with pytest.raises(FileNotFoundError):
            os.stat(mp / "data-old" / "readme.md")
        os.rename(mp / "data" / "new.txt", mp / "assets" / "moved.txt")
        assert (mp / "assets" / "moved.txt").read_bytes() == b"fresh"
        assert not fs.exists(f"bzz://{root}/data/new.txt")
        os.rename(mp / "newdir", mp / "renamed")
        assert (mp / "renamed" / "inside.txt").read_bytes() == b"in"

        # metadata a content address does not have: accepted, ignored
        os.chmod(mp / "index.html", 0o600)
        os.utime(mp / "index.html", (1, 1))
        shutil.copy2(mp / "index.html", mp / "copy.html")  # cp -p's dance
        assert (mp / "copy.html").read_bytes() == (mp / "index.html").read_bytes()

        # truncate an existing file in place
        os.truncate(mp / "copy.html", 4)
        assert (mp / "copy.html").read_bytes() == b"<h1>"

        # the shell's `> file`: open, dup2, close(fd), write, close(1) — the
        # kernel flushes on both closes; that must be ONE commit, of the
        # content (measured live: it used to commit an empty file first)
        n = len(fs.commit_log)
        subprocess.run(f"echo 'from a shell' > '{mp / 'shell.txt'}'", shell=True, check=True)
        assert (mp / "shell.txt").read_bytes() == b"from a shell\n"
        assert len(fs.commit_log) == n + 1
        # an untouched new file is still a (single, empty) commit at release
        subprocess.run(f"touch '{mp / 'empty.txt'}'", shell=True, check=True)
        deadline = time.monotonic() + 5
        while len(fs.commit_log) < n + 2 and time.monotonic() < deadline:
            time.sleep(0.05)  # release is asynchronous
        assert len(fs.commit_log) == n + 2 and (mp / "empty.txt").read_bytes() == b""
    finally:
        _unmount(str(mp))
        th.join(timeout=10)
    assert not th.is_alive()
    # and the whole history is snapshots: the original root is untouched
    assert fs.cat_file(f"bzz://{root}/index.html") != b"<h1>hello swarm</h1>"  # via head
    fresh = SwarmFileSystem(client=fs.client, skip_instance_cache=True)
    assert fresh.cat_file(f"bzz://{root}/index.html") == b"<h1>hello swarm</h1>"


def test_rw_mount_checks_the_stamp_before_mounting(manifest, tmp_path):
    from swarmfs import StampError
    from swarmfs.fuse import mount

    root, store = manifest
    fs = SwarmFileSystem(client=FakeClient(store, stamps=[]), skip_instance_cache=True)
    mp = tmp_path / "mnt"
    mp.mkdir()
    with pytest.raises(StampError):
        mount(f"bzz://{root}", str(mp), fs=fs, rw=True)
    assert os.listdir(mp) == []  # nothing was mounted


@pytest.mark.fuse
def test_rw_bzzf_mount_publishes_the_feed(manifest, tmp_path):
    reason = _fuse_unavailable()
    if reason:
        pytest.skip(reason)
    pytest.importorskip("eth_keys")
    from swarmfs import SwarmFeedFileSystem
    from swarmfs.feeds import FeedSigner
    from swarmfs.fuse import mount

    from conftest import FakeClient

    key = bytes(range(1, 33)).hex()
    owner = FeedSigner(key).owner_hex
    _, store = manifest
    writer = SwarmFeedFileSystem(client=FakeClient(store), signer=key, skip_instance_cache=True)
    writer.pipe_file(f"bzzf://{owner}/notes/first.txt", b"one")
    mp = tmp_path / "mnt"
    mp.mkdir()
    th = mount(f"bzzf://{owner}/notes", str(mp), fs=writer, foreground=False,
               ready_file=True, rw=True)
    try:
        _wait_ready(mp, th)
        (mp / "second.txt").write_bytes(b"two")
        os.unlink(mp / "first.txt")
    finally:
        _unmount(str(mp))
        th.join(timeout=10)
    reader = SwarmFeedFileSystem(client=FakeClient(store), skip_instance_cache=True)
    assert reader.ls(f"bzzf://{owner}/notes", detail=False) == [f"{owner}/notes/second.txt"]
    assert reader.cat_file(f"bzzf://{owner}/notes/second.txt") == b"two"
