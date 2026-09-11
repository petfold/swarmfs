"""``swarmfs`` console entry point.

One subcommand, ``mount``, because a mount is a *process* (it has to stay
running), which no library call can be. Everything else — buying stamps,
uploading, feeds — stays a library matter here and a CLI matter in
`swarm-cli <https://github.com/ethersphere/swarm-cli>`_ (scope boundary,
deliberate; see CLAUDE.md).
"""

from __future__ import annotations

import argparse
import re
import logging
import sys

from . import __version__


def _typed(value: str):
    """``-o key=value`` values: ``true``/``false`` → bool, digits → int,
    else the string."""
    low = value.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    try:
        return float(value)
    except ValueError:
        return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="swarmfs",
        description="swarmfs — Ethereum Swarm as an fsspec filesystem.",
    )
    parser.add_argument("--version", action="version", version=f"swarmfs {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    m = sub.add_parser(
        "mount",
        help="mount a bzz:// reference or bzzf:// feed as a local directory (read-only)",
        description=(
            "Mount a Swarm reference or feed as a local directory, read-only, "
            "via FUSE. Blocks until unmounted (Ctrl-C, or `fusermount -u "
            "<mountpoint>` from another shell). Needs `pip install "
            "\"swarmfs[fuse]\"` and a system libfuse 2."
        ),
        epilog=(
            "examples:\n"
            "  swarmfs mount bzz://<64-hex-ref> ~/mnt/dataset\n"
            "  swarmfs mount <64-hex-ref>/subdir ~/mnt/subdir\n"
            "  swarmfs mount bzzf://<owner>/<topic> ~/mnt/live   # follows the feed\n"
            "  swarmfs mount 'simplecache::bzz://<ref>' ~/mnt/cached "
            "-o simplecache-cache_storage=/tmp/swarm-cache\n"
            "  swarmfs mount --api-url https://gateway.example --allow-gateway bzz://<ref> ~/mnt/x"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    m.add_argument("url", help="bzz://<ref>[/path], bzzf://<owner>/<topic>[/path], "
                               "a bare 64/128-hex reference, or an fsspec chain "
                               "(simplecache::bzz://...)")
    m.add_argument("mountpoint", help="an existing directory")
    m.add_argument("--api-url", help="Bee API endpoint (default: $BEE_API_URL, "
                                     "then http://localhost:1633)")
    m.add_argument("--allow-gateway", action="store_true",
                   help="permit an endpoint that is not your own node "
                        "(chunk verification turns on automatically)")
    v = m.add_mutually_exclusive_group()
    v.add_argument("--verify", dest="verify", action="store_true", default=None,
                   help="force client-side chunk verification on")
    v.add_argument("--no-verify", dest="verify", action="store_false",
                   help="force it off")
    m.add_argument("--timeout", type=float, help="per-request timeout in seconds")
    m.add_argument("--feed-ttl", type=float,
                   help="bzzf:// only: seconds a feed resolution is cached (default 15)")
    m.add_argument("-o", "--option", action="append", default=[], metavar="KEY=VALUE",
                   help="extra storage option for the filesystem; for a chained URL "
                        "prefix the protocol: -o simplecache-cache_storage=/tmp/c")
    m.add_argument("--rw", action="store_true",
                   help="writable mount: each saved file is one commit (bzz://: a new "
                        "root, printed at unmount; bzzf://: a feed update). Needs a "
                        "usable stamp; checked before mounting")
    m.add_argument("--stamp", help="postage batch id for --rw (default: auto — the "
                                   "usable batch with the longest TTL)")
    m.add_argument("--signer", help="bzzf:// --rw: the feed owner's private key (hex)")
    m.add_argument("--threads", action="store_true",
                   help="let FUSE serve operations concurrently (default: serial)")
    m.add_argument("--allow-other", action="store_true",
                   help="let other users access the mount (needs user_allow_other "
                        "in /etc/fuse.conf)")
    m.add_argument("--debug", action="store_true", help="log every operation to stderr")
    return parser


def _storage_options(args) -> dict:
    """Assemble storage options; for a chained URL, key the Swarm ones by
    protocol the way ``fsspec.core.url_to_fs`` distributes them."""
    from .fuse import normalize_url

    url = normalize_url(args.url)
    swarm = {}
    if args.api_url:
        swarm["api_url"] = args.api_url
    if args.allow_gateway:
        swarm["allow_gateway"] = True
    if args.verify is not None:
        swarm["verify"] = args.verify
    if args.timeout is not None:
        swarm["timeout"] = args.timeout
    if args.feed_ttl is not None:
        swarm["feed_ttl"] = args.feed_ttl
    if getattr(args, "stamp", None):
        swarm["stamp"] = args.stamp
    if getattr(args, "signer", None):
        swarm["signer"] = args.signer

    chained = "::" in url
    proto = "bzzf" if "bzzf://" in url else "bzz"
    options: dict = {proto: swarm} if chained else dict(swarm)
    for item in args.option:
        key, sep, value = item.partition("=")
        if not sep:
            raise ValueError(f"bad -o option {item!r}: expected KEY=VALUE")
        value = _typed(value)
        if "-" in key:  # fsspec convention: <protocol>-<option>
            fs_name, setting = key.split("-", 1)
            options.setdefault(fs_name, {})[setting] = value
        elif chained:
            options.setdefault(proto, {})[key] = value
        else:
            options[key] = value
    if chained and not options.get(proto):
        options.pop(proto, None)
    return options


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 2
    if args.command == "mount":
        logging.basicConfig(
            level=logging.DEBUG if args.debug else logging.INFO,
            format="%(asctime)s %(name)s %(levelname)s %(message)s",
            stream=sys.stderr,
        )
        if not args.debug:
            logging.getLogger("fuse").setLevel(logging.WARNING)
        from .fuse import mount

        import aiohttp

        try:
            options = _storage_options(args)
            mount(args.url, args.mountpoint, threads=args.threads,
                  allow_other=args.allow_other, rw=args.rw, **options)
        except (ImportError, OSError, ValueError, aiohttp.ClientError) as e:
            # setup errors (no libfuse, bad URL, unreachable node, refused
            # gateway, missing reference): one line, exit 1 — the traceback
            # only with --debug
            if args.debug:
                logging.getLogger("swarmfs.cli").exception("mount failed")
            print(f"swarmfs mount: {e}", file=sys.stderr)
            return 1
        return 0
    parser.error(f"unknown command {args.command!r}")  # pragma: no cover
    return 2  # pragma: no cover
