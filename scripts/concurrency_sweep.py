#!/usr/bin/env python3
"""How many requests should be in flight against *your* Bee node?

Times two operations at a range of concurrency levels and prints a table:

- ``read``: ``GET /bytes/<ref>`` of blobs the node does not hold, so it
  has to fetch them from the network. A read leaves the chunk on the node,
  after which it is answered in about a millisecond, so every level gets
  its own unread refs; give the script enough of them (it says how many).
  Refs of a store you published *from another node*, or have not read
  since publishing it from this one, work: a light node keeps few of the
  chunks it uploads.
- ``stewardship``: ``GET /stewardship/<ref>``, the network check the sync
  worker uses to confirm a root. Its answer is not cached, so refs can be
  reused.

The best number depends on the node (light or full, its peers, how its
bandwidth accounting throttles) more than on the machine running this:
the client spends about a millisecond of CPU per request. swarmfs's
defaults were measured with this script; see ``CHECK_CONCURRENCY`` in
``swarmfs/localsync.py``.

    scripts/concurrency_sweep.py --refs refs.txt --op read
    scripts/concurrency_sweep.py --refs refs.txt --op stewardship --levels 8,16,32,64

``refs.txt`` holds one 64-hex reference per line. Nothing is uploaded and
no postage is used; reads from the network cost the node's usual (tiny)
bandwidth payments.
"""

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from swarmfs._client import SwarmClient  # noqa: E402


async def measure(api, op, refs, concurrency):
    client = SwarmClient(api, timeout=300)
    gate = asyncio.Semaphore(concurrency)
    latencies, errors = [], []

    async def one(ref):
        async with gate:
            start = time.perf_counter()
            try:
                if op == "read":
                    await client.bytes_get(ref)
                elif not await client.stewardship_get(ref):
                    errors.append("not retrievable")
            except Exception as e:  # count it; one failure must not end the run
                errors.append(type(e).__name__)
            latencies.append(time.perf_counter() - start)

    wall = time.perf_counter()
    await asyncio.gather(*(one(ref) for ref in refs))
    wall = time.perf_counter() - wall
    await client.close()
    latencies.sort()
    return wall, latencies[len(latencies) // 2], errors


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--api", default=os.environ.get("BEE_API_URL", "http://localhost:1633"))
    ap.add_argument("--refs", required=True, help="file with one reference per line")
    ap.add_argument("--op", choices=("read", "stewardship"), default="read")
    ap.add_argument("--levels", default="1,2,4,8,16,32,48,64,128",
                    help="comma-separated concurrency levels")
    ap.add_argument("--per-request", type=int, default=25,
                    help="requests per level and per unit of concurrency "
                         "(at least --min, at most --cap)")
    ap.add_argument("--min", type=int, default=100, help="fewest requests per level")
    ap.add_argument("--cap", type=int, default=800, help="most requests per level")
    ap.add_argument("--pause", type=float, default=20,
                    help="seconds of quiet between levels, so one level's load "
                         "has drained before the next starts")
    args = ap.parse_args()

    with open(args.refs) as fh:
        refs = [line.strip() for line in fh if line.strip()]
    levels = [int(x) for x in args.levels.split(",")]
    sizes = [max(args.min, min(args.cap, args.per_request * c)) for c in levels]
    if args.op == "read" and sum(sizes) > len(refs):
        sys.exit(f"reads need {sum(sizes)} unread refs for these levels; "
                 f"{args.refs} has {len(refs)}")

    print(f"{args.op} against {args.api}")
    print(f"{'at once':>8} {'requests':>9} {'per s':>8} {'median':>9}  errors")
    used = 0
    for i, (c, n) in enumerate(zip(levels, sizes)):
        if i:
            time.sleep(args.pause)
        if args.op == "read":
            batch, used = refs[used:used + n], used + n
        else:
            batch = (refs * (n // len(refs) + 1))[:n]
        wall, median, errors = asyncio.run(measure(args.api, args.op, batch, c))
        print(f"{c:8d} {n:9d} {n / wall:8.1f} {1000 * median:7.0f} ms  "
              f"{len(errors)}{' ' + str(sorted(set(errors))) if errors else ''}",
              flush=True)


if __name__ == "__main__":
    main()
