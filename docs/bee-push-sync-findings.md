# Bee: uploads report success while chunks are unretrievable (shallow receipts swallowed)

**Where this lives:** `swarmfs/docs/` because the push path this describes is
what `localstore`/`localsync` sit on, though it was found while publishing
ontodag's packs. Raw evidence is in `bee-push-sync-evidence/`; `topology.json`
(892 KB) and `buckets.json` (2.2 MB) were summarised rather than committed —
see `topology-and-buckets-summary.json`, and regenerate the originals with
`GET /topology` and `GET /stamps/<batch>/buckets`.

**Not filed upstream.** The observability claim holds, but the data-loss claim
could not be reproduced with a plain HTTP upload, and Bee offers tags and
`query_upload_progress` for exactly the "did it land" question — see the
negative-result section below. Re-read that before filing anything.

**Observed on:** Bee `2.8.2-7e703f49`, API `8.1.1`, **light node**, Gnosis mainnet,
via Swarm Desktop. 2026-09-11, ~03:00-03:30 UTC.

## Summary

Uploading ~250k chunks in eleven batches, the node reported every upload as
accepted and its pusher queue as fully drained — `bee_pusher_total_to_push ==
bee_pusher_total_synced` — while **two of eleven root references were not
retrievable**, and stayed unretrievable indefinitely.

The cause is visible in the metrics: **29,602 shallow receipts**, tracking the
**29,354 pusher "errors"** almost one-for-one. A shallow receipt means the chunk
was receipted by a peer that is not deep enough to be its storer.

Bee does retry such chunks by design — v1.0.0 added "retrying the upload of those
chunks from which they suspect they did not land in their natural location", and
#1423 tuned the timeouts around it. What this report is about is what happens
**after those retries are exhausted**: the chunk is counted in
`bee_pusher_total_synced`, `to_push` equals `synced`, the HTTP upload returned
success, and nothing in the node's state distinguishes "stored" from "handed to
a peer that was not its storer". The data is stranded silently.

Three things seem wrong, in decreasing order of importance:

1. **An exhausted shallow-receipt retry is recorded as success.** The chunk
   lands in `total_synced`, so the node's "queue drained" state coexists with
   content that no one can retrieve. Whether the right fix is more retries or
   an honest failure state is for maintainers to judge — but the two should
   not be indistinguishable from outside.
2. **Upload success is not a meaningful signal.** The HTTP upload succeeded and
   the CLI exited 0 for all eleven. The only way to discover the failure was to
   poll `GET /stewardship/<ref>` per root afterwards.
3. **`PUT /stewardship/<ref>` fails opaquely.** While the node no longer held
   the chunks locally it returned `500 {"code":500,"message":"re-upload
   failed"}` with no indication of *why* — it cannot re-upload what it does not
   have, which a light node routinely will not have. It also requires
   `swarm-postage-batch-id` (`400 invalid header params` without it), which is
   surprising for re-transmitting already-stamped content.

## Evidence

Metrics at the end of the run:

| metric | value |
|---|---|
| `bee_pusher_total_to_push` | 322,161 |
| `bee_pusher_total_synced` | 322,033 |
| `bee_pusher_total_errors` | 29,354 |
| `bee_pushsync_shallow_receipt` | **29,602** |
| `bee_pushsync_total_outgoing` | 322,161 |
| `bee_pushsync_total_outgoing_errors` | **0** |
| `bee_pushsync_invalid_stamps` | **0** |
| `bee_pushsync_total_failed_send_attempts` | 12 |
| `bee_pushsync_overdraft_refresh` | 142,180 |

Node and network:

- `beeMode: light`, `chequebookEnabled: true`, `swapEnabled: true`
- topology: `depth 9`, `connected 138`, `population 3873`,
  `reachability: Private`, `networkAvailability: Available`

## Ruled out

- **Postage bucket overflow.** Depth-20 batch, `bucketUpperBound` 16, **max
  bucket occupancy 13, zero full buckets**, utilization 11%, `usable: true`,
  ~21.6 days TTL.
- **Invalid stamps.** `bee_pushsync_invalid_stamps = 0`.
- **Send failures.** 12 failed send attempts out of 322,161 outgoing.
- **Funds.** Chequebook 5.65 BZZ total, **2.51 BZZ available**, 3.61 BZZ already
  settled across 782 peers. Not an insolvency.
- **Waiting longer.** Nine retrievability polls over ~8 minutes changed nothing;
  the pusher counters were byte-identical across samples 30 s apart, i.e. idle.

## The overdraft counter is a separate, milder issue

`bee_pushsync_overdraft_refresh = 142,180`, with 477 of ~1,016 peers sitting at
roughly -1e8 PLUR, the default payment threshold. This is *time-metered*
refreshment throttling and is independent of the chequebook balance — more funds
do not help. It is not the cause of the unretrievable content, but it is why
every one of the eleven uploads exceeded the client's 60-second sync window, so
the two failures were easy to mistake for "just slow".

## Scope and what this report does not claim

This is **one light node**, NAT'd (`reachability: Private`), 138 peers, pushing
a ~322k-chunk burst. It may well be specific to that configuration, and a
full, publicly-reachable node may never see it — a full-node comparison is the
obvious control and has not been run.

It is also consistent with workloads that do **not** report problems. Live
video streaming over Swarm reportedly works without re-uploads, but that is a
steady trickle rather than a burst, consumed immediately rather than read back
cold, and — decisively — never audited for retrievability: a chunk that failed
to land is a momentary glitch there, not a permanently broken reference. So
the absence of complaints from streaming is not evidence this does not happen;
it is evidence nobody is checking.

The claim that survives all of that is the observability one: **a successful
upload is not a landed upload, and Bee gives the client no way to tell the
difference** short of polling `GET /stewardship/<ref>` for every reference.

## Reproduction shape

1. Light node, NAT'd (`reachability: Private`), ~138 peers.
2. Upload a few hundred thousand chunks in batches (here: eleven content-
   addressed stores, largest ~1,350 records).
3. Wait for `to_push == synced`.
4. `GET /stewardship/<root>` for each root — some report `isRetrievable: false`
   permanently, with no corresponding backlog in the pusher.

## Workaround, and what it implies

**Re-uploading the same content from source eventually succeeds**, which means
the shallow receipts are probabilistic rather than a structural inability to
reach those neighbourhoods:

- `computing` — unretrievable after upload 1; retrievable after upload 2.
- `geography` — unretrievable after uploads 1 and 2; retrievable after a third
  upload followed by `PUT /stewardship` (which returned 200 once the chunks were
  back in the local store, having returned 500 before).

So the data *can* be placed; the pusher simply gives up on the first shallow
receipt and reports success anyway.

## Attempted isolation without our client — a negative result

To remove our uploader from the picture, random bytes were pushed straight to
`POST /bytes` and then checked with `GET /stewardship`:

| size | upload time | outcome |
|---|---|---|
| 2 KB (one chunk) | <1 s | `isRetrievable: true` in 1.4 s; `GET /bytes` 0.25 s |
| 4 MB (~1k chunks) | 2 s | `isRetrievable: true` — but only after **291 s**; `GET /bytes` 5.6 s |
| 1 / 4 / 16 MB, twice each | 1-6 s | stewardship exceeded a 60 s budget in every case |

**These uploads all landed.** At these sizes a plain Bee upload on this node
does not lose data, so the unretrievable packs are *not* reproduced by a simple
HTTP upload, and this report cannot claim a minimal reproduction of the loss
itself. What failed originally was an eleven-store, ~322k-chunk burst; the loss
may need that scale, that burst rate, or something specific to it.

Two things the exercise did establish, both independent of our client:

1. **Upload success is returned long before landing is known** — 16 MB returns
   in 6 s, while the push continues in the background. The HTTP response
   carries no information about whether the chunks reached their storers.
2. **`GET /stewardship` does not scale as a verification tool.** 291 s for
   4 MB, and unresolved at 60 s for every size tried. It is the only mechanism
   offered for "did my upload actually land", and it is unusable precisely for
   the large uploads most at risk. Verifying the eleven packs this way would
   have taken hours had the roots not already been well distributed.

## Suggested fixes

- Re-queue chunks whose receipt was shallow, with bounded retries, instead of
  counting them synced.
- Expose the shallow-receipt state per upload (or per tag/root) so a client can
  tell "stored" from "handed to someone who isn't the storer" without polling
  stewardship for every reference.
- Make `PUT /stewardship` say why it failed — "chunk not held locally" is a
  different problem from "re-upload attempted and failed".
