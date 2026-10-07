# Draft issue for ethersphere/bee (not filed)

To file at https://github.com/ethersphere/bee/issues once Peter agrees.
Evidence for everything below is in `bee-push-sync-findings.md` and
`bee-push-sync-evidence/`.

---

**Title:** Light node reports uploads fully synced while chunks are unretrievable (shallow receipts counted as synced)

### Summary

On a light node, a burst of uploads finishes with the pusher queue fully
drained (`bee_pusher_total_to_push == bee_pusher_total_synced`) and every
upload accepted, yet some of the uploaded content is not retrievable from
the network afterwards, and stays that way. In both runs about 10% of
pushes got a shallow receipt, and the shallow-receipt count tracks
`bee_pusher_total_errors` almost one-for-one. Nothing in the node's API
distinguishes those chunks from delivered ones. We have seen this twice,
a month apart, on the same node.

### Environment

- Bee `2.8.2-7e703f49`, API `8.1.1`, light node, Gnosis mainnet, run by
  Swarm Desktop; `reachability: Private` (NAT), ~138 connected peers.
- Immutable postage batch, usable, no bucket full (fullest bucket 13/16 in
  September, 24/64 in October), no `overissued` errors,
  `bee_pushsync_invalid_stamps 0`.
- Uploads: `POST /bytes`, deferred, no `Swarm-Redundancy-Level` header (so
  the default MEDIUM applies), several thousand single-chunk blobs per
  content tree, eleven trees per run.

### What happened

| | 2026-09-11 | 2026-10-07 |
|---|---|---|
| chunks pushed (`total_to_push` = `total_synced`) | 322,161 | 372,313 |
| `bee_pusher_total_errors` | 29,354 | 36,118 |
| `bee_pushsync_shallow_receipt` | 29,602 | 36,589 |
| roots not retrievable afterwards (of 11) | 2 | 3 |
| repair | re-uploading from source, 2–3 times, then `PUT /stewardship` | `PUT /stewardship` once each |

"Not retrievable" means `GET /stewardship/<root>` returned
`isRetrievable: false`, checked repeatedly after the pusher had drained,
with no backlog left in the pusher.

On 2026-10-07, 14,854 of the shallow receipts were at depth 0
(`bee_pushsync_shallow_receipt_depth{depth="0"}`), i.e. from peers sharing
no address prefix with the chunk. The receipt-depth histogram for the same
run peaks at depth 9 (156,150) and 10 (84,911); `GET /topology` reports
depth 9.

### Expected

Either the chunk is pushed again until a receipt comes from its
neighbourhood (with bounded retries), or, once retries are exhausted, the
chunk is reported as not delivered: not counted in `total_synced`, and
visible per upload (per tag) so a client can tell "stored by its
neighbourhood" from "handed to a peer that is not its storer".

### Why it matters

An application cannot tell from Bee that its data did not land. The HTTP
upload succeeded, the pusher says done, and other nodes cannot retrieve
the content; the uploader's light node holds it only until it evicts it.
`GET /stewardship/<ref>` per reference is the only way to find out, and it
is slow for large content (we measured 291 s for a 4 MB upload). Clients
now have to verify every reference themselves and re-push what is missing
(we do this now in our own client libraries).

The default MEDIUM redundancy did not prevent the loss: the root chunks'
dispersed replicas go through the same push-sync.

### What we could not reproduce

Pushing random bytes straight to `POST /bytes` (2 KB to 16 MB, outside our
client) never lost data. The loss appeared only in the eleven-tree bursts
of 250k–370k chunks. It may need that scale or rate; we have no minimal
reproduction. We can share full metrics, the topology summary, the stamp
bucket summary and logs from both runs.

### Questions

1. After a shallow receipt, how many retries does the pusher make, and
   what does it record when they are exhausted? From outside, the chunk
   appears in `total_synced`.
2. How can a light node get a receipt from a peer at proximity 0 to the
   chunk? That looks as if the chunk was not forwarded towards its
   neighbourhood at all.
3. `PUT /stewardship/<ref>` returned `500 {"message":"re-upload failed"}` in
   September while the node no longer held the chunks locally. Could it
   say why ("not held locally" vs "re-upload attempted and failed")? It
   also requires `swarm-postage-batch-id` to re-transmit already-stamped
   content, which surprised us.
