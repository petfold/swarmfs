# Comment on ethersphere/bee#5400 (posted 2026-10-08)

Posted as https://github.com/ethersphere/bee/issues/5400#issuecomment-6064777994

Our report turned out to be a duplicate of
https://github.com/ethersphere/bee/issues/5400 ("Research Review: Pushsync
Silent Chunk Loss", open since 2026-03, confirmed on mainnet by a maintainer
on 2026-04-30; the proposed fix is PR #5390). So instead of a new issue,
this adds our two occurrences as field evidence, plus one point #5390 does
not cover. Evidence is in `bee-push-sync-findings.md` and
`bee-push-sync-evidence/`; the source references were checked at `v2.8.2`
(2026-10-08).

---

A data point from outside the team, in case it helps #5390: a Bee 2.8.2
light node (Swarm Desktop, behind NAT, Gnosis mainnet, ~140 peers) lost
content this way twice, a month apart.

Each time we published eleven content trees in one burst (deferred
`POST /bytes`, default redundancy, an immutable batch with no full bucket
and no invalid stamps). Every upload returned success and the pusher
drained, yet afterwards 2 of the 11 root chunks (2026-09-11) and 3 of 11
(2026-10-07) were not retrievable: `GET /stewardship` said false,
repeatedly. Pushing them again fixed them (in September only after two
or three attempts).

(September's counters are a snapshot taken during the run's tail, with 128
pushes still in flight; October's were taken after the queue drained.)

| | 2026-09-11 | 2026-10-07 |
|---|---|---|
| `bee_pusher_total_to_push` / `total_synced` | 322,161 / 322,033 | 372,313 / 372,313 |
| `bee_pusher_total_errors` | 29,354 | 36,118 |
| `bee_pushsync_shallow_receipt` | 29,602 | 36,589 |
| of which `shallow_receipt_depth{depth="0"}` | 11,777 | 14,854 |

Reading v2.8.2 against those counters:

1. A shallow receipt increments `total_errors` except on the sixth
   attempt, which reports the chunk `ChunkSynced`
   (`pkg/pusher/pusher.go:282-289`, `pkg/pusher/inflight.go:58-65`); other
   errors increment `total_errors` alone. So `shallow_receipt -
   total_errors` is a lower bound on chunks accepted after six shallow
   receipts: at least 471 on 2026-10-07 (one node process), and 248 on
   2026-09-11 if those counters also covered one process.
2. 11,777 (September) and 14,854 (October) of the shallow receipts were
   signed by storers at proximity 0 to the chunk (`pkg/pushsync/pushsync.go:593,606`): copies
   stored as far from their neighbourhood as possible, which fits the
   out-of-AOR storing discussed here and in #5237.
3. `total_synced` increments on every push attempt, failed ones included
   (`pkg/pusher/pusher.go:178-179`), so `total_to_push == total_synced`
   only means that nothing is in flight. Its help text says "with valid
   receipts". #5390 adds `total_could_not_sync`, but as far as we can see
   it leaves this increment unconditional, so the counter would still
   overstate delivery. For us it was the misleading signal: we read the
   drained queue as "delivered".

Separately: in September `PUT /stewardship/<ref>` answered
`500 {"message":"re-upload failed"}` for content whose deferred upload had
already deleted the local copies, and the message cannot tell "not held
locally" from "re-push failed". That may deserve its own small issue.

On our side we now check every reference with `GET /stewardship` after
uploading and push again what is missing. We can share the full metrics
and logs from both runs.
