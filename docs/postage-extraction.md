# Extracting postage-stamp management (design sketch)

**Status**: proposed, not started. Waiting on
[ethswarm-tools/bee-py#3](https://github.com/ethswarm-tools/bee-py/issues/3),
where the same logic was offered to the Python Bee client that already exists.
This document records the boundary so the decision can be executed quickly if
that offer goes unanswered — and so it is not re-derived from scratch.

## Why

Postage management currently lives in `swarmfs/stamps.py`, i.e. inside an fsspec
filesystem. Everything that writes to Swarm needs stamps; almost nothing that
needs stamps wants a filesystem. Concretely:

- **recordstore imports `swarmfs._client`** — a *private* module — purely to get
  `StampManager`, and ends up running two HTTP stacks in one process (`aiohttp`
  for stamps, its own `requests` for blobs). A private cross-package import is
  the clearest symptom that the layer is in the wrong place.
- **swarmlite** wraps the same manager in `swarmlite/stamps.py` to reach it from
  sync code.
- **swarmfs is not heavy** (runtime deps `fsspec` + `aiohttp`, 59.7 KB wheel), so
  the complaint is not size — it is that stamps are a *payment* concern with no
  business depending on a filesystem or on `aiohttp`.
- **51% of `stamps.py` needs no HTTP at all** (414 of 816 lines, measured): the
  erasure tables, sizing, arithmetic, dataclasses, semantics and recovery
  messages. That half is the part worth having exactly once.

## Decision trigger (decided 2026-07-29)

**Silence means we do it ourselves.** bee-py#3 was filed 2026-07-29; #1 (2026-07-19)
and #2 (2026-07-20) are also unanswered and the last push there was 2026-05-20.
Peter's call: give it a chance to get a reply, and absent a positive one, prefer
our own repos rather than waiting on an upstream that may not be maintained. A
second downstream consumer is no longer required to justify it — recordstore's
private `swarmfs._client` import already is one.

So: a **yes** on bee-py#3 → contribute there and skip this document. Anything
else → execute the plan below.

Until then the seam is kept clean and nothing is published: `stamps.py` imports
only stdlib + `._client` + `.exceptions`, and the async `SwarmClient` is itself
fsspec-free, so extraction stays close to a file move.

## The boundary

Measured by introspection, not guessed.

### Moves (pure: no I/O, no dependencies)

| kind | symbols |
|---|---|
| constants | `CHUNK_SIZE`, `BUCKET_DEPTH`, `BUCKETS`, `MIN_DEPTH`, `BRANCHES`, `ENC_BRANCHES`, `BLOCK_SECS`, `PLUR_PER_BZZ`, `DEFAULT_RISK` |
| bee's appendix-F tables | `_ET`, `_ENC_ET`, `_REPLICA_COUNTS`, `_group_shape` |
| arithmetic | `ttl_to_amount`, `amount_to_ttl`, `batch_cost_bzz` |
| sizing | `stamped_chunks`, `overflow_risk`, `suggest_depth` |
| exact sizing | `bucket_histogram`, `depth_for_addresses` |
| parsers / plans | `StampInfo`, `BucketStats`, `BatchPlan`, `TopupPlan`, `DilutePlan` (they parse API dicts; they do not fetch) |

### Stays per-library (thin transport)

`resolve`, `list_batches`, `get_batch`, `buckets`, `balance_bzz`, `plan`,
`plan_topup`, `buy`, `topup`, `dilute` — ten methods, each "call one endpoint,
hand the dict to a pure parser", plus one polling loop.

### The refactor that makes this work

Today `plan_topup` fetches (`get_batch`, `chainstate`) and *then* does maths.
Split it so the maths is a pure function of already-fetched values:

```python
# core, pure
def plan_topup(info: StampInfo, price: int, *, ttl_secs=None,
               total_ttl_secs=None, budget_bzz=None) -> TopupPlan: ...
def topup_applied(before: StampInfo, after: StampInfo) -> bool: ...
def dilute_plan(info: StampInfo, to_depth: int) -> DilutePlan: ...
```

`topup_applied` matters as much as the arithmetic: it encodes that `amount` is
unreliable local bookkeeping and a `batchTTL` jump also counts — the finding that
cost a real debugging session and that bee-py currently lacks.

The library-side verb then collapses to:

```python
info = StampInfo.from_api(await client.stamp_get(batch_id))
plan = postage.plan_topup(info, price, ttl_secs=ttl)
tx = await client.stamp_topup(batch_id, plan.added_amount)
# poll until postage.topup_applied(info, StampInfo.from_api(...))
```

## Transport policy

The core does **no I/O and has no dependencies** — that is what makes it cheap
enough for anyone to adopt. Consumers keep the transport they already have
(`aiohttp` in swarmfs, `requests` in recordstore, `httpx` in bee-py). Async/sync
never becomes a schism because the core has no awaits.

For scripts that have no transport at all, an optional `postage.http` module over
stdlib `urllib.request` (still zero third-party deps, sync) would let a cron job
use it directly. Deliberately *not* the primary interface: duplicating a
transport is the thing this extraction is meant to stop.

## Layout

```
swarm_postage/
    __init__.py      # re-exports the public surface
    constants.py     # chunk size, bucket depth, min depth, block time, plur
    erasure.py       # bee's appendix-F tables + stamped_chunks
    sizing.py        # overflow_risk, suggest_depth, bucket_histogram,
                     #   depth_for_addresses
    pricing.py       # ttl_to_amount, amount_to_ttl, batch_cost_bzz
    model.py         # StampInfo, BucketStats + from_api parsers
    plans.py         # BatchPlan/TopupPlan/DilutePlan builders, guards, warnings
    messages.py      # canonical recovery texts (402-overissued, dilute-first,
                     #   expired-cannot-be-revived)
    http.py          # optional stdlib transport (not required)
```

`messages.py` is not padding: those texts are the most-duplicated and
least-obvious content in the whole area, and they are what turns a 402 into a
two-step recovery.

## Migration (no breakage)

1. Publish the core. `swarmfs.stamps` re-exports every moved symbol, so
   `from swarmfs.stamps import suggest_depth` keeps working for 0.4.x users and
   swarmlite needs no change at all.
2. swarmfs's `StampManager` keeps its ten methods and its async verbs, now built
   on the core's pure functions. One new runtime dep, itself dependency-free.
3. recordstore depends on the core directly and **drops the private
   `swarmfs._client` import** and the second HTTP stack, keeping `requests`.
   Its `[stamps]` extra points at the core instead of `swarmfs>=0.4.0`.
4. Offer bee-py a PR that uses the core, per bee-py#3.

Most of swarmfs's stamp tests are pure-maths tests (live-measured numbers pinned
as constants) and move with the code. The live/integration tests stay where the
transports are.

## Naming

All free on PyPI as of 2026-07-29: `swarm-postage`, `bee-postage`,
`swarm-stamps`, `postage-stamps`, `swarmpostage`, `beestamps`. Recommend
**`swarm-postage`** (import `swarm_postage`): says what it is, does not imply a
full Bee client, and does not collide with `swarm-bee`.

## Non-goals

- Not another Bee client. No feeds, no chunks, no manifests — bee-py and swarmfs
  already cover those, and duplicating them is how this problem started.
- No purchase *policy*: risk targets, confirm-before-spending, monitoring
  cadence and the batch↔publication mapping stay with the caller. The core
  prices and explains; it never decides to spend.
- No CLI. `swarmlite stamps` is the CLI, by decision.

## What a better Bee would absorb

Worth stating so the module is not built to be permanent. Bee could own:
durations in the API instead of plur-per-chunk-per-block; a `remainingBalance`
field (or documenting that `amount` is cumulative and issuer-local); and
observability of a submitted-but-unindexed topup. Bee will never own the risk
threshold, the spend confirmation, or which batch keeps which publication alive.
So the core shrinks toward *policy* over time rather than disappearing — which is
also the argument for keeping the arithmetic and the transport strictly apart.
