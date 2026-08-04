# swarmfs Reference

Compact, definition-first, no narrative. The tutorial lives in the
[User Guide](USER_GUIDE.md); the quick tour in the [README](../README.md);
decisions and live-measured Bee facts in [CLAUDE.md](../CLAUDE.md); the
local-first design in [localstore-design.md](localstore-design.md) and its
**normative on-disk format** in [localstore-format.md](localstore-format.md).
Tables here are pinned against the code by `tests/test_reference.py` — if a
name or parameter in this file and the code disagree, the suite fails.

Package version this file describes: `0.8.0`.

## 1. Vocabulary

| term | definition |
|---|---|
| reference / ref | 64-hex Swarm BMT address of content (128-hex when encrypted). The address is the *result* of a write. |
| manifest | Mantaray trie mapping paths to content refs; a directory tree behind one root ref. |
| lineage | The chain of roots produced by commits from one origin; `fs.latest(ref)` follows it. |
| `bzz://` | Immutable protocol: `bzz://<ref>/path`. Every commit yields a new root; old roots are snapshots. |
| `bzzf://` | Feed-mounted mutable protocol: `bzzf://<owner>/<topic>/path` — stable URL, owner-signed updates. |
| postage batch / stamp | Prepaid storage rights; every upload needs one. Expiry takes the content with it. |
| local-first | Commits land in a local store directory; a background syncer pushes to Swarm and confirms. |
| durability rung | `committed` (local disk) → `pushed` (on the node) → `confirmed` (provably on the network). |
| pinned / evictable | Unconfirmed blobs never evict; confirmed ones may, healing by verified re-fetch. |
| journal | The store directory's append-only JSONL event log — the single source of truth (see the format doc). |
| witness | Optional independent read-only endpoint for confirmation fetches — for a distrusted own node only. |

## 2. Install

| command | gives |
|---|---|
| `pip install swarmfs` | `bzz://` + local-first; runtime deps are only `fsspec>=2023.6.0` and `aiohttp` |
| `pip install "swarmfs[feeds]"` | signed feeds (`bzzf://` writes) + client-side BMT addressing (eth-keys, eth-hash) |

## 3. Exports

Everything importable from `swarmfs` (exactly `__all__`, minus
`__version__`):

| name | one line |
|---|---|
| `SwarmFileSystem` | the fsspec filesystem: `bzz://` reads, transactional writes, local-first mode |
| `SwarmFeedFileSystem` | `bzzf://`: feed-mounted mutable view |
| `SwarmFile` | file object with range reads (block caching / readahead) |
| `SwarmClient` | async client tier: direct Bee endpoints, no filesystem semantics |
| `SyncSwarmClient` | blocking twin of `SwarmClient`, same methods |
| `SwarmError` | base for node/network errors (an `OSError`) |
| `BeeAPIError` | HTTP error from Bee with `.status`/`.url`/`.detail` |
| `BeePermissionError` | 401/403 — gateway blocking the node-owner API |
| `StampError` | no usable postage stamp (local validation or node 402) |
| `split` | chunk a payload the way Bee does: `(root, chunks)` offline |
| `content_address` | the Swarm reference of `data`, computed offline (BMT, erasure coding off) |

## 4. `SwarmFileSystem`

Storage options (constructor / `fsspec.filesystem("bzz", ...)`):

| option | default | meaning |
|---|---|---|
| `api_url` | `$BEE_API_URL` → `http://localhost:1633` | the Bee endpoint |
| `stamp` | None (= `"auto"` at commit) | batch id, or auto-pick the usable batch with longest TTL |
| `pin` | False | ask the node to pin uploads |
| `redundancy` | 2 | erasure-coding level 0–4; **must be 0 with `local_store`** |
| `encrypt` | False | node-side encryption for every write (files and manifest nodes); refs become 128-hex (address + key). A lineage never mixes; incompatible with `local_store` and refused by `verify` |
| `allow_gateway` | False | explicit opt-in for a non-owned endpoint |
| `verify` | None | BMT-verify fetched chunks; auto: on for gateways, off for own node |
| `local_store` | None | path (or `LocalStore`) — local-first mode: offline commits, background push, local-first reads |
| `block_size` | 1 MiB | readahead/block-cache size for opened files |
| `timeout` | 120 | per-request seconds |

Methods beyond the fsspec standard surface:

| member | signature | semantics |
|---|---|---|
| `SwarmFileSystem.upload` | `(lpath, rpath=None, content_type=None, encrypt=False, redundancy=None)` | one-liner: upload a file or directory, return the new reference. |
| `SwarmFileSystem.download` | `(rpath, lpath, **kwargs)` | alias of `get`. |
| `SwarmFileSystem.latest` | `(ref)` | the current head of `ref`'s lineage (read-your-writes). |
| `SwarmFileSystem.sync` | `(timeout=None)` | local-first barrier: block until every commit is network-confirmed. |
| `SwarmFileSystem.sync_status` | `()` | the local store's `StoreStatus`. |
| `SwarmFileSystem.read_reference` | `(ref, start=None, end=None)` | raw-reference read (bytes behind a ref, optionally a range) through the policy reader — verification and local-first apply. For paths inside manifests use `cat`/`open`. |
| `SwarmFileSystem.reference_size` | `(ref)` | size behind a raw reference, same path (local-first answers without reading the blob). |
| `SwarmFileSystem.discard_staged` | `()` | drop staged writes without committing. |
| `SwarmFileSystem.modified` | `(path)` | fixed epoch constant (content is immutable at a ref); checks existence. |

`fs.transaction` batches writes into one commit per lineage; rollback
discards without uploading. `fs.commit_log` lists `CommitResult`s.
`SwarmFeedFileSystem` adds `signer=` (owner's private key hex, required
for writes) and `feed_ttl=` (feed resolution cache, 15 s); feeds are
last-write-wins. In local-first mode the feed update publishes only after
network confirmation.

## 5. Client tier

`SwarmClient` (async) and `SyncSwarmClient` (blocking, same signatures)
expose Bee endpoints directly: `bytes_get`, `bytes_post` (with
`deferred=`), `bytes_size`, `bytes_iter`, `bzz_get`, `bzz_post`,
`chunk_get`, `soc_post`, `feed_head`, `stamps_list`, `stamp_get`,
`stamp_buy`, `stamp_topup`, `stamp_dilute`, `stamp_buckets`,
`stewardship_get`, `tag_create`, `tag_get`, `chainstate`, `wallet`,
`health`, `close`. A test keeps the two surfaces in lockstep.

## 6. Stamps (policy tier, `swarmfs.stamps`)

Plans are pure questions; only the verbs spend money.

| name | signature | semantics |
|---|---|---|
| `stamps.StampManager` | `(client, min_ttl=60)` | resolve `"auto"`/explicit stamp to a validated batch id; `plan`/`buy`, `plan_topup`/`topup`, `plan_dilute`/`dilute`, `list_batches`, `buckets`. |
| `stamps.StampInfo` | dataclass | one batch: `batch_id`, `usable`, `ttl`, `utilization_ratio`, `depth`, …; `problem(min_ttl)` → why it's unusable, or None. |
| `stamps.suggest_depth` | `(size_bytes, *, redundancy=2, encrypted=False, risk=0.01)` | depth for a payload from Bee's own erasure tables + the bucket-overflow bound. |
| `stamps.depth_for_addresses` | `(addresses, *, extra_chunks=0, risk=0.01)` | exact depth when every chunk address is known (pair with `split`); only node-generated parity stays probabilistic. |
| `stamps.stamped_chunks` | `(data_chunks, *, redundancy=2, encrypted=False)` | how many chunks a batch is actually charged for. |
| `stamps.bucket_histogram` | `(addresses)` | Bee's `toBucket` histogram for known addresses. |
| `stamps.overflow_risk` | `(chunks, depth, *, loads=None)` | probability a bucket overflows. |
| `stamps.ttl_to_amount` | `(ttl_secs, price)` | per-chunk amount for a target TTL. |
| `stamps.amount_to_ttl` | `(amount, price)` | the inverse estimate. |
| `stamps.batch_cost_bzz` | `(amount, depth)` | total price of a batch in xBZZ. |

Facts to trust: `batchTTL` is authoritative for remaining life (`amount`
is not); topups are additive and index ~40 s after the tx; expired batches
cannot be revived; the shallowest sellable depth is 17.

## 7. Local-first store (`swarmfs.localstore`)

The invariant: a blob is deleted locally only when the journal proves
Swarm holds it. On-disk format: [localstore-format.md](localstore-format.md).

| member | signature | semantics |
|---|---|---|
| `localstore.LocalStore` | `(path, addressing="swarm", max_bytes=None, min_free_bytes=None, min_evict_ttl=604800, fetcher=None, verify_fetch=True, durability="commit")` | open/create a store directory (flock: single writer). `max_bytes` is soft for pinned data. `durability`: `"commit"` batches fsyncs at the commit barrier, `"blob"` fsyncs per put. |
| `localstore.LocalStore.put` | `(data)` | store a blob → ref. Orphans (not yet committed) are never evicted. |
| `localstore.LocalStore.get` | `(ref)` | bytes; heals evicted refs through `fetcher` (verified); `KeyError` unknown, `BlobEvicted` known-but-offline. |
| `localstore.LocalStore.commit_root` | `(root, parent, blobs, structure=())` | journal a commit: `blobs` = the NEW blobs, `structure` = the index-node subset (eviction priority). |
| `localstore.LocalStore.mark_pushed` | `(root)` | rung: the node accepted the push (call after the fact — the lag rule). |
| `localstore.LocalStore.mark_confirmed` | `(root, batch=None, ttl=None)` | rung: verified on the network; parents first; flips blobs evictable. |
| `localstore.LocalStore.network_confirmed` | `(root)` | True iff the root and every ancestor are confirmed. |
| `localstore.LocalStore.wait_for` | `(root=None, rung="confirmed", timeout=None)` | block until a root (or all) reaches a rung. |
| `localstore.LocalStore.pin` | `(name, refs)` / `unpin(name)` | named pins: exempt refs from eviction. |
| `localstore.LocalStore.evict` | `(nbytes)` | drop up to `nbytes` of evictable blobs (payload before structure, LRU, TTL-aware). |
| `localstore.LocalStore.rebase_root` | `(root, blobs, structure=())` | app-assisted squash: collapse the lineage onto `root` with its FULL reachable set. |
| `localstore.LocalStore.gc_orphans` | `()` | delete blobs no event lists (sparing this session's staging) → `(count, bytes)`. |
| `localstore.LocalStore.scrub` | `()` | re-hash every blob; corrupt evictable dropped (re-fetch heals), corrupt pinned raises. |
| `localstore.LocalStore.status` | `()` | `StoreStatus`. |
| `localstore.LocalStore.add_listener` | `(fn)` | push notifications: `fn(event)` after every journal append. Cross-process: tail `journal.jsonl`. |
| `localstore.LocalStore.latest_root` | `()` / `has_root(root)` / `parent_of(root)` / `roots_below(rung)` | the journal as pointer and reflog. |
| `localstore.MemoryCacheStore` | `(inner, max_bytes=67108864)` | byte-budgeted LRU cache in front of any blob store (transparent, not a replica). |
| `localstore.BlobEvicted` | exception (`KeyError`) | known ref, bytes on Swarm, no way to fetch right now. |
| `localstore.BlobVerificationFailed` | exception | bytes do not hash to their ref (fetch, push echo, scrub, torn orphan). |
| `localstore.StoreLocked` | exception | another process holds the store's writer lock. |
| `localstore.BudgetExceededWarning` | warning | budget exceeded entirely by pinned (unpushed) data — the limit is soft. |

`StoreStatus` fields: `blob_count`, `total_bytes`, `pinned_bytes`,
`evictable_bytes`, `max_bytes`, `only_on_swarm_count`, `roots` (→ rung),
`remote_roots`, `pins`, `batch_expiries` (batch → earliest estimated
expiry — the number to watch once local is partial).

## 8. Sync worker (`swarmfs.localsync`)

| member | signature | semantics |
|---|---|---|
| `localsync.Syncer` | `(store, remote, policy=None, witness=None)` | wires itself in (journal listener + the store's fetcher); `start()`/`stop()`; context manager. |
| `localsync.Syncer.sync` | `(timeout=None)` | block until everything is network-confirmed; `TimeoutError` names the last sync error. |
| `localsync.Syncer.trusting_node_claims` | property | True when `confirm_sample == 0` — eviction safety rests on stewardship alone. |
| `localsync.BeeRemote` | `(api_url=None, stamp="auto", client=None, min_batch_ttl=86400)` | the Swarm side. `"auto"` resolves lazily (offline construction works); `stamp=None` = read-only witness shape. |
| `localsync.BeeRemote.push_blob` | `(ref, data, deferred=True)` | upload; **asserts the node returns the locally computed ref** (erasure-coding tripwire). |
| `localsync.SyncPolicy` | dataclass | `debounce=10.0`, `max_staleness=300.0`, `pinned_bytes_limit=None` (→ budget/4), `confirm_sample=0.25`, `direct_upload=False`, `backoff_base=1.0`, `backoff_max=60.0`. |

Confirmation is p2p-native: Bee's stewardship check retrieves every chunk
through the retrieval protocol from remote peers (verified from the Bee
source) — node claims alone promote no further than *pushed*.

## 9. Feeds (`swarmfs.feeds`)

The single-owner-chunk / feed primitives behind `bzzf://` — a supported
surface: swarmlite builds its snapshot history and publish path on it
(needs the `feeds` extra).

| name | signature | semantics |
|---|---|---|
| `feeds.FeedSigner` | `(private_key)` | the owner's key; signs feed updates (`.owner` / `.owner_hex`). |
| `feeds.FeedOps` | `(client)` | feed operations over a `SwarmClient` — `update(signer, topic, index, ref, stamp)` publishes one signed update. |
| `feeds.owner_bytes` | `(owner)` | 40-hex owner address → bytes (0x tolerated). |
| `feeds.topic_bytes` | `(topic)` | human topic string (keccak'd, bee-js convention) or raw 64-hex → bytes. |
| `feeds.feed_identifier` | `(topic, index)` | the SOC identifier of update `index` of a sequence feed. |
| `feeds.soc_address` | `(identifier, owner)` | the address the single-owner chunk lives at. |
| `feeds.verify_soc` | `(data, owner, address)` | full SOC verification: address recomputation + owner-signature recovery; raises `FeedError`. |
| `feeds.SOC_PAYLOAD_OFFSET` | constant (`105`) | where the payload starts inside a raw SOC. |
| `feeds.FeedError` | exception (`RuntimeError`) | malformed/misowned feed data, missing signer, bad SOC. |

## 10. Errors

| raised | when |
|---|---|
| `SwarmError` | base: anything node/network (an `OSError`). |
| `BeeAPIError` | HTTP error from Bee (`.status`, `.url`, `.detail`). |
| `BeePermissionError` | 401/403 — typically a gateway refusing the node-owner API. |
| `StampError` | no usable postage stamp — caught locally before upload, or a node 402. |
| `FileNotFoundError` | path/reference not found (fsspec semantics). |
| `PermissionError` | endpoint looks like a gateway and `allow_gateway` is False. |
| `ConnectionError` | node unreachable at first contact (except in local-first mode, where offline is normal). |
