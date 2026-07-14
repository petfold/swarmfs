<!--
This is the text of the upstream feature request filed against Bee:
  ethersphere/bee#5535 — https://github.com/ethersphere/bee/issues/5535
  Author: petfold. Status: open, no maintainer reply when filed.

A read-only listing implementation is being prototyped on a bee fork; the
contributor's working plan lives outside this repo (LISTING_ENDPOINT_PLAN.md
in the bee checkout, not committed to bee). Keep this doc in sync if the API
shape changes during review.
-->

# API: server-side manifest listing endpoint (S3-style), with optional manifest mutation as a follow-up

## Summary

Bee currently has no HTTP endpoint to enumerate the contents of a manifest. Clients that need a directory listing (`ls`, `find`, `glob`) must download and traverse the Mantaray trie themselves, chunk by chunk, via `/bytes` — which is what bee-js does today. This proposal adds a read-only listing endpoint that walks the trie server-side and returns entries as JSON, with S3-`ListObjectsV2`-style semantics (prefix, delimiter, pagination).

All the primitives already exist in Bee (`pkg/manifest`, `pkg/manifest/mantaray.WalkNode`, the manifest resolution already performed by the `/bzz` handler); this is essentially a new handler in `pkg/api` plus OpenAPI spec and tests.

A companion write-side endpoint (server-side manifest mutation: add/remove entries, return the new root) uses the same interface and is sketched at the end; it can be split into its own issue, but note that the two endpoints together are exactly the node-side primitives missing for an **S3-compatible gateway** in front of Bee — listing is the read path (ListObjectsV2), mutation is the write path (PutObject/DeleteObject).

## Motivation

**Ecosystem integrations need listing to be cheap.** We are building an [fsspec](https://filesystem-spec.readthedocs.io/) backend for Swarm (`bzz://` protocol), which would make Swarm a first-class storage backend for pandas, dask, zarr, xarray, pyarrow, DuckDB and everything else in the Python data ecosystem. These tools generate S3-shaped access patterns: before reading a byte of data, they list. Dask opening a partitioned Parquet dataset calls the equivalent of `find()` over the whole collection; zarr resolves thousands of chunk keys. The same applies to an rclone backend, S3-compatibility layers, and any file-manager-style UI over Swarm content.

**Client-side traversal doesn't scale for this.** Walking the trie from the client costs one round trip per Mantaray node along the walk. For a collection with thousands of entries this is hundreds to thousands of sequential-ish HTTP requests just to produce a listing, against a node that could answer from its local chunk store (or its network fetch path) in a single request/response. Server-side listing turns O(trie nodes) round trips into O(pages).

**An S3-compatible gateway becomes a thin translator.** A longer-term goal is exposing Swarm through the S3 API, so the vast tooling universe that already speaks S3 (boto3, s3fs, rclone, backup tools, MinIO clients, CI systems) works against Swarm unchanged. Most of the mapping already exists: `GetObject`/`HeadObject` → `GET/HEAD /bzz/{ref}/{path}` (range requests included), ETag → the Swarm reference (a true content hash), bucket → a feed-backed mutable root. The two missing node-side primitives are exactly this proposal: `ListObjectsV2` → the listing endpoint (prefix/delimiter/max-keys/continuation semantics below are modeled on it directly), and `PutObject`/`DeleteObject` → the mutation endpoint. With them, an S3 gateway is a stateless protocol translator (auth, bucket→feed mapping, multipart spooling); without them, every gateway must embed a Mantaray implementation and pay O(trie) round trips per listing.

**Every thin client re-implements Mantaray today.** Listing currently requires a full Mantaray codec in every client language. bee-js has one; other languages mostly don't, which in practice gates ecosystem integrations on re-implementing a binary trie format before they can do `ls`. A listing endpoint removes that barrier for read-side use cases entirely.

**Precedent.** The pre-Bee client had `bzz-list:`. Listing was lost in the Bee rewrite when manifests became binary Mantaray tries; this restores the capability where it is cheapest to provide — on the node.

## Proposed API

New resource (avoids overloading content-serving `GET /bzz/{reference}/{path}` and keeps CDN/caching behavior of `/bzz` untouched):

```
GET /manifest/{reference}/{prefix}
```

- `reference` — 64-hex or 128-hex (encrypted) manifest reference. Feed owners resolve their feed to a manifest reference first, as today.
- `prefix` — optional path prefix to list under (default: root).

### Query parameters

| Param | Default | Meaning |
|---|---|---|
| `delimiter` | none | If set (typically `/`), return a shallow listing: entries directly under `prefix`, plus `commonPrefixes` for deeper paths — S3-style pseudo-directories. If unset, walk recursively. |
| `limit` | 1000 | Max entries per page. Node-enforced hard cap (see gateway considerations). |
| `after` | none | Continuation: return entries strictly after this path. Works because `mantaray.WalkNode` sorts fork keys, so traversal order is deterministic and lexicographic. |
| `sizes` | `false` | If `true`, resolve each entry's byte length by reading the 8-byte span from the file's root chunk. Opt-in because it costs one additional chunk read per entry; `ls` doesn't need it, but data tools calling `info()` on thousands of files do. |

### Headers

Reuse the ACT headers already accepted by `GET /bzz` (`swarm-act`, `swarm-act-publisher`, `swarm-act-history-address`, `swarm-act-timestamp`) so ACT-protected manifests are listable by authorized clients. Encrypted (128-hex) references work unchanged, since decryption happens in the load path.

### Response

```json
{
  "entries": [
    {
      "path": "data/part-0000.parquet",
      "reference": "c0ffee…",
      "metadata": {
        "Content-Type": "application/octet-stream",
        "Filename": "part-0000.parquet"
      },
      "size": 134217728
    }
  ],
  "commonPrefixes": ["data/2025/", "data/2026/"],
  "truncated": true,
  "nextMarker": "data/part-0000.parquet"
}
```

- `size` present only when `sizes=true` was requested and the span could be read.
- Root-level manifest metadata that affects interpretation (e.g. `website-index-document`, `website-error-document`) could be surfaced in a top-level `manifestMetadata` object so clients can distinguish websites from plain collections.

### Entry sizes, timestamps, ETags (S3 alignment)

S3 `ListObjectsV2` responses unconditionally include `Size`, `LastModified`, and `ETag` per object — S3 clients depend on them. Mapping:

- **ETag** is free: the entry's Swarm reference is a content hash, strictly stronger than S3's ETag semantics.
- **Size and mtime are not stored in Mantaray entries today.** Resolving size via span reads (`sizes=true`) works but puts an extra chunk read per entry on what is, for an S3 gateway, the hottest path in the API. Proposal: standardize entry-metadata keys (e.g. `Content-Length` and `Last-Modified`, mirroring the existing `Content-Type`/`Filename` conventions) and have Bee's own collection upload (tar/multipart `POST /bzz`) populate them at write time. The listing endpoint returns them from metadata when present and falls back to span reads only for legacy manifests. This is a small, backward-compatible change to the upload path that makes listings S3-complete at zero per-entry read cost.

### Error and partial-retrieval semantics

A manifest may be partially unretrievable (some trie nodes missing from the network). Proposed default: fail the request with `404`/`503` and an error body identifying the deepest reachable path — listings that silently omit subtrees are dangerous for tools that use them to decide what exists. An opt-in `strict=false` mode could instead return retrievable entries plus an `errors` array of unreachable subtree prefixes. Open to discussion; the important part is that the semantics are explicit.

## Implementation sketch

Everything needed is already in the codebase:

1. Handler in `pkg/api`, mirroring how the `/bzz` handler resolves a manifest: `manifest.NewDefaultManifestReference(ref, loadsaver)` over the node's getter.
2. Descend to `prefix` via the existing lookup path (`LookupNode`), then enumerate with `mantaray.WalkNode`, which already loads nodes on demand through a `Loader` and visits forks in sorted key order.
3. Emit entries for value-carrying nodes; with `delimiter` set, stop descent at the delimiter boundary and aggregate `commonPrefixes`; apply `after`/`limit` as a filter on the ordered stream (deterministic order makes the continuation token just "the last path returned").
4. For `sizes=true`, fetch each entry's root chunk and read the 8-byte span header.
5. OpenAPI spec + handler tests over fixture manifests (flat, deep, delimiter cases, encrypted, ACT, pagination boundaries, partially missing subtree).

No new storage, protocol, or incentive-layer behavior; purely a local read amplification of data the node already serves.

### Gateway / DoS considerations

Recursive walks over adversarially deep or wide tries are a cheap amplification vector on public gateways. Mitigations: hard `limit` cap (configurable, e.g. 1000–10000), per-request node budget on trie nodes loaded, request timeout, and possibly gating `sizes=true` behind a lower cap since it multiplies chunk reads. Pagination makes large listings possible without unbounded single requests.

## Alternatives considered

- **Status quo (client-side traversal).** Works, and clients will keep needing it for old nodes; but it is O(trie nodes) round trips per listing, and forces a Mantaray implementation in every client language before basic `ls` is possible.
- **Content negotiation on `GET /bzz/{ref}/{path}`** (e.g. `Accept: application/json` or `?list=true`). Fewer routes, but overloads an endpoint whose caching and redirect behavior is tuned for content serving, and complicates the website index-document logic. A dedicated resource is cleaner.
- **Separate indexer service outside Bee.** Pushes the problem to another daemon that would re-implement trie traversal Bee already contains; poor fit for the common single-node setup.

## Companion proposal (follow-up issue if preferred): server-side manifest mutation

`manifest.Interface` already exposes `Add`, `Remove`, and `Store`. That makes a mutation endpoint nearly the same effort as listing:

```
POST /manifest/{reference}
swarm-postage-batch-id: …
```

```json
{
  "operations": [
    {"op": "add", "path": "data/part-0001.parquet", "reference": "beef…", "metadata": {"Content-Type": "application/octet-stream"}},
    {"op": "remove", "path": "old/tmp.bin"}
  ]
}
```

Response: the new root reference. Semantics are copy-on-write: only new/changed trie nodes are created and stamped; the input manifest is untouched, so every edit is a cheap snapshot. Together with listing, this lets thin clients get complete read/write manifest functionality with zero client-side Mantaray code — e.g. updating one file in a large website or collection without re-uploading it, from any language, with two HTTP calls.

For the S3 direction, this endpoint is the write path: `PutObject` = upload data + `add` + feed update; `DeleteObject` = `remove` + feed update; `DeleteObjects` batches map directly onto the `operations` array. Everything else S3 needs — SigV4 auth, bucket→feed mapping, multipart upload assembly, serializing concurrent bucket-root updates — belongs in a gateway service in front of Bee, not in Bee itself; these two endpoints are what make such a gateway stateless and thin.

Happy to contribute the implementation for either or both endpoints — we want this for the fsspec backend and an S3-compatible gateway, and can drive a PR if the API shape is agreed.
