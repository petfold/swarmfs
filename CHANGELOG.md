# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

Back-filled 2026-09-11 from the git history and from the GitHub Release notes
that had served as the only record until then (0.1.0 and 0.2.0 keep their
original wording). Entries for versions whose notes were never written are
summarised from their release commits, so they say what shipped without
claiming more detail than the history holds.

## [Unreleased]

## [0.13.0] — 2026-10-08

### Added

- **`swarmfs.signer`: one secp256k1 signer, with no cryptography of our
  own.** Signing, verification and key recovery are libsecp256k1's
  (Bitcoin Core's secp256k1 library) through coincurve; the module owns
  only Bee's encoding: the Ethereum signed-message digest, the 65-byte
  `r ‖ s ‖ v` form, and the address (last 20 bytes of the public key's
  keccak256). `Signer(key).sign(data)` / `.sign_digest(digest32)`,
  `recover`, `recover_digest`, `verify`. It refuses to sign without
  coincurve; recovery falls back to pure Python, which handles no secret,
  so readers verify feed updates where coincurve cannot install
  (Pyodide). Its signatures are byte-identical to swarm-bee's and to
  eth-keys' pure-Python backend over 60 random keys and messages; the
  tests pin four vectors, two of them external anchors (key 1's
  well-known Ethereum address; the bee-js test key's address).
  recordstore's feed pointer and ontodag's provenance records are to sign
  with it in place of swarm-bee.
- **`FeedOps.latest(..., after=N)` / `SwarmClient.feed_head(..., after=N)`**:
  Bee's lookup hint, an index known to exist, so the lookup resumes near
  the tip instead of searching from the start (cheaper, and less flaky on
  a long feed). recordstore needed it through swarm-bee's private API.

### Changed

- **The `feeds` extra is coincurve instead of eth-keys.** One compiled
  package with no dependencies of its own, where eth-keys brought ten
  (eth-utils, cytoolz, pydantic and pydantic-core among them).
  `FeedSigner` signs through `signer.Signer`; signatures are unchanged.
- **Verifying a feed update needs no extra** (`verify_soc`, `FeedOps`
  with `verify=True`): recovery falls back to pure Python.

## [0.12.0] — 2026-10-08

### Added

- **Network reads go 32 at a time.** `LocalStore.get_many` fetched whatever
  was not on disk one blob at a time; it now keeps `fetch_concurrency`
  (default `FETCH_CONCURRENCY` = 32) requests in flight, and heals enforce
  the byte budget once per batch instead of once per blob. Measured against
  a Bee 2.8.2 light node: a read of a chunk the node must fetch takes about
  270 ms, 4/s one at a time, 85-108/s at 32 at once.
- **`read_through`: a store can read blobs it never held**, as a fresh
  replica following a published root must. With `read_through=True` and a
  `fetcher` attached (a Syncer attaches one), such a read is fetched,
  hash-verified and returned *without* being stored: the blob belongs to no
  root of this store, which could account for it neither as pinned nor as
  evictable. Off by default — such a read is then a `KeyError`, as before —
  and a ref the network does not have either is a `KeyError` too.
- **Uploads go 32 at a time** (`SyncPolicy.push_concurrency`, default
  `PUSH_CONCURRENCY`): the worker's pushes and its repair re-pushes.
  Measured, same node: deferred uploads (the worker's kind: the node stores
  them and pushes on its own) 249/s one at a time, 609/s at 4, 869/s at 16,
  914/s at 32, 896/s at 64, where the node's local work has levelled off; direct uploads (the
  repairs: the request returns once the network has the chunk, ~300 ms)
  2.6/s one at a time, 34/s at 16, 64/s at 32, 85/s at 64.
- `scripts/concurrency_sweep.py` measures uploads too (`--op upload`,
  `--op upload-direct`, with `--batch`; one chunk of the batch per request).

### Changed

- `LocalStore.get_many` refuses a ref it cannot fetch (never held and no
  read-through, or evicted with no fetcher) before making any request,
  where it used to fail on reaching it after fetching the ones before.

## [0.11.2] — 2026-10-08

### Fixed

- **The sync worker repairs what the network lost instead of waiting for
  it.** Bee can count a chunk delivered on a "shallow receipt" and never
  retry it (`docs/bee-push-sync-findings.md`: 2026-09-11, and again on
  2026-10-07 with 36,589 of 372,313 pushes). The confirmation pass found
  such a blob missing and then only checked again, forever, so the root
  never confirmed and nothing re-sent it. Now, when a sampled blob of a
  root is not retrievable, every blob of that root is checked, each
  missing one is pushed again directly (not deferred) from the local
  copy, which stays pinned until confirmation, and the next round checks
  again. Only missing blobs are resent; `Syncer.repaired` counts them.
- **A small root is checked whole.** The confirmation sample was a
  quarter of a root's blobs but at least one, so a commit of four blobs
  checked one, and a lost blob slipped past three times in four. It now
  takes at least `MIN_CONFIRM_SAMPLE` (16) blobs, or the whole root when
  it is smaller.
- **Confirmation checks run 32 at a time** (`SyncPolicy.check_concurrency`,
  default `CHECK_CONCURRENCY`). They ran one at a time, and a stewardship
  check takes about 1.2 s on a light node, so confirming a quarter of a
  10,000-blob root took most of an hour: the reason every large publish
  overran a 60 s sync wait, with or without lost blobs. Measured, not
  guessed, against a Bee 2.8.2 light node: 0.9 checks/s one at a time,
  12/s at 16, 20/s at 32, 23/s at 64, 26-33/s at 128 with each check then
  waiting 3-4 s.

### Added

- `docs/bee-issue-draft.md`: the issue for Bee, drafted after the second
  occurrence; and that occurrence's evidence in `bee-push-sync-evidence/`.
- `scripts/concurrency_sweep.py`: how many requests to keep in flight
  against *your* node, for reads of chunks it must fetch and for
  stewardship checks. The best number depends on the node more than on
  the machine (the client spends about 1 ms of CPU per request).

## [0.11.1] — 2026-09-11

### Fixed

- Writable mount: a freshly created file is dirty only once bytes are written.
  The shell's `> file` closes a duplicated descriptor before writing, the
  kernel flushes on that close too, and the mounter committed an empty file
  followed by the real content — on a content-addressed object store
  (ontodag-fs) that left a stale empty object with the same label. Now one
  commit, of the content; an untouched new file is committed once at release.

## [0.11.0] — 2026-09-11

### Added

- **Writable FUSE mount** — `swarmfs mount --rw` / `swarmfs.fuse.mount(rw=True)`
  (+ `--stamp`, `--signer`): every saved file is one commit, made on close
  (`flush`/`fsync`) so a refused commit is the application's error;
  `rm`/`mv`/`mkdir` map to the filesystem's verbs (directory rename in one
  transaction; `mkdir` phantom until content lands); `chmod`/`chown`/`utimens`
  accepted and ignored; stamp checked before mounting; a `bzz://` mount prints
  the new root at unmount, a `bzzf://` mount publishes the feed. Any fsspec
  filesystem mounted via `fs=` can be writable too.

## [0.10.1] — 2026-09-11

### Changed

- `swarmfs.fuse.mount(fs=...)` accepts **any** fsspec filesystem, not only a
  Swarm one: the read-only policy, attributes and errno mapping are reusable
  by other backends (ontodag-fs mounts its lattice view through it). New
  `fsname=` parameter for the displayed mount source. `kernel_cache` is
  enabled only when a plain `bzz://` filesystem is inside.

## [0.10.0] — 2026-09-11

### Added

- **Standalone FUSE mount** — `swarmfs mount <url> <mountpoint>` (the
  package's first console script; also `python -m swarmfs`) and
  `swarmfs.fuse.mount()`: a `bzz://` reference, a sub-directory of one, or a
  `bzzf://` feed (a live, read-only view) as a local directory, via fsspec's
  FUSE wrapper. Read-only by design; `simplecache::` chaining works; needs
  the new `fuse` extra (fusepy) and a system libfuse 2. Verified against
  the offline fake node, a local Bee 2.8.2 and the public gateway.

- **ACT access control** — `act=True` protects every commit and upload
  (the root is ACT-wrapped, content encrypted by default); `act_history` +
  `act_publisher` read protected content; `publisher_key()`,
  `create_grantees()`, `grantees()`, `patch_grantees()` manage who may read.
  Client tier: `act=` on reads, `act=`/`act_history=` on uploads (returning
  `ActUpload`), `addresses`, `grantee_create/get/patch`. New module
  `swarmfs.act`. Live-validated against Bee 2.8.2.

### Fixed

- `SwarmClient.health()` accepted only a JSON body; the proxy in front of
  `api.gateway.ethswarm.org` answers plain-text `OK`, so first contact with
  that gateway failed with an aiohttp decode error. Any 2xx is healthy now.

## [0.9.0] — 2026-08-04

### Added

- **Encrypted storage and recall** — 128-hex references in the load path,
  live-validated against node-side decryption and `bzzf://` over 128-hex refs.
- `bytes_size` over encrypted references uses a ranged GET rather than HEAD.
- `REFERENCE.md` documents `swarmfs.feeds`, the surface swarmlite consumes.

### Changed

- **Dependency split:** keccak moves into the base install; the `feeds` extra
  now covers signing only.

## [0.8.0] — 2026-08-04

### Added

- Public raw-reference reads.

## [0.7.1] — 2026-08-04

### Documentation

- `REFERENCE.md` — definition-first, and pinned to the code by tests.
- README and user guide catch up with v3 (local-first).

## [0.7.0] — 2026-08-04

### Added

- **Local-first filesystem:** swarmfs writes go local-first; scrub, and batch
  expiries (roadmap L3+L4).
- Reads are local-first for known references.

### Fixed

- Transaction crash on fsspec < 2024.3.0; validated on the current stack.

## [0.6.0] — 2026-08-04

### Added

- **Retention primitives:** `rebase_root` + `gc_orphans`, the app-assisted
  squash.

## [0.5.0] — 2026-08-04

### Added

- **The local-first store** (`swarmfs.localstore` + `localsync`), built as a
  ladder: the offline core first, then the push worker and the network half,
  live-validated against Bee 2.8.1.
- `has_root` and `latest_root` — the journal as the pointer.
- **Commit-boundary fsync batching** as a durability knob: a many-small-blob
  commit pays one batch of fsyncs rather than one per put.
- Confirmation is p2p-native — no gateways, witness optional.

## [0.4.0] — 2026-07-29

### Changed

- Batch sizing derives from Bee's own tables.

### Fixed

- Topup detection.

## [0.3.0] — 2026-07-29

### Added

- **Stamp lifecycle:** topup, dilute, and renewal planning.
- PyPI keywords; tests, PyPI and license badges in the README.

### Changed

- Packaging and CI normalised across the stack; publishing on a `v*` tag push,
  uniformly with the sibling projects.

## [0.2.0] — 2026-07-28

**Compute Swarm references offline.** `swarmfs.content_address(data)` and
`swarmfs.split(data)` build the chunk tree and its BMT addresses with no node,
no network and no postage stamp — the exact inverse of the verifying joiner,
which previously existed only as a test helper.

- Know a reference (and whether the network already has the content) *before*
  spending a stamp.
- Name blobs in a local store by their Swarm address, so an offline directory
  and a published store share one address space (recordstore's
  `DirBytesStore(addressing="swarm")` does exactly this).
- Build test fixtures at real addresses.

Verified against a live Bee 2.8.1 at every tree shape: with erasure coding off,
the local reference equals what `POST /bytes` returns, exactly.

**Caveat, stated in the docs:** the computed reference is for a *plain* upload.
Erasure coding adds parity chunks that change every intermediate and therefore
the root, and parity is the node's to generate — so a redundant upload's
reference cannot be predicted offline. swarmfs writes with `redundancy=2` by
default, so pass `redundancy=0` if you want uploads to match the address you
computed. Such roots are recognisable: Bee sets the span's top byte to
`0x80 | level`.

Also in this release (earlier in the 0.1.x line's history): postage-stamp
purchase as a capability (`StampManager.plan`/`buy`, never implicit), and a fix
to the verifying joiner for erasure-coded trees, which assumed a 128-way fanout
and failed on redundancy-uploaded files.

Needs the `feeds` extra for keccak256: `pip install "swarmfs[feeds]"`.

## [0.1.0] — 2026-07-24

First PyPI release: fsspec backend for Ethereum Swarm (`bzz://` and `bzzf://`
URLs) over the Bee HTTP API — range reads, transactional writes, feeds, stamps
(selection, validation, purchase), client-side verification.

- `bzz://` read-only backend, validated against a live Bee node.
- `bzzf://` — mutable, feed-mounted filesystem.
- Transactional writes: stamps and the commit engine; zarr/xarray on Swarm.
- Gateway reads are opt-in, with client-side chunk verification (trustless
  reads).
- `redundancy=` write option (erasure coding), default level 2.
