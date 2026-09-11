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

### Added

- **Standalone FUSE mount** — `swarmfs mount <url> <mountpoint>` (the
  package's first console script; also `python -m swarmfs`) and
  `swarmfs.fuse.mount()`: a `bzz://` reference, a sub-directory of one, or a
  `bzzf://` feed (a live, read-only view) as a local directory, via fsspec's
  FUSE wrapper. Read-only by design; `simplecache::` chaining works; needs
  the new `fuse` extra (fusepy) and a system libfuse 2. Verified against
  the offline fake node, a local Bee 2.8.2 and the public gateway.

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
