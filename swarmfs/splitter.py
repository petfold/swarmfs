"""A splitter: content -> Swarm chunk tree, addressed locally.

The exact inverse of `join.py`'s verifying reader. Where the joiner descends a
tree of content-addressed chunks and checks each against the reference it was
fetched by, this builds that tree from bytes and returns the same root
reference Bee would have returned — **without a node, without a network, and
without a postage stamp**.

Why that is worth having:

- **Address before you upload.** You can know a file's `bzz://` reference (and
  therefore whether the network already has it) before spending a stamp.
- **One address space for every backend.** A local store that names blobs by
  this reference is a genuine offline mirror of Swarm: develop offline, publish
  later, and nothing is re-addressed. Hash locally with `hashlib` instead and
  the same dataset gets a different identity in each backend.
- **Testing without a node.** Fixtures can be built at real addresses, which
  is what makes the verifying joiner's tests meaningful.

**Scope: plain trees only.** The reference computed here is the one Bee
returns for an upload with erasure coding *disabled* (`redundancy=0`), which
is verified byte-for-byte against a live node at every tree shape. A node may
well default to adding parity — the node used for these tests defaults to
redundancy level 1 — and then the root differs, because parity chunks are the
node's to generate and they change every intermediate. Such a root is
recognisable: Bee marks it by setting the top byte of the span to
`0x80 | level` (which is why `join.decode_span` masks it off). So:

- to make a local address match what you upload, upload with `redundancy=0`;
- to use redundancy, let the node tell you the reference — do not expect to
  predict it.

Wire format (mirrors bee's `pkg/file/splitter`, and verified against a live
Bee 2.8.1 — see `tests/test_integration.py::test_local_split_matches_bee`):

- a **leaf** is `span(8, little-endian) + payload`, payload ≤ 4096 bytes;
- an **intermediate** is `span + child references` (32 bytes each), up to 128
  children, where span is the total *content* length beneath it;
- a level with a single entry is **promoted**, not wrapped in a one-child
  intermediate (the joiner treats `span <= 4096` as a leaf, so wrapping would
  produce a tree it reads differently);
- empty content is one chunk whose span is 0 and whose payload is empty.

Erasure coding is *not* applied (see "Scope" above). Redundancy adds parity
references to intermediates, which the joiner tolerates — it derives the
fanout from the first child's own span rather than assuming 128 — but which
only the node can generate.

Requires keccak256, i.e. the ``feeds`` extra (``pip install swarmfs[feeds]``).
"""

from __future__ import annotations

from .bmt import CHUNK_PAYLOAD_SIZE, chunk_address

REF_SIZE = 32
BRANCHES = CHUNK_PAYLOAD_SIZE // REF_SIZE  # 128 children per intermediate


def _leaf(payload: bytes) -> bytes:
    return len(payload).to_bytes(8, "little") + payload


def _intermediate(refs: list[bytes], span: int) -> bytes:
    return span.to_bytes(8, "little") + b"".join(refs)


def split(data: bytes) -> tuple[bytes, dict[bytes, bytes]]:
    """``(root reference, {address: chunk})`` for ``data``.

    The chunks are every node of the tree — leaves and intermediates — keyed by
    their own BMT address, which is exactly what ``POST /chunks`` expects and
    what `join.VerifyingReader` will ask for by reference.
    """
    chunks: dict[bytes, bytes] = {}

    def store(chunk: bytes) -> bytes:
        ref = chunk_address(chunk)
        chunks[ref] = chunk
        return ref

    if not data:
        return store(_leaf(b"")), chunks

    # level is [(reference, content length beneath it), ...]
    level = [
        (store(_leaf(data[i : i + CHUNK_PAYLOAD_SIZE])),
         len(data[i : i + CHUNK_PAYLOAD_SIZE]))
        for i in range(0, len(data), CHUNK_PAYLOAD_SIZE)
    ]

    while len(level) > 1:
        parents = []
        for i in range(0, len(level), BRANCHES):
            group = level[i : i + BRANCHES]
            if len(group) == 1:
                parents.append(group[0])       # promote, never wrap
                continue
            span = sum(size for _, size in group)
            parents.append(
                (store(_intermediate([ref for ref, _ in group], span)), span))
        level = parents

    return level[0][0], chunks


def content_address(data: bytes) -> bytes:
    """The Swarm reference ``data`` would have, computed locally.

    Discards the chunks; use `split` when you intend to upload or store them.
    """
    return split(data)[0]
