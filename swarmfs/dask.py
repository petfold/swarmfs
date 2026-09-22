"""Write a dask dataset to Swarm as **one** manifest, from many processes.

``fs.transaction`` is per-process. Point dask's own ``dd.to_parquet`` at
``bzz://new/ds`` and each worker stages into its own lineage and commits its
own root: N unrelated references, N stamp validations, and the driver never
learns any of them. Nothing is broken — there is simply no channel from a
worker's ``open(..., "wb")`` back to the process that should assemble the
dataset.

This module is that channel, and it is deliberately thin because content
addressing does the work (docs/distributed-writes.md):

1. each worker writes its partition to Parquet **in memory** and uploads it
   with ``fs.put_blob``, getting back a bare data reference;
2. the references come home to the driver, which ``fs.link``s them into one
   lineage inside a single transaction;
3. one commit builds one manifest — and, for ``bzzf://``, publishes one feed
   update.

A Mantaray entry is a reference: it does not care which process uploaded the
bytes, through which node, or on which postage batch. That is the whole
trick.

Nothing unpicklable crosses a process boundary — the worker builds its own
filesystem from ``storage_options`` — so unlike the generic path this works
on a real cluster (processes, distributed), not just the threaded scheduler.
A ``signer`` never leaves the driver: publishing the feed is the driver's
job, so workers are not handed the feed owner's private key.

    import swarmfs.dask as sd

    res = sd.to_parquet(ddf, "bzz://new/sales",
                        storage_options={"stamp": "auto"})
    res.root        # the dataset's reference — one manifest, one commit
    res.batches     # {(node, batch)} — the dataset lives as long as the
                    # shortest-lived of these (see 'Stamps' below)

Stamps: a batch belongs to the node that issued it, so with one node per
cluster every worker resolves the same batch and there is nothing to think
about. With several nodes each worker spends its own, and the dataset's
lifetime is the shortest ``batchTTL`` among them — which the driver cannot
see, hence ``batches``. Renewal stays with the caller (``list_batches()``,
``StampInfo.problem``); swarmfs never chases foreign batches.
"""

from __future__ import annotations

import io
import posixpath
from dataclasses import dataclass, field

import fsspec

# Storage options that belong to the driver alone. The signer is the feed
# owner's private key: the driver publishes the feed update, workers only
# upload blobs (``put_blob`` touches no feed), so shipping it to every
# worker would spread a secret for nothing.
DRIVER_ONLY_OPTIONS = frozenset({"signer"})


@dataclass
class DatasetWrite:
    """What one distributed write produced."""

    url: str  # where it was written (the bzz://new/… or bzzf:// URL given)
    root: str  # the new root reference: the whole dataset, after one commit
    written: dict[str, str] = field(default_factory=dict)  # path -> reference
    sizes: dict[str, int] = field(default_factory=dict)  # path -> bytes
    # (node api_url, postage batch) actually used — one entry per node when
    # a cluster spans several. The dataset's lifetime is the shortest
    # batchTTL among these.
    batches: set[tuple[str, str | None]] = field(default_factory=set)

    @property
    def paths(self) -> list[str]:
        return sorted(self.written)

    def __len__(self) -> int:
        return len(self.written)


def to_parquet(
    ddf,
    url: str,
    storage_options: dict | None = None,
    *,
    name_function=None,
    write_index: bool = True,
    partition_on=None,
    write_metadata_file: bool = False,
    compute_kwargs: dict | None = None,
    **parquet_kwargs,
) -> DatasetWrite:
    """Write a dask DataFrame to ``url`` as one Swarm manifest.

    ``url`` is a directory-style destination: ``bzz://new/sales`` for a fresh
    manifest, ``bzz://<root>/sales`` to add to an existing one, or
    ``bzzf://<owner>/<topic>/sales`` to advance a feed. Returns a
    ``DatasetWrite`` — ``.root`` is the new reference, ``.batches`` the
    (node, batch) pairs the partitions were stamped with.

    Partitions are named ``part.<i>.parquet`` unless ``name_function(i)``
    says otherwise; ``partition_on=[col, ...]`` writes hive-style
    ``col=value/`` directories instead (those columns are dropped from the
    data, as dask does). ``write_index`` and any further keyword arguments
    go to pandas' ``to_parquet`` for each partition. ``compute_kwargs`` go
    to ``dask.compute`` (``scheduler="processes"``, a ``Client``, …).

    Local-first workers (``local_store=``) call ``fs.sync()`` before handing
    their reference over: a manifest may not name content the network does
    not hold yet.
    """
    import dask

    if write_metadata_file:
        raise NotImplementedError(
            "write_metadata_file=True is not supported: a real _metadata "
            "footer has to aggregate every partition's Parquet metadata "
            "across processes, which this helper does not carry back. Read "
            "the dataset by directory instead — dd.read_parquet lists the "
            "manifest and reads each footer, which is what the swarmfs "
            "tests do.")

    protocol, base = _split_url(url)
    options = dict(storage_options or {})
    worker_options = {k: v for k, v in options.items()
                      if k not in DRIVER_ONLY_OPTIONS}
    fs = fsspec.filesystem(protocol, **options)
    # fail early, in the driver, before a cluster spends anything: an
    # unusable batch here is an unusable batch on every worker
    if getattr(fs, "_local", None) is None:
        fs.resolve_stamp()

    name_function = name_function or _default_name
    partition_on = list(partition_on) if partition_on else None

    parts = ddf.to_delayed()
    tasks = [
        dask.delayed(_write_partition, pure=False)(
            part, name_function(i), protocol, worker_options,
            write_index, partition_on, parquet_kwargs,
        )
        for i, part in enumerate(parts)
    ]
    results = dask.compute(*tasks, **(compute_kwargs or {}))

    res = DatasetWrite(url=url, root="")
    with fs.transaction:
        for records in results:
            for path, reference, size, api_url, batch in records:
                fs.link(f"{base}/{path}", reference, size=size)
                res.written[path] = reference
                res.sizes[path] = size
                res.batches.add((api_url, batch))
    res.root = fs.latest(url)
    return res


def _write_partition(df, filename, protocol, storage_options, write_index,
                     partition_on, parquet_kwargs):
    """Worker side: partition -> Parquet bytes -> data reference(s).

    Builds its own filesystem from ``storage_options`` (nothing unpicklable
    travels) and returns plain tuples, so any scheduler can carry them.
    """
    fs = fsspec.filesystem(protocol, **storage_options)
    local_first = getattr(fs, "_local", None) is not None
    batch = None if local_first else fs.resolve_stamp()

    out = []
    for frame, path in _partition_frames(df, filename, partition_on):
        buf = io.BytesIO()
        frame.to_parquet(buf, engine="pyarrow", index=write_index,
                         **parquet_kwargs)
        data = buf.getvalue()
        reference = fs.put_blob(data, stamp=batch)
        out.append([path, reference, len(data), fs.api_url, batch])

    if local_first and out:
        # §5: a linked reference must already be on the network — the
        # manifest the driver commits would otherwise advertise content only
        # this worker's disk holds.
        fs.sync()
        batch = fs.resolve_stamp()  # what the push actually spent
        for record in out:
            record[4] = batch
    return [tuple(record) for record in out]


def _partition_frames(df, filename, partition_on):
    """(frame, path) pairs for one partition: one, or one per hive group."""
    if not partition_on:
        yield df, filename
        return
    for keys, group in df.groupby(partition_on, observed=True, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        if group.empty:
            continue
        prefix = "/".join(f"{col}={value}"
                          for col, value in zip(partition_on, keys))
        yield group.drop(columns=partition_on), posixpath.join(prefix, filename)


def _default_name(i: int) -> str:
    return f"part.{i}.parquet"


def _split_url(url: str) -> tuple[str, str]:
    """(protocol, url without a trailing slash) — the destination directory."""
    protocol, sep, _ = url.partition("://")
    if not sep or protocol not in ("bzz", "bzzf"):
        raise ValueError(
            f"{url!r} is not a Swarm destination: expected bzz://new/<dir>, "
            "bzz://<root>/<dir> or bzzf://<owner>/<topic>/<dir>")
    return protocol, url.rstrip("/")
