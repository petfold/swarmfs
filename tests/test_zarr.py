"""The v1 flagship demo: a zarr store on Swarm, written through swarmfs and
read back with zarr + xarray (the v1 exit criterion). Offline via FakeClient.

zarr 3's FsspecStore drives the *async* filesystem interface directly
(`_pipe_file`/`_cat_file`/`_find`/`_rm_file`), and writes chunks concurrently
— which also exercises the commit lock / lineage serialization.
"""

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")
zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from swarmfs import SwarmFileSystem  # noqa: E402

from conftest import FakeClient  # noqa: E402


def make_fs(store):
    return SwarmFileSystem(client=FakeClient(store), asynchronous=True, skip_instance_cache=True)


def test_zarr_array_roundtrip():
    from zarr.storage import FsspecStore

    swarm_store: dict = {}
    fs = make_fs(swarm_store)

    data = np.arange(10_000, dtype="f8").reshape(100, 100)
    z = zarr.create_array(
        store=FsspecStore(fs, path="new/array"),
        shape=data.shape,
        chunks=(25, 25),
        dtype=data.dtype,
    )
    z[:] = data

    new_root = fs.latest("new")
    assert len(new_root) == 64
    assert not fs._staged  # everything committed

    # read back through a *fresh* filesystem instance and the real root
    fs2 = make_fs(swarm_store)
    z2 = zarr.open_array(store=FsspecStore(fs2, read_only=True, path=f"{new_root}/array"))
    assert z2.shape == (100, 100)
    np.testing.assert_array_equal(z2[:], data)
    # partial (chunk-level) read
    np.testing.assert_array_equal(z2[10:30, 40:60], data[10:30, 40:60])


def test_xarray_dataset_roundtrip():
    """v1 exit criterion: create a zarr store on Swarm, read it back with xarray."""
    from zarr.storage import FsspecStore

    swarm_store: dict = {}
    fs = make_fs(swarm_store)

    ds = xr.Dataset(
        {"temperature": (("x", "y"), np.random.default_rng(7).normal(15, 3, (20, 30)))},
        coords={"x": np.arange(20), "y": np.arange(30)},
    )
    ds.to_zarr(FsspecStore(fs, path="new/climate"), mode="w", consolidated=False)

    new_root = fs.latest("new")
    fs2 = make_fs(swarm_store)
    out = xr.open_zarr(
        FsspecStore(fs2, read_only=True, path=f"{new_root}/climate"), consolidated=False
    ).load()
    xr.testing.assert_identical(out, ds)
