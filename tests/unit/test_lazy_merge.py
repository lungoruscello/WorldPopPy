"""
Lazy Merging of Many Rasters.

Lazy merges (`chunks` with two or more rasters) read the rasters as one virtual
mosaic, a VRT file in the cache directory (see `worldpoppy.mosaic` and
`worldpoppy.raster._open_lazy_mosaic`).

The performance test measures the overhead of preparing a lazy merge of many
rasters (building the mosaic and the Dask task graph), without computing it:

1. West Africa (ECOWAS): ~15 countries.
2. Continental Scale: ~50 countries.

Metrics:
- Graph Construction Time: Time taken to build the dask graph (the "recipe").
- Graph Complexity: Number of layers in the dask graph (proxy for total graph mass).

**Disclaimer:** Merging ~50 individual country files is NOT the intended or
recommended workflow for continental-scale analysis in `worldpoppy`. Ideally,
users will prefer pre-built continental rasters for such tasks.

The other tests check the VRT file: where it is written, when it is reused,
and that `purge_cache` removes it.
"""

# TODO Actually offer support for continental / global rasters

import time

import numpy as np
import pytest

from worldpoppy import merge_rasters

# ECOWAS (West Africa) has 15 member states
WEST_AFRICA_COUNT = 15

# Approximate count for a full continent (e.g., Africa ~54)
CONTINENTAL_COUNT = 54


@pytest.mark.performance
@pytest.mark.parametrize("n_files", [WEST_AFRICA_COUNT, CONTINENTAL_COUNT])
def test_lazy_merge_graph_construction(mock_raster_factory, n_files):
    """
    Benchmark the Dask graph construction for lazy-merging of N files.
    """
    print(f"\n\n--- Benchmarking Graph Construction with N={n_files} files ---")

    # 1. Setup: Create N overlapping mock rasters
    # We use 100x100 rasters with a 50% overlap (shifted by 50px).
    # This simulates a diagonal chain of neighbouring countries.
    tile_size = 100
    step = 50  # 50 pixels shift = 50% overlap with previous tile

    fpaths = []
    for i in range(n_files):
        # Calculate origin for a diagonal chain
        origin = (i * step, i * step)

        path = mock_raster_factory(
            f"rast_{i:03d}.tif",
            np.ones((tile_size, tile_size)),
            origin=origin
        )
        fpaths.append(path)

    # 2. Measure Graph Construction Time
    # We use chunks='auto' to force Lazy Dask execution
    start_time = time.perf_counter()

    merged = merge_rasters(fpaths, chunks='auto')

    # Remove the 'band' dimension to get a clean 2D array (y, x)
    merged = merged.squeeze()

    end_time = time.perf_counter()
    duration = end_time - start_time

    # 3. Analyse Graph Complexity
    # The 'data' attribute of the xarray DataArray is a dask.array
    dask_arr = merged.data

    try:
        # Modern Dask (HighLevelGraph)
        n_layers = len(dask_arr.dask.layers)
    except AttributeError:
        # Older Dask versions
        n_layers = len(dask_arr.dask)

    print(f"Time to build graph: {duration:.4f} seconds")
    print(f"Total Dask Layers (Proxy for Graph Mass): {n_layers}")

    # 4. Correctness Sanity Check (Metadata only)
    # The final shape is determined by the extent of the chain.
    max_coord = (n_files - 1) * step + tile_size
    expected_shape = (max_coord, max_coord)

    assert merged.shape == expected_shape

    # 5. Soft Empirical Check
    # We print a warning when graph construction seems too slow for continental scales.
    if n_files == CONTINENTAL_COUNT:
        if duration < 5.0:
            print(f"Success: Graph construction for {n_files} files was efficient ({duration:.2f}s).")
        else:
            print(f"Warning: Graph construction was slow ({duration:.2f}s).")


# --- Tests: The Mosaic File ---

def _mosaic_files():
    from worldpoppy.config import get_cache_dir
    from worldpoppy.raster import MOSAIC_DIRNAME

    return sorted((get_cache_dir() / MOSAIC_DIRNAME).glob("*.vrt"))


def test_lazy_merge_reads_a_mosaic_file_in_the_cache(mock_raster_factory, recwarn):
    """
    A lazy merge of two rasters reads one VRT file in the cache subfolder
    MOSAIC_DIRNAME, gives the values of the eager merge, and no longer warns
    that lazy merging is experimental.
    """
    data = np.arange(9, dtype="float32").reshape(3, 3)
    a = mock_raster_factory("a.tif", data, origin=(0, 3))
    b = mock_raster_factory("b.tif", data + 100, origin=(2, 2))

    lazy = merge_rasters([a, b], chunks=2)
    eager = merge_rasters([a, b])

    assert len(_mosaic_files()) == 1
    assert lazy.chunks is not None, "The lazy merge returned a computed (not chunked) array."
    np.testing.assert_array_equal(lazy.compute().values, eager.values)
    assert not [w for w in recwarn if "experimental" in str(w.message)]


def test_mosaic_file_is_reused_and_renewed(mock_raster_factory):
    """
    The same rasters in the same order reuse their VRT file; another order
    (which changes the priority) or a replaced raster gets a new one.
    """
    data = np.ones((3, 3), dtype="float32")
    a = mock_raster_factory("a.tif", data, origin=(0, 3))
    b = mock_raster_factory("b.tif", data * 2, origin=(3, 3))

    merge_rasters([a, b], chunks=2)
    merge_rasters([a, b], chunks=2)
    assert len(_mosaic_files()) == 1

    merge_rasters([b, a], chunks=2)
    assert len(_mosaic_files()) == 2

    mock_raster_factory("b.tif", np.full((3, 4), 5.0, dtype="float32"), origin=(3, 3))  # replaced
    merge_rasters([a, b], chunks=2)
    assert len(_mosaic_files()) == 3


def test_new_vrt_format_gets_a_new_mosaic_file(mock_raster_factory, monkeypatch):
    """
    A new VRT format version gets a new mosaic file, so that outdated cached
    files are not reused.
    """
    import worldpoppy.raster as raster

    data = np.ones((3, 3), dtype="float32")
    a = mock_raster_factory("a.tif", data, origin=(0, 3))
    b = mock_raster_factory("b.tif", data * 2, origin=(3, 3))

    merge_rasters([a, b], chunks=2)
    monkeypatch.setattr(raster, "VRT_FORMAT_VERSION", raster.VRT_FORMAT_VERSION + 1)
    merge_rasters([a, b], chunks=2)

    assert len(_mosaic_files()) == 2


def test_lazy_merge_of_different_resolutions_raises(mock_raster_factory):
    """
    Rasters that cannot form one mosaic (here: different resolutions) raise an
    IncompatibleRasterError naming the raster.
    """
    from worldpoppy.raster import IncompatibleRasterError

    a = mock_raster_factory("a.tif", np.ones((3, 3), dtype="float32"), origin=(0, 3))
    b = mock_raster_factory("b.tif", np.ones((6, 6), dtype="float32"), origin=(3, 3), res=0.5)

    with pytest.raises(IncompatibleRasterError, match="b.tif has a different resolution"):
        merge_rasters([a, b], chunks=2)


def test_read_only_cache_gives_a_clear_error(mock_raster_factory):
    """
    If the cache directory is not writable, a lazy merge of several rasters
    raises an OSError that says why the cache is written and how to avoid it.
    """
    from worldpoppy.config import get_cache_dir
    from worldpoppy.raster import MOSAIC_DIRNAME

    data = np.ones((3, 3), dtype="float32")
    a = mock_raster_factory("a.tif", data, origin=(0, 3))
    b = mock_raster_factory("b.tif", data, origin=(3, 3))
    mosaic_dir = get_cache_dir() / MOSAIC_DIRNAME
    mosaic_dir.mkdir(parents=True)
    mosaic_dir.chmod(0o555)  # read-only
    try:
        with pytest.raises(OSError, match="not writable.*WORLDPOPPY_CACHE_DIR"):
            merge_rasters([a, b], chunks=2)
    finally:
        mosaic_dir.chmod(0o755)


def test_purge_cache_removes_mosaic_files(mock_raster_factory):
    from worldpoppy import purge_cache

    data = np.ones((3, 3), dtype="float32")
    a = mock_raster_factory("a.tif", data, origin=(0, 3))
    b = mock_raster_factory("b.tif", data, origin=(3, 3))
    merge_rasters([a, b], chunks=2)
    assert len(_mosaic_files()) == 1

    purge_cache(dry_run=False)

    assert _mosaic_files() == []
