"""
Graph Construction Test When "Lazy Merging" Many Rasters.

This test measures the overhead of constructing the Dask task graph for the
lazy merge strategy implemented in `worldpoppy.raster._lazy_merge_helper`.

It empirically validates that the 'Tree Reduction' algorithm remains performant,
and avoids fatal recursion errors, when scaling to continental-level merges.
Importantly, the test does NOT actually execute the merge graph (compute).
It only measures the metadata-processing and graph-generation phase.

It simulates:
1. West Africa (ECOWAS): ~15 countries.
2. Continental Scale: ~50 countries.

Metrics:
- Graph Construction Time: Time taken to build the dask graph (the "recipe").
- Graph Complexity: Number of layers in the dask graph (proxy for total graph mass).

**Disclaimer:** Merging ~50 individual country files is NOT the intended or
recommended workflow for continental-scale analysis in `worldpoppy`. Ideally,
users will prefer pre-built continental rasters for such tasks. This test only
serves as a safety check against recursion limits.
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
    # We raise a warning when graph construction seems too slow for continental scales.
    # This indirectly corroborates whether we have avoided the worst-case recursion overhead
    # of a naive, linear merge strategy.
    if n_files == CONTINENTAL_COUNT:
        if duration < 5.0:
            print(f"Success: Graph construction for {n_files} files was efficient ({duration:.2f}s).")
        else:
            print(f"Warning: Graph construction was slow ({duration:.2f}s).")
