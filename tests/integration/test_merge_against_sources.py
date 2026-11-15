"""
Integration Tests: Merged Results Against Their Source Files.

Rationale:
    `wp_raster` merges, pre-clips and clips country rasters in two ways (eager
    and lazy), and stacks the years of multi-year products. Other tests compare
    these paths with each other, or check totals. This module checks merged
    results against an independent reference instead: the expected value of
    every cell is read directly from the source files with `rasterio`, without
    any of worldpoppy's merging code. A shifted, flipped or misplaced grid, a
    lost or added row or column, nodata covering valid data, or a wrong clip
    therefore fails these tests, even if eager and lazy results agree.

    Not covered here: which file wins where two files both hold valid data.
    The rasters of neighbouring WorldPop countries overlap only where at least
    one of them holds nodata (checked on 2026-11-15: making the last file win
    passes every case), so the priority rule is tested with synthetic rasters
    in the unit tests instead.

Reference:
    For the centre of each cell of a result, the expected value is that of the
    first source file (in the order of the merge, i.e. of `wpy_source_files`)
    that holds a valid value at that point; if no file does, the expected value
    is NaN. This restates the documented rule of merges: where rasters overlap,
    the first raster wins, and nodata never covers valid data. In addition,
    every cell centre must lie on a cell centre of each source file that
    covers it, within MAX_GRID_NOISE pixels. (worldpoppy is used only to find
    the cached source files, not to read or place their values.)

Extent:
    Values that are right for their coordinates do not show a grid that is
    shifted as a whole, or that lost or added a row or column at its edge. So
    the grid of each result is also compared with the grid that the source
    files define: their union, on the grid of the first file, and for a
    bounding-box AoI only the cells that the box touches.

Cells:
    Every cell of each result is checked. A cell's column in a source file
    depends only on its x coordinate, and its row only on its y coordinate, so
    the expected values of a whole result come from one indexed read of each
    source file.

Cases:
    Mostly small countries, each adding a different situation: two countries
    sharing one island (overlapping rasters along a land border; also for two
    stacked years, and for an integer elevation product), separate islands (a
    gap between files), pixel sizes that differ by floating-point noise,
    negative longitudes and latitudes, a medium-sized merge of three countries,
    a bounding box across a border (AoI clip edges), and a 30-arcsecond
    product. Each case runs eagerly and lazily.

Note on Terminology:
    This is classified as an **Integration Test** rather than a strict
    End-to-End (E2E) test because it respects the local raster-data cache. If
    raster files exist locally, the downloader is bypassed. (The VRT files of
    lazy merges are built anew in every test, see `tests/integration/conftest.py`.)

Warning:
    This test requires an internet connection and will download approx. 63 MB
    *if* files are not already present in the cache.
"""

from pathlib import Path

import numpy as np
import pytest
import rasterio

from tests.helpers import needs_internet_or_cache
from worldpoppy.mosaic import MAX_GRID_NOISE

# Number of mismatching cells listed in a failure message
NUM_MISMATCHES_SHOWN = 5


# --- Helpers ---

def _expected_grid(source_paths, bbox=None):
    """
    Return the cell centres (x, y) of the grid that the source files define:
    the union of the files on the grid of the first file, in whole pixels,
    and for a bounding box (min_x, min_y, max_x, max_y) only the cells that
    the box touches.
    """
    transforms_and_sizes = []
    for path in source_paths:
        with rasterio.open(path) as src:
            transforms_and_sizes.append((src.transform, src.width, src.height))
    ref = transforms_and_sizes[0][0]

    # Column and row ranges of each file on the first file's grid
    cols, rows = [], []
    for transform, width, height in transforms_and_sizes:
        col0 = round((transform.c - ref.c) / ref.a)
        row0 = round((transform.f - ref.f) / ref.e)
        cols.append((col0, col0 + width))
        rows.append((row0, row0 + height))
    col = np.arange(min(c for c, _ in cols), max(c for _, c in cols))
    row = np.arange(min(r for r, _ in rows), max(r for _, r in rows))

    if bbox is not None:
        min_x, min_y, max_x, max_y = bbox
        left, right = ref.c + col * ref.a, ref.c + (col + 1) * ref.a
        top, bottom = ref.f + row * ref.e, ref.f + (row + 1) * ref.e
        col = col[(left < max_x) & (right > min_x)]
        row = row[(bottom < max_y) & (top > min_y)]

    return ref.c + (col + 0.5) * ref.a, ref.f + (row + 0.5) * ref.e


def _assert_grid_matches_sources(result, source_paths, bbox, label):
    """
    Assert that the grid of `result` is the grid that the source files define
    (see `_expected_grid`), within MAX_GRID_NOISE pixels.
    """
    x, y = _expected_grid(source_paths, bbox)
    res = abs(x[1] - x[0])
    for dim, expected in (("x", x), ("y", y)):
        actual = result[dim].values
        assert actual.size == expected.size, (
            f"{label}: {actual.size} cells along {dim}, but the source files define {expected.size} "
            f"(from {expected[0]:.6f} to {expected[-1]:.6f}; result from {actual[0]:.6f} to {actual[-1]:.6f})"
        )
        np.testing.assert_allclose(
            actual, expected, rtol=0, atol=MAX_GRID_NOISE * res,
            err_msg=f"{label}: the {dim} coordinates differ from the grid of the source files",
        )


def _source_paths(result, product_name, year):
    """
    Return the local paths of the source files of `result` (for one year), in
    the order of the merge. worldpoppy's downloader only locates the cached
    files here (it downloads nothing that the merge did not already need).
    """
    from worldpoppy.config import get_cache_dir
    from worldpoppy.download import WorldPopDownloader

    countries = result.attrs["wpy_countries"].split()
    used_names = set(result.attrs["wpy_source_files"].split("\n"))
    paths, rows = WorldPopDownloader(directory=get_cache_dir()).download(
        product_name, countries, None if year is None else [year], True
    )
    return [Path(p) for p, row in zip(paths, rows.itertuples()) if row.remote_name in used_names]


def _expected_values(x, y, source_paths):
    """
    Return the expected value of every cell (y by x), and the index of the
    source file that provides it (-1 for none), read directly from the
    source files.

    Steps, for each source file in the order of the merge:
    1. Find the file's column for every x and its row for every y (cell
       centres; separable, so two short index arrays suffice).
    2. Check that these centres lie on the file's cell centres, within
       MAX_GRID_NOISE pixels.
    3. Read the file's values (nodata masked) at these rows and columns, and
       keep them for the cells that no earlier file has filled with a valid
       value: the first file with a valid value wins.
    """
    expected = np.full((y.size, x.size), np.nan)
    source_index = np.full((y.size, x.size), -1, dtype=np.int8)

    for i, path in enumerate(source_paths):
        with rasterio.open(path) as src:
            values = src.read(1, masked=True).astype("float64").filled(np.nan)
            transform, width, height = src.transform, src.width, src.height

        # 1. Column of every x, row of every y (fractional, then the nearest)
        col_exact = (x - transform.c) / transform.a - 0.5
        row_exact = (y - transform.f) / transform.e - 0.5
        col, row = np.round(col_exact).astype(int), np.round(row_exact).astype(int)
        in_x = (col >= 0) & (col < width)
        in_y = (row >= 0) & (row < height)
        if not in_x.any() or not in_y.any():
            continue

        # 2. Cell centres on the file's grid
        off_grid = max(np.abs(col_exact - col)[in_x].max(), np.abs(row_exact - row)[in_y].max())
        assert off_grid <= MAX_GRID_NOISE, (
            f"The result's cells lie {off_grid:.3g} pixels off the grid of {path.name}"
        )

        # 3. The first file with a valid value wins
        ys, xs = np.flatnonzero(in_y), np.flatnonzero(in_x)
        block = values[np.ix_(row[in_y], col[in_x])]
        take = np.isfinite(block) & (source_index[np.ix_(ys, xs)] == -1)
        expected[np.ix_(ys, xs)] = np.where(take, block, expected[np.ix_(ys, xs)])
        source_index[np.ix_(ys, xs)] = np.where(take, i, source_index[np.ix_(ys, xs)])

    return expected, source_index


def _assert_matches_sources(result, source_paths, label):
    """
    Assert that every cell of the 2-D `result` holds its expected value (see
    `_expected_values`), listing the first mismatches in the failure message.
    """
    x, y = result.x.values, result.y.values
    actual = result.values.astype("float64")
    expected, source_index = _expected_values(x, y, source_paths)

    assert np.isfinite(expected).any(), f"{label}: no valid cell at all (the check would be empty)"
    mismatch = ~((actual == expected) | (np.isnan(actual) & np.isnan(expected)))
    if mismatch.any():
        lines = []
        for r, c in np.argwhere(mismatch)[:NUM_MISMATCHES_SHOWN]:
            i = source_index[r, c]
            source = source_paths[i].name if i >= 0 else "no file"
            lines.append(
                f"  lon {x[c]:.6f}, lat {y[r]:.6f}: expected {expected[r, c]} ({source}), "
                f"got {actual[r, c]}"
            )
        pytest.fail(
            f"{label}: {int(mismatch.sum())} of {mismatch.size} cells differ from the source "
            f"files, e.g.:\n" + "\n".join(lines)
        )


# --- Tests ---

@pytest.mark.integration
@needs_internet_or_cache
@pytest.mark.parametrize("chunks", [None, 128], ids=["eager", "lazy"])
# (Chunks of 128 cells, so that even the small results span several chunks)
@pytest.mark.parametrize("product_name, aoi, years", [
    # Two countries on one small island: their rasters overlap along a land border
    pytest.param("pop_g1", ["SXM", "MAF"], 2020, id="Saint-Martin"),
    # The same, for two stacked years: each year must match its own source files
    pytest.param("pop_g1", ["SXM", "MAF"], [2000, 2020], id="Saint-Martin-two-years"),
    # A static product with integer source files, read masked
    pytest.param("srtm_elevation_g1", ["SXM", "MAF"], None, id="Saint-Martin-elevation"),
    # Separate islands: the gap between the files must stay empty (NaN)
    pytest.param("pop_g1", ["GGY", "JEY"], 2020, id="Guernsey+Jersey"),
    # Pixel sizes that differ by floating-point noise (1.6e-9, relative)
    pytest.param("pop_g1", ["LIE", "LUX"], 2020, id="Liechtenstein+Luxembourg"),
    # Negative longitudes and latitudes (South Pacific)
    pytest.param("pop_g1", ["WLF", "NIU"], 2020, id="Wallis+Niue"),
    # A medium-sized merge of three neighbouring countries
    pytest.param("pop_g1", ["BEL", "NLD", "LUX"], 2020, id="Benelux"),
    # A bounding box with round-number edges across the Belgian-Dutch border:
    # the clip must keep exactly the cells that the box touches
    pytest.param("pop_g1", (4.5, 51.2, 5.0, 51.5), 2020, id="Belgian-Dutch-bbox"),
    # 30 arcseconds instead of 3
    pytest.param("pop_g2_1km_r25a", ["RWA", "BDI"], 2020, id="Rwanda+Burundi-30arcsec"),
])
def test_merged_cells_match_their_source_files(product_name, aoi, years, chunks):
    """
    Every cell of a `wp_raster` result holds the value of the first source
    file that has a valid value at the cell's centre (NaN if none has one),
    and lies on that file's grid. See the module docstring for the reference
    and the choice of cases.
    """
    from worldpoppy import wp_raster

    result = wp_raster(product_name, aoi=aoi, years=years, chunks=chunks).compute()

    bbox = aoi if isinstance(aoi, tuple) else None

    if "year" in result.dims:
        # The years share one grid: the union of all years' grids
        all_paths = [p for year in result.year.values for p in _source_paths(result, product_name, int(year))]
        _assert_grid_matches_sources(result, all_paths, bbox, "stacked years")
        for year in result.year.values:
            paths = _source_paths(result, product_name, int(year))
            _assert_matches_sources(result.sel(year=year), paths, f"year {int(year)}")
    else:
        year = years if isinstance(years, int) else None
        paths = _source_paths(result, product_name, year)
        _assert_grid_matches_sources(result, paths, bbox, "result")
        _assert_matches_sources(result, paths, "result")
