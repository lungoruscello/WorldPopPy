"""
Consistency Check for Eager vs Lazy Raster Merges.

All test cases are identical to the ones used in `test_pop_conservation_sanity.py`.
See that other module for context.

Note on Terminology:
    This is classified as an **Integration Test** rather than a strict End-to-End (E2E)
    test because it respects the local cache. If raster files exist locally, the downloader
    is bypassed.

Warning:
    This test requires an internet connection and will download approx. 60-100MB per case
    *if* files are not already present in the cache.
"""

import numpy as np
import pytest

from tests.helpers import needs_internet_or_cache

# Largest coordinate difference allowed between the eager and the lazy grid, in
# pixels. The two grids are computed in different ways from the same files and
# differ by floating-point drift. WorldPop rasters use 3 or 30 arcseconds (about
# 90 m or 900 m at the equator), so 0.1 pixels is about 9 m or 90 m. In the
# 3-arcsecond data used here (`pop_g1`), the largest actual difference was
# 0.05 pixels (about 4 m; measured on 2026-11-15).
MAX_COORD_DIFF_PIXELS = 0.1


@pytest.mark.integration
@needs_internet_or_cache
@pytest.mark.parametrize(
    "case_name, iso_codes",
    [
        ("BeNeLux", ["BEL", "NLD", "LUX"]),
        ("Adriatic Quintet", ["SVN", "HRV", "BIH", "MNE", "ALB"]),
        ("Senegal-Gambia", ["SEN", "GMB"]),
        ("Caucasus", ["ARM", "AZE", "GEO"]),
    ],
)
def test_eager_vs_lazy_merge_consistency(case_name, iso_codes):
    """
    Parametrised test verifying that 'Eager' and 'Lazy' raster merges yield
    identical data, apart from an empty edge: the eager grid may extend by one
    column on the right and one row at the bottom, which must hold no data.
    On the shared cells, coordinates agree to a fraction of a pixel and values
    are identical.

    This integration test focuses only on the merge logic in `wp_raster`. It
    does NOT test the warping pipeline (`wp_warp`). For an integration test
    involving warping, see the separate `test_pop_conservation_sanity` module.

    """
    from worldpoppy import wp_raster

    print(f"\n--- Testing Consistency for: {case_name} ---")
    shared_opts = dict(product_name='pop_g1', aoi=iso_codes, years=2020, masked=True)

    # --- Merge Twice ---
    da_eager = wp_raster(chunks=None, **shared_opts)
    da_lazy_computed = wp_raster(chunks='auto', **shared_opts).compute()

    # --- Shape Check ---
    # The eager grid may be larger by at most one row and one column
    num_rows, num_cols = da_lazy_computed.sizes['y'], da_lazy_computed.sizes['x']
    extra_rows = da_eager.sizes['y'] - num_rows
    extra_cols = da_eager.sizes['x'] - num_cols
    assert extra_rows in (0, 1) and extra_cols in (0, 1), (
        f"Unexpected shapes: eager {da_eager.shape} vs lazy {da_lazy_computed.shape}"
    )

    # --- Empty Edge Check ---
    # The extra row (bottom) and column (right) of the eager grid hold no data
    edge = np.concatenate([
        da_eager.isel(y=slice(num_rows, None)).values.ravel(),
        da_eager.isel(x=slice(num_cols, None)).values.ravel(),
    ])
    assert np.isnan(edge).all(), (
        f"The extra edge of the eager grid holds {int(np.isfinite(edge).sum())} valid cells"
    )

    # --- Coordinate Alignment ---
    # The shared cells start at the top-left corner of both grids
    da_eager_shared = da_eager.isel(y=slice(0, num_rows), x=slice(0, num_cols))
    pixel_size = abs(float(da_lazy_computed.x[1] - da_lazy_computed.x[0]))
    for dim in ['x', 'y']:
        np.testing.assert_allclose(
            da_eager_shared[dim].values,
            da_lazy_computed[dim].values,
            rtol=0,
            atol=MAX_COORD_DIFF_PIXELS * pixel_size,
            err_msg=f"Coordinate mismatch in {dim}: the lazy grid is shifted against the eager grid.",
        )

    # --- Value Check ---
    # Values on the shared cells are identical (NaN where both are NaN)
    np.testing.assert_array_equal(
        da_eager_shared.values,
        da_lazy_computed.values,
        err_msg="Eager and lazy merges give different values on the shared cells.",
    )

    # Metadata Check
    assert da_eager.rio.crs == da_lazy_computed.rio.crs, "CRS Mismatch"

    print(f"Success: {case_name} is identical apart from an empty edge.")
