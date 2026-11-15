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


@pytest.mark.integration
@pytest.mark.accuracy
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
    identical results: the same grid (shape and coordinates) and the same
    values. (Before 0.5.0, the eager grid could have an extra, empty row and
    column, TODO 1.36.)

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

    # --- Grid Check ---
    assert da_eager.shape == da_lazy_computed.shape, (
        f"Different shapes: eager {da_eager.shape} vs lazy {da_lazy_computed.shape}"
    )
    for dim in ['x', 'y']:
        np.testing.assert_array_equal(
            da_eager[dim].values,
            da_lazy_computed[dim].values,
            err_msg=f"Coordinate mismatch in {dim}: the eager and lazy grids differ.",
        )

    # --- Value Check ---
    # Values are identical (NaN where both are NaN)
    np.testing.assert_array_equal(
        da_eager.values,
        da_lazy_computed.values,
        err_msg="Eager and lazy merges give different values.",
    )

    # Metadata Check
    assert da_eager.rio.crs == da_lazy_computed.rio.crs, "CRS Mismatch"

    print(f"Success: {case_name} is identical.")
