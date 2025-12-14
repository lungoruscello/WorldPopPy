"""
Consistency Check for Eager vs Lazy Raster Merges.

Test Cases:
    1. **BeNeLux** (BEL, NLD, LUX)
    2. **Adriatic Quintet** (SVN, HRV, BIH, MNE, ALB)

    These two specific regions are chosen because they are also used in the separate
    `test_pop_conservation_sanity` module. By testing on raster data for these regions,
    we maximise cache hits and avoid downloading additional unnecessary raster data.

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
import xarray as xr

from tests.test_utils import needs_internet_or_cache


def normalise_grid(da, precision=5):
    """
    Round raster coordinates to a fixed precision to prevent floating-point
    mis-alignments (e.g., 10.0000001 vs 10.0) from breaking the merge test.
    """
    da['x'] = np.round(da['x'], precision)
    da['y'] = np.round(da['y'], precision)
    return da


@pytest.mark.integration
@needs_internet_or_cache
@pytest.mark.parametrize(
    "case_name, iso_codes",
    [
        ("BeNeLux", ["BEL", "NLD", "LUX"]),
        ("Adriatic Quintet", ["SVN", "HRV", "BIH", "MNE", "ALB"]),
    ],
)
def test_eager_vs_lazy_merge_consistency(case_name, iso_codes):
    """
    Parametrised test verifying that  'Eager' and 'Lazy' raster merges yield
    statistically identical data, tolerating minor grid floating-point noise.

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

    # --- Normalise Grids ---
    # We round coordinates to ~1 meter precision (5 decimals)
    # to allow for floating-point differences
    da_eager = normalise_grid(da_eager)
    da_lazy_computed = normalise_grid(da_lazy_computed)

    # --- Align ---
    # Eager and Lazy merging can result in merged rasters whose dimensions
    # are off by 1 pixel. 'join="inner"' finds the intersection of the two grids.
    eager_aligned, lazy_aligned = xr.align(da_eager, da_lazy_computed, join="inner")

    # --- Checks ----
    # Total Sum Check
    sum_eager = float(da_eager.sum())
    sum_lazy = float(da_lazy_computed.sum())
    diff = abs(sum_eager - sum_lazy)
    discrepancy_ratio = diff / sum_eager

    # Assert that the *total* discrepancy is less than 0.01%
    assert discrepancy_ratio < 0.0001, (
        f"Total sum of raster values differs too much: {sum_eager} vs {sum_lazy}"
    )

    # Shape Check
    shape_diff = np.abs(np.array(da_eager.shape) - np.array(da_lazy_computed.shape))
    assert np.all(shape_diff <= 1), (
        f"Dimensions differ by more than 1 pixel: {da_eager.shape} vs {da_lazy_computed.shape}"
    )

    assert eager_aligned.size > 0, "Grids do not overlap! Alignment resulted in empty array."

    # Pixel-wise Data Check (Relaxed Tolerances)
    #  >> rtol=1e-5: Allows 0.001% relative difference (resampling noise)
    #  >> atol=1e-6: Allows tiny absolute differences near zero
    #  >> equal_nan=True: Treats NaN in both as a match (CRITICAL for masked rasters)
    np.testing.assert_allclose(
        eager_aligned.values,
        lazy_aligned.values,
        rtol=1e-05,
        atol=1e-06,
        equal_nan=True,
        err_msg=f"Mismatch in {case_name}: Eager vs Lazy values differ."
    )

    # Metadata Check
    assert da_eager.rio.crs == da_lazy_computed.rio.crs, "CRS Mismatch"

    print(f"Success: {case_name} matches within tolerance.")
