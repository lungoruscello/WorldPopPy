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

from tests.test_utils import needs_internet_or_cache


def normalise_grid(da, precision=4):
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
    "case_name, iso_codes, pxl_rtol, pxl_bad_tol",
    [
        ("BeNeLux", ["BEL", "NLD", "LUX"],  0.001, 0.001),
        ("Adriatic Quintet", ["SVN", "HRV", "BIH", "MNE", "ALB"], 0.01, 0.05),
        ("Senegal-Gambia", ["SEN", "GMB"],  0.01, 0.05),
        ("Caucasus", ["ARM", "AZE", "GEO"], 0.05, 0.1),
    ],
)
def test_eager_vs_lazy_merge_consistency(case_name, iso_codes, pxl_rtol, pxl_bad_tol):
    """
    Parametrised test verifying that 'Eager' and 'Lazy' raster merges yield
    statistically similar data.

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
    # We only allow for a 1-pixel footprint difference
    shape_diff = np.abs(np.array(da_eager.shape) - np.array(da_lazy_computed.shape))
    assert np.all(shape_diff <= 1), (
        f"Dimensions differ by more than 1 pixel: {da_eager.shape} vs {da_lazy_computed.shape}"
    )

    # --- Normalise Grids ---
    # We round coordinates to 4 decimals, which corresponds to
    # below ~10 meter precision using lat/lon coordinates. While
    # these tolerances are fairly relaxed, we are using hard test
    # cases.
    da_eager = normalise_grid(da_eager, precision=4)
    da_lazy_computed = normalise_grid(da_lazy_computed, precision=4)

    # --- Check Coordinate Alignment ---
    # 1. Determine the common intersection shape
    # We take the minimum size in each dimension.
    min_x = min(da_eager.sizes['x'], da_lazy_computed.sizes['x'])
    min_y = min(da_eager.sizes['y'], da_lazy_computed.sizes['y'])

    # 2. Manually Slice to the shared area
    # We assume the top-left (index 0) is the shared origin.
    # We slice both arrays to [:min_y, :min_x]
    da_eager_sliced = da_eager.isel(x=slice(0, min_x), y=slice(0, min_y))
    da_lazy_sliced = da_lazy_computed.isel(x=slice(0, min_x), y=slice(0, min_y))

    # 3. Coordinate Comparison
    # Now that shapes match, we can compare coordinates directly.
    for dim in ['x', 'y']:
        np.testing.assert_allclose(
            da_eager_sliced[dim].values,
            da_lazy_sliced[dim].values,
            atol=2e-4,  # Double the rounding precision from `normalise_grid`
            err_msg=f"Coordinate mismatch in {dim}! Lazy merge drifted from Eager.",
        )

    # --- Pop-Data Checks ----

    # 1. Total Sum Check: Overall population should be VERY CLOSE
    sum_eager = float(da_eager.sum())
    sum_lazy = float(da_lazy_computed.sum())
    diff = abs(sum_eager - sum_lazy)
    discrepancy_ratio = diff / sum_eager

    # Assert that the *total* population discrepancy is less than 0.01%
    assert discrepancy_ratio < 0.0001, (
        f"Total sum of raster values differs too much: {sum_eager} vs {sum_lazy}"
    )

    # 2. Pixel-wise NaN Alignment Check
    # A pixel is a mismatch if it is NaN in one array but valid in the other.
    arr_eager = da_eager_sliced.values
    arr_lazy = da_lazy_sliced.values
    nan_mismatch = np.isnan(arr_eager) != np.isnan(arr_lazy)
    nan_fail_ratio = np.sum(nan_mismatch) / nan_mismatch.size

    # We allow < 0.1% of pixels to disagree on "validity".
    assert nan_fail_ratio < 0.001, (
        f"NaN Mismatch Ratio too high: {nan_fail_ratio:.6%} "
        f"({np.sum(nan_mismatch)} pixels disagree on being NaN)"
    )

    # 3. Pixel-wise Value Check (Distortion/Resampling Error)
    # Identify pixels that differ beyond tolerance.
    # `equal_nan=True` ensures two matching NaN's are treated equal.
    value_mismatch = ~np.isclose(
        arr_eager,
        arr_lazy,
        rtol=pxl_rtol,
        atol=1e-05,
        equal_nan=True
    )

    value_fail_ratio = np.sum(value_mismatch) / value_mismatch.size

    # We use a parameterised threshold here.
    assert value_fail_ratio < pxl_bad_tol, (
        f"Value Mismatch Ratio too high: {value_fail_ratio:.6%} "
        f"({np.sum(value_mismatch)} valid pixels differ significantly)"
    )

    # Metadata Check
    assert da_eager.rio.crs == da_lazy_computed.rio.crs, "CRS Mismatch"

    print(f"Success: {case_name} matches within tolerance.")
