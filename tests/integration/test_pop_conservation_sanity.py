"""
Sanity Check for the Conservation of Population Mass.

This test performs a sanity check of the `worldpoppy` pipeline using real-world data.
It validates that merging and warping adjacent countries does not introduce significant
artefacts into population data.

Test Cases:
1. **BeNeLux (BEL, NLD, LUX):**
   Three small countries with highly irregular, interlocking borders. This maximises
   the ratio of "border pixels" relative to total area, making edge effects easier
   to detect.

2. **Adriatic Quintet (SVN, HRV, BIH, MNE, ALB):**
   Five small Adriatic countries with especially complex borders, where Croatia
   wraps around Bosnia like a crescent, creating a complex "hole-filling" scenario
   that challenges the merging logic differently than simple stacking.

Note on Terminology:
    This is classified as an **Integration Test** rather than a strict End-to-End (E2E)
    test because it respects the local cache. If raster files exist locally, the downloader
    is bypassed.

Warning:
    This test requires an internet connection and will download approx. 60-130MB per case
    *if* files are not already present in the cache.
"""

import pytest

from tests.test_utils import needs_internet_or_cache

# Common equal-area projection for Europe (ETRS89-extended / Lambert Equal Area)
LAEA_EUROPE = "EPSG:3035"


@pytest.mark.integration
@needs_internet_or_cache
@pytest.mark.parametrize(
    "case_name, iso_codes, target_crs",
    [
        ("BeNeLux", ["BEL", "NLD", "LUX"], LAEA_EUROPE),
        ("Adriatic Quintet", ["SVN", "HRV", "BIH", "MNE", "ALB"], LAEA_EUROPE),
    ],
)
def test_mass_conservation_eager(case_name, iso_codes, target_crs):
    """
    Parametrised test verifying that merging and warping adjacent countries
    preserves the total population count relative to individual raw rasters.

    This integration test focuses only on the case with eager data loading
    and merging (`chunks=None`). For an integration test involving a comparison
    between eager and lazy raster merges , see the separate
    `test_lazy_eager_merge_consistency` module.
    """
    from worldpoppy import wp_raster, wp_warp

    print(f"\n--- Testing Case: {case_name} ---")
    year = 2020

    # --- Pipeline Operation ---
    # Merge & Warp
    # Fetch and merge the countries into one grid
    combined_da = wp_raster(
        product_name='pop_g1',
        aoi=iso_codes,
        years=year,
        masked=True,
    )

    # Warp to the target metric projection
    combined_warped = wp_warp(
        combined_da,
        to_crs=target_crs,
        res=2_500,        # 2.5km resolution
        resampling='sum'  # Population must be summed
    )

    # Calculate the total mass of the processed result
    processed_total = float(combined_warped.sum())

    # --- Control Operation ---
    # Get Sum of Individual Raw Country Rasters
    # Load input rasters individually to get the "ground truth" sum.
    # We do not warp these; we just want the raw sum of people in the files.
    raw_total = 0.0

    for iso3 in iso_codes:
        da = wp_raster(
            product_name='pop_g1',
            aoi=iso3,
            years=year,
            masked=True,
            mask_and_scale=True,
        )
        raw_total += float(da.sum())
        da.close()

    # --- Check Discrepancy Size ---
    # We expect some discrepancy due to interpolation noise and border rasterisation.
    diff = abs(processed_total - raw_total)
    discrepancy_ratio = diff / raw_total

    print(f"Raw Total: {raw_total:,.0f}")
    print(f"Processed: {processed_total:,.0f}")
    print(f"Discrepancy: {discrepancy_ratio:.4%}")

    # Assert that the error is less than 1.5%
    assert discrepancy_ratio < 0.015, (
        f"Significant mass loss detected for {case_name}! "
        f"Discrepancy: {discrepancy_ratio:.2%} (Limit: 1.5%)"
    )
