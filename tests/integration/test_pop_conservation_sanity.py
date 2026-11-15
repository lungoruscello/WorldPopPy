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
   Five small Adriatic countries with complex borders, where Croatia wraps around
   Bosnia like a crescent, creating an interesting "hole-filling" scenario.

3. **Senegal-Gambia:**
    Gambia is a small, thin country entirely surrounded by larger Senegal, making
    for a challenging topology test.

4. **Caucasus ("AZE", "ARM", "GEO"):
    Three further small countries with complex borders, including an *exclave*
    scenario since AZE is split by ARM. This tests the case in which a country
    is not a single contiguous blob of pixels.

Note on Terminology:
    This is classified as an **Integration Test** rather than a strict End-to-End (E2E)
    test because it respects the local raster-data cache. If raster files exist locally, the
    downloader is bypassed. (The VRT files of lazy merges are built anew in every test, see
    `tests/integration/conftest.py`.)

Warning:
    This test requires an internet connection and will download approx. 60-130MB per case
    *if* files are not already present in the cache.
"""

import pytest
from pyproj import CRS

from tests.helpers import needs_internet_or_cache

# Lambert equal-area projection for Europe
LAEA_EUROPE = "EPSG:3035"

# Custom Albers equal-area projection for Senegal
# Thanks to: https://projectionwizard.org/
AEQA_SEN = CRS(
    "+proj=cea +lon_0=-14.5 +lat_ts=14.5 "
    "+datum=WGS84 +units=m +no_defs"
)

# Custom Albers equal-area projection for the Caucasus
AEQA_CAUC = CRS(
    "+proj=aea +lat_1=39 +lat_2=43 +lat_0=41 +lon_0=45 +x_0=0 +y_0=0 "
    "+datum=WGS84 +units=m +no_defs"
)

# Largest relative change in the total population allowed after merging and
# warping. The largest change measured on 2026-11-15 was 0.0011% ("Caucasus" case,
# with GDAL 3.12 and rasterio 1.5). Older libraries do much worse (0.18% with GDAL 3.5
# and rasterio 1.3); see the nox session `accuracy_versions`.
MAX_DISCREPANCY = 0.0001


@pytest.mark.integration
@pytest.mark.accuracy
@needs_internet_or_cache
@pytest.mark.parametrize(
    "case_name, iso_codes, target_crs",
    [
        ("BeNeLux", ["BEL", "NLD", "LUX"], LAEA_EUROPE),
        ("Adriatic Quintet", ["SVN", "HRV", "BIH", "MNE", "ALB"], LAEA_EUROPE),
        ("Senegal-Gambia", ["SEN", "GMB"], AEQA_SEN),
        ("Caucasus", ["ARM", "AZE", "GEO"], AEQA_CAUC),
    ],
)
def test_mass_conservation_eager(case_name, iso_codes, target_crs):
    """
    Parametrised test verifying that merging and warping adjacent countries
    preserves the total population count relative to individual raw rasters.

    This integration test focuses only on the case with eager data loading
    and merging (`chunks=None`). For an integration test involving a comparison
    between eager and lazy raster merges, see the separate
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

    # Calculate the total mass of the processed result (summed in float64,
    # since float32 sums of millions of people are off by a few people)
    processed_total = float(combined_warped.sum(dtype="float64"))

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
        raw_total += float(da.sum(dtype="float64"))
        da.close()

    # --- Check Discrepancy Size ---
    # We expect some discrepancy due to interpolation noise and border rasterisation.
    diff = abs(processed_total - raw_total)
    discrepancy_ratio = diff / raw_total

    print(f"Raw Total: {raw_total:,.0f}")
    print(f"Processed: {processed_total:,.0f}")
    print(f"Discrepancy: {discrepancy_ratio:.4%}")

    assert discrepancy_ratio < MAX_DISCREPANCY, (
        f"Significant change in the total population for {case_name}! "
        f"Discrepancy: {discrepancy_ratio:.4%} (Limit: {MAX_DISCREPANCY:.2%})"
    )
