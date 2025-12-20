"""
Core Integration Tests for `wp_raster`.

Rationale:
    These tests verify the structural correctness of the data returned by
    `wp_raster` (dimensions, coordinates, keyword resolution) without
    performing expensive numerical validation.

    We use **Liechtenstein (LIE)** for all tests because it is very small
    (~160 km²), resulting in tiny downloads (<1MB).

Note on Terminology:
    This is classified as an **Integration Test** rather than a strict End-to-End (E2E)
    test because it respects the local cache. If raster files exist locally, the downloader
    is bypassed.
"""
import pytest

from tests.test_utils import needs_internet_or_cache

# Tiny, fast test case
TEST_ISO = "LIE"


@pytest.mark.integration
@needs_internet_or_cache
@pytest.mark.parametrize("product_name, kind", [
    ("admin0", "yearless"),      # Case A: Static, no year associated (Grid areas)
    ("srtm_elevation_g1", "linked"),  # Case B: Static, but linked to a specific year (2007)
])
def test_static_products_are_2d(product_name, kind):
    """
    Verify that static products are always returned as 2D arrays (y, x),
    never 3D (year, y, x), regardless of whether they are technically linked
    to a year in the manifest.
    """
    from worldpoppy import wp_raster

    # We rely on the default `years=None` argument here
    da = wp_raster(product_name, aoi=TEST_ISO)

    # Check Dimensions
    assert da.ndim == 2, (
        f"Static product '{product_name}' ({kind}) returned {da.ndim} dimensions "
        f"{da.dims}. Expected exactly 2 (y, x)."
    )
    assert set(da.dims) == {'y', 'x'}


@pytest.mark.integration
@needs_internet_or_cache
def test_explicit_single_year_is_2d():
    """
    Verify that requesting a *single* explicit year from a multi-year
    product returns a squeezed 2D array, not a 3D array of size 1.
    """
    from worldpoppy import wp_raster

    product = "pop_g1"
    target_year = 2020

    # Passing one year
    da = wp_raster(product, aoi=TEST_ISO, years=target_year)

    # 1. Check Dimensions (Should be squeezed to 2D)
    assert da.ndim == 2, (
        f"Single-year request returned {da.ndim}D array {da.dims}. "
        "Expected 2D (y, x)."
    )
    assert set(da.dims) == {'y', 'x'}

    # 2. Check that year information is preserved as a scalar coordinate
    assert 'year' in da.coords
    assert int(da.year) == target_year


@pytest.mark.integration
@needs_internet_or_cache
def test_raster_keyword_resolution():
    """
    Verify that passing `years='last'` works end-to-end:
    1. Resolves to a concrete integer (e.g., 2020).
    2. Fetches that specific slice.
    3. Returns a 2D array (because the single year is squeezed).
    """
    from worldpoppy import wp_raster
    from worldpoppy.manifest_loader import get_product_info

    product = "pop_g1"

    # 1. Determine expected "last" year from manifest ground truth
    info = get_product_info(product)
    expected_year = max(info['years'])

    # 2. Fetch using keyword
    da = wp_raster(product, aoi=TEST_ISO, years='last')

    # 3. Verify Structure (2D)
    assert da.ndim == 2
    assert set(da.dims) == {'y', 'x'}

    # 4. Verify Content
    # The coordinate label should match the max year
    actual_year = int(da.year)
    assert actual_year == expected_year, (
        f"Keyword 'last' resolved to {actual_year}, but manifest max is {expected_year}"
    )


@pytest.mark.integration
@needs_internet_or_cache
def test_raster_multi_year_stacking():
    """
    Verify that requesting multiple explicit years returns a correctly
    stacked 3D array sorted by time.
    """
    from worldpoppy import wp_raster

    product = "pop_g1"
    # Request unordered years; results should be sorted
    target_years = [2015, 2010]

    da = wp_raster(product, aoi=TEST_ISO, years=target_years)

    # 1. Check Shape
    assert da.ndim == 3
    assert da.sizes['year'] == 2

    # 2. Check Sort Order (Time should inevitably increase)
    assert da.year.values.tolist() == [2010, 2015]


@pytest.mark.integration
@needs_internet_or_cache
def test_mixed_year_keywords_resolution():
    """
    Verify that passing a list of keywords (e.g., ['first', 'last']) works.
    """
    from worldpoppy import wp_raster
    from worldpoppy.manifest_loader import get_product_info

    product = "pop_g1"

    # Ground Truth
    info = get_product_info(product)
    min_year = min(info['years'])
    max_year = max(info['years'])

    # 1. Request ['first', 'last']
    da = wp_raster(product, aoi=TEST_ISO, years=['first', 'last'])

    assert da.ndim == 3
    assert da.sizes['year'] == 2
    assert da.year.values.tolist() == [min_year, max_year]

    # 2. Request Mixed [2010, 'last'] (assuming max_year != 2010)
    # We use a set in case max_year happens to be 2010
    expected_years = sorted(list({2010, max_year}))

    da_mixed = wp_raster(product, aoi=TEST_ISO, years=[2010, 'last'])
    assert da_mixed.year.values.tolist() == expected_years
