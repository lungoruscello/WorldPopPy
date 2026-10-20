"""
Integration Tests for Resolving AoIs to Countries.

Rationale:
    The unit tests in `tests/unit/test_geo.py` verify the AoI-resolution logic
    against a tiny synthetic "world". These tests check that the same logic
    also works with the *real* country borders that `worldpoppy` uses (the
    simplified, buffered polygons returned by `load_country_borders`).

Note on Terminology:
    This is classified as an **Integration Test** because it uses real border
    data. The buffered borders are built from a bundled asset (no internet
    needed) and then cached locally; if the cache is cold, the first test
    takes a few seconds longer.
"""
import geopandas as gpd
import pytest
from shapely.geometry import box

# Well inside both Belgium and Luxembourg, across their shared border
BEL_LUX_BORDER_BOX = box(5.60, 49.80, 6.00, 49.90)
# Mid-Atlantic, far from any coast (incl. the ~1 km buffer of the borders)
MID_ATLANTIC_BOX = box(-30.0, -20.0, -29.0, -19.0)


@pytest.mark.integration
def test_real_borders_resolve_multi_country_aoi_and_skip_offshore(caplog):
    """
    Verify that an AoI across the BEL/LUX border resolves to exactly these
    two countries, and that an offshore AoI geometry is skipped with a warning.
    """
    from worldpoppy.raster import _standardise_aoi

    aoi = gpd.GeoDataFrame(geometry=[BEL_LUX_BORDER_BOX, MID_ATLANTIC_BOX], crs="EPSG:4326")

    _, iso3_codes, _ = _standardise_aoi(aoi)

    assert iso3_codes == ["BEL", "LUX"]
    assert "1 of 2 AoI geometries do not intersect" in caplog.text


@pytest.mark.integration
def test_real_borders_aoi_at_sea_raises():
    """
    Verify that an AoI far out at sea intersects no real country.
    """
    from worldpoppy.raster import _standardise_aoi

    with pytest.raises(ValueError, match="does not intersect any country"):
        _standardise_aoi(MID_ATLANTIC_BOX.bounds)
