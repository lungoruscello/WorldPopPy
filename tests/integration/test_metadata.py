"""
Integration Tests for the Metadata of WorldPopPy Results.

Rationale:
    The unit tests in `tests/unit/test_metadata.py` check the metadata rules
    of `docs/metadata.md` with synthetic rasters and fake manifest rows. This
    test checks that a *real* request yields the provenance of the *real*
    data manifest (official titles, summary URLs, source files, institution)
    and survives saving to a GeoTIFF.

Note on Terminology:
    This is classified as an **Integration Test** because it uses real
    WorldPop rasters and the real data manifest. The rasters are downloaded
    if they are not cached yet (two small 1 km files per year).
"""
import numpy as np
import pytest
import rioxarray

from tests.helpers import needs_internet_or_cache

PRODUCT = "pop_g2_1km_r25a"
COUNTRIES = ["GHA", "TGO"]
YEARS = [2015, 2024]


@pytest.mark.integration
@needs_internet_or_cache
def test_real_result_carries_manifest_provenance(tmp_path):
    """
    A real multi-year, multi-country result records the official titles and
    summary URLs, source files, countries, years and institution from the
    data manifest, and keeps them (and its band names) when saved as GeoTIFF.
    """
    from worldpoppy import wp_raster
    from worldpoppy.manifest_loader import _get_active_manifest_info, wp_manifest_constrained

    rows = wp_manifest_constrained(PRODUCT, COUNTRIES, YEARS)
    da = wp_raster(PRODUCT, aoi=COUNTRIES, years=YEARS)
    attrs = da.attrs

    references = attrs["references"].split("\n")
    for row in rows.itertuples():
        assert f"{row.api_entry_title}: {row.summary_url}" in references
    assert sorted(attrs["wpy_source_files"].split("\n")) == sorted(rows.remote_name)
    assert attrs["wpy_countries"] == "GHA TGO"
    assert attrs["wpy_years"] == "2015 2024"
    assert attrs["wpy_product_type"] == "multi-year"
    assert attrs["creator_institution"] == rows.api_source.iloc[0]
    assert attrs["wpy_manifest_built_at"] != "unknown"
    assert attrs["wpy_manifest_built_at"].startswith(
        _get_active_manifest_info().built_at.date().isoformat()
    )
    assert np.isnan(da.rio.nodata)

    da.rio.to_raster(tmp_path / "saved.tif")
    saved = rioxarray.open_rasterio(tmp_path / "saved.tif")
    assert saved.attrs["long_name"] == ("2015", "2024")
    assert saved.attrs["references"] == attrs["references"]
    assert np.isnan(saved.rio.nodata)
