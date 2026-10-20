import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import box

# A tiny synthetic "world" of rectangular countries (WGS84). Belgium and
# Luxembourg share an edge at lon=5.0; everything outside the boxes is sea.
SYNTHETIC_BORDERS = {
    "BEL": box(3.0, 50.0, 5.0, 51.0),
    "LUX": box(5.0, 49.5, 6.0, 50.5),
    "KEN": box(34.0, -4.0, 41.0, 4.0),
}
BEL_LUX_STRADDLE_BOX = box(4.5, 50.2, 5.5, 50.4)  # across the BEL/LUX edge
KEN_INLAND_BOX = box(36.7, -1.4, 36.9, -1.2)
SEA_BOX_A = box(-30.0, -20.0, -29.0, -19.0)
SEA_BOX_B = box(-35.0, -25.0, -34.0, -24.0)


@pytest.fixture
def synthetic_borders(monkeypatch):
    """
    Fixture that replaces the real country borders (68k buffered polygons,
    built from a bundled asset) with SYNTHETIC_BORDERS. This keeps AoI tests
    fast and independent of any cache, and makes the test geometry explicit.
    """
    import worldpoppy.borders
    import worldpoppy.raster

    world = gpd.GeoDataFrame(
        {"iso3": list(SYNTHETIC_BORDERS)},
        geometry=list(SYNTHETIC_BORDERS.values()),
        crs="EPSG:4326",
    )
    # `raster` imports the function by name; `plot_utils` looks it up in
    # `borders` at call time. Patch both.
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)
    monkeypatch.setattr(worldpoppy.borders, "load_country_borders", lambda: world)
    return world


def test_bbox_area_km_moderate_lats():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 2_000

    for lat in np.linspace(-60, 60, 50):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)


def test_bbox_area_km_high_lats():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 1_000

    for lat in np.linspace(-70, 70, 50):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 percent


def test_bbox_area_km_extreme_lats():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 500

    for lat in np.linspace(-80, 80, 50):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 percent


def test_bbox_date_line_crash():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.func_utils import BboxInvalidError

    for lat in np.linspace(-80, 80, 20):
        with pytest.raises(BboxInvalidError):
            bbox_from_location((180.0, lat), width_degrees=2.0)


def test_bbox_pole_crash_latitude():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.func_utils import BboxInvalidError

    for lon in np.linspace(-180, 180, 20):
        with pytest.raises(BboxInvalidError):
            bbox_from_location((lon, 89.0), width_degrees=4.0)


def test_aoi_at_sea_raises_clear_error(synthetic_borders):
    """
    An AoI that intersects no country raises a clear ValueError, rather than
    passing a NaN country code downstream.
    """
    from worldpoppy.raster import _standardise_aoi

    with pytest.raises(ValueError, match="does not intersect any country"):
        _standardise_aoi(SEA_BOX_A.bounds)


@pytest.mark.parametrize("empty_aoi", [[], ()], ids=["list", "tuple"])
def test_aoi_empty_sequence_raises_clear_error(empty_aoi):
    """
    An empty list or tuple raises a clear ValueError, rather than an
    IndexError from deep inside the AoI parsing.
    """
    from worldpoppy.raster import _standardise_aoi

    with pytest.raises(ValueError, match="`aoi` is empty"):
        _standardise_aoi(empty_aoi)


def test_aoi_empty_gdf_raises_clear_error(synthetic_borders):
    """
    An AoI GeoDataFrame without any geometries is reported as empty (not as
    lying at sea).
    """
    from worldpoppy.raster import _standardise_aoi

    empty_aoi = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    with pytest.raises(ValueError, match="is empty"):
        _standardise_aoi(empty_aoi)


def test_aoi_spanning_several_countries(synthetic_borders):
    """
    An AoI that straddles a border resolves to all countries it intersects.
    """
    from worldpoppy.raster import _standardise_aoi

    _, iso3_codes, _ = _standardise_aoi(BEL_LUX_STRADDLE_BOX.bounds)

    assert iso3_codes == ["BEL", "LUX"]


def test_aoi_ignores_offshore_geometries_with_warning(synthetic_borders, caplog):
    """
    If only some AoI geometries lie at sea, the others still resolve to their
    countries, and a warning reports the geometries that contribute no data.
    (The land geometry straddles two countries, so the count of AoI geometries
    differs from the number of rows in the spatial join.)
    """
    from worldpoppy.raster import _standardise_aoi

    aoi = gpd.GeoDataFrame(geometry=[BEL_LUX_STRADDLE_BOX, SEA_BOX_A], crs="EPSG:4326")

    _, iso3_codes, _ = _standardise_aoi(aoi)

    assert iso3_codes == ["BEL", "LUX"]
    assert "1 of 2 AoI geometries do not intersect" in caplog.text


def test_aoi_offshore_count_with_duplicate_index(synthetic_borders, caplog):
    """
    The offshore warning counts geometries, not index labels: duplicate index
    values in the user's GeoDataFrame (e.g., after `pd.concat`) must not
    distort the count.
    """
    from worldpoppy.raster import _standardise_aoi

    aoi = gpd.GeoDataFrame(
        geometry=[KEN_INLAND_BOX, SEA_BOX_A, SEA_BOX_B],
        index=[0, 1, 1],
        crs="EPSG:4326",
    )

    _, iso3_codes, _ = _standardise_aoi(aoi)

    assert iso3_codes == ["KEN"]
    assert "2 of 3 AoI geometries do not intersect" in caplog.text


def test_aoi_gdf_with_own_iso3_column(synthetic_borders):
    """
    A user GeoDataFrame may carry its own 'iso3' column (e.g., admin
    boundaries). This must not clash with the country lookup.
    """
    from worldpoppy.raster import _standardise_aoi

    aoi = gpd.GeoDataFrame({"iso3": ["user value"]}, geometry=[KEN_INLAND_BOX], crs="EPSG:4326")

    _, iso3_codes, _ = _standardise_aoi(aoi)

    assert iso3_codes == ["KEN"]


@pytest.mark.parametrize("aoi, expected", [
    ("lux", ["LUX"]),
    ([" tha", "khm"], ["THA", "KHM"]),
])
def test_aoi_iso3_codes_are_standardised(aoi, expected):
    """
    Country codes are accepted in any case and with surrounding whitespace.
    """
    from worldpoppy.raster import _standardise_aoi

    _, iso3_codes, orig_aoi_type = _standardise_aoi(aoi)

    assert iso3_codes == expected
    assert orig_aoi_type == "iso"


def _record_plotted_isos(monkeypatch):
    """
    Replace `GeoDataFrame.plot` with a recorder, and return the list to which
    the ISO3 codes of every plotted GeoDataFrame are appended (as a set).
    (These tests only need to know *which* countries `plot_country_borders`
    selects, not how matplotlib renders them.)
    """
    plotted = []

    def fake_plot(self, *args, **kwargs):
        plotted.append(set(self.iso3))

    monkeypatch.setattr(gpd.GeoDataFrame, "plot", fake_plot)
    return plotted


@pytest.mark.parametrize("iso3_codes", [["LUX", "BEL"], ["lux", " bel"]])
def test_plot_country_borders_selects_requested_countries(
    synthetic_borders, monkeypatch, iso3_codes
):
    """
    `plot_country_borders` draws exactly the requested countries, whatever
    the case of their codes. (Previously, lowercase codes selected nothing:
    the plot then either stayed empty or crashed.)
    """
    from matplotlib.figure import Figure
    from worldpoppy.plot_utils import plot_country_borders

    plotted = _record_plotted_isos(monkeypatch)
    plot_country_borders(iso3_codes, ax=Figure().add_subplot())

    assert plotted == [{"BEL", "LUX"}]


@pytest.mark.parametrize("keyword", ["all", " ALL "])
def test_plot_country_borders_all_keyword(synthetic_borders, monkeypatch, keyword):
    """
    The 'all' keyword (in any case) draws the borders of every supported
    country, exactly as if all their codes had been passed explicitly.
    """
    from matplotlib.figure import Figure
    import worldpoppy.plot_utils
    from worldpoppy.plot_utils import plot_country_borders

    # Synthetic list of supported countries (instead of the real manifest).
    # FRA is "supported" but has no border in the synthetic world.
    all_isos = ["BEL", "FRA", "KEN", "LUX"]
    monkeypatch.setattr(worldpoppy.plot_utils, "get_all_isos", lambda: all_isos)

    plotted = _record_plotted_isos(monkeypatch)
    plot_country_borders(keyword, ax=Figure().add_subplot())
    plot_country_borders(all_isos, ax=Figure().add_subplot())

    via_keyword, via_codes = plotted
    assert via_keyword == set(SYNTHETIC_BORDERS)
    assert via_keyword == via_codes
