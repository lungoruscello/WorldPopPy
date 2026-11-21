from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import Polygon, box

from worldpoppy.func_utils import BboxInvalidError

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
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 per cent


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
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 per cent


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


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize("position", range(4))
@pytest.mark.parametrize("validator", ["validate_bbox", "validate_bbox_wgs84"])
def test_bbox_validation_rejects_non_finite_values(validator, position, bad_value):
    """
    Bounding boxes with NaN or infinite values are rejected (bug 1.25: NaN
    passed `validate_bbox_wgs84`, since every comparison with NaN is false).
    """
    import worldpoppy.func_utils

    bounds = [1.0, 2.0, 3.0, 4.0]
    bounds[position] = bad_value
    with pytest.raises(BboxInvalidError, match="finite"):
        getattr(worldpoppy.func_utils, validator)(tuple(bounds))


def test_bbox_validation_in_any_crs():
    """
    `validate_bbox` accepts boxes in any CRS (e.g. metres), but rejects
    boxes whose minimum exceeds the maximum on either axis.
    """
    from worldpoppy.func_utils import validate_bbox

    validate_bbox((500_000, 5_500_000, 600_000, 5_600_000))
    for inverted in [(3.0, 2.0, 1.0, 4.0), (1.0, 4.0, 3.0, 2.0)]:
        with pytest.raises(BboxInvalidError, match="minimum"):
            validate_bbox(inverted)


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
    values in the user's GeoDataFrame (e.g. after `pd.concat`) must not
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
    A user GeoDataFrame may carry its own 'iso3' column (e.g. admin
    boundaries). This must not clash with the country lookup.
    """
    from worldpoppy.raster import _standardise_aoi

    aoi = gpd.GeoDataFrame({"iso3": ["user value"]}, geometry=[KEN_INLAND_BOX], crs="EPSG:4326")

    _, iso3_codes, _ = _standardise_aoi(aoi)

    assert iso3_codes == ["KEN"]


def _write_aoi_file(path, crs="EPSG:4326"):
    """
    Write BEL_LUX_STRADDLE_BOX (in the given CRS) to a vector file, whose
    format follows from the file suffix, and return the path.
    """
    gpd.GeoSeries([BEL_LUX_STRADDLE_BOX], crs="EPSG:4326").to_crs(crs).to_file(path)
    return path


@pytest.mark.parametrize("make_aoi", [
    lambda tmp_path: BEL_LUX_STRADDLE_BOX,
    lambda tmp_path: gpd.GeoSeries([BEL_LUX_STRADDLE_BOX], crs="EPSG:4326"),
    lambda tmp_path: gpd.GeoSeries([BEL_LUX_STRADDLE_BOX], crs="EPSG:4326").to_crs("EPSG:3857"),
    lambda tmp_path: str(_write_aoi_file(tmp_path / "aoi.geojson")),
    lambda tmp_path: _write_aoi_file(tmp_path / "aoi.gpkg", crs="EPSG:3857"),
], ids=["shapely", "geoseries", "geoseries-projected", "geojson-str", "gpkg-path-projected"])
def test_aoi_geometry_inputs(synthetic_borders, tmp_path, make_aoi):
    """
    A shapely geometry, a GeoSeries, or the path to a vector file (as str or
    Path) becomes a GeoDataFrame with the same geometry (respecting the input's
    CRS), and resolves to all countries that this geometry intersects.
    """
    from worldpoppy.raster import _standardise_aoi

    aoi, iso3_codes, orig_aoi_type = _standardise_aoi(make_aoi(tmp_path))

    assert isinstance(aoi, gpd.GeoDataFrame)
    aoi_geom = aoi.to_crs("EPSG:4326").geometry.iloc[0]
    assert aoi_geom.equals_exact(BEL_LUX_STRADDLE_BOX, tolerance=1e-6)
    assert iso3_codes == ["BEL", "LUX"]
    assert orig_aoi_type == "gdf"


@pytest.mark.parametrize("aoi, error, match", [
    (box(500_000, 5_500_000, 600_000, 5_600_000), BboxInvalidError, "must use longitude/latitude"),
    (Polygon(), ValueError, "geometry is empty"),
    (gpd.GeoSeries([KEN_INLAND_BOX]), ValueError, "no coordinate reference system"),
    (gpd.GeoDataFrame(geometry=[KEN_INLAND_BOX]), ValueError, "no coordinate reference system"),
    ("missing.gpkg", FileNotFoundError, "path separator or a file suffix"),
    ("data/missing", FileNotFoundError, "Cannot find the AoI file"),
    (Path("missing"), FileNotFoundError, "Cannot find the AoI file"),
    ("Congo", ValueError, "'Congo' is ambiguous"),
    (["KEN", "Atlantis"], ValueError, "neither a three-letter country code nor a known"),
    (42, TypeError, "Cannot parse `aoi` of type int"),
], ids=["shapely-projected", "shapely-empty", "geoseries-no-crs", "gdf-no-crs",
        "str-suffix-missing", "str-separator-missing", "path-missing", "name-ambiguous",
        "name-unknown-in-list", "unsupported-type"])
def test_aoi_unusable_inputs_raise_clear_errors(
    synthetic_borders, synthetic_country_names, monkeypatch, tmp_path, aoi, error, match
):
    """
    Unusable AoIs raise errors that say what is wrong. In particular, a
    shapely geometry (which carries no CRS) must use WGS84 coordinates,
    GeoSeries/GeoDataFrames must have a CRS (rather than being silently taken
    to be WGS84), and strings that look like paths are read as files.
    """
    from worldpoppy.raster import _standardise_aoi

    monkeypatch.chdir(tmp_path)  # relative paths must not exist
    with pytest.raises(error, match=match):
        _standardise_aoi(aoi)


@pytest.mark.parametrize("aoi, expected", [
    ("lux", ["LUX"]),
    ([" tha", "khm"], ["THA", "KHM"]),
    ("Ivory Coast", ["CIV"]),
    (("St. Lucia",), ["LCA"]),
    (["Kenya", "tur"], ["KEN", "TUR"]),
])
def test_aoi_country_codes_and_names(synthetic_country_names, monkeypatch, aoi, expected):
    """
    Country codes are accepted in any case and with surrounding whitespace;
    longer strings are resolved as country names, also within lists. Codes
    and names are used as they are: no country borders are loaded and no
    geometry is created (so the merged raster is not clipped).
    """
    import worldpoppy.raster
    from worldpoppy.raster import _standardise_aoi

    def fail():
        raise AssertionError("Country borders were loaded for country codes.")

    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", fail)

    std_aoi, iso3_codes, orig_aoi_type = _standardise_aoi(aoi)

    assert std_aoi == aoi
    assert iso3_codes == expected
    assert orig_aoi_type == "iso"


@pytest.mark.parametrize("aoi, expected", [
    ("ken", ["KEN"]),
    ("Kenya", ["KEN"]),
    ("St. Lucia", ["LCA"]),
])
def test_aoi_country_code_or_name_takes_precedence_over_file(
    synthetic_country_names, monkeypatch, tmp_path, aoi, expected
):
    """
    A string without a path separator or file suffix is always a country code
    or name, even if a file of that name exists in the working directory.
    """
    from worldpoppy.raster import _standardise_aoi

    monkeypatch.chdir(tmp_path)
    (tmp_path / aoi).touch()

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


@pytest.mark.parametrize("to_crs", [None, "EPSG:3035"], ids=["wgs84", "projected"])
@pytest.mark.parametrize("labels", [("", ""), ("my x", "my y")], ids=["no-labels", "own-labels"])
def test_plot_country_borders_keeps_axis_labels(synthetic_borders, to_crs, labels):
    """
    `plot_country_borders` draws borders onto an existing map and leaves its
    axis labels as they were. (geopandas labels empty axes after the CRS, e.g.
    "Easting [metre]", which put labels back on maps cleaned by `clean_axes`.)
    """
    from matplotlib.figure import Figure
    from worldpoppy.plot_utils import plot_country_borders

    ax = Figure().add_subplot()
    ax.set_xlabel(labels[0])
    ax.set_ylabel(labels[1])

    plot_country_borders(["LUX", "BEL"], ax=ax, to_crs=to_crs)

    assert (ax.get_xlabel(), ax.get_ylabel()) == labels
    assert len(ax.collections) > 0, "no borders drawn"


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
