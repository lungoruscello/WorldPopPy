"""
Unit tests for the metadata of WorldPopPy results (`docs/metadata.md`).

All tests use synthetic rasters for two fake countries side by side:
AAA covers lon 0-10 and BBB covers lon 10-20 (both lat 0-10). The
downloader is mocked and returns these files together with fake manifest
rows (see `tests.helpers.fake_manifest_rows`).
"""
import copy
import logging
import re
from datetime import datetime, timedelta, timezone

import geopandas as gpd
import numpy as np
import pytest
import rasterio
import rioxarray
import xarray as xr
from shapely.geometry import box

from tests.helpers import fake_manifest_rows

PRODUCT = "pop_test"

# Origins (top-left corners) of the fake country rasters (10 x 10 pixels, 1 degree)
COUNTRY_ORIGINS = {"AAA": (0, 10), "BBB": (10, 10)}

# AoIs that span both fake countries
AOIS = {
    "countries": ["AAA", "BBB"],
    "bbox": (5.0, 2.0, 15.0, 8.0),
    "geometry": box(5.0, 2.0, 15.0, 8.0),
}
EXPECTED_AOI_ATTR = {
    "countries": "countries: AAA BBB",
    "bbox": "bbox: 5.0 2.0 15.0 8.0",
    "geometry": "geometry: 1 feature, bounds 5.0 2.0 15.0 8.0",
}

# Attributes that every `wp_raster` result carries (docs/metadata.md, "Attributes of `wp_raster` results")
ALWAYS_PRESENT = [
    "title", "creator_name", "creator_url", "creator_institution", "institution",
    "source", "references", "comment", "history", "wpy_version", "wpy_product_name",
    "wpy_product_type", "wpy_manifest_built_at", "wpy_source_files", "wpy_countries",
    "wpy_aoi", "wpy_read_masked", "wpy_read_mask_and_scale",
]

# A line of `history`: UTC timestamp, worldpoppy version, function call and steps
HISTORY_LINE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ worldpoppy \S+: \w+\(.*\)")


# --- Fixtures ---

@pytest.fixture
def two_country_world(monkeypatch):
    """
    Fixture that replaces the real country borders with the two fake
    countries (for bbox and geometry AoIs).
    """
    import worldpoppy.raster

    world = gpd.GeoDataFrame(
        {"iso3": ["AAA", "BBB"]},
        geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)],
        crs="EPSG:4326",
    )
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)


@pytest.fixture
def fake_wp(mock_raster_factory, mock_downloader, two_country_world):
    """
    Fixture that returns a function to set up a fake WorldPop request: it
    writes one synthetic raster per country (and year), and makes the mocked
    downloader return them with matching fake manifest rows.
    """
    def _setup(years=None, multi_year=False, static_year=None, citations=False, nodata=-9999):
        datasets, paths = [], []
        for year in years or [static_year]:
            for iso3, origin in COUNTRY_ORIGINS.items():
                fname = f"{PRODUCT}_{iso3}_{year}.tif" if year else f"{PRODUCT}_{iso3}.tif"
                data = np.ones((10, 10), dtype="float32")
                paths.append(mock_raster_factory(fname, data, origin=origin, nodata=nodata))
                datasets.append((iso3, year))

        mdf = fake_manifest_rows(datasets, PRODUCT, multi_year=multi_year, citations=citations)
        mock_downloader.download.return_value = (paths, mdf)
        return mdf

    return _setup


# --- Helpers ---

def _history_lines(da):
    return da.attrs["history"].split("\n")


def _without_timestamps(attrs):
    """Return the attrs with the timestamps in `history` removed."""
    attrs = dict(attrs)
    attrs["history"] = re.sub(r"^\S+Z ", "", attrs["history"], flags=re.MULTILINE)
    return attrs


# --- wp_raster: attributes ---

@pytest.mark.parametrize("aoi_kind", list(AOIS))
@pytest.mark.parametrize("years", [None, [2015, 2024]], ids=["static", "multi-year"])
@pytest.mark.parametrize("chunks", [None, "auto"], ids=["eager", "lazy"])
def test_wp_raster_attributes(fake_wp, aoi_kind, years, chunks):
    """
    Every `wp_raster` result carries all attributes of the contract, with
    the documented format, for every path: static and multi-year products;
    country codes, bbox and geometry AoIs; eager and lazy loading.
    """
    from worldpoppy import __version__, wp_raster

    mdf = fake_wp(years=years, multi_year=years is not None)
    da = wp_raster(PRODUCT, aoi=AOIS[aoi_kind], years=years, chunks=chunks)
    attrs = da.attrs

    assert da.name == PRODUCT
    assert set(ALWAYS_PRESENT) <= set(attrs)
    assert attrs["title"] == (
        f"{PRODUCT} (curated WorldPopPy product name): Estimated number of people per grid-cell."
    )
    assert attrs["creator_name"] == "WorldPop"
    assert attrs["creator_url"] == "https://www.worldpop.org"
    assert attrs["creator_institution"] == attrs["institution"] == "WorldPop, University of Southampton, UK"
    assert attrs["source"] == (
        "WorldPop gridded datasets (see 'references'), processed with WorldPopPy (see 'history')"
    )
    assert "independent client not affiliated with WorldPop" in attrs["comment"]
    assert "inferred by WorldPopPy" in attrs["comment"]
    assert attrs["wpy_version"] == __version__
    assert attrs["wpy_product_name"] == PRODUCT
    assert attrs["wpy_product_type"] == ("multi-year" if years else "static")
    assert attrs["wpy_manifest_built_at"] == "2025-12-08T19:24:34"
    assert attrs["wpy_countries"] == "AAA BBB"
    assert attrs["wpy_aoi"] == EXPECTED_AOI_ATTR[aoi_kind]
    assert attrs["wpy_source_files"].split("\n") == list(mdf.remote_name)
    assert attrs["wpy_read_masked"] == attrs["wpy_read_mask_and_scale"] == "true"
    for row in mdf.itertuples():
        assert f"{row.api_entry_title}: {row.summary_url}" in attrs["references"].split("\n")

    # One `history` line for the whole `wp_raster` call
    (line,) = _history_lines(da)
    assert HISTORY_LINE.match(line)
    assert f"wp_raster('{PRODUCT}'" in line
    assert ("years=[2015, 2024]" in line) == (years is not None)
    assert f"merged {len(mdf)} files" in line
    assert ("clipped to AoI" in line) == (aoi_kind != "countries")


def test_wp_raster_uses_custom_name(fake_wp):
    """A `name` passed by the user replaces the product name."""
    from worldpoppy import wp_raster

    fake_wp()
    da = wp_raster(PRODUCT, aoi=AOIS["countries"], name="people")

    assert da.name == "people"
    assert da.attrs["wpy_product_name"] == PRODUCT


@pytest.mark.parametrize("years", [None, [2015, 2024]], ids=["static", "multi-year"])
def test_wp_raster_attribute_values_are_saveable(fake_wp, years):
    """All attribute values are strings or numbers."""
    from worldpoppy import wp_raster

    fake_wp(years=years, multi_year=years is not None)
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"], years=years)

    for key, value in da.attrs.items():
        assert isinstance(value, (str, int, float)), f"{key}: {value!r}"


def test_eager_and_lazy_results_have_the_same_attributes(fake_wp):
    """Eager and lazy results carry the same attributes (apart from timestamps)."""
    from worldpoppy import wp_raster

    fake_wp(years=[2015, 2024], multi_year=True)
    eager = wp_raster(PRODUCT, aoi=AOIS["geometry"], years=[2015, 2024])
    lazy = wp_raster(PRODUCT, aoi=AOIS["geometry"], years=[2015, 2024], chunks="auto")

    assert _without_timestamps(lazy.attrs) == _without_timestamps(eager.attrs)


def test_citations_and_licence_come_from_the_manifest(fake_wp):
    """
    Where the manifest has them, WorldPop's recommended citations (deduplicated)
    come first in `references`, and the licence is recorded in `license`.
    """
    from worldpoppy import wp_raster

    mdf = fake_wp(citations=True, static_year=2024)
    da = wp_raster(PRODUCT, aoi=AOIS["countries"])

    references = da.attrs["references"].split("\n")
    assert references[0] == mdf.citation.iloc[0]
    assert references.count(mdf.citation.iloc[0]) == 1
    assert da.attrs["license"] == "https://hub.example/licence.txt"


def test_no_licence_without_manifest_information(fake_wp):
    """Without licence information in the manifest, `license` is left out."""
    from worldpoppy import wp_raster

    fake_wp()
    da = wp_raster(PRODUCT, aoi=AOIS["countries"])

    assert "license" not in da.attrs


@pytest.mark.parametrize("static_year, expected", [(2024, "2024"), (None, None)])
def test_years_of_static_products(fake_wp, static_year, expected):
    """
    Static products record their year in `wpy_years` where the datasets have
    one (it is the only record of the year, since there is no `year`
    dimension). Static products without a year have no `wpy_years`.
    """
    from worldpoppy import wp_raster

    fake_wp(static_year=static_year)
    da = wp_raster(PRODUCT, aoi=AOIS["countries"])

    assert "year" not in da.dims
    assert da.attrs.get("wpy_years") == expected


def test_multi_year_results_record_their_years(fake_wp):
    """
    Multi-year results record their years in the `year` coordinate and in
    `wpy_years`. They set no `long_name`: band names held in an attribute
    would no longer match the bands once a year is selected.
    """
    from worldpoppy import wp_raster

    fake_wp(years=[2015, 2024], multi_year=True)
    da = wp_raster(PRODUCT, aoi=AOIS["countries"], years=[2015, 2024])

    assert list(da.year.values) == [2015, 2024]
    assert da.attrs["wpy_years"] == "2015 2024"
    assert "long_name" not in da.attrs


def test_single_year_of_multi_year_product(fake_wp):
    """
    A single year of a multi-year product has no `year` dimension, but still
    records its year.
    """
    from worldpoppy import wp_raster

    fake_wp(years=[2015], multi_year=True)
    da = wp_raster(PRODUCT, aoi=AOIS["countries"], years=[2015])

    assert "year" not in da.dims
    assert da.attrs["wpy_product_type"] == "multi-year"
    assert da.attrs["wpy_years"] == "2015"


@pytest.mark.parametrize(
    "read_kwargs, expected",
    [
        ({}, "true"),
        ({"masked": True, "mask_and_scale": True}, "true"),
        ({"masked": False}, "false"),  # `mask_and_scale` follows `masked`
        ({"masked": False, "mask_and_scale": False}, "false"),
    ],
)
def test_read_settings_are_recorded(fake_wp, read_kwargs, expected):
    """
    Both effective read settings are always recorded, as 'true' or 'false'
    (bug 1.17), including the value `mask_and_scale` takes from `masked`.
    """
    from worldpoppy import wp_raster

    fake_wp()
    da = wp_raster(PRODUCT, aoi=AOIS["countries"], **read_kwargs)

    assert da.attrs["wpy_read_masked"] == da.attrs["wpy_read_mask_and_scale"] == expected


# --- wp_raster: nodata and pre-clipping ---

@pytest.mark.parametrize("years", [None, [2015, 2024]], ids=["static", "multi-year"])
@pytest.mark.parametrize("chunks", [None, "auto"], ids=["eager", "lazy"])
def test_masked_results_have_nan_nodata(fake_wp, tmp_path, years, chunks):
    """
    Masked results have NaN as their nodata value, also after saving to a
    GeoTIFF and reading it back (bug 1.14: static results had none).
    """
    from worldpoppy import wp_raster

    fake_wp(years=years, multi_year=years is not None)
    da = wp_raster(PRODUCT, aoi=AOIS["geometry"], years=years, chunks=chunks)

    assert np.isnan(da.rio.nodata)
    da.rio.to_raster(tmp_path / "saved.tif")
    assert np.isnan(rioxarray.open_rasterio(tmp_path / "saved.tif").rio.nodata)


@pytest.mark.parametrize("years", [None, [2015, 2024]], ids=["static", "multi-year"])
def test_unmasked_results_keep_the_source_nodata(fake_wp, tmp_path, years):
    """
    Unmasked results keep the source files' nodata value (here -9999), also
    after saving to a GeoTIFF and reading it back; they never claim NaN.
    (Unmasked reads are always eager: lazy reads require masked data.)
    """
    from worldpoppy import wp_raster

    fake_wp(years=years, multi_year=years is not None)
    da = wp_raster(PRODUCT, aoi=AOIS["countries"], years=years, masked=False)

    assert da.rio.nodata == -9999
    da.rio.to_raster(tmp_path / "saved.tif")
    assert rioxarray.open_rasterio(tmp_path / "saved.tif").rio.nodata == -9999


@pytest.mark.parametrize("chunks", [None, "auto"], ids=["eager", "lazy"])
def test_masked_results_from_integer_files_save_as_float(mock_raster_factory, tmp_path, chunks):
    """
    Masked results from integer source files (e.g. uint8 with fill value
    255) are saved as floating point with NaN as nodata, eager and lazy alike:
    they do not inherit the source files' data type or fill value.
    """
    from worldpoppy import merge_rasters

    path = mock_raster_factory("int.tif", np.ones((10, 10), "uint8"), origin=(0, 10), nodata=255)
    da = merge_rasters([path], chunks=chunks)
    da.rio.to_raster(tmp_path / "saved.tif")
    saved = rioxarray.open_rasterio(tmp_path / "saved.tif")

    assert np.issubdtype(saved.dtype, np.floating)
    assert np.isnan(saved.rio.nodata)


def test_pre_clip_skips_countries_outside_the_box(fake_wp, caplog):
    """
    `pre_clip_bbox` skips country rasters that lie entirely outside the box,
    instead of falling back to the full raster (bug 1.16). Skipped files do
    not count as sources, and a log message reports them.
    """
    from worldpoppy import wp_raster

    mdf = fake_wp()
    with caplog.at_level(logging.INFO):
        da = wp_raster(PRODUCT, aoi=AOIS["countries"], pre_clip_bbox=(2.0, 2.0, 8.0, 8.0))

    assert float(da.x.max()) < 8.0  # nothing from BBB (lon 10-20)
    assert da.attrs["wpy_countries"] == "AAA"
    assert da.attrs["wpy_source_files"] == mdf.remote_name.iloc[0]
    assert da.attrs["wpy_pre_clip_bbox"] == "2.0 2.0 8.0 8.0"
    assert "pre-clipped to bbox" in da.attrs["history"]
    assert "BBB" in caplog.text


def test_no_pre_clip_attribute_without_pre_clip_bbox(fake_wp):
    """`wpy_pre_clip_bbox` is only present if the user passed a pre-clip box."""
    from worldpoppy import wp_raster

    fake_wp()
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"])

    assert "wpy_pre_clip_bbox" not in da.attrs


# --- merge_rasters ---

def test_merge_rasters_attributes(mock_raster_factory):
    """
    `merge_rasters` works on arbitrary local files: it records the processing
    (one `history` line, read settings, local file names), but no WorldPop
    provenance. Its default name is 'wpy_data'.
    """
    from worldpoppy import __version__, merge_rasters

    paths = [
        mock_raster_factory(f"{iso3}.tif", np.ones((10, 10), "float32"), origin=origin)
        for iso3, origin in COUNTRY_ORIGINS.items()
    ]
    da = merge_rasters(paths)

    assert da.name == "wpy_data"
    assert da.attrs["wpy_version"] == __version__
    assert da.attrs["wpy_source_files"] == "AAA.tif\nBBB.tif"
    assert da.attrs["wpy_read_masked"] == da.attrs["wpy_read_mask_and_scale"] == "true"
    (line,) = _history_lines(da)
    assert HISTORY_LINE.match(line) and "merge_rasters(" in line
    assert not {"title", "references", "creator_name", "license"} & set(da.attrs)
    assert np.isnan(da.rio.nodata)


def test_merge_rasters_records_pre_clipping(mock_raster_factory):
    """
    With `pre_clip_bbox`, `merge_rasters` records the box and the step, and
    lists (and counts) only the files that overlap the box.
    """
    from worldpoppy import merge_rasters

    paths = [
        mock_raster_factory(f"{iso3}.tif", np.ones((10, 10), "float32"), origin=origin)
        for iso3, origin in COUNTRY_ORIGINS.items()
    ]
    da = merge_rasters(paths, pre_clip_bbox=(2.0, 2.0, 8.0, 8.0))

    assert da.attrs["wpy_source_files"] == "AAA.tif"
    assert da.attrs["wpy_pre_clip_bbox"] == "2.0 2.0 8.0 8.0"
    assert "merged 1 file; pre-clipped to bbox" in da.attrs["history"]


def test_merge_rasters_accepts_nan_nodata(mock_raster_factory):
    """
    Raw (unmasked) files whose nodata value is NaN are compatible with each
    other (bug 1.18: NaN != NaN made them look incompatible).
    """
    from worldpoppy import merge_rasters

    paths = [
        mock_raster_factory(f"{iso3}.tif", np.ones((10, 10), "float32"), origin=origin, nodata=np.nan)
        for iso3, origin in COUNTRY_ORIGINS.items()
    ]
    da = merge_rasters(paths, masked=False, mask_and_scale=False)

    assert da.sizes["x"] == 20


# --- wp_warp ---

def test_wp_warp_keeps_attributes_and_appends_history(fake_wp):
    """
    `wp_warp` keeps all attributes of its input and appends exactly one
    `history` line.
    """
    from worldpoppy import wp_raster, wp_warp

    fake_wp()
    da = wp_raster(PRODUCT, aoi=AOIS["countries"])
    warped = wp_warp(da, to_crs="EPSG:3857", res=100_000, resampling="sum")

    for key, value in da.attrs.items():
        if key != "history":
            assert warped.attrs[key] == value
    assert np.isnan(warped.rio.nodata)
    first, second = _history_lines(warped)
    assert first == da.attrs["history"]
    assert HISTORY_LINE.match(second)
    assert "wp_warp(to_crs='EPSG:3857', res=100000, resampling='sum')" in second


@pytest.mark.parametrize("chunks", [None, {"x": 5, "y": 5}], ids=["eager", "lazy"])
def test_wp_warp_does_not_modify_its_input(chunks):
    """
    `wp_warp` leaves its input unchanged: attributes, encoding, data, and
    laziness (bug 1.15: it wrote a nodata value into the input).
    """
    from worldpoppy import wp_warp

    da = xr.DataArray(
        np.arange(100, dtype="float32").reshape(10, 10),
        dims=("y", "x"),
        coords={"y": np.arange(9.5, 0, -1), "x": np.arange(0.5, 10)},
        attrs={"history": "earlier step"},
    ).rio.write_crs("EPSG:4326")
    if chunks:
        da = da.chunk(chunks)
    attrs, encoding = copy.deepcopy(da.attrs), copy.deepcopy(da.encoding)

    wp_warp(da, to_crs="EPSG:3857", res=100_000)

    assert da.attrs == attrs
    assert da.encoding == encoding
    assert (da.chunks is not None) == bool(chunks)
    np.testing.assert_array_equal(da.values, np.arange(100, dtype="float32").reshape(10, 10))


# --- Formatting ---

@pytest.mark.parametrize("built_at, expected", [
    (None, "unknown"),
    (datetime(2025, 12, 8, 19, 24, 34), "2025-12-08T19:24:34"),  # as recorded (no time zone)
    (datetime(2025, 12, 8, 20, 24, 34, tzinfo=timezone(timedelta(hours=1))), "2025-12-08T19:24:34Z"),
])
def test_manifest_build_time_format(built_at, expected):
    """
    The manifest's build time is shown as recorded; times with a time zone
    are converted to UTC and marked 'Z'.
    """
    from worldpoppy.raster import _format_build_time

    assert _format_build_time(built_at) == expected


# --- Saving ---

def test_geotiff_keeps_attributes(fake_wp, tmp_path):
    """A saved GeoTIFF keeps all attributes (read back as strings)."""
    from worldpoppy import wp_raster

    fake_wp(years=[2015, 2024], multi_year=True)
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"], years=[2015, 2024])
    da.rio.to_raster(tmp_path / "saved.tif")
    saved = rioxarray.open_rasterio(tmp_path / "saved.tif")

    for key, value in da.attrs.items():
        assert str(saved.attrs[key]) == str(value), key


def test_selected_years_and_derived_results_can_be_saved(fake_wp, tmp_path):
    """
    A single year selected from a multi-year result, and a difference of two
    years, can be saved as GeoTIFF (bug 1.39: a `long_name` naming every year
    made rioxarray refuse to save fewer bands).
    """
    from worldpoppy import wp_raster

    fake_wp(years=[2015, 2024], multi_year=True)
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"], years=[2015, 2024])

    for name, result in [
        ("selected", da.sel(year=2024)),
        ("difference", da.sel(year=2024) - da.sel(year=2015)),
    ]:
        result.rio.to_raster(tmp_path / f"{name}.tif")
        saved = rioxarray.open_rasterio(tmp_path / f"{name}.tif")
        np.testing.assert_array_equal(saved.squeeze("band").values, result.values)


def test_geotiff_nodata_recipe_for_derived_results(fake_wp, tmp_path):
    """
    Arithmetic loses the nodata value; the documented recipe (docs/metadata.md,
    "Saving") restores it, so that the saved GeoTIFF declares NaN as nodata.
    """
    from worldpoppy import wp_raster

    fake_wp(years=[2015, 2024], multi_year=True)
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"], years=[2015, 2024])

    result = da.sel(year=2024) - da.sel(year=2015)
    assert result.rio.nodata is None  # as documented in "Your own processing"

    result = result.rio.write_nodata(np.nan)
    result.rio.to_raster(tmp_path / "derived.tif")
    with rasterio.open(tmp_path / "derived.tif") as src:
        assert np.isnan(src.nodata)


def test_geotiff_year_labels_recipe(fake_wp, tmp_path):
    """
    The documented recipe (docs/metadata.md, "Saving") labels the bands of
    a saved multi-year GeoTIFF with their years.
    """
    from worldpoppy import wp_raster

    fake_wp(years=[2015, 2024], multi_year=True)
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"], years=[2015, 2024])

    years = tuple(str(year) for year in da.year.values)
    da.assign_attrs(long_name=years).rio.to_raster(tmp_path / "saved.tif")

    with rasterio.open(tmp_path / "saved.tif") as src:
        assert src.descriptions == ("2015", "2024")


@pytest.mark.parametrize("years", [None, [2015, 2024]], ids=["static", "multi-year"])
def test_netcdf_recipe(fake_wp, tmp_path, years):
    """
    The documented netCDF recipe (docs/metadata.md, "Saving") stores the
    attributes at file level, and keeps the years of multi-year results.
    """
    from worldpoppy import wp_raster

    fake_wp(years=years, multi_year=years is not None)
    da = wp_raster(PRODUCT, aoi=AOIS["bbox"], years=years)

    ds = da.to_dataset()
    ds.attrs.update(da.attrs)
    ds.to_netcdf(tmp_path / "saved.nc", engine="netcdf4")

    with xr.open_dataset(tmp_path / "saved.nc", engine="netcdf4") as saved:
        for key in ALWAYS_PRESENT:
            assert saved.attrs[key] == da.attrs[key], key
        if years:
            assert list(saved.year.values) == [2015, 2024]
