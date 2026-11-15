import re

import dask.array as da
import numpy as np
import pytest
import rioxarray
import xarray as xr

from tests.helpers import fake_manifest_rows


def test_merge_preserves_count(mock_raster_factory):
    """
    Verify that merging two spatially adjacent rasters preserves the
    total sum of their values.
    """
    from worldpoppy import merge_rasters

    # --- Setup ---
    # Create two 10x10 grids
    data1 = np.ones((10, 10))  # Sum = 100
    data2 = np.full((10, 10), 2.0)  # Sum = 200

    # Create mock files
    # Raster 1: Starts at (0, 20)
    path1 = mock_raster_factory("left.tif", data1, origin=(0, 20), res=1.0)

    # Raster 2: Starts at (10, 20) -> Perfectly adjacent to the right
    path2 = mock_raster_factory("right.tif", data2, origin=(10, 20), res=1.0)

    # --- Act ---
    merged = merge_rasters([path1, path2])

    # Remove the 'band' dimension to get a clean 2D array (y, x)
    merged = merged.squeeze()

    # --- Assert ---
    # 1. Check shape: Should be 10 pixels high, 20 pixels wide
    assert merged.shape == (10, 20)

    # 2. Check mass conservation: 100 + 200 = 300
    total_mass = merged.sum().item()
    assert np.isclose(total_mass, 300.0)


@pytest.mark.filterwarnings("ignore:Merging multiple country rasters lazily")
@pytest.mark.parametrize("res", [1.0, 1 / 1200], ids=["1-degree", "3-arcsecond"])
@pytest.mark.parametrize("chunks", [None, "auto"], ids=["eager", "lazy"])
def test_merge_places_every_cell_exactly(mock_raster_factory, chunks, res):
    """
    Merging puts every source cell at its own location, with NaN where no
    raster has data: the merged array equals one assembled by hand, cell by
    cell, and its coordinates are the expected pixel centres. At WorldPop's
    resolution of 3 arcseconds, coordinates may differ by at most 1e-5
    degrees (about 1 m), well below a pixel (about 90 m).
    """
    from worldpoppy import merge_rasters

    # In pixels: A covers x 0-4, y 0-3; B covers x 4-8, y -1-2 (one row lower),
    # both offset from lon 40, lat 41. Every cell holds a distinct value, so
    # that any misplacement shows.
    data_a = np.arange(1, 13, dtype="float32").reshape(3, 4)
    data_b = np.arange(101, 113, dtype="float32").reshape(3, 4)
    path_a = mock_raster_factory("A.tif", data_a, origin=(40, 41 + 3 * res), res=res)
    path_b = mock_raster_factory("B.tif", data_b, origin=(40 + 4 * res, 41 + 2 * res), res=res)

    expected = np.full((4, 8), np.nan, dtype="float32")
    expected[0:3, 0:4] = data_a
    expected[1:4, 4:8] = data_b

    merged = merge_rasters([path_a, path_b], chunks=chunks).squeeze("band").compute()

    np.testing.assert_array_equal(merged.values, expected)
    np.testing.assert_allclose(merged.x.values, 40 + (np.arange(8) + 0.5) * res, rtol=0, atol=1e-5)
    np.testing.assert_allclose(
        merged.y.values, 41 + np.array([2.5, 1.5, 0.5, -0.5]) * res, rtol=0, atol=1e-5
    )


def test_warp_preserves_count(mock_raster_factory):
    """
    Verify that reprojecting (warping) a raster using 'sum' resampling
    preserves the total mass.
    """
    from worldpoppy import wp_warp

    # --- Setup ---
    # Create a 100x100 grid with total mass 10,000
    data = np.ones((100, 100))

    # Create mock file in WGS84
    # Note: The mocked raster's origin is the intersection of the Equator
    # and the Prime Meridian. At that point, a resolution of 0.01 degrees
    # corresponds to roughly ~1.1km.
    path = mock_raster_factory("source.tif", data, origin=(0, 0), res=0.01)

    # Load it back as a DataArray (simulating `wp_raster` output)
    da_source = rioxarray.open_rasterio(path)

    # --- Act ---
    # Warp to Web Mercator (Metric)
    pseudo_mercator = "EPSG:3857"

    # We choose a target resolution of 1200m because it is roughly equivalent
    # to the source resolution (~1111m) at the equator. This avoids massive
    # oversampling (which slows down tests) or extreme downsampling (which
    # trivialises the test by reducing the number of partial pixel overlaps).
    warped = wp_warp(da_source, to_crs=pseudo_mercator, res=1200, resampling="sum")

    # --- Assert ---
    expected_mass = 10000.0
    actual_mass = warped.sum().item()

    discrepancy_ratio = abs(actual_mass - expected_mass) / expected_mass

    # Allow < 0.5% error due to reprojection interpolation
    assert discrepancy_ratio < 0.005


@pytest.mark.parametrize("kwargs", [{}, {"resampling": "sum"}], ids=["no-arguments", "resampling-only"])
def test_wp_warp_without_crs_or_resolution_raises(mock_raster_factory, kwargs):
    """
    `wp_warp` without `to_crs` and `res` has nothing to do and raises a
    `ValueError` (bug 1.32: it returned its input object, without a history
    line, and silently ignored `resampling`).
    """
    from worldpoppy import merge_rasters, wp_warp

    da = merge_rasters([mock_raster_factory("A.tif", np.ones((5, 5)))])

    with pytest.raises(ValueError, match=re.escape("`wp_warp` has nothing to do")):
        wp_warp(da, **kwargs)


def test_pre_clipping_optimisation(mock_raster_factory, mock_downloader):
    """
    Verify that providing `pre_clip_bbox` restricts the resulting array shape
    *immediately* (lazy shape inspection).

    Proof of Pre-Clipping:
    We use `aoi=['MCK']` (ISO codes), which results in `clipping_gdf=None`.
    Since `merge_rasters` only performs *post-clipping* if a GDF is present,
    any reduction in output shape MUST be due to the *pre-clipping* logic
    inside the loading loop.
    """
    from worldpoppy import wp_raster

    # --- Setup ---
    # Create a "Huge" 100x100 raster
    # Origin (0, 100), Resolution 1.0 -> Covers X=0..100, Y=0..100
    huge_data = np.zeros((100, 100))
    path_huge = mock_raster_factory("huge.tif", huge_data, origin=(0, 100))

    mock_downloader.download.return_value = ([path_huge], fake_manifest_rows([("MCK", None)]))

    # Define a tiny clip box: X=10..20, Y=10..20 (10x10 area)
    tiny_box = (10, 10, 20, 20)

    # --- Act 1: WITH Pre-Clipping ---
    result_optimised = wp_raster(
        product_name="pop_g1", aoi=["MCK"], pre_clip_bbox=tiny_box, chunks="auto"
    )

    # --- Act 2: WITHOUT Pre-Clipping (Control) ---
    result_full = wp_raster(
        product_name="pop_g1",
        aoi=["MCK"],
        pre_clip_bbox=None,  # <--- The only difference
        chunks="auto",
    )

    # --- Assert ---
    # The Optimised result should be tiny (~10x10)
    h_opt, w_opt = result_optimised.shape
    assert h_opt == 10
    assert w_opt == 10

    # The Control result should be huge (100x100)
    # This proves that the 'pre_clip_bbox' argument is what caused the reduction.
    h_full, w_full = result_full.shape
    assert h_full == 100
    assert w_full == 100


@pytest.mark.parametrize("aoi_kind", ["bbox", "geometry"])
def test_automatic_pre_clipping_does_not_change_results(
    mock_raster_factory, mock_downloader, monkeypatch, aoi_kind
):
    """
    The automatic pre-clipping for bbox and geometry AoIs only saves memory:
    results are identical to those without any pre-clipping. (The AoI spans
    two rasters and does not align with their grid; as for real data, the
    pixels are much smaller than the pre-clip buffer of 0.1 degrees.)
    """
    import geopandas as gpd
    import worldpoppy.raster
    from shapely.geometry import Polygon, box
    from worldpoppy import wp_raster

    world = gpd.GeoDataFrame({"iso3": ["MCK"]}, geometry=[box(0, 0, 4, 2)], crs="EPSG:4326")
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)

    # Two adjacent rasters (x 0-2 and x 2-4, y 0-2) with 0.01-degree pixels
    data = np.arange(200 * 200, dtype="float32").reshape(200, 200)
    path_a = mock_raster_factory("A.tif", data, origin=(0, 2), res=0.01)
    path_b = mock_raster_factory("B.tif", data + 1e5, origin=(2, 2), res=0.01)
    mock_downloader.download.return_value = (
        [path_a, path_b], fake_manifest_rows([("MCK", None), ("MCK", None)])
    )

    aoi = {
        "bbox": (1.234, 0.567, 2.891, 1.789),
        "geometry": Polygon([(1.234, 0.567), (2.891, 0.712), (2.013, 1.789)]),
    }[aoi_kind]

    pre_clipped = wp_raster(product_name="pop_g1", aoi=aoi)
    not_pre_clipped = wp_raster(product_name="pop_g1", aoi=aoi, suppress_pre_clip=True)

    # Identical values; coordinates equal up to floating-point rounding
    np.testing.assert_array_equal(pre_clipped.values, not_pre_clipped.values)
    for dim in ["x", "y"]:
        np.testing.assert_allclose(pre_clipped[dim], not_pre_clipped[dim], rtol=0, atol=1e-9)


@pytest.mark.filterwarnings("ignore:Merging multiple country rasters lazily")
@pytest.mark.parametrize("chunks", [None, "auto"], ids=["eager", "lazy"])
def test_pre_clip_keeps_a_narrow_overlap(mock_raster_factory, chunks):
    """
    A raster that overlaps the pre-clip box by less than two pixels still
    contributes its overlapping cells (bug 1.26: it was skipped as if it did
    not overlap at all).
    """
    from worldpoppy import merge_rasters

    # A covers x 0-10 (value = column index), B covers x 10-20 (value 100).
    # The box starts at x 9.2, so it overlaps only A's last column (x 9-10).
    data_a = np.tile(np.arange(10, dtype="float32"), (10, 1))
    path_a = mock_raster_factory("A.tif", data_a, origin=(0, 10))
    path_b = mock_raster_factory("B.tif", np.full((10, 10), 100.0), origin=(10, 10))

    result = merge_rasters([path_a, path_b], pre_clip_bbox=(9.2, 2, 12, 8), chunks=chunks)
    result = result.squeeze("band").compute()

    np.testing.assert_array_equal(result.sel(x=9.5).values, 9.0)
    assert result.attrs["wpy_source_files"].split("\n") == ["A.tif", "B.tif"]
    # Pre-clipped to the box (columns x 9-12), not loaded in full
    np.testing.assert_array_equal(result.x.values, [9.5, 10.5, 11.5])


@pytest.mark.parametrize(
    "bad_box", [(12, 2, 9, 8), (9, 2, float("nan"), 8)], ids=["inverted", "nan"]
)
@pytest.mark.parametrize("func", ["wp_raster", "merge_rasters"])
def test_invalid_pre_clip_boxes_are_rejected(mock_downloader, func, bad_box):
    """
    An invalid `pre_clip_bbox` is rejected before anything is downloaded or
    read (bug 1.25: such boxes made the pre-clip fail, so the full rasters
    were used while the history claimed a pre-clip).
    """
    import worldpoppy
    from worldpoppy.func_utils import BboxInvalidError

    with pytest.raises(BboxInvalidError):
        if func == "wp_raster":
            worldpoppy.wp_raster("pop_g1", aoi=["MCK"], pre_clip_bbox=bad_box)
        else:
            worldpoppy.merge_rasters(["does_not_exist.tif"], pre_clip_bbox=bad_box)
    mock_downloader.download.assert_not_called()


def test_failing_pre_clip_box_raises_instead_of_loading_everything(mock_raster_factory, monkeypatch):
    """
    If a user-provided `pre_clip_bbox` cannot be applied to a raster, the call
    fails, instead of silently using the full raster while the history claims
    a pre-clip (bug 1.25).
    """
    from rioxarray.raster_array import RasterArray
    from worldpoppy import merge_rasters
    from worldpoppy.raster import RasterReadError

    def failing_clip_box(self, *args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(RasterArray, "clip_box", failing_clip_box)
    path = mock_raster_factory("A.tif", np.ones((10, 10)), origin=(0, 10))

    with pytest.raises(RasterReadError, match=r"^Failed to apply `pre_clip_bbox`.*A\.tif.*simulated failure"):
        merge_rasters([path], pre_clip_bbox=(2, 2, 8, 8))


def test_failing_automatic_pre_clip_falls_back_to_the_full_raster(
    mock_raster_factory, mock_downloader, monkeypatch, caplog
):
    """
    The automatic pre-clip (bbox and geometry AoIs) only saves memory: if it
    fails, the full raster is used with a warning, and the final clip to the
    AoI still gives the correct result.
    """
    import geopandas as gpd
    import worldpoppy.raster
    from rioxarray.raster_array import RasterArray
    from shapely.geometry import box
    from worldpoppy import wp_raster

    world = gpd.GeoDataFrame({"iso3": ["MCK"]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:4326")
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)
    data = np.arange(100, dtype="float32").reshape(10, 10)
    path = mock_raster_factory("A.tif", data, origin=(0, 10))
    mock_downloader.download.return_value = ([path], fake_manifest_rows([("MCK", None)]))

    expected = wp_raster(product_name="pop_g1", aoi=(2.5, 2.5, 6.5, 6.5))

    def failing_clip_box(self, *args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(RasterArray, "clip_box", failing_clip_box)
    result = wp_raster(product_name="pop_g1", aoi=(2.5, 2.5, 6.5, 6.5))

    xr.testing.assert_equal(result, expected)
    assert "Proceeding with full raster load" in caplog.text


def test_wp_raster_clips_to_shapely_aoi(mock_raster_factory, mock_downloader, monkeypatch):
    """
    A shapely geometry AoI clips the merged raster exactly like the
    equivalent GeoDataFrame does.
    """
    import geopandas as gpd
    import worldpoppy.raster
    from shapely.geometry import Polygon, box
    from worldpoppy import wp_raster

    # A synthetic world with one country that covers the whole mock raster
    world = gpd.GeoDataFrame({"iso3": ["MCK"]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:4326")
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)

    path = mock_raster_factory("mock.tif", np.ones((100, 100)), origin=(0, 100))
    mock_downloader.download.return_value = ([path], fake_manifest_rows([("MCK", None)]))
    triangle = Polygon([(10, 10), (40, 10), (10, 40)])

    from_shapely = wp_raster(product_name="pop_g1", aoi=triangle)
    from_gdf = wp_raster(
        product_name="pop_g1", aoi=gpd.GeoDataFrame(geometry=[triangle], crs="EPSG:4326")
    )

    xr.testing.assert_equal(from_shapely, from_gdf)
    assert max(from_gdf.shape) < 100  # clipped to the triangle's bounding box ...
    assert from_gdf.isnull().any()  # ... and masked outside the triangle


@pytest.mark.parametrize("suppress_pre_clip", [False, True], ids=["pre-clipped", "not-pre-clipped"])
def test_clip_keeps_exactly_the_cells_the_aoi_touches(
    mock_raster_factory, mock_downloader, monkeypatch, suppress_pre_clip
):
    """
    Clipping to a geometry AoI keeps exactly the cells that the AoI touches
    (`all_touched=True`), with their source values, sets all other cells to
    NaN, and crops the result to the rows and columns with kept cells. The
    expected cells are computed independently with shapely. (Without the
    automatic pre-clip, the final clip alone must produce this extent.)
    """
    import geopandas as gpd
    import worldpoppy.raster
    from shapely.geometry import Polygon, box
    from worldpoppy import wp_raster

    world = gpd.GeoDataFrame({"iso3": ["MCK"]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:4326")
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)

    # 10 x 10 cells of 1 degree (x 0-10, y 0-10); the value encodes the cell:
    # row * 10 + column (row 0 at the top)
    data = np.arange(100, dtype="float32").reshape(10, 10)
    path = mock_raster_factory("mock.tif", data, origin=(0, 10))
    mock_downloader.download.return_value = ([path], fake_manifest_rows([("MCK", None)]))

    # Corners off the cell edges, and no edge passes within 0.02 degrees of a
    # cell corner, so that no cell is a borderline case
    triangle = Polygon([(1.3, 1.6), (8.7, 2.45), (4.2, 8.8)])
    expected = {
        (row, col)
        for row in range(10) for col in range(10)
        if triangle.intersection(box(col, 9 - row, col + 1, 10 - row)).area > 0
    }

    result = wp_raster(product_name="pop_g1", aoi=triangle, suppress_pre_clip=suppress_pre_clip)

    rows = (10 - result.y.values - 0.5).round().astype(int)
    cols = (result.x.values - 0.5).round().astype(int)
    kept = {
        (rows[i], cols[j])
        for i, j in zip(*np.where(np.isfinite(result.values)))
    }
    assert kept == expected
    for i, j in zip(*np.where(np.isfinite(result.values))):
        assert result.values[i, j] == rows[i] * 10 + cols[j], f"wrong value in cell {(rows[i], cols[j])}"

    # The result is cropped to exactly the rows and columns with kept cells
    expected_rows = [row for row, _ in expected]
    expected_cols = [col for _, col in expected]
    assert list(rows) == list(range(min(expected_rows), max(expected_rows) + 1))
    assert list(cols) == list(range(min(expected_cols), max(expected_cols) + 1))


@pytest.mark.parametrize(
    "aoi, expected_shape",
    [((2.21, 2.21, 2.24, 6.39), (42, 1)), ((2.21, 2.21, 2.24, 2.24), (1, 1))],
    ids=["one-column", "one-cell"],
)
@pytest.mark.parametrize("years", [None, [2015, 2020]], ids=["static", "multi-year"])
def test_narrow_results_keep_their_spatial_dimensions(
    mock_raster_factory, mock_downloader, monkeypatch, tmp_path, aoi, expected_shape, years
):
    """
    A result only one cell wide (or one cell in total) keeps its `y` and `x`
    dimensions and can be saved as a GeoTIFF (bug 1.30: a bare `squeeze()`
    dropped them). Only `band`, and `year` for a single year, are dropped.
    """
    import geopandas as gpd
    import worldpoppy.raster
    from shapely.geometry import box
    from worldpoppy import wp_raster

    world = gpd.GeoDataFrame({"iso3": ["MCK"]}, geometry=[box(0, 0, 10, 10)], crs="EPSG:4326")
    monkeypatch.setattr(worldpoppy.raster, "load_country_borders", lambda: world)

    # 0.1-degree cells (x 0-10, y 0-10); one file per year
    file_years = years or [None]
    paths = [
        mock_raster_factory(f"MCK_{year}.tif", np.ones((100, 100)), origin=(0, 10), res=0.1)
        for year in file_years
    ]
    mdf = fake_manifest_rows([("MCK", year) for year in file_years], multi_year=years is not None)
    mock_downloader.download.return_value = (paths, mdf)

    result = wp_raster(product_name="pop_test", aoi=aoi, years=years)

    expected_dims = ("y", "x") if years is None else ("year", "y", "x")
    assert result.dims == expected_dims
    assert (result.sizes["y"], result.sizes["x"]) == expected_shape
    result.rio.to_raster(tmp_path / "narrow.tif")


@pytest.mark.parametrize(
    "chunks_arg, expected_backend",
    [
        ("auto", da.Array),  # Case 1: Lazy (Dask)
        (None, np.ndarray),  # Case 2: Eager (Numpy) - Negative Control
    ]
)
def test_wp_raster_execution_modes(
    mock_raster_factory, mock_downloader, chunks_arg, expected_backend
):
    """
    Parametrised test verifying that `wp_raster` correctly toggles
    between Lazy (Dask) and Eager (Numpy) execution modes based on
    the `chunks` argument.

    Checks:
    1. The merge logic produces the correct shape in both modes.
    2. The underlying data structure matches the expected backend.
    """
    from worldpoppy import wp_raster

    # --- Setup ---
    # Create 2 mock rasters that overlap to force a merge
    # Raster A: X=0..10; Y=0..10
    path_a = mock_raster_factory("A.tif", np.ones((10, 10)), origin=(0, 10))
    # Raster B: X=5..15 (reaches beyond A by 5 pxl); Y=2..12 (reaches beyond A by 2 pixels)
    path_b = mock_raster_factory("B.tif", np.ones((10, 10)), origin=(5, 12))

    # Configure the mock downloader
    datasets = [("MCK", None), ("MCK", None)]
    mock_downloader.download.return_value = ([path_a, path_b], fake_manifest_rows(datasets))

    # --- Act ---
    result = wp_raster(
        product_name="pop_g1",
        aoi=["MCK"],
        years=None,
        chunks=chunks_arg  # <--- Toggles Lazy vs Eager
    )

    # --- Assert ---
    # Always returns an xarray DataArray
    assert isinstance(result, xr.DataArray)

    # Check Shape (Merge Correctness)
    assert result.shape == (12, 15)

    # Check Backend Type (Lazy vs Eager Contract)
    assert isinstance(result.data, expected_backend)

    # Check Chunks Property
    if expected_backend == da.Array:
        assert result.chunks is not None
    else:
        # For numpy-backed xarrays, .chunks is usually None or implies eager loading
        assert result.chunks is None


@pytest.mark.filterwarnings("ignore:Merging multiple country rasters lazily")
def test_lazy_and_eager_merges_agree_where_rasters_overlap(mock_raster_factory):
    """
    Lazy and eager merges give identical results. Where the valid data of
    several rasters overlaps, the first raster wins in both (bug 1.22: lazy
    merges used to let the last raster win). Three rasters also exercise the
    later rounds of the lazy pairwise merge, including the leftover raster.
    """
    from worldpoppy import merge_rasters

    # A covers x 0-10 (value 1), B x 5-15 (value 2), C x 3-13 (value 3):
    # all three overlap at x 5-10
    path_a = mock_raster_factory("A.tif", np.full((10, 10), 1.0), origin=(0, 10))
    path_b = mock_raster_factory("B.tif", np.full((10, 10), 2.0), origin=(5, 10))
    path_c = mock_raster_factory("C.tif", np.full((10, 10), 3.0), origin=(3, 10))
    paths = [path_a, path_b, path_c]

    eager = merge_rasters(paths)
    lazy = merge_rasters(paths, chunks="auto").compute()

    xr.testing.assert_equal(lazy, eager)
    assert float(eager.sel(x=7.5, y=5.5).squeeze()) == 1.0


def test_multi_year_bands_hold_their_own_years_values(mock_raster_factory, mock_downloader):
    """
    Each band of a multi-year result holds the data of its own year, also if
    the manifest lists the years in descending order.
    """
    from worldpoppy import wp_raster

    # Every file holds its year as value; manifest rows (and paths) in
    # descending year order
    years = [2020, 2015]
    paths = [
        mock_raster_factory(f"MCK_{year}.tif", np.full((5, 5), float(year)), origin=(0, 5))
        for year in years
    ]
    mdf = fake_manifest_rows([("MCK", year) for year in years], multi_year=True)
    mock_downloader.download.return_value = (paths, mdf)

    result = wp_raster(product_name="pop_test", aoi=["MCK"], years=[2015, 2020])

    assert sorted(result.year.values.tolist()) == [2015, 2020]
    for year in [2015, 2020]:
        np.testing.assert_array_equal(result.sel(year=year).values, float(year))

    # The band descriptions (written to GeoTIFFs) name the year of each band
    assert result.attrs["long_name"] == tuple(str(year) for year in result.year.values)


@pytest.mark.parametrize(
    "mismatch_kwargs, error_substring",
    [
        # Case 1: CRS Mismatch
        ({"crs": "EPSG:3857"}, "'CRS'"),
        # Case 2: Nodata (_FillValue) Mismatch
        # (Reference uses -9999, we pass -1)
        ({"nodata": -1}, "'_FillValue'"),
        # Case 3: Scale Factor Mismatch
        # (Reference uses 1.0, we pass 10.0)
        ({"scale_factor": 10.0}, "'scale_factor'"),
    ],
)
def test_merge_raises_incompatible_error(
    mock_raster_factory, mismatch_kwargs, error_substring
):
    """
    Parametrised test verifying that `merge_rasters` raises IncompatibleRasterError
    when critical metadata (CRS, NoData, Scale Factor) does not match.
    """
    from worldpoppy.config import WGS84_CRS
    from worldpoppy.raster import merge_rasters, IncompatibleRasterError

    # --- Setup ---
    # Define a standard "Reference" configuration
    # We explicit set these to ensure we have a baseline to mismatch against
    base_config = {"crs": WGS84_CRS, "nodata": -9999, "scale_factor": 1.0}

    # Create Reference Raster
    path_ref = mock_raster_factory("ref.tif", np.ones((5, 5)), **base_config)

    # Create Incompatible Raster
    # Start with base config, then overwrite with the mismatching argument
    bad_config = base_config.copy()
    bad_config.update(mismatch_kwargs)

    path_bad = mock_raster_factory("bad.tif", np.ones((5, 5)), **bad_config)

    # --- Act & Assert ---
    with pytest.raises(IncompatibleRasterError) as exc:
        merge_rasters([path_ref, path_bad], masked=False, mask_and_scale=False)

    # Ensure the error message specifically identifies WHICH attribute failed
    assert f"Input rasters do not share the same {error_substring}" in str(exc.value)


@pytest.mark.parametrize(
    "cache_downloads, skip_download_if_exists, expected_skip, expect_warning",
    [
        (True, None, True, False),    # Default (omitted) with cache: skip cached files
        (False, None, False, False),  # Default (omitted) without cache: no spurious warning
        (False, True, False, True),   # Explicit contradiction: warn and override
        (False, False, False, False),
        (True, True, True, False),    # Explicit skip with cache
        (True, False, False, False),  # Explicit re-download with cache
    ]
)
def test_wp_raster_skip_download_default(
    mock_raster_factory, mock_downloader, caplog,
    cache_downloads, skip_download_if_exists, expected_skip, expect_warning
):
    """
    `skip_download_if_exists` follows `cache_downloads` by default. A warning
    is only logged if the user explicitly combines it with `cache_downloads=False`.
    (A `skip_download_if_exists` of None means the argument is omitted.)
    """
    import inspect
    from worldpoppy import wp_raster
    from worldpoppy.download import WorldPopDownloader

    path = mock_raster_factory("A.tif", np.ones((10, 10)), origin=(0, 10))
    mock_downloader.download.return_value = ([path], fake_manifest_rows([("MCK", None)]))

    kwargs = dict(cache_downloads=cache_downloads)
    if skip_download_if_exists is not None:
        kwargs["skip_download_if_exists"] = skip_download_if_exists
    wp_raster(product_name="pop_g1", aoi=["MCK"], **kwargs)

    # Bind the recorded call to the real signature, so that the test does not
    # depend on whether `wp_raster` passes the argument by position or keyword
    call = mock_downloader.download.call_args
    bound = inspect.signature(WorldPopDownloader.download).bind(None, *call.args, **call.kwargs)
    passed_skip = bound.arguments["skip_download_if_exists"]
    assert passed_skip is expected_skip
    assert ("has no effect" in caplog.text) is expect_warning


@pytest.mark.parametrize(
    "masked, mask_and_scale, message",
    [
        (True, False, "masked but unscaled data"),  # A mode that is no longer supported
        (False, True, "requires `masked=True`"),    # Contradictory: scaling also masks
    ],
)
@pytest.mark.parametrize("func", ["wp_raster", "merge_rasters"])
def test_conflicting_read_settings_are_rejected(
    mock_downloader, func, masked, mask_and_scale, message
):
    """
    `masked` and `mask_and_scale` must agree. Conflicting values raise a
    `ValueError` before anything is downloaded or read.
    """
    import worldpoppy

    kwargs = dict(masked=masked, mask_and_scale=mask_and_scale)
    with pytest.raises(ValueError, match=re.escape(message)):
        if func == "wp_raster":
            worldpoppy.wp_raster("pop_g1", aoi=["MCK"], **kwargs)
        else:
            worldpoppy.merge_rasters(["does_not_exist.tif"], **kwargs)
    mock_downloader.download.assert_not_called()


@pytest.mark.parametrize("chunks", ["auto", 4, {}, {"x": 4, "y": 4}])
@pytest.mark.parametrize("func", ["wp_raster", "merge_rasters"])
def test_lazy_reads_require_masked_data(mock_downloader, func, chunks):
    """
    Lazy reading (any `chunks` other than None, including `{}`) is only
    supported for masked data, since lazy merging marks missing data with NaN
    (bugs 1.20, 1.21, 1.23, 1.24).
    """
    import worldpoppy

    with pytest.raises(ValueError, match=re.escape("Lazy reading (`chunks`) requires `masked=True`")):
        if func == "wp_raster":
            worldpoppy.wp_raster("pop_g1", aoi=["MCK"], chunks=chunks, masked=False)
        else:
            worldpoppy.merge_rasters(["does_not_exist.tif"], chunks=chunks, masked=False)
    mock_downloader.download.assert_not_called()


@pytest.mark.parametrize("key", ["masked", "mask_and_scale"])
@pytest.mark.parametrize("func", ["wp_raster", "merge_rasters"])
def test_read_settings_in_other_read_kwargs_are_rejected(mock_downloader, func, key):
    """
    The read settings cannot be passed through `other_read_kwargs`, where they
    would bypass the checks of `masked` and `mask_and_scale`.
    """
    import worldpoppy

    kwargs = dict(other_read_kwargs={key: False})
    with pytest.raises(ValueError, match=re.escape("`other_read_kwargs` must not contain")):
        if func == "wp_raster":
            worldpoppy.wp_raster("pop_g1", aoi=["MCK"], **kwargs)
        else:
            worldpoppy.merge_rasters(["does_not_exist.tif"], **kwargs)
    mock_downloader.download.assert_not_called()


def test_masked_false_alone_reads_raw_values(mock_raster_factory):
    """
    `mask_and_scale` follows `masked` unless given, so `masked=False` alone
    reads the raw values: the source data type, with nodata cells kept.
    """
    from worldpoppy import merge_rasters

    data = np.ones((5, 5), dtype="int16")
    data[0, 0] = -1
    path = mock_raster_factory("A.tif", data, nodata=-1)

    result = merge_rasters([path], masked=False)

    assert result.dtype == np.int16
    assert result.values[0, 0, 0] == -1
    assert result.attrs["wpy_read_masked"] == result.attrs["wpy_read_mask_and_scale"] == "false"


@pytest.mark.parametrize("chunks", [None, "auto"], ids=["eager", "lazy"])
def test_masked_reads_apply_scale_and_offset(mock_raster_factory, chunks):
    """
    Masked reads also apply the source files' scale and offset (rioxarray's
    `mask_and_scale=True`); raw reads keep the stored values.
    """
    from worldpoppy import merge_rasters

    data = np.full((5, 5), 3, dtype="int16")
    data[0, 0] = -1
    path = mock_raster_factory("A.tif", data, nodata=-1, scale_factor=10.0)

    masked = merge_rasters([path], chunks=chunks).compute()
    raw = merge_rasters([path], masked=False)

    assert float(masked.values[0, 1, 1]) == 30.0
    assert np.isnan(masked.values[0, 0, 0])
    assert raw.values[0, 1, 1] == 3
