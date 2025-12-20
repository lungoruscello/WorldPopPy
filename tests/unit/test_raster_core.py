from unittest.mock import MagicMock

import dask.array as da
import numpy as np
import pytest
import rioxarray
import xarray as xr


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


def test_pre_clipping_optimisation(mock_raster_factory, mock_downloader):
    """
    Verify that providing `pre_clip_bbox` restricts the resulting array shape
    *immediately* (lazy shape inspection).

    Proof of Pre-Clipping:
    We use `aoi=['MOCK']` (ISO codes), which results in `clipping_gdf=None`.
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

    mock_downloader.download.return_value = ([path_huge], MagicMock())

    # Define a tiny clip box: X=10..20, Y=10..20 (10x10 area)
    tiny_box = (10, 10, 20, 20)

    # --- Act 1: WITH Pre-Clipping ---
    result_optimised = wp_raster(
        product_name="pop_g1", aoi=["MOCK"], pre_clip_bbox=tiny_box, chunks="auto"
    )

    # --- Act 2: WITHOUT Pre-Clipping (Control) ---
    result_full = wp_raster(
        product_name="pop_g1",
        aoi=["MOCK"],
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
    mock_df = MagicMock()
    mock_downloader.download.return_value = ([path_a, path_b], mock_df)

    # --- Act ---
    result = wp_raster(
        product_name="pop_g1",
        aoi=["MOCK"],
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
