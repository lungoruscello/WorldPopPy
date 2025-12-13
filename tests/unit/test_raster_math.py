import numpy as np
import rioxarray


def test_merge_preserves_count(mock_raster_factory):
    """
    Unit Test: Verify that merging two spatially adjacent rasters
    preserves the total sum of their values.
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
