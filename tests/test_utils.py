import functools
import socket
from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr
from affine import Affine


# --- Fixtures ---

@pytest.fixture
def no_manifest_update(monkeypatch):
    """
    Fixture to prevent the manifest builder from running.

    This patches `build_raw_manifest_from_api` in the builder module
    with a no-op function. This ensures unit tests do not trigger
    internet activity or file I/O related to manifest updates.
    """
    import worldpoppy as wpy

    monkeypatch.setattr(
        wpy.manifest_builder,
        "build_raw_manifest_from_api",
        lambda *args, **kwargs: None,  # do nothing
    )


@pytest.fixture
def isolated_manifest_assets(monkeypatch, tmp_path):
    """
    Fixture to isolate manifest assets (Feather + Timestamp Sidecar).

    This redirects the manifest paths to a temporary directory.
    Critically, it patches the variables in `config`, `manifest_loader`,
    AND `manifest_builder` to ensure consistency, as these modules
    import the path constants directly.

    Yields
    ------
    pathlib.Path
        The temporary directory containing the isolated manifest assets.
    """
    import worldpoppy as wpy

    # 1. Define temp paths
    new_manifest_path = tmp_path / "raw_api_manifest.feather"
    new_timestamp_path = tmp_path / "raw_api_manifest_timestamp.txt"

    # 2. Patch 'RAW_MANIFEST_CACHE_PATH' in all locations
    # (Required because modules use: from worldpoppy.config import RAW_MANIFEST_CACHE_PATH)
    monkeypatch.setattr(wpy.config, "RAW_MANIFEST_CACHE_PATH", new_manifest_path)
    monkeypatch.setattr(
        wpy.manifest_loader, "RAW_MANIFEST_CACHE_PATH", new_manifest_path
    )
    monkeypatch.setattr(
        wpy.manifest_builder, "RAW_MANIFEST_CACHE_PATH", new_manifest_path
    )

    # 3. Patch 'RAW_MANIFEST_TIMESTAMP_PATH' in all locations
    # (Note: Loader might not import this yet, but Config and Builder do)
    monkeypatch.setattr(
        wpy.config, "RAW_MANIFEST_TIMESTAMP_PATH", new_timestamp_path
    )
    monkeypatch.setattr(
        wpy.manifest_builder, "RAW_MANIFEST_TIMESTAMP_PATH", new_timestamp_path
    )

    # Yield the temp path for inspection in tests
    yield tmp_path


@pytest.fixture
def isolated_raster_cache(monkeypatch, tmp_path):
    """
    Fixture to isolate the WorldPopPy raster cache.

    This patches the 'WORLDPOPPY_CACHE_DIR' environment variable to
    point to a new, empty temporary directory. `worldpoppy.config.get_cache_dir`
    will pick this up dynamically.
    """
    new_cache_dir = tmp_path / "test_raster_cache"
    new_cache_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv('WORLDPOPPY_CACHE_DIR', str(new_cache_dir))

    yield new_cache_dir


@pytest.fixture
def mock_raster_factory(tmp_path):
    """
    Returns a factory function that creates a GeoTIFF in a temporary directory.

    Arguments:
    - filename: Name of the file to create.
    - data_array: Numpy array of values.
    - origin: Tuple (min_x, max_y) specifying the top-left corner coordinates.
    - res: Pixel resolution (default 1.0).
    - crs: Coordinate reference system (default EPSG:4326).
    - nodata: The nodata value to burn into the file metadata (default -9999).
    - **attrs: Arbitrary metadata to attach to the raster (e.g. scale_factor=10).
    """
    from worldpoppy.config import WGS84_CRS

    def _create(
        filename,
        data_array,
        origin=(0, 0),
        res=1.0,
        crs=WGS84_CRS,
        nodata=-9999,
        **attrs
    ):
        height, width = data_array.shape
        x_start, y_start = origin

        # Create the transform automatically
        # (Top-left X, Pixel Width, 0, Top-left Y, 0, -Pixel Height)
        transform = Affine.translation(x_start, y_start) * Affine.scale(res, -res)

        # Generate Coordinate Arrays (Pixel Centers)
        #   X: start + half_res, start + 1.5*res, ...
        xs = np.arange(width) * res + x_start + (res / 2)
        #   Y: start - half_res, start - 1.5*res, ... (negative because Y goes down)
        ys = y_start - (np.arange(height) * res) - (res / 2)

        # Build DataArray
        da = xr.DataArray(data_array, dims=("y", "x"), coords={"y": ys, "x": xs})

        # Attach arbitrary attributes (NEW: supports scale_factor checks)
        if attrs:
            da.attrs.update(attrs)

        # Write Geospatial Metadata
        da.rio.write_crs(crs, inplace=True)
        da.rio.write_transform(transform, inplace=True)
        da.rio.write_nodata(nodata, inplace=True)

        # Save
        file_path = tmp_path / filename
        da.rio.to_raster(file_path)

        return file_path

    return _create

@pytest.fixture
def mock_downloader(monkeypatch):
    """
    Fixture that mocks `WorldPopDownloader` to return a MagicMock instance.

    This ensures that tests do not attempt real HTTP requests. The returned
    mock instance can be configured in tests (e.g. `mock_dl.download.return_value = ...`).
    """
    # We patch the class where it is imported/used: worldpoppy.raster
    import worldpoppy.raster as raster_module

    # 1. Create the mock instance (the object returned when class is instantiated)
    mock_instance = MagicMock()

    # 2. Create the mock class (the factory)
    mock_class = MagicMock(return_value=mock_instance)

    # 3. Patch the class in the module
    monkeypatch.setattr(raster_module, "WorldPopDownloader", mock_class)

    return mock_instance


# --- Network Helpers ---

def is_online():
    """Check if we can connect to a known external server."""
    try:
        # 8.8.8.8 is Google's DNS. 53 is the DNS port.
        socket.create_connection(("8.8.8.8", 53), timeout=1)
        return True
    except OSError:
        return False

# Strict Mark: Skip immediately if offline
# (For e2e tests, which ALWAYS need internet)
needs_internet = pytest.mark.skipif(not is_online(), reason="No internet")


# Relaxed Mark: Run test if raster data is cached OR system is online
# (For integration tests, which use caching)
def needs_internet_or_cache(func):
    """
    Decorator: Run test if online OR if data is cached.

    Behavior:
    1. If online: Run normally. All failures are reported as real failures.
    2. If offline: Try running.
       - If `DownloadError` or `httpx.HTTPError`: SKIP (Data missing & cannot fetch).
       - If any other Exception (Assertion, Type, Value): FAIL (Real bug detected).
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Import inside wrapper to avoid top-level circular imports
        import httpx
        from worldpoppy.download import DownloadError

        try:
            return func(*args, **kwargs)
        except (DownloadError, httpx.HTTPError) as e:
            # We caught a specific network/download failure.
            # If we are offline, this is an expected "skip" condition.
            if not is_online():
                pytest.skip(f"No cached rasters and no internet): {e}")

            # If we ARE online, this is a real failure (e.g., 404, Server Error).
            raise e

    return wrapper


# --- Other Decorators ---

def needs_raw_manifest(func):
    """
    Decorator that skips the test if the raw manifest file (feather)
    does not exist in the assets directory.
    """
    from worldpoppy.config import RAW_MANIFEST_CACHE_PATH

    if not RAW_MANIFEST_CACHE_PATH.exists():
        return pytest.mark.skip(reason="Raw manifest file not found in assets.")(func)
    return func
