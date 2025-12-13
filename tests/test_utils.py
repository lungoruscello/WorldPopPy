import functools
import socket

import pytest


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
