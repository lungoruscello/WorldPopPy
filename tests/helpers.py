import functools
import socket

import pytest


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
