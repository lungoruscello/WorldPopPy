import functools
import socket

import pandas as pd
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
    Decorator that skips the test if the bundled raw manifest file (feather)
    does not exist in the assets directory.
    """
    from worldpoppy.config import BUNDLED_MANIFEST_PATH

    if not BUNDLED_MANIFEST_PATH.exists():
        return pytest.mark.skip(reason="Raw manifest file not found in assets.")(func)
    return func


# --- Fake Manifest Helpers ---

def fake_manifest_rows(datasets, product_name="pop_test", multi_year=False, citations=False):
    """
    Return fake (cleaned) manifest rows for unit tests that mock the
    downloader, with one row per dataset in the same order.

    Parameters
    ----------
    datasets : list of (str, int or None)
        The (ISO3 code, year) of each dataset. Use None as year for static
        datasets without a year.
    product_name : str
        The product name of all rows.
    multi_year : bool
        The value of the `multi_year` column.
    citations : bool
        If True, add the columns `citation`, `doi` and `license` (with the same
        values for all rows, as for a real data series).
    """
    rows = []
    for iso3, year in datasets:
        tag = f"{iso3}{year or ''}"
        row = {
            "product_name": product_name,
            "iso3": iso3,
            "year": pd.NA if year is None else year,
            "multi_year": multi_year,
            "product_notes": "Estimated number of people per grid-cell.",
            "remote_name": f"{iso3.lower()}_pop_{year or 'static'}.tif",
            "summary_url": f"https://hub.example/summary?id={tag}",
            "api_entry_title": f"Population of {iso3} in {year or 'all years'}",
            "api_source": "WorldPop, University of Southampton, UK",
        }
        if citations:
            row["citation"] = "WorldPop (2018). A test citation. https://dx.doi.org/10.0/TEST"
            row["doi"] = "10.0/TEST"
            row["license"] = "https://hub.example/licence.txt"
        rows.append(row)
    return pd.DataFrame(rows)

