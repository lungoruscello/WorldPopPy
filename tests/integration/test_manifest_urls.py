import logging
import time

import httpx
import pytest

from worldpoppy.config import ENABLE_HEAVY_TESTS
from worldpoppy.manifest_loader import wp_manifest

logger = logging.getLogger(__name__)

# --- Configuration ---
SAMPLE_SIZE = 50           # Be polite
DELAY_BETWEEN_REQS = 0.5   # Seconds (Be polite)
MAX_RETRIES = 3            # Resilience
BACKOFF_FACTOR = 1.0       # Seconds (Exponential backoff base)


@pytest.mark.skipif(
    not ENABLE_HEAVY_TESTS,
    reason="Heavy E2E tests are disabled (WPY_RUN_HEAVY_TESTS != 1)"
)
def test_random_raster_urls_valid():
    """
    Randomly sample raster URLs and ensure they return 200 OK and
    look like image files.
    """
    mdf = wp_manifest()
    urls = _get_sample_urls(mdf, column="remote_path", n=SAMPLE_SIZE)

    # Check them (Expecting image content)
    failures = _check_url_batch(urls, expect_image=True)

    if failures:
        pytest.fail(f"Raster URL validation failed for {len(failures)} items:\n" + "\n".join(failures))


@pytest.mark.skipif(
    not ENABLE_HEAVY_TESTS,
    reason="Heavy E2E tests are disabled (WPY_RUN_HEAVY_TESTS != 1)"
)
def test_random_summary_urls_valid():
    """
    Randomly sample summary page URLs and ensure they return 200 OK.
    """
    mdf = wp_manifest()
    urls = _get_sample_urls(mdf, column="summary_url", n=SAMPLE_SIZE)

    # Check them (Expecting HTML, so expect_image=False)
    failures = _check_url_batch(urls, expect_image=False)

    if failures:
        pytest.fail(f"Summary URL validation failed for {len(failures)} items:\n" + "\n".join(failures))


# --- Shared Helpers ---

def _get_sample_urls(df, column, n):
    """Safely extract a random sample of non-null URLs."""
    valid_rows = df[df[column].notna()]
    if valid_rows.empty:
        pytest.skip(f"No valid URLs found in column '{column}'")

    # Do not try to sample more than we have
    n_safe = min(n, len(valid_rows))

    # We use a fixed seed so that if this test fails today, it will
    # fail on the exact same URLs tomorrow (easier debugging).
    return valid_rows[column].sample(n=n_safe, random_state=42).tolist()


def _check_url_batch(urls, expect_image=False):
    """
    Check a list of URLs with retries, backoff, and delays.
    Returns a list of error strings (empty if all good).
    """
    failures = []
    headers = {"User-Agent": "WorldPopPy-Maintenance-Test/1.0"}

    # Use a single client for connection pooling
    with httpx.Client(headers=headers, timeout=10.0, follow_redirects=True) as client:

        for i, url in enumerate(urls):
            # 1. Politeness Delay (Throttle)
            if i > 0:
                time.sleep(DELAY_BETWEEN_REQS)

            error_msg = None

            # 2. Retry Logic
            for attempt in range(MAX_RETRIES):
                try:
                    # HEAD request saves bandwidth!
                    resp = client.head(url)

                    # Validate Status
                    if resp.status_code != 200:
                        error_msg = f"Status {resp.status_code}"
                        # Do not retry 404s (file is definitely gone), only 5xx
                        if resp.status_code < 500:
                            break
                    else:
                        # Validate Content-Type (Optional)
                        if expect_image:
                            ct = resp.headers.get("content-type", "").lower()
                            # Allow image, tiff, octet-stream, or binary
                            if not any(x in ct for x in ["image", "tiff", "octet", "binary"]):
                                error_msg = f"Invalid Content-Type: {ct}"
                                break

                        # Success! Clear error and exit retry loop
                        error_msg = None
                        break

                except httpx.RequestError as e:
                    error_msg = f"Network Exception: {e}"

                # If we are here, we failed. Wait before retrying.
                sleep_time = BACKOFF_FACTOR * (2 ** attempt)
                time.sleep(sleep_time)

            # 3. Log Result
            if error_msg:
                logger.error(f"[FAIL] {url} -> {error_msg}")
                failures.append(f"{url} ({error_msg})")
            else:
                logger.info(f"[OK] {url}")

    return failures
