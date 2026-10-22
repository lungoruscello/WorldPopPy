import logging
import time

import pytest

import worldpoppy.manifest_builder  # Import target for patching
from tests.helpers import needs_internet
from worldpoppy.config import ENABLE_HEAVY_TESTS


@pytest.mark.e2e
@needs_internet
@pytest.mark.skipif(
    not ENABLE_HEAVY_TESTS,
    reason="Heavy E2E tests are disabled (WPY_RUN_HEAVY_TESTS != 1)"
)
def test_e2e_manifest_builder_lifecycle(isolated_manifest_assets, caplog, monkeypatch):
    """
    Runs a full e2e test of the manifest builder logic.
    Verifies creation of the Feather file and the Timestamp sidecar,
    and ensures caching logic respects the 'force_rebuild' flag.

    WARNING:
    --------
    This test triggers TWO full traversals of the WorldPop API (one initial,
    one forced re-build). Since this involves many API calls, this test
    should only be performed occasionally.

    We force `DEBUG=True` here to limit the API traversals.
    """
    from worldpoppy.manifest_builder import build_raw_manifest_from_api, _read_manifest_info

    # Force DEBUG=True to speed up the build (limits API scope to 'covariates').
    # We patch the module variable directly because it is imported as
    # `from ...config import DEBUG` inside the builder module.
    monkeypatch.setattr(worldpoppy.manifest_builder, "DEBUG", True)

    # Expecting logs from the builder module
    caplog.set_level(logging.INFO, logger="worldpoppy.manifest_builder")

    # Define paths based on the isolated fixture (builds are saved to the cache dir)
    temp_dir = isolated_manifest_assets
    feather_path = temp_dir / "raw_api_manifest.feather"

    # --- 1. First call (Cold Start) ---
    # Cache is empty, so this MUST download/build.
    report = build_raw_manifest_from_api(force_rebuild=False)
    assert report.saved_to == feather_path, report.summary()

    # Check the file exists, with its build time embedded
    assert feather_path.is_file()
    assert feather_path.stat().st_size > 0
    dt_1 = _read_manifest_info(feather_path, source="user").built_at
    assert dt_1.year >= 2025  # Sanity check

    # Check logs
    assert "Traversing WorldPop's meta-data API" in caplog.text
    assert "Manifest build finished" in caplog.text

    # --- 2. Second call (Warm Cache) ---
    # Nothing should happen; cache is fresh (< 180 days).
    caplog.clear()
    time.sleep(0.1)  # Sleep to ensure system time advances

    # No build is needed, so no report is returned
    assert build_raw_manifest_from_api(force_rebuild=False) is None

    # Check logs: Should verify it found cached results
    assert "Traversing WorldPop's meta-data API" not in caplog.text

    # Verify the manifest was NOT rebuilt
    assert _read_manifest_info(feather_path, source="user").built_at == dt_1

    # --- 3. Third call (Forced Rebuild) ---
    # Force rebuild should ignore the fresh cache.
    caplog.clear()
    time.sleep(1.1)  # Sleep > 1s to ensure significant timestamp diff

    report = build_raw_manifest_from_api(force_rebuild=True)
    assert report.saved_to == feather_path, report.summary()

    # Check logs: Should verify it ran the traversal again
    assert "Traversing WorldPop's meta-data API" in caplog.text
    assert "Manifest build finished" in caplog.text

    # Verify the manifest WAS rebuilt
    dt_3 = _read_manifest_info(feather_path, source="user").built_at
    assert dt_3 > dt_1
