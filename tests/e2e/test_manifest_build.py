import logging
import time
import pytest
from datetime import datetime
from tests.test_utils import needs_internet
import worldpoppy.manifest_builder  # Import target for patching


@pytest.mark.e2e
@needs_internet
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
    from worldpoppy.manifest_builder import build_raw_manifest_from_api

    # Force DEBUG=True to speed up the build (limits API scope to 'covariates').
    # We patch the module variable directly because it is imported as
    # `from ...config import DEBUG` inside the builder module.
    monkeypatch.setattr(worldpoppy.manifest_builder, "DEBUG", True)

    # Expecting logs from the builder module
    caplog.set_level(logging.INFO, logger="worldpoppy.manifest_builder")

    # Define paths based on the isolated fixture
    temp_dir = isolated_manifest_assets
    feather_path = temp_dir / "raw_api_manifest.feather"
    timestamp_path = temp_dir / "raw_api_manifest_timestamp.txt"

    # --- 1. First call (Cold Start) ---
    # Cache is empty, so this MUST download/build.
    build_raw_manifest_from_api(force_rebuild=False)

    # Check files exist
    assert feather_path.is_file()
    assert feather_path.stat().st_size > 0
    assert timestamp_path.is_file()

    # Read the first timestamp
    with open(timestamp_path, 'r') as f:
        ts_str_1 = f.read().strip()

    # Verify it looks like an ISO date
    dt_1 = datetime.fromisoformat(ts_str_1)
    assert dt_1.year >= 2025  # Sanity check

    # Check logs
    assert "Traversing WorldPop's meta-data API" in caplog.text
    assert "Updated raw data manifest saved" in caplog.text

    # --- 2. Second call (Warm Cache) ---
    # Nothing should happen; cache is fresh (< 180 days).
    caplog.clear()
    time.sleep(0.1)  # Sleep to ensure system time advances

    build_raw_manifest_from_api(force_rebuild=False)

    # Check logs: Should verify it found cached results
    assert "Traversing WorldPop's meta-data API" not in caplog.text

    # Verify the timestamp file was NOT touched/changed
    with open(timestamp_path, 'r') as f:
        ts_str_2 = f.read().strip()

    assert ts_str_1 == ts_str_2

    # --- 3. Third call (Forced Rebuild) ---
    # Force rebuild should ignore the fresh cache.
    caplog.clear()
    time.sleep(1.1)  # Sleep > 1s to ensure significant timestamp diff

    build_raw_manifest_from_api(force_rebuild=True)

    # Check logs: Should verify it ran the traversal again
    assert "Traversing WorldPop's meta-data API" in caplog.text
    assert "Updated raw data manifest saved" in caplog.text

    # Verify the timestamp file WAS updated
    with open(timestamp_path, 'r') as f:
        ts_str_3 = f.read().strip()

    assert ts_str_3 != ts_str_2
    dt_3 = datetime.fromisoformat(ts_str_3)
    assert dt_3 > dt_1
