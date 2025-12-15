import pandas as pd
import pytest
from unittest.mock import MagicMock
from worldpoppy.download import WorldPopDownloader, DownloadError, DownloadSizeCheckError


@pytest.fixture
def mock_wp_manifest_constrained(monkeypatch):
    """
    Mocks wp_manifest_constrained to return a predictable DataFrame
    without needing the full manifest architecture.
    """
    def _mock_return(*args, **kwargs):
        # Return a dummy DF representing 2 files: AFG and ALB
        return pd.DataFrame({
            'product_name': ['pop_g1', 'pop_g1'],
            'iso3': ['AFG', 'ALB'],
            'year': [2020, 2020],
            'remote_path': ['https://fake.url/afg.tif', 'https://fake.url/alb.tif']
        })

    monkeypatch.setattr("worldpoppy.download.wp_manifest_constrained", _mock_return)


def test_download_smart_filtering(isolated_raster_cache, mock_wp_manifest_constrained, monkeypatch):
    """
    Verifies the "Filter-then-Execute" strategy in the main `download` function.
    If one file exists and one is missing, `pqdm` should only receive ONE task.
    """
    # 1. Mock pqdm to intercept the task list
    # We return a success object so the function doesn't crash
    mock_result = MagicMock(success=True)
    mock_pqdm = MagicMock(return_value=[mock_result])
    monkeypatch.setattr("worldpoppy.download.pqdm", mock_pqdm)

    # 2. Setup: Create the 'AFG' file locally so it counts as "cached"
    # Logic in _build_local_fpath: {product}_{iso3}_{year}.tif
    cached_file = isolated_raster_cache / "pop_g1_AFG_2020.tif"
    cached_file.touch()

    # 3. Act
    downloader = WorldPopDownloader()
    downloader.download("pop_g1", ["AFG", "ALB"], 2020, skip_download_if_exists=True)

    # 4. Assert
    # pqdm should have been called exactly once
    assert mock_pqdm.call_count == 1

    # Inspect the arguments passed to pqdm
    # pqdm(args, function, ...) -> get the 'args' list
    call_args_list = mock_pqdm.call_args[0][0]

    # We expect exactly 1 task (ALB), because AFG was filtered out
    assert len(call_args_list) == 1

    # Verify it is indeed the missing file (ALB)
    target_remote_url = call_args_list[0][0]
    assert "alb.tif" in target_remote_url


def test_download_skips_pqdm_if_all_cached(isolated_raster_cache, mock_wp_manifest_constrained, monkeypatch):
    """
    Verifies that if ALL files exist, `pqdm` is not called at all.
    """
    mock_pqdm = MagicMock()
    monkeypatch.setattr("worldpoppy.download.pqdm", mock_pqdm)

    # Setup: Create BOTH files locally
    (isolated_raster_cache / "pop_g1_AFG_2020.tif").touch()
    (isolated_raster_cache / "pop_g1_ALB_2020.tif").touch()

    downloader = WorldPopDownloader()
    downloader.download("pop_g1", ["AFG", "ALB"], 2020)

    # Assert: pqdm should NOT be called
    mock_pqdm.assert_not_called()


def test_download_dry_run_logic(isolated_raster_cache, mock_wp_manifest_constrained, monkeypatch):
    """
    Verifies that dry_run=True switches the worker function to
    `_get_required_file_download_size` and passes correct args.
    """
    mock_pqdm = MagicMock(return_value=[MagicMock(success=True, value=100)])
    monkeypatch.setattr("worldpoppy.download.pqdm", mock_pqdm)

    downloader = WorldPopDownloader()
    downloader.download("pop_g1", ["AFG", "ALB"], 2020, dry_run=True)

    # Assert
    assert mock_pqdm.call_count == 1

    # Check the function argument passed to pqdm (positional arg #2)
    # It should be the size-check method, not the download method
    passed_func = mock_pqdm.call_args[0][1]
    assert passed_func == downloader._get_required_file_download_size


def test_download_raises_on_worker_failure(isolated_raster_cache, mock_wp_manifest_constrained, monkeypatch):
    """
    Verifies that if pqdm returns a failure result, the main function
    raises a specific DownloadError.
    """
    # Mock pqdm returning a failed result object
    failure_result = MagicMock(success=False, error=RuntimeError("Connection Reset"))
    mock_pqdm = MagicMock(return_value=[failure_result, failure_result])
    monkeypatch.setattr("worldpoppy.download.pqdm", mock_pqdm)

    downloader = WorldPopDownloader()

    # Act & Assert
    with pytest.raises(DownloadError) as exc:
        downloader.download("pop_g1", ["AFG", "ALB"], 2020)

    assert "2 download(s) failed" in str(exc.value)
    assert "Connection Reset" in str(exc.value)
