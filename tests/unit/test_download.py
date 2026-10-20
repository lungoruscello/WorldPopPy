from contextlib import contextmanager
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest
from worldpoppy.download import WorldPopDownloader, DownloadError, DownloadSizeCheckError


FAKE_URL = "https://fake.url/afg.tif"
FAKE_CONTENT = b"fake-tif-bytes"


# --- Fixtures ---

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


@pytest.fixture
def no_backoff_sleep(monkeypatch):
    """Patch out backoff's sleep between retries to keep tests fast."""
    monkeypatch.setattr("time.sleep", lambda seconds: None)


# --- Fake HTTP Helpers ---
# These helpers fake `httpx.stream` / `httpx.head` with a scripted sequence
# of outcomes, one per request. Each outcome is either an exception to raise,
# an HTTP status code to respond with, or (for `httpx.stream` only) a custom
# `httpx.SyncByteStream` to serve as a 200 response body.


class _MidStreamFailure(httpx.SyncByteStream):
    """A response body that delivers some bytes and then drops the connection."""

    def __iter__(self):
        yield b"PARTIAL-"
        raise httpx.ReadError("Connection dropped mid-stream")


def _next_outcome(outcomes):
    # Fail loudly if a test makes more requests than scripted. (`pytest.fail`
    # raises a BaseException, so the downloader's `except Exception` cannot
    # swallow it into an ordinary failed DownloadResult.)
    if not outcomes:
        pytest.fail("Fake server received more requests than scripted.")
    return outcomes.pop(0)


def _scripted_stream(outcomes, calls):
    @contextmanager
    def fake_stream(method, url, **kwargs):
        calls.append(url)
        outcome = _next_outcome(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, httpx.SyncByteStream):
            status, stream, headers = 200, outcome, {}
        else:
            # Use an unread stream (not `content=`) so that `iter_raw` works
            status = outcome
            stream = httpx.ByteStream(FAKE_CONTENT)
            headers = {"Content-Length": str(len(FAKE_CONTENT))}
        yield httpx.Response(
            status,
            headers=headers,
            stream=stream,
            request=httpx.Request(method, url),
        )

    return fake_stream


def _scripted_head(outcomes, calls):
    def fake_head(url, **kwargs):
        calls.append(url)
        outcome = _next_outcome(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        # A real HEAD response has no body, only the Content-Length header
        return httpx.Response(
            outcome,
            headers={"Content-Length": str(len(FAKE_CONTENT))},
            request=httpx.Request("HEAD", url),
        )

    return fake_head


# --- Tests: Download Orchestration ---

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
    Verify that if ALL files exist, `pqdm` is not called at all.
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
    Verify that dry_run=True switches the worker function to
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
    Verify that if pqdm returns a failure result, the main function
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


# --- Tests: Retry Behaviour ---

def test_download_retries_transient_errors(tmp_path, monkeypatch, no_backoff_sleep):
    """
    Transient network errors and 5xx responses must be retried until the
    download succeeds.
    """
    calls = []
    outcomes = [httpx.ConnectError("Connection Reset"), 503, 200]
    monkeypatch.setattr(httpx, "stream", _scripted_stream(outcomes, calls))

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path)

    assert res.success
    assert len(calls) == 3
    assert local_path.read_bytes() == FAKE_CONTENT
    assert not list(tmp_path.glob("*.download"))


def test_download_retry_discards_partial_data(tmp_path, monkeypatch, no_backoff_sleep):
    """
    If a connection drops mid-stream, the retry must start from an empty temp
    file. Otherwise, bytes from the failed attempt would corrupt the cached
    raster.
    """
    calls = []
    outcomes = [_MidStreamFailure(), 200]
    monkeypatch.setattr(httpx, "stream", _scripted_stream(outcomes, calls))

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(
        FAKE_URL, local_path, chunk_size=4
    )

    assert res.success
    assert len(calls) == 2
    assert local_path.read_bytes() == FAKE_CONTENT
    assert not list(tmp_path.glob("*.download"))


def test_download_gives_up_after_persistent_network_errors(tmp_path, monkeypatch, no_backoff_sleep):
    """
    If the connection keeps failing, the download gives up after the maximum
    number of attempts and reports the network error.
    """
    calls = []
    outcomes = [httpx.ConnectError("Connection Reset")] * 5
    monkeypatch.setattr(httpx, "stream", _scripted_stream(outcomes, calls))

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path)

    assert not res.success
    assert isinstance(res.error, httpx.ConnectError)
    assert len(calls) == 5
    assert not local_path.exists()
    assert not list(tmp_path.glob("*.download"))


def test_download_retries_rate_limit_then_gives_up(tmp_path, monkeypatch, no_backoff_sleep):
    """
    A 429 is retried (unlike other 4xx codes). If it persists, the download
    fails after the maximum number of attempts.
    """
    calls = []
    monkeypatch.setattr(httpx, "stream", _scripted_stream([429] * 5, calls))

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path)

    assert not res.success
    assert isinstance(res.error, httpx.HTTPStatusError)
    assert res.error.response.status_code == 429
    assert len(calls) == 5
    assert not local_path.exists()
    assert not list(tmp_path.glob("*.download"))


def test_download_does_not_retry_fatal_client_errors(tmp_path, monkeypatch, no_backoff_sleep):
    """
    A 404 is final: no retries, a failed result, and no leftover temp file.
    """
    calls = []
    monkeypatch.setattr(httpx, "stream", _scripted_stream([404], calls))

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path)

    assert not res.success
    assert isinstance(res.error, httpx.HTTPStatusError)
    assert len(calls) == 1
    assert not local_path.exists()
    assert not list(tmp_path.glob("*.download"))


def test_size_check_retries_transient_errors(tmp_path, monkeypatch, no_backoff_sleep):
    """
    HEAD requests during a dry run must also be retried.
    """
    calls = []
    outcomes = [httpx.ReadTimeout("Timed out"), 200]
    monkeypatch.setattr(httpx, "head", _scripted_head(outcomes, calls))

    res = WorldPopDownloader(directory=tmp_path)._get_required_file_download_size(
        FAKE_URL, tmp_path / "afg.tif"
    )

    assert res.success
    assert res.value == len(FAKE_CONTENT)
    assert len(calls) == 2


@pytest.mark.parametrize("status_code", [503, 429])
def test_size_check_retries_retryable_status_codes(
    tmp_path, monkeypatch, no_backoff_sleep, status_code
):
    """
    HEAD requests answered with 5xx or 429 must be retried until they succeed.
    """
    calls = []
    monkeypatch.setattr(httpx, "head", _scripted_head([status_code, 200], calls))

    res = WorldPopDownloader(directory=tmp_path)._get_required_file_download_size(
        FAKE_URL, tmp_path / "afg.tif"
    )

    assert res.success
    assert res.value == len(FAKE_CONTENT)
    assert len(calls) == 2


def test_size_check_does_not_retry_fatal_client_errors(tmp_path, monkeypatch, no_backoff_sleep):
    """
    A 404 on a HEAD request is final: no retries, and a failed result rather
    than a silent file size of zero.
    """
    calls = []
    monkeypatch.setattr(httpx, "head", _scripted_head([404], calls))

    res = WorldPopDownloader(directory=tmp_path)._get_required_file_download_size(
        FAKE_URL, tmp_path / "afg.tif"
    )

    assert not res.success
    assert isinstance(res.error, httpx.HTTPStatusError)
    assert res.error.response.status_code == 404
    assert len(calls) == 1
