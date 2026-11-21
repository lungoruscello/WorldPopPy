import os
import stat
import time
from contextlib import contextmanager
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest
from worldpoppy.download import (
    WorldPopDownloader,
    DownloadError,
    DownloadSizeCheckError,
    _repair_cache,
    _STALE_DOWNLOAD_AGE_SECONDS,
)


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


# --- Fake HTTP Helpers ---
# Most helpers below fake `httpx.stream` / `httpx.head` with a scripted
# sequence of outcomes, one per request. Each outcome is either an exception
# to raise, an HTTP status code to respond with, or (for `httpx.stream` only)
# a custom `httpx.SyncByteStream` to serve as a 200 response body.
#
# The `_fake_..._handler` helpers instead fake the network one level lower,
# by replacing `httpx.HTTPTransport.handle_request`. This keeps httpx's own
# redirect handling in play, which replacing `httpx.stream` / `httpx.head`
# wholesale would bypass. (Because the patch is applied to the class, it also
# covers any proxy transports httpx creates from environment variables.)


class _MidStreamFailure(httpx.SyncByteStream):
    """A response body that delivers some bytes and then drops the connection."""

    def __iter__(self):
        yield b"PARTIAL-"
        raise httpx.ReadError("Connection dropped mid-stream")


class _InterruptedStream(httpx.SyncByteStream):
    """
    A response body that calls `interruption()` halfway through the download,
    e.g. to simulate another process acting on the same cache meanwhile.
    """

    def __init__(self, interruption):
        self.interruption = interruption

    def __iter__(self):
        yield FAKE_CONTENT[:4]
        self.interruption()
        yield FAKE_CONTENT[4:]


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


def _fake_https_redirect_handler(calls):
    """
    Return a fake `HTTPTransport.handle_request` that redirects every
    'http://' URL to its 'https://' equivalent (as WorldPop's data server
    does) and serves FAKE_CONTENT for 'https://' URLs. Each request is
    recorded in `calls` as a (method, url) tuple.
    """
    def fake_handle_request(self, request):
        url = str(request.url)
        calls.append((request.method, url))
        if request.url.scheme == "http":
            https_url = url.replace("http://", "https://", 1)
            return httpx.Response(302, headers={"Location": https_url}, request=request)
        return httpx.Response(
            200,
            headers={"Content-Length": str(len(FAKE_CONTENT))},
            stream=httpx.ByteStream(b"" if request.method == "HEAD" else FAKE_CONTENT),
            request=request,
        )

    return fake_handle_request


def _fake_redirect_loop_handler(calls):
    """
    Return a fake `HTTPTransport.handle_request` that redirects every URL to
    itself, so httpx eventually raises `TooManyRedirects`. Each request is
    recorded in `calls` as a (method, url) tuple.
    """
    def fake_handle_request(self, request):
        url = str(request.url)
        calls.append((request.method, url))
        return httpx.Response(302, headers={"Location": url}, request=request)

    return fake_handle_request


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


def test_download_does_not_retry_unsupported_protocol(tmp_path, monkeypatch, no_backoff_sleep):
    """
    httpx files an unsupported URL scheme (e.g. 'ftp://') under transport
    errors, but retrying cannot fix it, so the download fails immediately.
    """
    calls = []
    outcomes = [httpx.UnsupportedProtocol("Request URL has an unsupported protocol 'ftp://'.")]
    monkeypatch.setattr(httpx, "stream", _scripted_stream(outcomes, calls))

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path)

    assert not res.success
    assert isinstance(res.error, httpx.UnsupportedProtocol)
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


# --- Tests: Redirect Handling ---

def test_download_follows_redirects(tmp_path, monkeypatch, no_backoff_sleep):
    """
    Downloads must follow redirects (e.g. WorldPop's 'http://' -> 'https://').
    """
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _fake_https_redirect_handler(calls))

    http_url = FAKE_URL.replace("https://", "http://", 1)
    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(http_url, local_path)

    # Check `calls` first: on failure, it shows whether the redirect was ignored
    assert calls == [("GET", http_url), ("GET", FAKE_URL)]
    assert res.success
    assert local_path.read_bytes() == FAKE_CONTENT


def test_size_check_follows_redirects(tmp_path, monkeypatch, no_backoff_sleep):
    """
    HEAD requests during a dry run must follow redirects, just like downloads.
    """
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _fake_https_redirect_handler(calls))

    http_url = FAKE_URL.replace("https://", "http://", 1)
    res = WorldPopDownloader(directory=tmp_path)._get_required_file_download_size(
        http_url, tmp_path / "afg.tif"
    )

    # Check `calls` first: on failure, it shows whether the redirect was ignored
    assert calls == [("HEAD", http_url), ("HEAD", FAKE_URL)]
    assert res.success
    assert res.value == len(FAKE_CONTENT)


def test_download_does_not_retry_redirect_loop(tmp_path, monkeypatch):
    """
    A redirect loop is final: retrying cannot fix it, so the download fails
    after a single attempt.
    """
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _fake_redirect_loop_handler(calls))

    # Record backoff's sleeps instead of skipping them: one attempt means
    # backoff never sleeps. (This avoids pinning httpx's redirect limit,
    # which determines how many requests a single attempt makes.)
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)

    local_path = tmp_path / "afg.tif"
    res = WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path)

    assert sleeps == []
    assert not res.success
    assert isinstance(res.error, httpx.TooManyRedirects)
    assert set(calls) == {("GET", FAKE_URL)}
    assert not local_path.exists()
    assert not list(tmp_path.glob("*.download"))


# --- Tests: Concurrent Downloads ---

def test_download_survives_cache_cleanup_by_another_process(isolated_raster_cache, monkeypatch):
    """
    Another process that starts up mid-download (and cleans up partial
    downloads) must not delete the temporary file of a running download.
    """
    calls = []
    monkeypatch.setattr(httpx, "stream", _scripted_stream([_InterruptedStream(_repair_cache)], calls))

    local_path = isolated_raster_cache / "afg.tif"
    res = WorldPopDownloader(directory=isolated_raster_cache)._download_file(FAKE_URL, local_path)

    assert res.success, res.error
    assert local_path.read_bytes() == FAKE_CONTENT


def test_overlapping_downloads_of_same_raster(tmp_path, monkeypatch):
    """
    A second download of the same raster that runs while the first one is
    still in progress (e.g. by a parallel job) must not break either of them:
    each writes to its own temporary file in the cache dir.
    """
    downloader = WorldPopDownloader(directory=tmp_path)
    local_path = tmp_path / "afg.tif"
    temp_files_during_first, second_result = [], []

    def second_download():
        temp_files_during_first.extend(tmp_path.glob("*.download"))
        second_result.append(downloader._download_file(FAKE_URL, local_path, skip_if_exists=False))

    outcomes = [_InterruptedStream(second_download), 200]
    monkeypatch.setattr(httpx, "stream", _scripted_stream(outcomes, []))

    first_result = downloader._download_file(FAKE_URL, local_path)

    assert len(temp_files_during_first) == 1  # the first download's own temp file
    assert first_result.success, first_result.error
    assert second_result[0].success, second_result[0].error
    assert local_path.read_bytes() == FAKE_CONTENT
    assert not list(tmp_path.glob("*.download"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX file permissions")
def test_downloaded_raster_has_usual_permissions(tmp_path, monkeypatch):
    """
    Cached rasters get the usual file permissions (subject to the umask), not
    owner-only ones, so that they stay readable in shared caches.
    """
    monkeypatch.setattr(httpx, "stream", _scripted_stream([200], []))
    local_path = tmp_path / "afg.tif"

    assert WorldPopDownloader(directory=tmp_path)._download_file(FAKE_URL, local_path).success

    umask = os.umask(0)
    os.umask(umask)
    assert stat.S_IMODE(local_path.stat().st_mode) == 0o666 & ~umask


def test_download_cleans_up_stale_leftovers_first(
    isolated_raster_cache, mock_wp_manifest_constrained, monkeypatch
):
    """
    `download()` removes stale partial downloads before it starts.
    """
    monkeypatch.setattr("worldpoppy.download.pqdm", MagicMock())
    (isolated_raster_cache / "pop_g1_AFG_2020.tif").touch()
    (isolated_raster_cache / "pop_g1_ALB_2020.tif").touch()
    leftover = isolated_raster_cache / "pop_g1_AFG_2020.tif.abc123.download"
    leftover.write_bytes(b"partial")
    long_ago = time.time() - _STALE_DOWNLOAD_AGE_SECONDS - 60
    os.utime(leftover, (long_ago, long_ago))

    WorldPopDownloader().download("pop_g1", ["AFG", "ALB"], 2020)

    assert not leftover.exists()


def test_repair_cache_deletes_only_stale_partial_downloads(isolated_raster_cache):
    """
    Partial downloads left behind by crashed runs are deleted; recent ones
    (possibly still being written by another process) are kept.
    """
    stale = isolated_raster_cache / "pop_g1_AFG_2020.tif.abc123.download"
    recent = isolated_raster_cache / "pop_g1_ALB_2020.tif.def456.download"
    stale.write_bytes(b"partial")
    recent.write_bytes(b"partial")
    an_hour_and_a_bit_ago = time.time() - _STALE_DOWNLOAD_AGE_SECONDS - 60
    os.utime(stale, (time.time(), an_hour_and_a_bit_ago))  # (atime, mtime): only mtime counts

    _repair_cache()

    assert not stale.exists()
    assert recent.exists()


# --- Tests: Cache Purging ---

def test_purge_cache_keeps_country_borders_by_default(isolated_raster_cache):
    """
    By default, `purge_cache` deletes downloaded rasters but keeps the cached
    country borders. With `keep_country_borders=False`, it deletes both.
    """
    from worldpoppy import purge_cache
    from worldpoppy.borders import _border_buffered_fpath

    raster_fpath = isolated_raster_cache / "pop_g1_AFG_2020.tif"
    raster_fpath.write_bytes(b"fake raster")
    borders_fpath = _border_buffered_fpath()
    # Safety: never write fake borders outside the isolated cache (e.g. into
    # the user's real cache, should the path resolution ever regress)
    assert borders_fpath.parent == isolated_raster_cache
    borders_fpath.write_bytes(b"fake borders")

    summary = purge_cache(dry_run=False)
    assert not raster_fpath.exists()
    assert borders_fpath.exists()
    assert (summary["matched_files"], summary["deleted_files"]) == (1, 1)

    summary = purge_cache(dry_run=False, keep_country_borders=False)
    assert not borders_fpath.exists()
    assert (summary["matched_files"], summary["deleted_files"]) == (1, 1)


def test_purge_cache_keeps_updated_manifest_by_default(isolated_raster_cache):
    """
    By default, `purge_cache` keeps a data manifest updated by the user
    (rebuilding it takes minutes). With `keep_manifest=False`, it deletes it.
    """
    from worldpoppy import purge_cache
    from worldpoppy.config import get_user_manifest_path

    manifest_fpath = get_user_manifest_path()
    # Safety: never write a fake manifest outside the isolated cache
    assert manifest_fpath.parent == isolated_raster_cache
    manifest_fpath.write_bytes(b"fake manifest")

    purge_cache(dry_run=False)
    assert manifest_fpath.exists()

    purge_cache(dry_run=False, keep_manifest=False)
    assert not manifest_fpath.exists()


def test_purge_cache_dry_run_deletes_nothing(isolated_raster_cache):
    """
    A dry run only reports what would be deleted.
    """
    from worldpoppy import purge_cache

    raster_fpath = isolated_raster_cache / "pop_g1_AFG_2020.tif"
    raster_fpath.write_bytes(b"fake raster")

    summary = purge_cache(dry_run=True)

    assert raster_fpath.exists()
    assert (summary["matched_files"], summary["deleted_files"]) == (1, 0)
