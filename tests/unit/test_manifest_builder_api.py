import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.feather
import pytest

import worldpoppy.manifest_builder as manifest_builder
from worldpoppy import __version__
from worldpoppy.config import DATA_SERVER_URL, GLOBAL1_FILE_LIST_URL, METADATA_API_URL, get_user_manifest_path
from worldpoppy.manifest_builder import (
    APIRequestError,
    ManifestBuildReport,
    _fetch_global1_file_list,
    _query_metadata_api,
    _read_manifest_info,
    _select_manifest,
    _traverse_api_node,
    _write_manifest,
    build_raw_manifest_from_api,
)
from worldpoppy.tracking import api_query_log

FAKE_API_URL = "https://fake.api/rest/data/pop"
FAKE_PAYLOAD = {"data": [{"alias": "wpgp", "name": "Unconstrained"}]}

# A tiny fake metadata API for manifest-build tests (URL -> payload, or an
# exception to raise). Its one top-level branch ('pop') holds a data series
# that works, one that fails to process, and a sub-branch that fails to crawl.
FAKE_API_TREE = {
    METADATA_API_URL: {"data": [{"alias": "pop", "name": "Population"}]},
    f"{METADATA_API_URL}/pop": {"data": [
        {"alias": "ok", "name": "Works"},
        {"alias": "bad_series", "name": "Fails to process"},
        {"alias": "bad_branch", "name": "Fails to crawl"},
    ]},
    f"{METADATA_API_URL}/pop/ok": {"data": [{"id": "1", "iso3": "AFG"}]},
    f"{METADATA_API_URL}/pop/bad_series": {"data": [{"id": "2", "iso3": "AFG"}]},
    f"{METADATA_API_URL}/pop/bad_branch": APIRequestError("HTTP 503"),
}
FAILING_SERIES = {"pop/bad_series"}

# The one raster file of each row in fake builds (a Global 1 file, so that the
# URL Check applies), and the same file in another folder
FAKE_FILE_PATH = "GIS/Population/Global_2000_2020_Constrained/2020/maxar_v1/AFG/afg_ppp_2020_constrained.tif"
FAKE_FILE_URL = f"{DATA_SERVER_URL}/{FAKE_FILE_PATH.removeprefix('GIS/')}"
MOVED_FILE_PATH = FAKE_FILE_PATH.replace("maxar_v1", "BSGM")


# --- Fixtures ---


@pytest.fixture(autouse=True)
def clean_api_query_log():
    """Keep the global API query log from leaking between tests."""
    api_query_log.reset()
    yield
    api_query_log.reset()


# --- Fake HTTP Helpers ---
# `_query_metadata_api` sends requests through an `httpx.Client`, so the fakes
# below replace `httpx.HTTPTransport.handle_request` (rather than a top-level
# httpx function). This also keeps httpx's own redirect handling in play.
#
# `_scripted_handler` serves a scripted sequence of outcomes, one per request.
# Each outcome is either:
#   - an exception to raise;
#   - an HTTP status code to respond with (200 carries FAKE_PAYLOAD as JSON);
#   - a dict to serve as a 200 JSON body;
#   - raw bytes to serve as a 200 body; or
#   - a ready-made `httpx.Response` (e.g. a redirect).


def _scripted_handler(outcomes, calls):
    def fake_handle_request(self, request):
        calls.append(str(request.url))
        # Fail loudly if a test makes more requests than scripted. (`pytest.fail`
        # raises a BaseException, so `_query_metadata_api` cannot swallow it.)
        if not outcomes:
            pytest.fail("Fake server received more requests than scripted.")
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, httpx.Response):
            return outcome
        if isinstance(outcome, dict):
            return httpx.Response(200, json=outcome, request=request)
        if isinstance(outcome, bytes):
            return httpx.Response(200, content=outcome, request=request)
        if outcome == 200:
            return httpx.Response(200, json=FAKE_PAYLOAD, request=request)
        return httpx.Response(outcome, request=request)

    return fake_handle_request


def _fake_redirect_loop_handler(calls):
    """
    Return a fake `HTTPTransport.handle_request` that redirects every URL to
    itself, so httpx eventually raises `TooManyRedirects`.
    """
    def fake_handle_request(self, request):
        url = str(request.url)
        calls.append(url)
        return httpx.Response(302, headers={"Location": url}, request=request)

    return fake_handle_request


# --- Fake Build Helpers ---


def _patch_build_backend(monkeypatch, api_tree, rows_per_series=1, file_list=(FAKE_FILE_PATH,)):
    """
    Serve `api_tree` as the metadata API, and stub the processing of each
    data series: it yields `rows_per_series` manifest rows (each with the
    download URL of FAKE_FILE_PATH), or fails for FAILING_SERIES. (This keeps
    build tests independent of real API payload formats.)

    `file_list` is served as WorldPop's Global 1 File List (by default, it
    confirms the rows' URL), or raised if it is an exception. With None, the
    real download function runs (a test then serves the file through httpx).
    """
    def fake_query(url):
        outcome = api_tree[url]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def fake_process_leaf_node(api_path, node_name, coverage_index):
        if api_path in FAILING_SERIES:
            raise ValueError(f"Cannot parse {api_path}")
        row = {"api_path": api_path, "iso3": "AFG", "remote_path": FAKE_FILE_URL}
        return [row] * rows_per_series

    def fake_fetch_file_list():
        if isinstance(file_list, Exception):
            raise file_list
        return list(file_list)

    monkeypatch.setattr(manifest_builder, "_query_metadata_api", fake_query)
    monkeypatch.setattr(manifest_builder, "_process_leaf_node", fake_process_leaf_node)
    if file_list is not None:
        monkeypatch.setattr(manifest_builder, "_fetch_global1_file_list", fake_fetch_file_list)


def _write_fake_manifest(path, days_old, version=__version__):
    """
    Write a tiny manifest file built `days_old` days ago by `version`. With
    `days_old="unknown"` or `version=None`, the respective metadata is left
    out (as in manifests written by older or other tools).
    """
    mdf = pd.DataFrame({"iso3": ["AFG"]})
    if days_old != "unknown" and version is not None:
        _write_manifest(mdf, path, built_at=datetime.now() - timedelta(days=days_old), version=version)
        return

    metadata = {}
    if days_old != "unknown":
        metadata[b"wpy_built_at"] = (datetime.now() - timedelta(days=days_old)).isoformat().encode()
    if version is not None:
        metadata[b"wpy_version"] = version.encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(mdf, preserve_index=False).replace_schema_metadata(metadata)
    pyarrow.feather.write_feather(table, path)


# --- Tests: Retry Behaviour ---


@pytest.mark.parametrize("transient_outcome, num_logged_responses", [
    (httpx.ConnectError("Connection refused"), 1),
    (httpx.ReadTimeout("Timed out"), 1),
    (httpx.RemoteProtocolError("Server disconnected without sending a response"), 1),
    (503, 2),
    (429, 2),
], ids=["connect_error", "read_timeout", "remote_protocol_error", "503", "429"])
def test_query_retries_transient_errors(
    monkeypatch, no_backoff_sleep, transient_outcome, num_logged_responses
):
    """
    Network errors, 5xx and 429 responses must be retried until the query
    succeeds. The API query log records every server *response* (including
    retried ones), but not requests that failed before a response arrived.
    """
    calls = []
    outcomes = [transient_outcome, 200]
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler(outcomes, calls))

    assert _query_metadata_api(FAKE_API_URL) == FAKE_PAYLOAD
    assert calls == [FAKE_API_URL] * 2
    assert api_query_log.get_count() == num_logged_responses


def test_query_gives_up_after_persistent_errors(monkeypatch, no_backoff_sleep):
    """
    If the server keeps failing, the query gives up after the maximum number
    of attempts and raises an APIRequestError that chains the last HTTP error.
    """
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler([503] * 3, calls))

    with pytest.raises(APIRequestError) as exc:
        _query_metadata_api(FAKE_API_URL)

    assert len(calls) == 3
    assert isinstance(exc.value.__cause__, httpx.HTTPStatusError)
    assert exc.value.__cause__.response.status_code == 503


def test_query_does_not_retry_fatal_client_errors(monkeypatch, no_backoff_sleep):
    """
    A 404 is final: no retries, and an APIRequestError straight away.
    """
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler([404], calls))

    with pytest.raises(APIRequestError) as exc:
        _query_metadata_api(FAKE_API_URL)

    assert len(calls) == 1
    assert exc.value.__cause__.response.status_code == 404


def test_query_does_not_retry_invalid_json(monkeypatch, no_backoff_sleep):
    """
    A response that is not valid JSON will not improve on retry, so the query
    fails after a single attempt.
    """
    calls = []
    outcomes = [b"<html>Maintenance</html>"]
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler(outcomes, calls))

    with pytest.raises(APIRequestError) as exc:
        _query_metadata_api(FAKE_API_URL)

    assert len(calls) == 1
    assert isinstance(exc.value.__cause__, ValueError)  # JSONDecodeError


# --- Tests: Redirect Handling ---


def test_query_follows_redirects(monkeypatch, no_backoff_sleep):
    """
    Metadata API queries must follow redirects.
    """
    calls = []
    moved_url = FAKE_API_URL + "/moved"
    outcomes = [httpx.Response(302, headers={"Location": moved_url}), 200]
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler(outcomes, calls))

    # Check `calls` first: on failure, it shows whether the redirect was ignored
    result = _query_metadata_api(FAKE_API_URL)
    assert calls == [FAKE_API_URL, moved_url]
    assert result == FAKE_PAYLOAD


def test_query_does_not_retry_redirect_loop(monkeypatch):
    """
    A redirect loop is final: retrying cannot fix it, so the query fails
    after a single attempt.
    """
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _fake_redirect_loop_handler(calls))

    # Record backoff's sleeps instead of skipping them: one attempt means
    # backoff never sleeps. (This avoids pinning httpx's redirect limit,
    # which determines how many requests a single attempt makes.)
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)

    with pytest.raises(APIRequestError) as exc:
        _query_metadata_api(FAKE_API_URL)

    assert sleeps == []
    assert isinstance(exc.value.__cause__, httpx.TooManyRedirects)
    assert set(calls) == {FAKE_API_URL}


# --- Tests: Crawler ---


def test_traverse_recovers_from_transient_error(monkeypatch, no_backoff_sleep):
    """
    A transient error while crawling a Branch Node must not cost the manifest
    that branch: after a retry, the crawl still returns the child Leaf Node
    (rather than skipping the branch with an empty result).
    """
    calls = []
    branch_payload = {"data": [{"alias": "wpgp", "name": "Unconstrained"}]}
    leaf_payload = {"data": [{"id": "1", "iso3": "AFG"}]}
    outcomes = [503, branch_payload, leaf_payload]
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler(outcomes, calls))

    report = ManifestBuildReport()
    leaf_nodes = _traverse_api_node("pop", node_name="Population", report=report)

    assert [node["api_path"] for node in leaf_nodes] == ["pop/wpgp"]
    assert report.failed_branches == []
    assert calls == [
        f"{METADATA_API_URL}/pop",
        f"{METADATA_API_URL}/pop",
        f"{METADATA_API_URL}/pop/wpgp",
    ]


# --- Tests: Build Summary ---


@pytest.mark.parametrize("branch_failure", [APIRequestError("HTTP 503"), None], ids=["api_error", "null_body"])
def test_build_reports_failed_branches_and_series(
    monkeypatch, isolated_manifest_assets, caplog, branch_failure
):
    """
    A build that loses a branch (API error, or a JSON `null` body) and a data
    series still saves the rest, but the returned report and the logged
    summary name what is missing.
    """
    api_tree = {**FAKE_API_TREE, f"{METADATA_API_URL}/pop/bad_branch": branch_failure}
    _patch_build_backend(monkeypatch, api_tree, rows_per_series=3)

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.failed_branches == ["pop/bad_branch"]
    assert report.failed_series == ["pop/bad_series"]
    assert (report.num_series, report.num_files) == (2, 3)
    assert report.saved_to is not None and report.saved_to.is_file()
    assert not report.is_complete
    # The "- " prefix is only used by the summary (not by the per-failure log lines)
    assert "INCOMPLETE" in caplog.text
    assert "- pop/bad_branch" in caplog.text and "- pop/bad_series" in caplog.text


def test_build_reports_complete_manifest(monkeypatch, isolated_manifest_assets, caplog):
    """
    A build without failures is reported as complete. A later call without
    `force_rebuild` finds the fresh manifest and builds nothing; with
    `force_rebuild`, it builds again.
    """
    clean_branch = {"data": [{"alias": "ok", "name": "Works"}]}
    _patch_build_backend(monkeypatch, {**FAKE_API_TREE, f"{METADATA_API_URL}/pop": clean_branch})

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.is_complete
    assert (report.num_series, report.num_files) == (1, 1)
    assert "1 raster file from" in caplog.text and "No failures" in caplog.text
    assert build_raw_manifest_from_api(force_rebuild=False) is None
    assert build_raw_manifest_from_api(force_rebuild=True).is_complete


def test_build_skips_listed_api_paths(monkeypatch, isolated_manifest_assets, caplog):
    """
    API paths in SKIPPED_API_PATHS are not requested and do not make the build
    incomplete: here a top-level branch, and a nested series whose listed
    alias has a leading space (as WorldPop's ' MERIT_Elevation_200').
    """
    monkeypatch.setitem(manifest_builder.SKIPPED_API_PATHS, "pop/broken", "not reachable (test)")
    api_tree = {  # requesting any other URL raises a KeyError
        METADATA_API_URL: {"data": [
            {"alias": "pop", "name": "Population"}, {"alias": "age_structures", "name": "Age"},
        ]},
        f"{METADATA_API_URL}/pop": {"data": [
            {"alias": "ok", "name": "Works"}, {"alias": " broken", "name": "Broken"},
        ]},
        f"{METADATA_API_URL}/pop/ok": {"data": [{"id": "1", "iso3": "AFG"}]},
    }
    _patch_build_backend(monkeypatch, api_tree)

    with caplog.at_level(logging.INFO, logger="worldpoppy.manifest_builder"):
        report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.is_complete
    assert (report.num_series, report.num_files) == (1, 1)
    assert "Skipping API path pop/broken" in caplog.text
    assert "Skipping API path age_structures" in caplog.text


@pytest.mark.parametrize("tree_override, rows_per_series, expected_error", [
    ({METADATA_API_URL: APIRequestError("HTTP 503")}, 1, "HTTP 503"),
    (
        {
            f"{METADATA_API_URL}/pop": {"data": [{"alias": "global", "name": "Global"}]},
            f"{METADATA_API_URL}/pop/global": {"data": [{"id": "9", "iso3": "WCD"}]},
        },
        1,
        "no supported data series",
    ),
    ({}, 0, "returned no data"),
], ids=["api_root_unreachable", "only_global_series", "no_rows"])
def test_build_reports_fatal_error(
    monkeypatch, isolated_manifest_assets, caplog, tree_override, rows_per_series, expected_error
):
    """
    If a build cannot produce a manifest, it is aborted, nothing is saved,
    and both the report and the logged summary say why.
    """
    _patch_build_backend(monkeypatch, {**FAKE_API_TREE, **tree_override}, rows_per_series)

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.saved_to is None
    assert not report.is_complete
    assert expected_error in report.fatal_error
    assert not list(isolated_manifest_assets.glob("*.feather"))
    assert "aborted" in caplog.text and expected_error in caplog.text


def test_build_reports_save_failure(monkeypatch, isolated_manifest_assets):
    """
    If the manifest cannot be written to disk, the report says so.
    """
    _patch_build_backend(monkeypatch, FAKE_API_TREE)

    def failing_write(*args, **kwargs):
        raise PermissionError("read-only file system")

    monkeypatch.setattr(manifest_builder, "_write_manifest", failing_write)

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.saved_to is None
    assert "failed to save" in report.fatal_error


def test_build_summary_caps_long_path_lists():
    """
    With many failures, the summary lists only the first few paths per kind
    (the report itself keeps all of them).
    """
    failed = [f"pop/series_{i}" for i in range(ManifestBuildReport.MAX_PATHS_IN_SUMMARY + 5)]
    report = ManifestBuildReport(num_series=100, failed_series=failed, saved_to=Path("manifest.feather"))

    summary = report.summary()

    assert "- pop/series_0" in summary
    assert f"- {failed[-1]}" not in summary
    assert "... and 5 more" in summary


# --- Tests: Series Metadata ---


@pytest.mark.parametrize("api_path, expect_citation", [
    ("pop/wpgp", True),
    ("pop/cic2020_100m", False),
], ids=["series-citation-stored", "country-specific-citation-left-out"])
def test_processing_a_series_stores_its_citation_only_where_it_applies(monkeypatch, api_path, expect_citation):
    """
    Processing a data series stores the sample dataset's citation and DOI in
    every row of the series, except for series whose citation differs by
    country (COUNTRY_SPECIFIC_CITATION_SERIES): their rows have neither.
    """
    url = "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/maxar_v1/AGO/ago_ppp_2020_constrained.tif"
    sample_details = {
        "id": "1", "title": "The spatial distribution of population in 2020, Angola", "desc": "People per cell.",
        "popyear": "2020", "citation": "WorldPop (2020). 51 countries.", "doi": "10.5258/SOTON/WP00682",
        "files": [url], "license": "https://hub.worldpop.org/data/licence.txt",
        "url_summary": "https://hub.worldpop.org/geodata/summary?id=1", "category": "Constrained",
        "project": "Population Counts", "source": "WorldPop",
    }
    monkeypatch.setattr(manifest_builder, "_query_metadata_api", lambda request_url: {"data": [sample_details]})
    coverage_index = [
        {"id": "1", "iso3": "AGO", "popyear": "2020", "title": "Angola"},
        {"id": "2", "iso3": "LUX", "popyear": "2020", "title": "Luxembourg"},
    ]

    rows = manifest_builder._process_leaf_node(api_path, "Some series", coverage_index)

    assert len(rows) == 2
    for row in rows:
        assert (row["citation"], row["doi"]) == (
            ("WorldPop (2020). 51 countries.", "10.5258/SOTON/WP00682") if expect_citation else (None, None)
        )


# --- Tests: URL Check ---


def test_fetch_global1_file_list_returns_the_raster_paths(monkeypatch):
    """
    The Global 1 File List is downloaded from WorldPop's data server, and the
    paths of its raster files are returned. Rows without a path are skipped.
    """
    csv_text = (
        '"ID","ISO","ISO3","Country","Covariate","PathToRaster","Description"\n'
        '1,4,"AFG","Afghanistan","ppp_2000","GIS/Population/Global_2000_2020/2000/AFG/afg_ppp_2000.tif","People"\n'
        '2,4,"AFG","Afghanistan","ppp_2001",,"No path"\n'
        '3,4,"AFG","Afghanistan","dmsp_100m_2001","GIS/Covariates/Global_2000_2020/AFG/DMSP/afg_dmsp_100m_2001.tif","Lights"\n'
    )
    calls = []
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler([csv_text.encode()], calls))

    assert _fetch_global1_file_list() == [
        "GIS/Population/Global_2000_2020/2000/AFG/afg_ppp_2000.tif",
        "GIS/Covariates/Global_2000_2020/AFG/DMSP/afg_dmsp_100m_2001.tif",
    ]
    assert calls == [GLOBAL1_FILE_LIST_URL]


def test_build_corrects_urls_with_the_global1_file_list(monkeypatch, isolated_manifest_assets, caplog):
    """
    A build checks the inferred URLs against the Global 1 File List: a URL
    whose file is listed under another path is saved corrected, with the
    result of the check in the manifest column 'url_check'. The report counts
    the results per data series, and its summary names the corrections.
    """
    clean_branch = {"data": [{"alias": "ok", "name": "Works"}]}
    api_tree = {**FAKE_API_TREE, f"{METADATA_API_URL}/pop": clean_branch}
    _patch_build_backend(monkeypatch, api_tree, rows_per_series=2, file_list=[MOVED_FILE_PATH])

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.is_complete
    assert report.url_check_counts == {"pop/ok": {"corrected": 2}}
    saved = pd.read_feather(report.saved_to)
    assert list(saved["url_check"]) == ["corrected", "corrected"]
    assert set(saved["remote_path"]) == {f"{DATA_SERVER_URL}/{MOVED_FILE_PATH.removeprefix('GIS/')}"}
    assert "2 corrected" in caplog.text and "- pop/ok: 2 corrected" in caplog.text


@pytest.mark.parametrize("file_list, served_csv, expected_error", [
    (httpx.ConnectError("Connection refused"), None, "Connection refused"),
    (None, b'"ID","Path"\n1,"GIS/x.tif"\n', "PathToRaster"),
    ([FAKE_FILE_PATH, None], None, "NoneType"),
], ids=["download_fails", "column_missing", "unusable_entry"])
def test_build_without_usable_global1_file_list_is_incomplete(
    monkeypatch, isolated_manifest_assets, caplog, file_list, served_csv, expected_error
):
    """
    If the Global 1 File List cannot be downloaded or used (e.g. because its
    format changed), the build does not stop: it counts as incomplete, does
    not replace an existing manifest, and the summary says why.
    """
    from worldpoppy import manifest_loader

    _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=300)
    clean_branch = {"data": [{"alias": "ok", "name": "Works"}]}
    api_tree = {**FAKE_API_TREE, f"{METADATA_API_URL}/pop": clean_branch}
    _patch_build_backend(monkeypatch, api_tree, file_list=file_list)
    if served_csv is not None:
        monkeypatch.setattr(httpx.HTTPTransport, "handle_request", _scripted_handler([served_csv], []))

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert expected_error in report.url_check_error
    assert report.kept_existing and report.saved_to is None and not report.is_complete
    assert manifest_loader._get_active_manifest_info().source == "bundled"
    assert "URL check failed" in caplog.text and "NOT saved" in caplog.text


def test_build_without_global1_file_list_saves_unchecked_urls_if_no_manifest_exists(
    monkeypatch, isolated_manifest_assets
):
    """
    Without any existing manifest, a build whose URLs could not be checked is
    still saved (as other incomplete builds are), with every URL marked as
    not checked.
    """
    clean_branch = {"data": [{"alias": "ok", "name": "Works"}]}
    api_tree = {**FAKE_API_TREE, f"{METADATA_API_URL}/pop": clean_branch}
    _patch_build_backend(monkeypatch, api_tree, file_list=httpx.ConnectError("Connection refused"))

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.saved_to is not None and not report.is_complete
    assert list(pd.read_feather(report.saved_to)["url_check"]) == ["not_checked"]


# --- Tests: Manifest Files ---


def test_manifest_metadata_round_trip(tmp_path):
    """
    Build time and version are embedded in the manifest file itself, readable
    without loading the data, and the data still loads as usual.
    """
    path = tmp_path / "manifest.feather"
    built_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    _write_manifest(pd.DataFrame({"iso3": ["AFG", "ALB"]}), path, built_at=built_at, version="0.4.9")

    info = _read_manifest_info(path, source="user")

    assert (info.built_at, info.version, info.source) == (built_at, "0.4.9", "user")
    assert list(pd.read_feather(path)["iso3"]) == ["AFG", "ALB"]
    assert not list(tmp_path.glob("*.tmp"))  # written atomically, no leftovers


def test_manifest_build_time_is_recorded_in_utc(tmp_path):
    """
    A new manifest records its build time in UTC, with the time zone (before
    0.5.0: the local time of the building machine, without a time zone).
    """
    path = tmp_path / "manifest.feather"
    before = datetime.now(timezone.utc)
    _write_manifest(pd.DataFrame({"iso3": ["AFG"]}), path)

    built_at = _read_manifest_info(path, source="user").built_at

    assert built_at.utcoffset() == timedelta(0)
    assert before <= built_at <= datetime.now(timezone.utc)


def test_build_times_without_time_zone_are_read_as_local_time(isolated_manifest_assets):
    """
    Build times without a time zone (manifests built before 0.5.0) are read as
    local time, so that they can be compared with new build times in UTC: the
    newer of an old bundled manifest and a new user manifest is selected.
    """
    _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=10)  # old style: no time zone
    _write_manifest(pd.DataFrame({"iso3": ["AFG"]}), get_user_manifest_path())  # new style: UTC

    bundled = _read_manifest_info(manifest_builder.BUNDLED_MANIFEST_PATH, source="bundled")
    assert bundled.built_at.tzinfo is not None
    assert abs((datetime.now(timezone.utc) - bundled.built_at) - timedelta(days=10)) < timedelta(minutes=1)
    assert _select_manifest().source == "user"


def test_manifest_stores_series_texts_dictionary_encoded(tmp_path):
    """
    Licence, citation and DOI, which repeat in every row of a data series, are
    stored dictionary-encoded (each distinct text once), and load unchanged.
    """
    path = tmp_path / "manifest.feather"
    citation = "WorldPop (2018). Global High Resolution Population Denominators Project."
    mdf = pd.DataFrame({
        "iso3": ["AFG", "ALB", "DZA"],
        "license": ["https://hub.example/licence.txt"] * 3,
        "citation": [citation, citation, None],
        "doi": ["10.5258/SOTON/WP00645"] * 3,
    })

    _write_manifest(mdf, path)

    schema = pyarrow.feather.read_table(path).schema
    assert [pa.types.is_dictionary(schema.field(name).type) for name in ("iso3", "license", "citation", "doi")] == [
        False, True, True, True
    ]
    loaded = pd.read_feather(path)
    assert loaded["citation"].tolist()[:2] == [citation, citation] and pd.isna(loaded["citation"][2])
    assert loaded["doi"].tolist() == ["10.5258/SOTON/WP00645"] * 3


def test_failed_manifest_write_leaves_existing_manifest_intact(tmp_path, monkeypatch):
    """
    If writing a manifest fails halfway (e.g. disk full), the existing
    manifest is untouched: the new one is only moved into place when complete.
    """
    path = tmp_path / "manifest.feather"
    _write_manifest(pd.DataFrame({"iso3": ["AFG"]}), path)
    original = path.read_bytes()

    def failing_write_feather(table, target, **kwargs):
        Path(target).write_bytes(b"half-written garbage")
        raise OSError("No space left on device")

    monkeypatch.setattr(manifest_builder.pyarrow.feather, "write_feather", failing_write_feather)

    with pytest.raises(OSError):
        _write_manifest(pd.DataFrame({"iso3": ["ALB"]}), path)

    assert path.read_bytes() == original
    assert not list(tmp_path.glob("*.tmp"))


_SAME_MINOR_OTHER_PATCH = ".".join(__version__.split(".")[:2] + ["999"])


@pytest.mark.parametrize("bundled_age, user_age, user_version, expected", [
    (300, None, None, "bundled"),                         # no user update
    (300, 10, __version__, "user"),                       # newer user update
    (10, 300, __version__, "bundled"),                    # newer bundled manifest (e.g. after an upgrade)
    (300, 10, "0.1.0", "bundled"),                        # user update by an older, incompatible version
    (300, 10, "99.0.0", "bundled"),                       # user update by a newer version (downgrade)
    (300, 10, _SAME_MINOR_OTHER_PATCH, "user"),           # patch versions are compatible
    (300, 10, None, "bundled"),                           # user update without a recorded version
    (300, "unknown", __version__, "bundled"),             # user update without a build time
    ("unknown", 10, __version__, "user"),                 # bundled manifest of unknown age
    (None, 10, __version__, "user"),                      # no bundled manifest (dev setup)
    (None, None, None, None),                             # no manifest at all
], ids=[
    "bundled_only", "user_newer", "bundled_newer", "user_older_version", "user_newer_version",
    "user_other_patch", "user_no_version", "user_no_build_time", "bundled_unknown_age", "user_only", "none",
])
def test_select_manifest(isolated_manifest_assets, bundled_age, user_age, user_version, expected):
    """
    The loader uses the newer of the bundled and the user-updated manifest,
    but only user updates built by a compatible worldpoppy version.
    """
    if bundled_age is not None:
        _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=bundled_age)
    if user_age is not None:
        _write_fake_manifest(get_user_manifest_path(), days_old=user_age, version=user_version)

    info = _select_manifest()

    assert (info.source if info else None) == expected


def _write_manifest_with_malformed_build_time(path):
    """Write a manifest whose recorded build time cannot be parsed."""
    metadata = {b"wpy_built_at": b"yesterday", b"wpy_version": __version__.encode()}
    table = pa.Table.from_pandas(pd.DataFrame({"iso3": ["AFG"]})).replace_schema_metadata(metadata)
    pyarrow.feather.write_feather(table, path)


@pytest.mark.parametrize("write_user_manifest, expected_reason", [
    (lambda path: _write_fake_manifest(path, days_old=1, version="99.0.0"), "Upgrade worldpoppy"),
    (lambda path: path.write_text("not a manifest"), "unreadable"),
    (_write_manifest_with_malformed_build_time, "build time is unknown"),
], ids=["newer_version", "corrupt_file", "malformed_build_time"])
def test_select_manifest_explains_ignored_user_manifest(
    isolated_manifest_assets, caplog, write_user_manifest, expected_reason
):
    """
    A rebuilt manifest that cannot be used is ignored with a warning that says
    why, and the bundled manifest is used instead (without crashing).
    """
    _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=300)
    user_path = get_user_manifest_path()
    user_path.parent.mkdir(parents=True, exist_ok=True)
    write_user_manifest(user_path)

    assert _select_manifest().source == "bundled"
    assert expected_reason in caplog.text


def test_incomplete_rebuild_keeps_existing_manifest(monkeypatch, isolated_manifest_assets, caplog):
    """
    An incomplete rebuild must not shadow an existing manifest: it is not
    saved, the existing manifest stays in use, and the summary says so.
    (Without any existing manifest, an incomplete one is saved; see
    `test_build_reports_failed_branches_and_series`.)
    """
    from worldpoppy import manifest_loader

    _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=300)
    _patch_build_backend(monkeypatch, FAKE_API_TREE)  # includes a failing branch and series

    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.kept_existing and report.saved_to is None and not report.is_complete
    assert not get_user_manifest_path().exists()
    assert manifest_loader._get_active_manifest_info().source == "bundled"
    assert "NOT saved" in caplog.text and "- pop/bad_branch" in caplog.text


def test_select_manifest_reads_legacy_bundled_sidecar(isolated_manifest_assets):
    """
    A bundled manifest without embedded metadata (as shipped up to 0.4.x) gets
    its build time from the legacy sidecar file, so that a newer user update
    still wins and an older one does not.
    """
    bundled = manifest_builder.BUNDLED_MANIFEST_PATH
    bundled.parent.mkdir(parents=True)
    pd.DataFrame({"iso3": ["AFG"]}).to_feather(bundled)  # no worldpoppy metadata
    manifest_builder.BUNDLED_MANIFEST_TIMESTAMP_PATH.write_text((datetime.now() - timedelta(days=100)).isoformat())

    _write_fake_manifest(get_user_manifest_path(), days_old=200)
    assert _select_manifest().source == "bundled"

    _write_fake_manifest(get_user_manifest_path(), days_old=1)
    assert _select_manifest().source == "user"


def test_rebuild_refreshes_the_loaded_manifest(monkeypatch, isolated_manifest_assets):
    """
    A rebuild is saved to the cache directory and used by the running session
    right away, instead of the (older) bundled manifest loaded before.
    """
    from worldpoppy import manifest_loader

    _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=300)
    assert manifest_loader._get_active_manifest_info().source == "bundled"  # loaded and cached

    clean_branch = {"data": [{"alias": "ok", "name": "Works"}]}
    _patch_build_backend(monkeypatch, {**FAKE_API_TREE, f"{METADATA_API_URL}/pop": clean_branch})
    report = build_raw_manifest_from_api(force_rebuild=True)

    assert report.is_complete and report.saved_to == get_user_manifest_path()
    assert manifest_loader._get_active_manifest_info().source == "user"
    assert list(manifest_loader._get_raw_manifest()["api_path"]) == ["pop/ok"]


def test_loader_rejects_manifest_with_missing_column(isolated_manifest_assets):
    """
    A manifest that lacks a column the builder writes (here: the summary
    URLs) is rejected with a clear error, rather than loaded with that column
    silently left empty.
    """
    from worldpoppy import manifest_loader
    from worldpoppy.manifest_builder import RAW_MANIFEST_COLUMNS
    from worldpoppy.manifest_loader import ManifestValidationError

    columns = [col for col in RAW_MANIFEST_COLUMNS if col != "summary_url"]
    mdf = pd.DataFrame({col: ["x"] for col in columns})
    _write_manifest(mdf, manifest_builder.BUNDLED_MANIFEST_PATH, built_at=datetime.now(), version=__version__)

    with pytest.raises(ManifestValidationError, match=r"lacks the column\(s\) \['summary_url'\]"):
        manifest_loader._get_cleaned_manifest()


def test_clear_manifest_caches_covers_all_cached_loader_functions(isolated_manifest_assets):
    """
    Every in-memory cache in the loader is cleared, so that a cached function
    added later cannot keep serving an outdated manifest.
    """
    from worldpoppy import manifest_loader

    _write_fake_manifest(manifest_builder.BUNDLED_MANIFEST_PATH, days_old=1)
    manifest_loader._get_active_manifest_info()  # fill at least one cache

    manifest_loader._clear_manifest_caches()

    cached = [f for f in vars(manifest_loader).values() if hasattr(f, "cache_info")]
    assert cached and all(f.cache_info().currsize == 0 for f in cached)


@pytest.mark.parametrize("age_days, expect_hint", [(400, True), (10, False)])
def test_stale_manifest_suggests_optional_update(isolated_manifest_assets, caplog, age_days, expect_hint):
    """
    An old manifest (older than the threshold of 365 days) is kept as is (no
    build). The log says so and points expert users to the rebuild function,
    asking them to use it sparingly. A recent manifest stays quiet.
    """
    _write_fake_manifest(get_user_manifest_path(), days_old=age_days)

    assert build_raw_manifest_from_api(force_rebuild=False) is None

    assert (f"built {age_days} days ago" in caplog.text) is expect_hint
    if expect_hint:
        assert "worldpoppy.manifest_builder.build_raw_manifest_from_api" in caplog.text
        assert "sparingly" in caplog.text
