import httpx
import pytest

from worldpoppy.config import METADATA_API_URL
from worldpoppy.manifest_builder import APIRequestError, _query_metadata_api, _traverse_api_node
from worldpoppy.tracking import api_query_log

FAKE_API_URL = "https://fake.api/rest/data/pop"
FAKE_PAYLOAD = {"data": [{"alias": "wpgp", "name": "Unconstrained"}]}


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
#   - a ready-made `httpx.Response` (e.g., a redirect).

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

    leaf_nodes = _traverse_api_node("pop", node_name="Population")

    assert [node["api_path"] for node in leaf_nodes] == ["pop/wpgp"]
    assert calls == [
        f"{METADATA_API_URL}/pop",
        f"{METADATA_API_URL}/pop",
        f"{METADATA_API_URL}/pop/wpgp",
    ]
