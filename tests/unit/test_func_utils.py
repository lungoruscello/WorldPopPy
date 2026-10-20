import httpx
import pytest

from worldpoppy.func_utils import is_fatal_http_error

FAKE_URL = "https://fake.url/afg.tif"


# --- Helpers ---

def _status_error(status_code):
    """Return the `HTTPStatusError` that `raise_for_status` raises for a status code."""
    request = httpx.Request("GET", FAKE_URL)
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)


# --- Tests: Retry Rule ---

@pytest.mark.parametrize("status_code, is_fatal", [
    (399, False),
    (400, True),
    (404, True),
    (428, True),
    (429, False),  # Too Many Requests: worth retrying
    (430, True),
    (499, True),
    (500, False),
    (599, False),
])
def test_is_fatal_http_error_status_codes(status_code, is_fatal):
    """
    4xx client errors are fatal, except 429. All other status codes are not.
    """
    assert is_fatal_http_error(_status_error(status_code)) is is_fatal


@pytest.mark.parametrize("error, is_fatal", [
    (httpx.TooManyRedirects("Exceeded maximum allowed redirects."), True),
    (httpx.UnsupportedProtocol("Request URL has an unsupported protocol 'ftp://'."), True),
    (httpx.ConnectError("Connection refused"), False),
    (httpx.ReadTimeout("Timed out"), False),
    (httpx.RemoteProtocolError("Server disconnected without sending a response"), False),
    (httpx.DecodingError("Malformed gzip data"), False),
], ids=lambda x: type(x).__name__ if isinstance(x, Exception) else None)
def test_is_fatal_http_error_exception_types(error, is_fatal):
    """
    Redirect loops and unsupported URL schemes are fatal. Network errors,
    timeouts and other transport problems are worth retrying.
    """
    assert is_fatal_http_error(error) is is_fatal
