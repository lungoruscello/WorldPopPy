import httpx
import pytest
from geopy.exc import (
    GeocoderInsufficientPrivileges,
    GeocoderQueryError,
    GeocoderRateLimited,
    GeocoderTimedOut,
    GeocoderUnavailable,
)
from geopy.location import Location
from matplotlib.figure import Figure

import worldpoppy.func_utils as func_utils
from worldpoppy.func_utils import (
    NominatimSearchEmptyError,
    geolocate_name,
    is_fatal_http_error,
    standardise_iso3_codes,
)
from worldpoppy.plot_utils import plot_location_markers

FAKE_URL = "https://fake.url/afg.tif"
FAKE_PLACE = "Nairobi, Kenya"
FAKE_LON_LAT = (36.82, -1.29)


# --- Fixtures ---

@pytest.fixture
def clear_geocoding_cache():
    """
    Clear the geocoding cache, so that every test actually reaches the
    (fake) Nominatim service.
    """
    func_utils._nominatim_query.cache_clear()
    yield
    func_utils._nominatim_query.cache_clear()


# --- Helpers ---

def _status_error(status_code):
    """Return the `HTTPStatusError` that `raise_for_status` raises for a status code."""
    request = httpx.Request("GET", FAKE_URL)
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)


def _scripted_nominatim(outcomes, calls):
    """
    Return a fake `Nominatim` class whose `geocode` serves a scripted sequence
    of outcomes, one per call. Each outcome is either an exception to raise,
    None (no hit), or a (lon, lat) tuple for a successful hit.
    """
    class FakeNominatim:
        def __init__(self, *args, **kwargs):
            pass

        def geocode(self, query):
            calls.append(query)
            # Fail loudly if a test makes more calls than scripted. (`pytest.fail`
            # raises a BaseException, so `geolocate_name` cannot swallow it.)
            if not outcomes:
                pytest.fail("Fake Nominatim received more queries than scripted.")
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if outcome is None:
                return None
            lon, lat = outcome
            return Location(query, (lat, lon), {})

    return FakeNominatim


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


# --- Tests: Geocoding Retries ---

@pytest.mark.parametrize("transient_error", [
    GeocoderTimedOut("Service timed out"),
    GeocoderUnavailable("Service not available"),
    GeocoderRateLimited("Too many requests"),
], ids=lambda e: type(e).__name__)
def test_geocoding_retries_transient_errors(
    monkeypatch, no_backoff_sleep, clear_geocoding_cache, transient_error
):
    """
    Timeouts, connection problems and rate limiting must be retried until the
    lookup succeeds.
    """
    calls = []
    outcomes = [transient_error, FAKE_LON_LAT]
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim(outcomes, calls))

    lon, lat = geolocate_name(FAKE_PLACE)

    assert (lon, lat) == pytest.approx(FAKE_LON_LAT)
    assert calls == [FAKE_PLACE] * 2


def test_geocoding_gives_up_after_persistent_errors(
    monkeypatch, no_backoff_sleep, clear_geocoding_cache
):
    """
    If Nominatim keeps timing out, the lookup gives up after the maximum
    number of attempts and raises a NominatimSearchEmptyError that chains
    the last geopy error.
    """
    calls = []
    outcomes = [GeocoderTimedOut("Service timed out")] * 5
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim(outcomes, calls))

    with pytest.raises(NominatimSearchEmptyError) as exc:
        geolocate_name(FAKE_PLACE)

    assert len(calls) == 5
    assert isinstance(exc.value.__cause__, GeocoderTimedOut)


@pytest.mark.parametrize("fatal_error", [
    GeocoderInsufficientPrivileges("HTTP 403: blocked user agent"),
    GeocoderQueryError("HTTP 400: malformed query"),
], ids=lambda e: type(e).__name__)
def test_geocoding_does_not_retry_fatal_errors(
    monkeypatch, no_backoff_sleep, clear_geocoding_cache, fatal_error
):
    """
    Errors that a retry cannot fix fail after a single attempt.
    """
    calls = []
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim([fatal_error], calls))

    with pytest.raises(NominatimSearchEmptyError) as exc:
        geolocate_name(FAKE_PLACE)

    assert len(calls) == 1
    assert exc.value.__cause__ is fatal_error


def test_geocoding_does_not_retry_empty_result(
    monkeypatch, no_backoff_sleep, clear_geocoding_cache
):
    """
    "No hit" is a valid answer, not a failure: it is reported straight away.
    """
    calls = []
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim([None], calls))

    with pytest.raises(NominatimSearchEmptyError, match="No hit"):
        geolocate_name(FAKE_PLACE)

    assert len(calls) == 1


# --- Tests: Geocoding Cache ---

def test_geocoding_caches_successful_lookups(monkeypatch, clear_geocoding_cache):
    """
    A successful lookup is cached, so repeating it sends no new query to
    Nominatim. (Its usage policy asks clients to avoid repeat queries.)
    """
    calls = []
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim([FAKE_LON_LAT], calls))

    first = geolocate_name(FAKE_PLACE)
    second = geolocate_name(FAKE_PLACE)

    assert first == second
    assert calls == [FAKE_PLACE]


def test_geocoding_does_not_cache_failures(monkeypatch, no_backoff_sleep, clear_geocoding_cache):
    """
    A failed lookup is not cached (`lru_cache` does not store exceptions), so
    the next call queries Nominatim again and can succeed.
    """
    calls = []
    outcomes = [GeocoderInsufficientPrivileges("HTTP 403"), FAKE_LON_LAT]
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim(outcomes, calls))

    with pytest.raises(NominatimSearchEmptyError):
        geolocate_name(FAKE_PLACE)
    lon, lat = geolocate_name(FAKE_PLACE)

    assert (lon, lat) == pytest.approx(FAKE_LON_LAT)
    assert calls == [FAKE_PLACE] * 2


# --- Tests: Location Markers ---

def test_location_markers_skip_failed_lookups(
    monkeypatch, caplog, no_backoff_sleep, clear_geocoding_cache
):
    """
    If a place name cannot be geolocated (here: persistent timeouts), the
    marker is skipped with a warning that names the cause, rather than
    crashing the plot.
    """
    calls = []
    outcomes = [GeocoderTimedOut("Service timed out")] * 5
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim(outcomes, calls))
    ax = Figure().add_subplot()

    plot_location_markers([FAKE_PLACE], ax=ax)

    assert len(calls) == 5
    assert len(ax.collections) == 0  # no marker was drawn

    # Check the warning from `plot_utils` itself (backoff's own "Giving up"
    # message also mentions the cause, so `caplog.text` alone is not enough)
    [warning] = [r.getMessage() for r in caplog.records if r.name == "worldpoppy.plot_utils"]
    assert "Skipping plotting" in warning
    assert "Service timed out" in warning


# --- Tests: ISO3 Codes ---

@pytest.mark.parametrize("iso3_codes, expected", [
    ("THA", ["THA"]),
    ("tha", ["THA"]),
    (" Tha \n", ["THA"]),
    (["tha", "KHM", " lao"], ["THA", "KHM", "LAO"]),
    (("vnm",), ["VNM"]),
    ([], []),
])
def test_standardise_iso3_codes(iso3_codes, expected):
    """
    ISO3 codes are returned as a list of upper-case strings without
    surrounding whitespace, whether passed as a single string or several.
    """
    assert standardise_iso3_codes(iso3_codes) == expected


def test_standardise_iso3_codes_rejects_non_strings():
    """
    Non-string codes raise a clear TypeError instead of an AttributeError.
    """
    with pytest.raises(TypeError, match="must be strings"):
        standardise_iso3_codes(["THA", 764])


@pytest.mark.parametrize("iso3_codes", [None, b"THA"], ids=["None", "bytes"])
def test_standardise_iso3_codes_rejects_non_iterables_of_strings(iso3_codes):
    """
    None and bytes raise a clear TypeError (bytes would otherwise be iterated
    as integers, giving a confusing message).
    """
    with pytest.raises(TypeError, match="string or an iterable of strings"):
        standardise_iso3_codes(iso3_codes)
