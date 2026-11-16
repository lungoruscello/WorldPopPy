import warnings

import httpx
import matplotlib.pyplot as plt
import numpy as np
import pytest
import xarray as xr
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
from worldpoppy import __url__, __version__
from worldpoppy.func_utils import (
    NominatimSearchEmptyError,
    geolocate_name,
    is_fatal_http_error,
    standardise_iso3_codes,
)
from worldpoppy.plot_utils import clean_axes, plot_location_markers

FAKE_URL = "https://fake.url/afg.tif"
FAKE_PLACE = "Nairobi, Kenya"
FAKE_LON_LAT = (36.82, -1.29)


# --- Fixtures ---

@pytest.fixture
def reset_geocoding_state():
    """
    Clear the geocoding cache, so that every test actually reaches the (fake)
    Nominatim service, and reset the request throttle, so that no test waits
    for a request made by a previous test.
    """
    # Keep a reference to the real throttle: some tests replace it with a stub
    # via monkeypatch, which is only undone after this fixture's teardown
    throttle = func_utils._nominatim_throttle

    func_utils._nominatim_query.cache_clear()
    throttle.reset()
    yield
    func_utils._nominatim_query.cache_clear()
    throttle.reset()


# --- Helpers ---

def _status_error(status_code):
    """Return the `HTTPStatusError` that `raise_for_status` raises for a status code."""
    request = httpx.Request("GET", FAKE_URL)
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)


def _scripted_nominatim(outcomes, calls, init_kwargs=None):
    """
    Return a fake `Nominatim` class whose `geocode` serves a scripted sequence
    of outcomes, one per call. Each outcome is either an exception to raise,
    None (no hit), or a (lon, lat) tuple for a successful hit. If given, the
    `init_kwargs` list records the keyword arguments of every instantiation.
    """
    class FakeNominatim:
        def __init__(self, *args, **kwargs):
            if init_kwargs is not None:
                init_kwargs.append(kwargs)

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


class _FakeClock:
    """
    Stand-in for the `time` module in `func_utils`: `sleep` advances a fake
    clock instead of waiting, and every sleep is recorded.
    """

    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class _RecordingThrottle:
    """Stand-in for `_nominatim_throttle` that records how it is used."""

    def __init__(self):
        self.num_waits = 0
        self.deferrals = []

    def wait(self):
        self.num_waits += 1

    def defer(self, seconds):
        self.deferrals.append(seconds)


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
    monkeypatch, no_backoff_sleep, reset_geocoding_state, transient_error
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
    monkeypatch, no_backoff_sleep, reset_geocoding_state
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
    monkeypatch, no_backoff_sleep, reset_geocoding_state, fatal_error
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
    monkeypatch, no_backoff_sleep, reset_geocoding_state
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

def test_geocoding_caches_successful_lookups(monkeypatch, reset_geocoding_state):
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


def test_geocoding_does_not_cache_failures(monkeypatch, no_backoff_sleep, reset_geocoding_state):
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


# --- Tests: Nominatim Politeness ---

def test_geocoding_identifies_itself(monkeypatch, reset_geocoding_state):
    """
    Nominatim's usage policy requires a user agent that identifies the
    application (stock library user agents are not accepted).
    """
    calls, init_kwargs = [], []
    monkeypatch.setattr(
        func_utils, "Nominatim", _scripted_nominatim([FAKE_LON_LAT], calls, init_kwargs)
    )

    geolocate_name(FAKE_PLACE)

    [kwargs] = init_kwargs
    assert kwargs["user_agent"] == f"worldpoppy/{__version__} (+{__url__})"
    assert kwargs["timeout"] > 0


def test_request_throttle_spaces_requests(monkeypatch):
    """
    The throttle lets the first request through immediately, and spaces all
    further requests by at least the minimum interval.
    """
    clock = _FakeClock()
    monkeypatch.setattr(func_utils, "time", clock)
    throttle = func_utils._RequestThrottle(min_interval=1.0)

    request_times = []
    for _ in range(3):
        throttle.wait()
        request_times.append(clock.now)

    assert request_times[1] - request_times[0] >= 1.0
    assert request_times[2] - request_times[1] >= 1.0
    assert clock.sleeps == [1.0, 1.0]


@pytest.mark.parametrize("defer_seconds, min_elapsed", [
    (7.0, 7.0),  # longer than the minimum interval: wait as requested
    (0.2, 1.0),  # shorter: the minimum interval still applies
])
def test_request_throttle_honours_deferral(monkeypatch, defer_seconds, min_elapsed):
    """
    A deferral (e.g., from a `Retry-After` header) delays the next request by
    at least the requested time, but never shortens the minimum interval.
    """
    clock = _FakeClock()
    monkeypatch.setattr(func_utils, "time", clock)
    throttle = func_utils._RequestThrottle(min_interval=1.0)

    throttle.wait()
    start = clock.now
    throttle.defer(defer_seconds)
    throttle.wait()

    assert clock.now - start >= min_elapsed


def test_request_throttle_reset_allows_immediate_request(monkeypatch):
    """
    After a reset, the next request goes through without waiting.
    """
    clock = _FakeClock()
    monkeypatch.setattr(func_utils, "time", clock)
    throttle = func_utils._RequestThrottle(min_interval=1.0)

    throttle.wait()
    throttle.reset()
    throttle.wait()

    assert clock.sleeps == []


def test_geocoding_waits_for_throttle_before_every_request(
    monkeypatch, no_backoff_sleep, reset_geocoding_state
):
    """
    Every request to Nominatim, including retries, goes through the throttle.
    """
    calls = []
    outcomes = [GeocoderTimedOut("Service timed out"), FAKE_LON_LAT]
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim(outcomes, calls))
    throttle = _RecordingThrottle()
    monkeypatch.setattr(func_utils, "_nominatim_throttle", throttle)

    geolocate_name(FAKE_PLACE)

    assert len(calls) == 2
    assert throttle.num_waits == 2


@pytest.mark.parametrize("retry_after, expected_deferrals", [(7, [7]), (None, [])])
def test_geocoding_respects_retry_after(
    monkeypatch, no_backoff_sleep, reset_geocoding_state, retry_after, expected_deferrals
):
    """
    If Nominatim rate-limits a request and says how long to wait (HTTP 429
    with `Retry-After`), the throttle defers the next request accordingly.
    """
    calls = []
    outcomes = [GeocoderRateLimited("Too many requests", retry_after=retry_after), FAKE_LON_LAT]
    monkeypatch.setattr(func_utils, "Nominatim", _scripted_nominatim(outcomes, calls))
    throttle = _RecordingThrottle()
    monkeypatch.setattr(func_utils, "_nominatim_throttle", throttle)

    geolocate_name(FAKE_PLACE)

    assert len(calls) == 2
    assert throttle.deferrals == expected_deferrals


# --- Tests: Location Markers ---

def test_location_markers_skip_failed_lookups(
    monkeypatch, caplog, no_backoff_sleep, reset_geocoding_state
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


# --- Tests: Axis Cleaning ---

def test_clean_axes_cleans_every_panel_of_a_facet_grid():
    """
    An xarray FacetGrid (e.g. one panel per year) has every panel cleaned,
    without xarray's FutureWarning about its deprecated `axes` attribute.
    """
    da = xr.DataArray(
        np.arange(8.0).reshape(2, 2, 2), dims=("year", "y", "x"), coords={"year": [2015, 2023]}
    )
    grid = da.plot(col="year", add_colorbar=False)

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        clean_axes(grid)

    for ax in grid.axs.flat:
        assert len(ax.get_xticks()) == 0 and len(ax.get_yticks()) == 0
        assert ax.get_xlabel() == "" and ax.get_ylabel() == ""
    plt.close(grid.fig)


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
