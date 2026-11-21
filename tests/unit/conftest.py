import socket

import pytest

# Synthetic country names (first name = display name). Real codes, but a
# deliberately small set that contains the cases the name lookup must handle:
# accents and punctuation, aliases, a name that is also an alias of another
# country ('Congo'), names within other names ('Oman' in 'Romania', 'Guinea'
# in 'Equatorial Guinea'), an abbreviation with a dot ('St. Lucia'), and a
# name that only sorts correctly without its accent ('Åland Islands').
SYNTHETIC_COUNTRY_NAMES = {
    "ALA": ["Åland Islands"],
    "CIV": ["Côte d'Ivoire", "Ivory Coast"],
    "COD": ["Congo, The Democratic Republic of the", "Congo"],
    "COG": ["Congo"],
    "GIN": ["Guinea"],
    "GNQ": ["Equatorial Guinea"],
    "KEN": ["Kenya"],
    "LCA": ["Saint Lucia", "St. Lucia"],
    "OMN": ["Oman"],
    "ROU": ["Romania"],
    "TUR": ["Türkiye", "Turkey"],
}


# --- Fixtures ---

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """
    Fixture that blocks all network access (DNS lookups and connections)
    in unit tests.

    Unit tests must fake every external service. This guard ensures that a
    fake which fails to intercept (e.g. due to a wrong patch target) makes
    the test fail immediately, rather than sending real traffic to WorldPop
    or Nominatim. (`pytest.fail` raises a BaseException, so the library's own
    `except Exception` blocks cannot swallow it.)
    """
    def _blocked(*args, **kwargs):
        pytest.fail(f"Unit tests must not access the network (attempted: {args!r}).")

    monkeypatch.setattr(socket, "getaddrinfo", _blocked)
    monkeypatch.setattr(socket.socket, "connect", _blocked)


@pytest.fixture(autouse=True)
def no_user_cache(monkeypatch, tmp_path):
    """
    Fixture that points the cache directory to an empty temporary directory
    in unit tests.

    Unit tests must never read from or write to the user's cache (e.g. lazy
    merges write VRT files there). Fixtures that need a cache of their own
    (e.g. `isolated_raster_cache`) set the variable again, which takes
    precedence.
    """
    monkeypatch.setenv("WORLDPOPPY_CACHE_DIR", str(tmp_path / "unit_test_cache"))


@pytest.fixture
def synthetic_country_names(monkeypatch):
    """
    Fixture that replaces the real country names (from `pycountry`, the alias
    table, and the data manifest) with SYNTHETIC_COUNTRY_NAMES.
    """
    import worldpoppy.country_names as country_names

    table = country_names._build_name_table(SYNTHETIC_COUNTRY_NAMES)
    monkeypatch.setattr(country_names, "_country_name_table", lambda: table)
