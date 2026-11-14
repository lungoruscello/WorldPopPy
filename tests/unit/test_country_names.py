"""
Unit tests for looking up country codes by country name.

All tests use the synthetic country names of the `synthetic_country_names`
fixture (see `tests/unit/conftest.py`). The real names (from `pycountry` and
the alias table) are tested in `tests/integration/test_country_names.py`.
"""
import logging

import pytest

from worldpoppy.country_names import find_country_code, resolve_country_name


# --- resolve_country_name ---

@pytest.mark.parametrize("name, expected", [
    ("Kenya", "KEN"),
    ("  KENYA ", "KEN"),
    ("cote d'ivoire", "CIV"),
    ("Côte d’Ivoire", "CIV"),  # typographic apostrophe
    ("Ivory Coast", "CIV"),  # alias
    ("turkey", "TUR"),  # alias
    ("St. Lucia", "LCA"),
    ("Oman", "OMN"),  # also contained in 'Romania'
    ("Guinea", "GIN"),  # also contained in 'Equatorial Guinea'
])
def test_resolve_exact_name(synthetic_country_names, caplog, name, expected):
    """
    A name that exactly matches one country's name or alias (ignoring case,
    accents, punctuation and surrounding whitespace) resolves to its code,
    even if it also occurs *within* other countries' names. Each conversion
    is reported with a warning.
    """
    with caplog.at_level(logging.WARNING):
        iso3 = resolve_country_name(name)

    assert iso3 == expected
    assert f"Interpreting {name!r} as country code '{expected}'" in caplog.text
    assert f"Pass '{expected}' to silence this message" in caplog.text


def test_resolve_warning_names_the_country(synthetic_country_names, caplog):
    """
    The warning shows the country's display name, so that users can see what
    an alias stands for.
    """
    with caplog.at_level(logging.WARNING):
        resolve_country_name("turkey")

    assert "'TUR' (Türkiye)" in caplog.text


def test_resolve_ambiguous_name_raises(synthetic_country_names):
    """
    A name that exactly matches several countries ('Congo') is not resolved;
    the error lists all of them.
    """
    with pytest.raises(ValueError, match="'Congo' is ambiguous") as exc_info:
        resolve_country_name("Congo")

    assert "COD (Congo, The Democratic Republic of the)" in str(exc_info.value)
    assert "COG (Congo)" in str(exc_info.value)


@pytest.mark.parametrize("name, candidates", [
    ("man", ["OMN (Oman)", "ROU (Romania)"]),
    ("Lucia", ["LCA (Saint Lucia)"]),  # a single candidate is not resolved either
])
def test_resolve_partial_name_raises_with_candidates(synthetic_country_names, name, candidates):
    """
    A name that only occurs within other names is never resolved (no
    guessing), even if only one country's name contains it; the error lists
    the countries whose names contain it.
    """
    with pytest.raises(ValueError, match=f"names contain '{name}'") as exc_info:
        resolve_country_name(name)

    for candidate in candidates:
        assert candidate in str(exc_info.value)


def test_resolve_unknown_name_points_to_search(synthetic_country_names):
    """
    A name without any match raises an error that points to the search.
    """
    with pytest.raises(ValueError, match="find_country_code"):
        resolve_country_name("Atlantis")


@pytest.mark.parametrize("name, error, match", [
    ("", ValueError, "nor a country name"),
    ("  ", ValueError, "nor a country name"),
    ("!", ValueError, "nor a country name"),
    (42, TypeError, "must be strings"),
])
def test_resolve_invalid_name_raises(synthetic_country_names, name, error, match):
    """
    Empty names (incl. names without any letters or digits) and non-strings
    raise clear errors. (An empty name must not be searched, since every
    country name contains it.)
    """
    with pytest.raises(error, match=match):
        resolve_country_name(name)


# --- find_country_code ---

@pytest.mark.parametrize("query", [None, "", " "])
def test_find_lists_all_countries_by_name(synthetic_country_names, query):
    """
    Without a query, all countries are listed once each, in alphabetical
    order of their names (with accents sorted like plain letters).
    """
    result = find_country_code(query)

    assert list(result.columns) == ["iso3", "name", "matched_name"]
    assert list(result.iso3) == [
        "ALA", "COG", "COD", "CIV", "GNQ", "GIN", "KEN", "OMN", "ROU", "LCA", "TUR"
    ]


def test_find_lists_exact_matches_first(synthetic_country_names):
    """
    Countries matching the query exactly come first, even if another match
    comes first alphabetically.
    """
    result = find_country_code("guinea")

    assert list(result.iso3) == ["GIN", "GNQ"]


def test_find_reports_matched_alias(synthetic_country_names):
    """
    The result shows which name or alias matched, next to the country's
    display name; partial queries work as well.
    """
    result = find_country_code("IVORY")

    assert result.to_dict("records") == [
        {"iso3": "CIV", "name": "Côte d'Ivoire", "matched_name": "Ivory Coast"}
    ]


def test_find_without_match_is_empty(synthetic_country_names):
    assert find_country_code("Atlantis").empty


def test_find_rejects_non_string_query(synthetic_country_names):
    with pytest.raises(TypeError, match="must be a string"):
        find_country_code(["Kenya"])
