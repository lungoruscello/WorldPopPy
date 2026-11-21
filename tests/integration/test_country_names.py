"""
Integration Tests for Looking Up Country Codes by Name.

Rationale:
    The unit tests in `tests/unit/test_country_names.py` verify the lookup
    logic against a few synthetic names. These tests check the *real* names:
    the ISO names from `pycountry`, the alias table in
    `worldpoppy.country_names`, and the countries listed in the bundled data
    manifest. In particular, they guard against aliases that accidentally
    make a name ambiguous, or that point to unsupported countries.

Note on Terminology:
    This is classified as an **Integration Test** because it uses real
    country names and the real (bundled) data manifest. No internet needed.
"""
from collections import defaultdict

import pycountry
import pytest

from tests.helpers import needs_raw_manifest

# Names users commonly type, incl. outdated and colloquial ones
COMMON_NAMES = {
    "Kenya": "KEN",
    "Turkey": "TUR",
    "Türkiye": "TUR",
    "Ivory Coast": "CIV",
    "Cote d'Ivoire": "CIV",
    "East Timor": "TLS",
    "UK": "GBR",
    "Great Britain": "GBR",
    "United Kingdom": "GBR",
    "Iran": "IRN",
    "Islamic Republic of Iran": "IRN",
    "Venezuela": "VEN",
    "Bolivarian Republic of Venezuela": "VEN",
    "Russia": "RUS",
    "Vietnam": "VNM",
    "South Korea": "KOR",
    "North Korea": "PRK",
    "India": "IND",
    "Oman": "OMN",
    "Niger": "NER",
    "China": "CHN",
    "Taiwan": "TWN",
    "United States": "USA",
    "US": "USA",
    "Czech Republic": "CZE",
    "Swaziland": "SWZ",
    "Burma": "MMR",
    "Kosovo": "KOS",
    "DR Congo": "COD",
    "Democratic Republic of the Congo": "COD",
    "Republic of the Congo": "COG",
}


@pytest.mark.integration
@needs_raw_manifest
@pytest.mark.parametrize("name, expected", COMMON_NAMES.items())
def test_common_names_resolve(name, expected):
    """
    Common country names, incl. outdated and colloquial ones, resolve to the
    right country code.
    """
    from worldpoppy.country_names import resolve_country_name

    assert resolve_country_name(name) == expected


@pytest.mark.integration
@needs_raw_manifest
def test_only_congo_is_ambiguous():
    """
    'Congo' is the only name (or alias) that exactly matches several
    countries. Any other collision (e.g. from a new alias, or from renamed
    countries in a new `pycountry` release) would make that name fail to
    resolve, or would point to an unexpected country, so it must be reviewed
    (and this test updated) deliberately.
    """
    from worldpoppy.country_names import _country_name_table

    table = _country_name_table()
    codes_by_key = defaultdict(set)
    for row in table.itertuples():
        codes_by_key[row.key].add(row.iso3)
    ambiguous = {key: codes for key, codes in codes_by_key.items() if len(codes) > 1}

    assert ambiguous == {"congo": {"COD", "COG"}}


@pytest.mark.integration
@needs_raw_manifest
def test_every_supported_country_has_a_name():
    """
    Every country in the data manifest has a proper name (not just its code),
    incl. countries that ISO 3166 lacks (e.g. Kosovo).
    """
    from worldpoppy.country_names import find_country_code
    from worldpoppy.manifest_loader import get_all_isos

    all_countries = find_country_code()

    assert sorted(all_countries.iso3) == get_all_isos()
    assert not (all_countries.name == all_countries.iso3).any()


@pytest.mark.integration
@needs_raw_manifest
def test_name_tables_only_list_supported_countries():
    """
    Every alias belongs to a country in the data manifest (otherwise it would
    be silently ignored), and every non-ISO name is still needed (`pycountry`
    lacks the code).
    """
    from worldpoppy.country_names import COUNTRY_NAME_ALIASES, NON_ISO_COUNTRY_NAMES
    from worldpoppy.manifest_loader import get_all_isos

    supported = set(get_all_isos())

    assert set(COUNTRY_NAME_ALIASES) <= supported
    assert set(NON_ISO_COUNTRY_NAMES) <= supported
    assert not any(pycountry.countries.get(alpha_3=iso3) for iso3 in NON_ISO_COUNTRY_NAMES)


@pytest.mark.integration
@needs_raw_manifest
@pytest.mark.parametrize("query, expected", [
    ("turk", {"TUR", "TKM", "TCA"}),
    ("ivory", {"CIV"}),
    ("britain", {"GBR"}),
    ("bolivarian", {"VEN"}),
    ("korea", {"KOR", "PRK"}),
])
def test_partial_names_find_the_country(query, expected):
    """
    Searching by part of a name finds the country, incl. via its long ISO
    name ('bolivarian') or an alias ('ivory').
    """
    from worldpoppy.country_names import find_country_code

    assert set(find_country_code(query).iso3) == expected
