"""
This module maps country names to the three-letter country codes (ISO 3166-1
alpha-3) that `worldpoppy` uses to identify countries.

Country names come from `pycountry` (official ISO 3166 names, incl. their
long and common forms), complemented by a small set of common English
aliases (e.g., 'Turkey', 'Ivory Coast'). Only countries for which WorldPop
offers data are considered.

Main methods
------------------------
    - :func:`find_country_code`
        Search the country codes of all supported countries by (part of)
        their name.
"""

import logging
import re
import unicodedata

import pandas as pd
import pycountry

from worldpoppy.manifest_loader import get_all_isos

logger = logging.getLogger(__name__)

__all__ = [
    "find_country_code",
]

# Common English names that ISO 3166 lacks, e.g., because a country was
# renamed or is usually known by a shorter name. Note that 'Congo' is a common
# name for both the Democratic Republic of the Congo (COD) and the Republic of
# the Congo (COG), so it is ambiguous on purpose. Three-letter aliases (e.g.,
# 'DRC') only help the search, since `wp_raster` reads any three-letter AoI
# string as a country code.
COUNTRY_NAME_ALIASES = {
    "ARE": ["UAE"],
    "BHS": ["The Bahamas"],
    "BRN": ["Brunei"],
    "CCK": ["Cocos Islands", "Keeling Islands"],
    "CIV": ["Ivory Coast"],
    "COD": ["Congo", "DR Congo", "DRC", "Democratic Republic of the Congo", "Congo-Kinshasa"],
    "COG": ["Congo-Brazzaville"],
    "CPV": ["Cape Verde"],
    "FLK": ["Falkland Islands", "Malvinas"],
    "FSM": ["Micronesia"],
    "GBR": ["UK", "Great Britain", "Britain"],
    "GMB": ["The Gambia"],
    "MAF": ["Saint Martin"],
    "MKD": ["Macedonia"],
    "MMR": ["Burma"],
    "NLD": ["Holland"],
    "PSE": ["Palestine"],
    "RUS": ["Russia"],
    "SHN": ["Saint Helena"],
    "SWZ": ["Swaziland"],
    "SXM": ["Sint Maarten"],
    "TLS": ["East Timor"],
    "TUR": ["Turkey"],
    "USA": ["US", "U.S.", "U.S.A."],
    "VAT": ["Vatican", "Vatican City"],
    "VIR": ["US Virgin Islands", "U.S. Virgin Islands"],
}

# Names for codes that WorldPop uses but ISO 3166 (and thus `pycountry`) lacks
NON_ISO_COUNTRY_NAMES = {
    "KOS": "Kosovo",
    "SPR": "Spratly Islands",
}


def find_country_code(query=None):
    """
    Search the three-letter country codes (ISO 3166-1 alpha-3) of all
    countries supported by `worldpoppy`, by (part of) their name.

    The search is case- and accent-insensitive and ignores punctuation. It
    covers the official ISO names in their short, long and common forms, plus
    common English aliases. For example, 'turk' finds Türkiye (TUR), and
    'ivory' finds Côte d'Ivoire (CIV).

    Parameters
    ----------
    query : str, optional
        (Part of) a country name. If None (default) or empty, all supported
        countries are listed.

    Returns
    -------
    pandas.DataFrame
        One row per matching country, with columns 'iso3' (the country code),
        'name' (the country's common name), and 'matched_name' (the name or
        alias that matched the query). Exact matches come first, then all
        other matches in alphabetical order of their names.

    Examples
    --------
    >>> from worldpoppy import find_country_code
    >>> find_country_code('congo')
      iso3                                   name matched_name
    0  COG                                  Congo        Congo
    1  COD  Congo, The Democratic Republic of the        Congo
    """
    return _search_country_names(_country_name_table(), query)


def resolve_country_name(name):
    """
    Return the country code for a country name, but only if the name
    unambiguously denotes one supported country.

    The name must match one of the country's names or aliases exactly (up to
    case, accents and punctuation). Each conversion is reported with a
    warning, so that users can see how the name was interpreted.

    Parameters
    ----------
    name : str
        A country name, e.g., 'Kenya' or 'Ivory Coast'.

    Returns
    -------
    str
        The three-letter country code.

    Raises
    ------
    ValueError
        If the name matches no country exactly, or several countries. The
        error message lists all countries whose names contain `name`.
    TypeError
        If `name` is not a string.
    """
    if not isinstance(name, str):
        raise TypeError(f"Country codes and names must be strings, got: {name!r}.")

    key = _normalise_name(name)
    if not key:
        raise ValueError(f"{name!r} is neither a country code nor a country name.")

    table = _country_name_table()
    exact_matches = table.loc[table['key'] == key, ['iso3', 'name']]
    exact_matches = exact_matches.drop_duplicates('iso3')

    if len(exact_matches) == 1:
        iso3, full_name = exact_matches.iloc[0]
        logger.warning(
            f"Interpreting {name!r} as country code '{iso3}' ({full_name}). "
            f"Pass '{iso3}' to silence this message."
        )
        return iso3

    if len(exact_matches) > 1:
        raise ValueError(
            f"{name!r} is ambiguous. It can refer to several countries:\n"
            f"{_format_candidates(exact_matches)}\n"
            "Please pass the three-letter country code of the one you mean."
        )

    candidates = _search_country_names(table, name)
    if candidates.empty:
        raise ValueError(
            f"{name!r} is neither a three-letter country code nor a known country "
            "name. To search country codes by (part of) a name, use "
            "`worldpoppy.find_country_code`, e.g., `find_country_code('turk')`. "
            "(To pass a vector file instead, use a path with a file suffix, or a "
            "`pathlib.Path`.)"
        )
    raise ValueError(
        f"{name!r} is neither a three-letter country code nor a known country "
        f"name. Countries whose names contain {name!r}:\n"
        f"{_format_candidates(candidates)}\n"
        "Please pass the three-letter country code of the one you mean."
    )


def _country_name_table():
    """
    Return a table of all names and aliases of the supported countries, with
    one row per (country, name) pair and the columns 'iso3', 'name' (the
    country's common name, used for display), 'variant' (the name or alias),
    and 'key' (the normalised variant used for matching).
    """
    names_by_iso3 = {}
    for iso3 in get_all_isos():
        country = pycountry.countries.get(alpha_3=iso3)
        if country is None:
            names = [NON_ISO_COUNTRY_NAMES.get(iso3, iso3)]
        else:
            # The common name (if any) comes first, as it is the display name
            names = [
                getattr(country, attr) for attr in ('common_name', 'name', 'official_name')
                if getattr(country, attr, None)
            ]
        names_by_iso3[iso3] = names + COUNTRY_NAME_ALIASES.get(iso3, [])

    return _build_name_table(names_by_iso3)


def _build_name_table(names_by_iso3):
    """
    Build the country name table (see `_country_name_table`) from a mapping
    of country codes to their names. The first name of each country is its
    display name.
    """
    rows = [
        (iso3, names[0], variant, _normalise_name(variant))
        for iso3, names in names_by_iso3.items()
        for variant in dict.fromkeys(names)  # drop duplicates, keep order
    ]
    return pd.DataFrame(rows, columns=['iso3', 'name', 'variant', 'key'])


def _search_country_names(table, query):
    """
    Return one row per country with a name or alias that contains the query
    (see `find_country_code`).
    """
    if query is not None and not isinstance(query, str):
        raise TypeError(f"The search query must be a string, got: {query!r}.")

    key = _normalise_name(query) if query is not None else ''
    if not key:
        result = table.drop_duplicates('iso3').assign(matched_name=None)
        return _tidy_search_result(result.sort_values('name', key=_normalise_names))

    hits = table[table['key'].str.contains(key, regex=False)].copy()

    # Report the best-matching name per country: an exact match if there is
    # one, otherwise the shortest name containing the query
    hits['is_exact'] = hits['key'] == key
    hits['key_len'] = hits['key'].str.len()
    hits = hits.sort_values(['is_exact', 'key_len'], ascending=[False, True])
    hits = hits.drop_duplicates('iso3').rename(columns={'variant': 'matched_name'})

    hits = hits.sort_values(['is_exact', 'name'], ascending=[False, True], key=_normalise_names)
    return _tidy_search_result(hits)


def _tidy_search_result(df):
    """Select and order the public columns of a search result."""
    return df[['iso3', 'name', 'matched_name']].reset_index(drop=True)


def _normalise_name(name):
    """
    Normalise a country name for matching: remove accents, lower the case,
    and replace punctuation with single spaces (e.g., "Côte d'Ivoire" ->
    'cote d ivoire').
    """
    decomposed = unicodedata.normalize('NFKD', name)
    without_accents = ''.join(c for c in decomposed if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9]+', ' ', without_accents.lower()).strip()


def _normalise_names(series):
    """
    Normalise a Series of names for sorting (other columns pass through
    unchanged). Used as `key` in `DataFrame.sort_values`.
    """
    return series.map(_normalise_name) if series.name == 'name' else series


def _format_candidates(df):
    """Format countries as indented 'ISO3 (name)' lines for error messages."""
    return "\n".join(f"  {row.iso3} ({row.name})" for row in df.itertuples())
