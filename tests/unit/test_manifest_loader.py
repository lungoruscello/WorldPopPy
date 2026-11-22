import pandas as pd
import pytest

from worldpoppy import manifest_loader


# --- Tests: Product Notes ---

def test_build_final_notes_map(monkeypatch):
    """
    Verify that banded products take their note from the band description,
    non-banded products from their API slug, and that each product name
    appears once (also when it has rows for several countries).
    """
    monkeypatch.setattr(manifest_loader, "ESA_LAND_COVER_DESC_MAP", {"band_a": "Description of band A"})
    monkeypatch.setattr(manifest_loader, "PRODUCT_NOTES_MAP", {"slug_x": "Note for slug X"})

    mdf = pd.DataFrame({
        "product_name": ["lc_a", "lc_a", "prod_x", "prod_x", "prod_unmapped"],
        "band": ["band_a", "band_a", None, None, None],
        "band_alias": ["a", "a", None, None, None],
        "api_slug": ["lc", "lc", "slug_x", "slug_x", "slug_unmapped"],
        "iso3": ["AAA", "BBB", "AAA", "BBB", "AAA"],
        "api_series_desc": ["long text"] * 5,  # a column the notes do not need
    })

    notes = manifest_loader._build_final_notes_map(mdf)

    assert set(notes) == {"lc_a", "prod_x", "prod_unmapped"}
    assert notes["lc_a"] == "Description of band A"
    assert notes["prod_x"] == "Note for slug X"
    assert pd.isna(notes["prod_unmapped"])


# --- Tests: Compact Text Columns ---

def test_categorical_text_comes_back_as_plain_text_with_missing_values():
    """
    `_as_plain_text` turns categorical columns back into the original text, in
    the original data type, with missing values still missing (a rebuilt
    manifest can lack a citation, DOI or licence), and leaves its input unchanged.
    """
    plain = pd.DataFrame({
        "citation": pd.Series(["Citation A", None, "Citation B", "Citation A"]),
        "year": [2000, 2001, 2002, 2003],
    })
    compact = plain.astype({"citation": "category"})

    result = manifest_loader._as_plain_text(compact)

    pd.testing.assert_frame_equal(result, plain)
    assert isinstance(compact["citation"].dtype, pd.CategoricalDtype)


# --- Tests: Year Keywords ---

@pytest.fixture
def fake_year_manifest(monkeypatch):
    """
    Replace the cleaned manifest with a small synthetic one: a multi-year
    product 'prod_a' (2000, 2010, 2020) and a static product 'prod_s'.
    """
    mdf = pd.DataFrame({
        "product_name": ["prod_a", "prod_a", "prod_a", "prod_s"],
        "iso3": ["AAA", "AAA", "AAA", "AAA"],
        "year": pd.array([2000, 2010, 2020, None], dtype="Int64"),
        "multi_year": [True, True, True, False],
    })
    monkeypatch.setattr(manifest_loader, "_get_cleaned_manifest", lambda: mdf)
    monkeypatch.setattr(manifest_loader, "get_all_product_names", lambda: ["prod_a", "prod_s"])
    monkeypatch.setattr(manifest_loader, "get_all_years", lambda: [2000, 2010, 2020])


@pytest.mark.parametrize("years, expected", [
    ("last", [2020]),
    ([2010, "last"], [2010, 2020]),
    (["first", 2010], [2000, 2010]),
    ([2010], [2010]),
])
def test_wp_manifest_resolves_year_keywords_in_lists(fake_year_manifest, years, expected):
    """
    `wp_manifest` accepts 'first' and 'last' on their own and within a list
    of years, as `wp_raster` and `wp_manifest_constrained` do.
    """
    result = manifest_loader.wp_manifest(product_name="prod_a", years=years)

    assert sorted(result["year"]) == expected


@pytest.mark.parametrize("years", ["last", [2010, "last"]])
def test_wp_manifest_year_keywords_need_a_product(fake_year_manifest, years):
    """
    'first' and 'last' depend on the product, so they require `product_name`.
    """
    with pytest.raises(ValueError, match="relative year keyword 'last'"):
        manifest_loader.wp_manifest(years=years)


# --- Tests: Unknown Product Names ---

@pytest.mark.parametrize("typed, suggested", [
    # Clear suggestions
    ("pop_g2_1km", "'pop_g2_1km_r25a'"),  # a name starting with the text
    ("ntl_viirs", "'ntl_viirs_filtered_g2', 'ntl_viirs_g1' or 'ntl_viirs_g2'"),
    ("popg2_r25a", "'pop_g2_r25a', 'pop_g2_1km_r25a' or 'pop_g2_r24b'"),  # look alike (typo), closest first
    ("POPG2_R25A", "'pop_g2_r25a', 'pop_g2_1km_r25a' or 'pop_g2_r24b'"),  # case-insensitive
    # No suggestion rather than a doubtful one
    ("pop_g2", None),  # more than three names start with it
    ("te", None),  # too short (would find 'temp_mean_g2')
    ("temperature", None),  # a topic word, left to the keyword search
    ("landcover", None),  # nothing similar
    ("", None),
])
def test_unknown_product_names_get_suggestions_only_when_clear(monkeypatch, typed, suggested):
    """
    An unknown product name raises an error that shows how to search the
    products by keyword and, only for a clearly mistyped or shortened name,
    suggests the names meant: none for ambiguous, very short or other text.
    """
    names = [
        "pop_g2_1km_r25a", "pop_g2_r25a", "pop_g2_r24b", "pop_g2_alt", "temp_mean_g2",
        "ntl_viirs_g2", "ntl_viirs_filtered_g2", "ntl_dmsp_g1", "ntl_viirs_g1",
    ]
    monkeypatch.setattr(manifest_loader, "get_all_product_names", lambda: names)

    with pytest.raises(ValueError) as exc:
        manifest_loader._validate_product_name(typed)

    message = str(exc.value)
    assert message.startswith(f"Product '{typed}' not found.")
    if suggested:
        assert f"Did you mean {suggested}?" in message
    else:
        assert "Did you mean" not in message
    assert f">>> show_supported_data_products(keywords={typed!r})" in message
