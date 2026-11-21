import pandas as pd

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
