import importlib
import logging
import sys

import pandas as pd
import pytest

from tests.helpers import needs_raw_manifest


# Text columns kept compact in the cached manifest (see `_get_raw_manifest`)
CATEGORICAL_COLUMNS = {
    'api_series_desc', 'api_series_category', 'api_source', 'citation', 'doi', 'license'
}


def _as_plain_text(mdf):
    """Return `mdf` with every categorical column converted to plain text."""
    categorical = [c for c in mdf.columns if isinstance(mdf[c].dtype, pd.CategoricalDtype)]
    return mdf.astype({c: mdf[c].cat.categories.dtype for c in categorical})


# --- Fixtures ---

@pytest.fixture(scope="module")
def cleaned_manifests():
    """
    The cleaned manifest as worldpoppy caches it, and a reference built from
    the same file with every text column read as plain text (no categories).
    """
    import worldpoppy.manifest_loader as ml

    ml._clear_manifest_caches()  # earlier tests may have cached another manifest
    path = ml._get_active_manifest_info().path
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ml, "_get_raw_manifest", lambda: _as_plain_text(pd.read_feather(path)))
        reference = ml._get_cleaned_manifest.__wrapped__()
    ml._clear_manifest_caches()
    actual = ml._get_cleaned_manifest()

    categorical = [c for c in reference.columns if isinstance(reference[c].dtype, pd.CategoricalDtype)]
    assert not categorical, (
        f"the plain-text reference has categorical columns {categorical}: the manifest must be "
        "encoded in `_get_raw_manifest`, which this fixture replaces"
    )
    return actual, reference


@pytest.fixture
def augmented_raw_manifest(tmp_path, monkeypatch):
    """
    Loads the REAL raw manifest, appends a set of fake 'unknown' dataset entries
    (multi-year AND multi-band), saves it to a temp file, and points the
    bundled-manifest path to it. The cache directory is redirected to an empty
    temp directory, so that no user-updated manifest takes precedence.

    Note: This fixture assumes the file exists because the test using it
    is guarded by @needs_raw_manifest.
    """
    from worldpoppy import config, manifest_builder

    # 1. Load Real Data
    real_df = pd.read_feather(config.BUNDLED_MANIFEST_PATH)

    # 2. Generate Fake Data (Combinatorial)
    fake_product_name = "alien_life_g2"
    years = [2025, 2026]
    bands = ["klingon_pop", "vulcan_pop"]

    fake_rows = []

    for year in years:
        for band in bands:
            # We must ensure dataset_name is unique per file, just like in reality
            filename_stem = f"alien_{year}_{band}"

            fake_rows.append(
                {
                    "wpy_id": f"999_{year}_{band}",
                    "iso3": "USA",
                    "dataset_name": filename_stem,
                    "remote_path": f"https://fake.url/{filename_stem}.tif",
                    "api_entry_title": f"Alien Life Detection {band} {year}",
                    "api_path": f"alien/{fake_product_name}",
                    "year": year,
                    "band": band,
                    "remote_name": f"{filename_stem}.tif",
                    "api_series_desc": "Detecting aliens",
                    "api_series_category": "Extraterrestrial",
                    "api_source": "NASA",
                    "api_project": "X-Files",
                    "summary_url": None,
                }
            )

    # 3. Append and Save
    augmented_df = pd.concat([real_df, pd.DataFrame(fake_rows)], ignore_index=True)

    temp_manifest_path = tmp_path / "augmented_manifest.feather"
    augmented_df.to_feather(temp_manifest_path, compression="zstd")

    # 4. Use the temp file as the bundled manifest (and ignore any user update)
    monkeypatch.setattr(manifest_builder, "BUNDLED_MANIFEST_PATH", temp_manifest_path)
    monkeypatch.setenv("WORLDPOPPY_CACHE_DIR", str(tmp_path / "cache"))

    return fake_product_name


# --- Tests ---

@pytest.mark.integration
@needs_raw_manifest
def test_bundled_manifest_has_expected_schema():
    """
    The bundled raw manifest has exactly the columns that the manifest builder
    writes (the dataset records, plus the result of the URL Check), and every
    column except 'year' (empty for static products) and 'band' (empty for
    single-band datasets) is filled in for all datasets, incl. the download and
    summary URLs.
    """
    from worldpoppy.config import BUNDLED_MANIFEST_PATH
    from worldpoppy.manifest_builder import RAW_MANIFEST_COLUMNS, URL_CHECK_COLUMN

    raw_mdf = pd.read_feather(BUNDLED_MANIFEST_PATH)
    empty_counts = raw_mdf.drop(columns=["year", "band"]).isna().sum()

    assert tuple(raw_mdf.columns) == RAW_MANIFEST_COLUMNS + (URL_CHECK_COLUMN,)
    assert not empty_counts.any(), f"Empty values per column:\n{empty_counts[empty_counts > 0]}"


@pytest.mark.integration
@needs_raw_manifest
def test_bundled_manifest_has_checked_global1_urls():
    """
    The bundled manifest was built with the URL Check: the download URL of
    Luxembourg's 2020 constrained population (`pop_g1_alt`) points to the
    folder 'BSGM', where WorldPop stores the file (corrected from the inferred
    'maxar_v1'; bug 1.41), while Rwanda's stays in 'maxar_v1'. No Global 1 URL
    is left unchecked. The user-facing manifest does not show the results of
    the URL Check.
    """
    from worldpoppy import wp_manifest
    from worldpoppy.config import BUNDLED_MANIFEST_PATH

    raw_mdf = pd.read_feather(BUNDLED_MANIFEST_PATH)
    global1 = raw_mdf["remote_path"].str.contains("/Global_2000_2020")
    mdf = wp_manifest(product_name="pop_g1_alt", iso3_codes=["LUX", "RWA"]).set_index("iso3")

    assert not (raw_mdf.loc[global1, "url_check"] == "not_checked").any()
    assert "/2020/BSGM/LUX/" in mdf.loc["LUX", "remote_path"]
    assert "/2020/maxar_v1/RWA/" in mdf.loc["RWA", "remote_path"]
    assert "url_check" not in wp_manifest().columns


@pytest.mark.integration
@needs_raw_manifest
def test_warns_on_new_unmapped_dataset(augmented_raw_manifest, caplog):
    """
    Verify that warnings are raised when encountering new datasets in
    the raw manifest file for which config entries are not yet present
    in the product_definitions.toml file.
    """
    from worldpoppy import manifest_loader

    target_unmapped_product = augmented_raw_manifest

    # Execution: Reload and Run
    # We must reload manifest_loader so it re-reads the (monkeypatched) file
    # and re-runs the `_check_missing_config` logic.
    importlib.reload(manifest_loader)

    caplog.clear()

    # Trigger loading logic.
    # Even though we query for 'pop_g1', the loader scans the whole file
    # on initialisation (via _get_cleaned_manifest), which is when the
    # warning should trigger.
    try:
        manifest_loader.wp_manifest_constrained('pop_g1', iso3_codes='USA', years=2020)
    except Exception:
        # We do not care if the query fails (e.g. if pop_g1 is not there),
        # we only care about the load-time warning.
        pass

    # Assertions
    warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]

    # We expect the warning to mention the unknown product AND the config file
    found_warning = any(
        target_unmapped_product in msg and "product_definitions.toml" in msg
        for msg in warnings
    )

    assert found_warning, (
        f"Loader failed to warn about new unmapped product '{target_unmapped_product}' "
        f"which we injected into the manifest.\nCaptured warnings: {warnings}"
    )


@pytest.mark.integration
@needs_raw_manifest
def test_wp_manifest_constrained_raises():
    """
    Verify that `wp_manifest_constrained` correctly enforces data constraints
    and raises descriptive errors for invalid inputs.
    """
    from worldpoppy import  manifest_loader

    # Ensure we are using a clean loader state
    # (safer due to the use of lru_cache in manifest_loader.py)
    importlib.reload(manifest_loader)

    from worldpoppy.manifest_loader import wp_manifest_constrained

    # Case 1: 'Year-less' static product improperly requested with a year
    # Note: 'admin0' is a standard static product in WorldPop
    with pytest.raises(ValueError, match="`years` must be None"):
        wp_manifest_constrained('admin0', iso3_codes='CHE', years=2000)

    # Case 2: 'Year-mapped' static product linked to wrong year
    # requested with the WRONG year.
    with pytest.raises(ValueError, match="only available for year 2007"):
        wp_manifest_constrained('merit_slope_g2', iso3_codes='CHE', years=2000)

    # Case 3: Completely unknown product
    with pytest.raises(ValueError, match="Product 'non_existent_product_xyz' not found"):
        wp_manifest_constrained('non_existent_product_xyz', iso3_codes='CHE', years=2020)

    # Case 4: Valid Multi-Year product requested for an unavailable year
    with pytest.raises(ValueError, match="not available for all requested years"):
        wp_manifest_constrained('pop_g1', iso3_codes='CHE', years=1800)


@pytest.mark.integration
@needs_raw_manifest
def test_manifest_years_keywords_integration():
    """
    Verify that 'first', 'last', and 'all' keywords work as expected.
    """
    from worldpoppy.manifest_loader import wp_manifest_constrained, get_product_info

    # --- 1. Test on a Multi-Year Product ---
    product = 'pop_g1'

    # Get ground truth
    available_years = get_product_info(product)['years']
    min_year = min(available_years)
    max_year = max(available_years)

    # Query with 'all'
    df_all = wp_manifest_constrained(product, iso3_codes='VNM', years='all')
    assert len(df_all) == len(available_years)
    assert set(df_all['year']) == available_years

    # Query with 'first'
    df_first = wp_manifest_constrained(product, iso3_codes='VNM', years='first')
    assert len(df_first) == 1
    assert df_first.iloc[0]['year'] == min_year

    # Query with 'last'
    df_last = wp_manifest_constrained(product, iso3_codes='VNM', years='last')
    assert len(df_last) == 1
    assert df_last.iloc[0]['year'] == max_year

    # --- 2. Test Error on Static, "Yearless" Product ---
    static_product = 'admin0'

    with pytest.raises(ValueError, match="not linked to any year"):
        wp_manifest_constrained(static_product, iso3_codes='VNM', years='first')


@pytest.mark.integration
@needs_raw_manifest
def test_manifest_accepts_lowercase_iso3_codes():
    """
    Verify that `wp_manifest` and `wp_manifest_constrained` accept country
    codes in any case (and with surrounding whitespace).
    """
    from worldpoppy.manifest_loader import wp_manifest, wp_manifest_constrained

    upper = wp_manifest(product_name='pop_g1', iso3_codes='LUX')
    lower = wp_manifest(product_name='pop_g1', iso3_codes=' lux')
    assert len(upper) > 0
    pd.testing.assert_frame_equal(lower, upper)

    constrained = wp_manifest_constrained('pop_g1', iso3_codes=['lux', 'bel'], years=2020)
    assert sorted(constrained['iso3']) == ['BEL', 'LUX']

    # Validation also sees the normalised codes
    with pytest.raises(ValueError, match="XXX"):
        wp_manifest(iso3_codes='xxx')


@pytest.mark.integration
@needs_raw_manifest
def test_show_supported_data_products_console_hides_summary_urls(capsys, monkeypatch):
    """
    Verify that the console overview lists the matching products without
    their summary URLs (the 'Example' column is shown only in notebooks,
    in debug mode).
    """
    from worldpoppy.manifest_loader import show_supported_data_products, wp_manifest

    monkeypatch.delitem(sys.modules, 'ipykernel', raising=False)  # console, not notebook

    show_supported_data_products(iso3_codes='LUX')
    output = capsys.readouterr().out

    expected = wp_manifest(iso3_codes='LUX')
    missing = [name for name in expected['product_name'].unique() if name not in output]
    assert not missing, f"Products missing from the console overview: {missing[:3]}"
    assert 'Example' not in output
    shown_urls = [url for url in expected['summary_url'].dropna() if url in output]
    assert not shown_urls, f"Summary URLs shown in the console: {shown_urls[:3]}"


@pytest.mark.integration
@needs_raw_manifest
@pytest.mark.parametrize("call, product_info_cached", [
    (lambda ml: ml.wp_manifest('pop_g1'), True),
    (lambda ml: ml.wp_manifest(keywords='population'), True),
    (lambda ml: ml.wp_manifest_constrained('pop_g1', iso3_codes='LUX', years=2020), True),
    (lambda ml: ml.get_product_info('pop_g1'), False),
], ids=["wp_manifest", "wp_manifest-keywords", "wp_manifest_constrained", "get_product_info"])
def test_filtering_the_manifest_gives_back_working_memory(monkeypatch, call, product_info_cached):
    """
    Functions that filter the full manifest give the unused memory of
    pyarrow's pool back to the system (otherwise 120-160 MB per call stay
    reserved for the session; see `_releases_arrow_memory`).
    """
    import pyarrow as pa
    import worldpoppy.manifest_loader as ml

    ml._get_cleaned_manifest()  # loaded before counting (loading releases memory too)
    # Other functions call `get_product_info`; its cached result keeps it from
    # releasing memory on their behalf, so only the function under test counts
    ml.get_product_info.cache_clear()
    if product_info_cached:
        ml.get_product_info('pop_g1')
    released = []

    class CountingPool:
        def release_unused(self):
            released.append(True)

    monkeypatch.setattr(pa, "default_memory_pool", CountingPool)
    call(ml)

    assert released, "the unused memory of pyarrow's pool was not given back"


@pytest.mark.integration
@needs_raw_manifest
def test_cached_manifest_stores_repeated_text_compactly(cleaned_manifests):
    """
    The cached manifest stores the long, repeated text columns (e.g. the
    series descriptions) as categories, which saves about 130 MB, and holds
    exactly the data of a manifest read as plain text: same values, same row
    order, and the same types for all other columns.
    """
    actual, reference = cleaned_manifests

    categorical = {c for c in actual.columns if isinstance(actual[c].dtype, pd.CategoricalDtype)}
    assert categorical == CATEGORICAL_COLUMNS
    pd.testing.assert_frame_equal(_as_plain_text(actual), reference)


@pytest.mark.integration
@needs_raw_manifest
@pytest.mark.parametrize("call", [
    lambda ml: ml.wp_manifest(),
    lambda ml: ml.wp_manifest('pop_g1'),
    lambda ml: ml.wp_manifest('pop_g2_r25a', iso3_codes=['GHA', 'TGO'], years='last'),
    lambda ml: ml.wp_manifest(keywords=['population', 'global2']),
    lambda ml: ml.wp_manifest(iso3_codes='KEN', years=2020),
    lambda ml: ml.wp_manifest(static_only=True, iso3_codes='BRA'),
    lambda ml: ml.wp_manifest(keywords='no such product'),
    lambda ml: ml.wp_manifest_constrained('pop_g1', iso3_codes=['LUX', 'BEL'], years=[2000, 2020]),
    lambda ml: ml.get_product_info('ntl_viirs_g2'),
    lambda ml: ml.resolve_product_years('pop_g2_r25a', ['first', 'last']),
    lambda ml: ml.show_supported_data_products(),
    lambda ml: ml.show_supported_data_products(iso3_codes='LUX', keywords='population'),
], ids=[
    "wp_manifest", "wp_manifest-product", "wp_manifest-countries-last-year", "wp_manifest-keywords",
    "wp_manifest-country-year", "wp_manifest-static", "wp_manifest-no-match", "wp_manifest_constrained",
    "get_product_info", "resolve_product_years", "show_supported_data_products",
    "show_supported_data_products-filtered",
])
def test_public_functions_return_what_a_plain_text_manifest_gives(
    cleaned_manifests, monkeypatch, capsys, call
):
    """
    The compact storage of the cached manifest is invisible to users: the public
    functions that return manifest rows, or information drawn from them, return
    and print exactly what they return for a manifest read as plain text,
    including the column types (users get plain text, so that operations such as
    `value_counts` and assigning new values behave as for any text column). The
    calls leave the cached manifest compact.
    """
    import worldpoppy.manifest_loader as ml

    actual, reference = cleaned_manifests
    monkeypatch.delitem(sys.modules, 'ipykernel', raising=False)  # console output

    def run(manifest):
        ml._clear_manifest_caches()
        monkeypatch.setattr(ml, "_get_cleaned_manifest", lambda: manifest)
        result = call(ml)
        return result, capsys.readouterr().out

    expected, expected_out = run(reference)
    result, out = run(actual)

    if isinstance(expected, pd.DataFrame):
        pd.testing.assert_frame_equal(result, expected)
    else:
        assert result == expected
    assert out == expected_out
    still_categorical = {c for c in actual.columns if isinstance(actual[c].dtype, pd.CategoricalDtype)}
    assert still_categorical == CATEGORICAL_COLUMNS, "the call changed the cached manifest"
    ml._clear_manifest_caches()


@pytest.mark.integration
@needs_raw_manifest
@pytest.mark.parametrize("validator, invalid_values", [
    ("_validate_isos", ["XXX"]),
    ("_validate_years", [1066]),
    ("_validate_product_name", "pop_g2_1km"),
])
def test_validation_errors_suggest_working_imports(validator, invalid_values):
    """
    Verify that the code snippets suggested in validation errors actually
    run (they used to refer to a non-existent module).
    """
    from worldpoppy import manifest_loader

    with pytest.raises(ValueError) as exc:
        getattr(manifest_loader, validator)(invalid_values)

    snippet_lines = [
        line.removeprefix(">>> ") for line in str(exc.value).splitlines()
        if line.startswith(">>> ")
    ]
    assert snippet_lines, "Error message should suggest a code snippet."
    exec("\n".join(snippet_lines), {})  # must not raise
