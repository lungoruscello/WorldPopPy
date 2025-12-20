import importlib
import logging

import pandas as pd
import pytest

from tests.test_utils import needs_raw_manifest


# --- Fixtures ---

@pytest.fixture
def augmented_raw_manifest(tmp_path, monkeypatch):
    """
    Loads the REAL raw manifest, appends a set of fake 'unknown' dataset entries
    (multi-year AND multi-band), saves it to a temp file, and monkeypatches
    the config to point to it.

    Note: This fixture assumes the file exists because the test using it
    is guarded by @needs_raw_manifest.
    """
    from worldpoppy import config

    # 1. Load Real Data
    real_df = pd.read_feather(config.RAW_MANIFEST_CACHE_PATH)

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

    # 4. Point config to the temp file
    monkeypatch.setattr(config, "RAW_MANIFEST_CACHE_PATH", temp_manifest_path)

    return fake_product_name


# --- Tests ---

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
