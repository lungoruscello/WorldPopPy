import pandas as pd
import pytest


def test_infer_url_template():
    from worldpoppy.manifest_builder import _infer_download_url_template

    expected = 'Global_2000_2020/{year}/{iso3_upper}/{iso3_lower}_ppp_{year}.tif'
    actual = _infer_download_url_template('Global_2000_2020/2000/AUS/aus_ppp_2000.tif', 'AUS', 2000)
    assert actual == expected

    actual = _infer_download_url_template('Global_2000_2020/2020/AUS/aus_ppp_2020.tif', 'AUS', 2020)
    assert actual == expected

    expected = 'Global_2015_2030/{year}/{iso3_upper}/{iso3_lower}_ppp_{year}.tif'
    actual = _infer_download_url_template('Global_2015_2030/2015/AUS/aus_ppp_2015.tif', 'AUS', 2015)
    assert actual == expected

    actual = _infer_download_url_template('Global_2015_2030/2030/AUS/aus_ppp_2030.tif', 'AUS', 2030)
    assert actual == expected



def test_validate_url_template_raises_on_missing_placeholder():
    import pytest
    from worldpoppy.manifest_builder import _validate_download_url_template

    # Valid: all required placeholders present (no exception)
    _validate_download_url_template('{year}/{iso3_upper}/{iso3_lower}_{year}.tif', 'pop/x', ['{year}'])

    # Invalid: e.g. WorldPop changed its file naming, so the year was not found
    with pytest.raises(ValueError, match="missing required placeholder"):
        _validate_download_url_template('2020/{iso3_upper}/{iso3_lower}.tif', 'pop/x', ['{year}'])


def test_dataset_record_has_raw_manifest_columns():
    """
    Each manifest record has exactly the columns that the manifest loader
    requires (`RAW_MANIFEST_COLUMNS`), incl. a filled-in summary URL and the
    series' licence, citation and DOI.
    """
    from worldpoppy.manifest_builder import RAW_MANIFEST_COLUMNS, _build_dataset_record

    record = _build_dataset_record(
        coverage_entry={"id": "7", "iso3": "AFG", "popyear": "2020", "title": "Population 2020"},
        download_url_template="https://data.example/{year}/{iso3_lower}_ppp_{year}.tif",
        summary_url_template="https://hub.example/summary?id={id}",
        id_for_summary="42",  # e.g. the country-level entry of a multi-year series
        api_path="pop/wpgp",
        series_metadata={"license": "https://hub.example/licence.txt", "citation": "WorldPop (2018).",
                         "doi": "10.5258/SOTON/WP00645"},
        node_name="Unconstrained",
    )

    assert tuple(record) == RAW_MANIFEST_COLUMNS
    assert record["summary_url"] == "https://hub.example/summary?id=42"
    assert (record["license"], record["citation"], record["doi"]) == (
        "https://hub.example/licence.txt", "WorldPop (2018).", "10.5258/SOTON/WP00645"
    )


@pytest.mark.parametrize("citation, expected", [
    ("WorldPop (2018). Global High Resolution Population Denominators Project.", "kept"),
    ("WorldPop (2018). Population of Afghanistan, 2000-2020.", None),
    ("WorldPop (2018). Population of the Islamic Republic of Afghanistan.", None),
    (None, None),
    ("", None),
], ids=["series-level", "names-the-country", "official-name", "missing", "empty"])
def test_series_citation_is_dropped_where_it_names_the_sample_country(citation, expected, caplog):
    """
    The sample dataset's citation is stored for the whole data series, unless
    it names the sample country (it might then not apply to the series' other
    countries), which is logged as a warning.
    """
    from worldpoppy.manifest_builder import _series_citation

    result = _series_citation({"citation": citation}, "AFG", "pop/wpgp")

    assert result == (citation if expected == "kept" else None)
    names_country = bool(citation) and "Afghanistan" in citation
    assert ("names the sample country" in caplog.text) == names_country


GLOBAL1_FILE_LIST = [
    "GIS/Population/Global_2000_2020/2020/LUX/lux_ppp_2020.tif",
    "GIS/Covariates/Global_2000_2020/LUX/DMSP/lux_dmsp_100m_2001.tif",
    "GIS/Population/Global_2000_2020_Constrained/2020/BSGM/LUX/lux_ppp_2020_constrained.tif",
]


def test_url_check_confirms_corrects_and_keeps_urls():
    """
    A Global 1 URL in the Global 1 File List is confirmed (also if it has a
    doubled slash). A Global 1 URL whose file name is listed under another
    path is corrected to that path. Any other Global 1 URL is kept as
    inferred (unconfirmed), and Global 2 URLs are not checked. No row is
    dropped, other columns are unchanged, and the input is not modified.
    """
    from worldpoppy.config import DATA_SERVER_URL
    from worldpoppy.manifest_builder import _check_urls_against_global1_file_list

    mdf = pd.DataFrame({
        "remote_path": [
            f"{DATA_SERVER_URL}/Population/Global_2000_2020/2020/LUX/lux_ppp_2020.tif",
            f"{DATA_SERVER_URL}/Covariates/Global_2000_2020/LUX/DMSP//lux_dmsp_100m_2001.tif",
            f"{DATA_SERVER_URL}/Population/Global_2000_2020_Constrained/2020/maxar_v1/LUX/lux_ppp_2020_constrained.tif",
            f"{DATA_SERVER_URL}/Covariates/Global_2000_2020/LUX/WDPA/WDPA_1/lux_wdpa_dst_cat1_100m_2015.tif",
            f"{DATA_SERVER_URL}/Population/Global_2015_2030/R2025A/2020/LUX/v1/100m/constrained/lux_pop_2020_CN_100m_R2025A_v1.tif",
        ],
        "iso3": ["LUX", "LUX", "LUX", "LUX", "LUX"],
    })
    original = mdf.copy()

    checked = _check_urls_against_global1_file_list(mdf, GLOBAL1_FILE_LIST)

    assert list(checked["url_check"]) == ["confirmed", "confirmed", "corrected", "unconfirmed", "not_checked"]
    expected_urls = list(original["remote_path"])
    expected_urls[2] = f"{DATA_SERVER_URL}/Population/Global_2000_2020_Constrained/2020/BSGM/LUX/lux_ppp_2020_constrained.tif"
    assert list(checked["remote_path"]) == expected_urls
    assert list(checked["iso3"]) == list(original["iso3"])
    pd.testing.assert_frame_equal(mdf, original)


def test_url_check_ignores_doubled_slashes_in_the_file_list():
    """
    Doubled slashes are ignored on both sides of the comparison: a URL is
    confirmed by a listed path that differs from it only by a doubled slash.
    """
    from worldpoppy.config import DATA_SERVER_URL
    from worldpoppy.manifest_builder import _check_urls_against_global1_file_list

    file_list = ["GIS/Covariates/Global_2000_2020/LUX/DMSP//lux_dmsp_100m_2001.tif"]
    url = f"{DATA_SERVER_URL}/Covariates/Global_2000_2020/LUX/DMSP/lux_dmsp_100m_2001.tif"

    checked = _check_urls_against_global1_file_list(pd.DataFrame({"remote_path": [url]}), file_list)

    assert list(checked["url_check"]) == ["confirmed"]
    assert list(checked["remote_path"]) == [url]


def test_url_check_does_not_choose_between_listed_paths():
    """
    If a file name appears under several paths in the Global 1 File List, the
    URL is not corrected (there is no way to tell which path is right): it is
    kept as inferred and marked as unconfirmed.
    """
    from worldpoppy.config import DATA_SERVER_URL
    from worldpoppy.manifest_builder import _check_urls_against_global1_file_list

    folder = "GIS/Population/Global_2000_2020_Constrained/2020"
    file_list = [f"{folder}/BSGM/LUX/lux.tif", f"{folder}/maxar_v1/LUX/lux.tif"]
    url = f"{DATA_SERVER_URL}/Population/Global_2000_2020_Constrained/2020/other/LUX/lux.tif"

    checked = _check_urls_against_global1_file_list(pd.DataFrame({"remote_path": [url]}), file_list)

    assert list(checked["url_check"]) == ["unconfirmed"]
    assert list(checked["remote_path"]) == [url]


def test_extract_year_from_filename():
    from worldpoppy.manifest_utils import extract_year_from_filename

    # --- Clean cases (unambiguous) ---

    # Simple case
    url = "afg_viirs_nvf_2020_100m_v1.tif"
    assert extract_year_from_filename(url) == 2020

    # Simple case with path
    url = "/GIS/Pop/Global_2000_2020/2010/AUS/aus_ppp_2010.tif"
    assert extract_year_from_filename(url) == 2010

    # Multiple *identical* matches
    url = "https://.../data_2019_report_2019_final.tif"
    assert extract_year_from_filename(url) == 2019

    # Multiple *identical* matches with range in year name
    url = "Global_2000_2020_report_for_2017.tif"
    assert extract_year_from_filename(url) == 2017

    # --- None cases (ambiguous or invalid) ---

    # Ambiguous: Multiple *different* years
    url = "project_2015_data_for_2020.tif"
    assert extract_year_from_filename(url) is None

    # Invalid: Year is part of a YYYY_YYYY range
    url = "data_prefix_2000_2020_suffix.tif"
    assert extract_year_from_filename(url) is None

    # Invalid: Year is part of a YYYY_YYYY range
    url = "data_prefix_2000_2020_2017_data.tif"
    assert extract_year_from_filename(url) is None

    # No year at all
    url = "https://.../srtm_slope_100m.tif"
    assert extract_year_from_filename(url) is None


def test_are_integers_consecutive():
    from worldpoppy.manifest_utils import are_unique_integers_consecutive

    # --- True cases ---
    assert are_unique_integers_consecutive([6, 4, 5])
    assert are_unique_integers_consecutive([6, 4, 7, 5])

    # --- False  cases ---
    assert not are_unique_integers_consecutive([0, 4, 5])
    assert not are_unique_integers_consecutive([4, 7, 5])


def test_extract_unique_bands(caplog):
    from worldpoppy.manifest_utils import extract_unique_bands

    # --- Good cases ---

    filenames = ['abc_foo_X_2017', 'abc_foo_Y_2017', 'abc_foo_Z_2017']
    assert extract_unique_bands(filenames) == ['X', 'Y', 'Z']

    filenames = ['abc_foo_X_2017', 'abc_foo_Y_2017', 'abc_foo_Z_2017']
    assert extract_unique_bands(filenames) == ['X', 'Y', 'Z']

    # --- Bad cases ---

    badfilenames = ['abc_foo_X_2017', 'ABC_foo_Y_2017']
    result = extract_unique_bands(badfilenames)
    assert result is None

    badfilenames= ['abc_foo_X_2017', 'abc_foo_Y_2018']
    result = extract_unique_bands(badfilenames)
    assert result is None

    badfilenames = ['abc_foo_X_2017', 'abc_foo_X_2017']
    result = extract_unique_bands(badfilenames)
    assert result is None

    badfilenames = ['abc_foo_Z_2017', 'abc_foo_X_2017', 'abc_foo_X_2017']
    result = extract_unique_bands(badfilenames)
    assert result is None

    badfilenames = ['abc_foo_Z_2017_A', 'abc_foo_X_2017_B']
    result = extract_unique_bands(badfilenames)
    assert result is None
