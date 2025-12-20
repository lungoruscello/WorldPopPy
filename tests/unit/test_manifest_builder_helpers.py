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
