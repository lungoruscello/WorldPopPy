import numpy as np
import pytest


def test_good_year_extraction():
    from worldpoppy.manifest import extract_year

    year = extract_year('some_dataset_2020')
    assert year == 2020


def test_bad_year_extraction_raises():
    from worldpoppy.manifest import extract_year

    with pytest.raises(ValueError):
        extract_year('bad_name')

    with pytest.raises(ValueError):
        extract_year('bad_name_1889')


def test_year_stripping():
    from worldpoppy.manifest import _strip_year
    assert _strip_year('some_dataset_2020') == 'some_dataset'
    assert _strip_year('some_dataset_2020_constrained') == 'some_dataset_constrained'


def test_looks_like_annual_name():
    from worldpoppy.manifest import _looks_like_annual_name

    assert _looks_like_annual_name('foo_2020') is True
    assert _looks_like_annual_name('foo_2020_to_2020') is False
    assert _looks_like_annual_name('foo') is False


def test_good_manifest_filter_annual():
    from worldpoppy.manifest import filter_global_manifest

    def _check_result():
        assert np.all(mdf.product_name == product_name)
        assert np.all(mdf.iso3.isin(iso3_codes))
        assert np.all(mdf.year.isin(years))

    # example 1
    iso3_codes = ['COD', 'CAF', 'SSD', 'SDN']
    product_name = 'ppp'
    years = [2018, 2019, 2020]
    mdf = filter_global_manifest(product_name, iso3_codes, years=years)
    _check_result()

    # example 2
    iso3_codes = ['DNK', 'NOR', 'SWE', 'FIN']
    product_name = 'agesex_f_60_constrained_UNadj'
    years = [2020]
    mdf = filter_global_manifest(product_name, iso3_codes, years=years)
    _check_result()


def test_good_manifest_filter_static():
    from worldpoppy.manifest import filter_global_manifest

    def _check_result():
        assert np.all(mdf.product_name == product_name)
        assert np.all(mdf.iso3.isin(iso3_codes))
        assert np.all(np.isnan(mdf.year))

    # example 1
    iso3_codes = ['USA', 'CAN', 'MEX']
    product_name = 'srtm_slope_100m'
    mdf = filter_global_manifest(product_name, iso3_codes, years=None)
    _check_result()

    # example 2
    iso3_codes = ['MYS', 'SGP', 'IDN']
    product_name = 'dst_coastline_100m_2000_2020'
    mdf = filter_global_manifest(product_name, iso3_codes, years=None)
    _check_result()


def test_bad_manifest_filter_raises():
    from worldpoppy.manifest import filter_global_manifest

    with pytest.raises(ValueError):
        # bad ISO code
        filter_global_manifest('bad_iso', 'ppp', years=2020)

    with pytest.raises(ValueError):
        # bad product name
        filter_global_manifest('NZL', 'bad_product', years=2020)

    with pytest.raises(ValueError):
        # un-stripped year identifier in annual product name
        filter_global_manifest('NZL', 'ppp_2020', years=2020)

    with pytest.raises(ValueError):
        # missing year for annual product
        filter_global_manifest('NZL', 'ppp', years=None)

    with pytest.raises(ValueError):
        # incomplete coverage
        filter_global_manifest('NZL', 'ppp', years=[2020, 2099])
