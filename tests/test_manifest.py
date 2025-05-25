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


def test_good_manifest_filter_drop_static():
    from worldpoppy.manifest import wp_manifest

    mdf = wp_manifest()  # full manifest
    expected = mdf[mdf.is_annual]
    actual = wp_manifest(years='all')
    assert np.all(expected.idx == actual.idx)


def test_good_manifest_filter_annual_combo():
    from worldpoppy.manifest import wp_manifest

    def _check_result():
        assert np.all(mdf.product_name == product_name)
        assert np.all(mdf.iso3.isin(iso3_codes))
        assert np.all(mdf.year.isin(years))

    # example 1
    iso3_codes = ['COD', 'CAF', 'SSD', 'SDN']
    product_name = 'ppp'
    years = [2018, 2019, 2020]
    mdf = wp_manifest(product_name, iso3_codes, years=years)
    _check_result()

    # example 2
    iso3_codes = ['DNK', 'NOR', 'SWE', 'FIN']
    product_name = 'agesex_f_60_constrained_UNadj'
    years = [2020]
    mdf = wp_manifest(product_name, iso3_codes, years=years)
    _check_result()


def test_good_manifest_filter_static_combo():
    from worldpoppy.manifest import wp_manifest

    def _check_result():
        assert np.all(mdf.product_name == product_name)
        assert np.all(mdf.iso3.isin(iso3_codes))
        assert np.all(np.isnan(mdf.year))

    # example 1
    iso3_codes = ['USA', 'CAN', 'MEX']
    product_name = 'srtm_slope_100m'
    mdf = wp_manifest(product_name, iso3_codes, years=None)
    _check_result()

    # example 2
    iso3_codes = ['MYS', 'SGP', 'IDN']
    product_name = 'dst_coastline_100m_2000_2020'
    mdf = wp_manifest(product_name, iso3_codes, years=None)
    _check_result()


def test_bad_manifest_filters_raise():
    from worldpoppy.manifest import wp_manifest

    with pytest.raises(ValueError):
        wp_manifest(product_name='no_real_product')

    with pytest.raises(ValueError):
        wp_manifest(iso3_codes='fantasia')

    with pytest.raises(ValueError):
        wp_manifest(years=1900)


def test_incomplete_manifest_coverage_raises():
    from worldpoppy.manifest import wp_manifest, wp_manifest_constrained
    eg_prod, eg_iso, eg_year = 'viirs_100m', 'NZL' , 2020

    wp_manifest(product_name=eg_prod)
    wp_manifest(iso3_codes=eg_iso)
    wp_manifest(years=eg_year)

    with pytest.raises(ValueError):
        # empty combo (incomplete coverage)
        wp_manifest_constrained(product_name=eg_prod, iso3_codes=eg_iso, years=eg_year)
