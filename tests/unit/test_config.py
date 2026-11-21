from pathlib import Path


def test_change_cache_dir(monkeypatch, tmp_path):
    from worldpoppy.config import DEFAULT_CACHE_DIR, get_cache_dir

    # without the environment variable, the default cache directory is used
    monkeypatch.delenv('WORLDPOPPY_CACHE_DIR', raising=False)
    assert get_cache_dir() == DEFAULT_CACHE_DIR

    # set new cache directory
    new_cache_dir = tmp_path / 'worldpoppy'
    monkeypatch.setenv('WORLDPOPPY_CACHE_DIR', str(new_cache_dir))
    assert get_cache_dir() == Path(new_cache_dir)

    # (monkeypatch restores the original environment after the test)


def test_cache_dir_set_after_import_applies_to_country_borders(monkeypatch, tmp_path):
    import worldpoppy.borders as borders
    from worldpoppy import purge_cache

    # worldpoppy is already imported at this point; the cache dir changes afterwards
    monkeypatch.setenv('WORLDPOPPY_CACHE_DIR', str(tmp_path))
    buffered_fpath = borders._border_buffered_fpath()
    assert buffered_fpath.parent == tmp_path

    # The cached borders are therefore also visible to `purge_cache`
    buffered_fpath.write_bytes(b"fake borders")
    assert purge_cache(dry_run=True, keep_country_borders=False)["matched_files"] == 1


def test_load_country_borders_builds_into_current_cache_dir(monkeypatch, tmp_path):
    import geopandas as gpd
    from shapely.geometry import box
    import worldpoppy.borders as borders

    # A synthetic stand-in for the shipped (unbuffered) border asset
    raw_fpath = tmp_path / "raw_borders.feather"
    gpd.GeoDataFrame({"iso3": ["LUX"]}, geometry=[box(5, 49, 6, 50)], crs="EPSG:4326").to_feather(raw_fpath)
    monkeypatch.setattr(borders, "_border_raw_fpath", raw_fpath)

    cache_dir = tmp_path / "cache"
    monkeypatch.setenv('WORLDPOPPY_CACHE_DIR', str(cache_dir))
    # Safety: never let the synthetic borders be built outside the test's cache dir
    # (e.g. into the user's real cache, should the path resolution ever regress)
    assert borders._border_buffered_fpath().parent == cache_dir

    borders.load_country_borders.cache_clear()
    try:
        world = borders.load_country_borders()
    finally:
        borders.load_country_borders.cache_clear()

    # The buffered borders are built *and read* in the current cache dir
    assert (cache_dir / borders._border_buffered_fname).is_file()
    assert list(world.iso3) == ["LUX"]
    assert world.geometry.iloc[0].area > 1.0  # buffered beyond the 1x1 degree box


def test_change_max_concurrency(monkeypatch):
    from worldpoppy.config import DEFAULT_MAX_CONCURRENCY, get_max_concurrency

    # without the environment variable, the default concurrency is used
    monkeypatch.delenv('WORLDPOPPY_MAX_CONCURRENCY', raising=False)
    assert get_max_concurrency() == DEFAULT_MAX_CONCURRENCY

    # set new value
    monkeypatch.setenv('WORLDPOPPY_MAX_CONCURRENCY', "10")
    assert get_max_concurrency() == 10

    # (monkeypatch restores the original environment after the test)
