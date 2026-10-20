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


def test_change_max_concurrency(monkeypatch):
    from worldpoppy.config import DEFAULT_MAX_CONCURRENCY, get_max_concurrency

    # without the environment variable, the default concurrency is used
    monkeypatch.delenv('WORLDPOPPY_MAX_CONCURRENCY', raising=False)
    assert get_max_concurrency() == DEFAULT_MAX_CONCURRENCY

    # set new value
    monkeypatch.setenv('WORLDPOPPY_MAX_CONCURRENCY', "10")
    assert get_max_concurrency() == 10

    # (monkeypatch restores the original environment after the test)
