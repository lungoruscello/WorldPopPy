import os


def test_change_cache_dir():
    from pathlib import Path
    from worldpoppy.config import get_cache_dir

    default_cache_dir = get_cache_dir()

    # set new cache directory
    new_cache_dir = '/tmp/worldpoppy'
    os.environ['WORLDPOPPY_CACHE_DIR'] = new_cache_dir
    assert get_cache_dir() == Path(new_cache_dir)

    # switch back to default
    del os.environ['WORLDPOPPY_CACHE_DIR']
    assert get_cache_dir() == default_cache_dir


def test_change_max_concurrency():
    from worldpoppy.config import get_max_concurrency

    default_threads = get_max_concurrency()

    # set new value
    os.environ['WORLDPOPPY_MAX_CONCURRENCY'] = "10"
    assert get_max_concurrency() == 10

    # switch back to default
    del os.environ['WORLDPOPPY_MAX_CONCURRENCY']
    assert get_max_concurrency() == default_threads
