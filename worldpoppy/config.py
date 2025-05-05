import os
from multiprocessing import cpu_count
from pathlib import Path

__all__ = ["get_cache_dir", "get_max_concurrency"]

DEFAULT_CACHE_DIR = Path.home() / ".cache" / "worldpoppy"
DEFAULT_MAX_CONCURRENCY = cpu_count() - 1


def get_cache_dir():
    """
    Return the local cache directory for downloaded WorldPop datasets.

    Users can override the default directory by setting the "worldpoppy_CACHE_DIR"
    environment variable.
    """
    cache_dir = os.getenv("worldpoppy_CACHE_DIR", str(DEFAULT_CACHE_DIR))
    cache_dir = Path(cache_dir)
    return cache_dir


def get_max_concurrency():
    """
    Return the maximum concurrency for parallel raster downloads.

    Users can override the default directory by setting the "worldpoppy_MAX_CONCURRENCY"
    environment variable.
    """
    return os.getenv("worldpoppy_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)
