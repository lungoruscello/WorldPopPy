import os
from multiprocessing import cpu_count
from pathlib import Path

__all__ = [
    "ROOT_DIR",
    "ASSET_DIR",
    "WGS84_CRS",
    "RED",
    "BLUE",
    "GOLDEN",
    "get_cache_dir",
    "get_max_concurrency"
]

DEFAULT_CACHE_DIR = Path.home() / ".cache" / "worldpoppy"
DEFAULT_MAX_CONCURRENCY = cpu_count() - 1
ROOT_DIR = Path(__file__).parent
ASSET_DIR = ROOT_DIR / 'assets'
WGS84_CRS = 'EPSG:4326'

RED = 'xkcd:brick red'
BLUE = 'xkcd:sea blue'
GOLDEN = 'xkcd:goldenrod'


def get_cache_dir():
    """
    Return the local cache directory for downloaded WorldPop datasets.

    Users can override the default directory by setting the "worldpoppy_CACHE_DIR"
    environment variable.
    """
    cache_dir = os.getenv("WORLDPOPPY_CACHE_DIR", str(DEFAULT_CACHE_DIR))
    cache_dir = Path(cache_dir)
    return cache_dir


def get_max_concurrency():
    """
    Return the maximum concurrency for parallel raster downloads.

    Users can override the default directory by setting the "worldpoppy_MAX_CONCURRENCY"
    environment variable.
    """
    num_threads = os.getenv("WORLDPOPPY_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)
    return int(num_threads)
