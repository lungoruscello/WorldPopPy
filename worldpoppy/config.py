import json
import os
from multiprocessing import cpu_count
from pathlib import Path

import platformdirs

__all__ = [
    "DEBUG",
    "ROOT_DIR",
    "ASSET_DIR",
    "RAW_MANIFEST_CACHE_PATH",
    "METADATA_API_URL",
    "DATA_SERVER_URL",
    "METADATA_API_TIMEOUT",
    "DATA_DOWNLOAD_TIMEOUT",
    "SUPPORTED_ISO3_CODES",
    "WGS84_CRS",
    "RED",
    "BLUE",
    "GOLDEN",
    "get_cache_dir",
    "get_max_concurrency",
]

DEBUG = False

DEFAULT_CACHE_DIR = Path(platformdirs.user_cache_dir(appname="worldpoppy"))
DEFAULT_MAX_CONCURRENCY = max(1, cpu_count() - 2)
ROOT_DIR = Path(__file__).parent
ASSET_DIR = ROOT_DIR / 'assets'
RAW_MANIFEST_CACHE_PATH = ASSET_DIR / "raw_api_manifest.feather"

METADATA_API_URL = "https://hub.worldpop.org/rest/data"
DATA_SERVER_URL = "https://data.worldpop.org/GIS"
METADATA_API_TIMEOUT = 10.0
DATA_DOWNLOAD_TIMEOUT = 10.0

with open(ASSET_DIR / 'global_nb_db.json') as file:  # TODO add to asset README (https://hub.worldpop.org/data/licence.txt)
    _nb_dict = json.loads(file.read())
    _iso3_codes = _nb_dict.keys()

SUPPORTED_ISO3_CODES = sorted(_iso3_codes)

WGS84_CRS = 'EPSG:4326'

RED = 'xkcd:brick red'
BLUE = 'xkcd:sea blue'
GOLDEN = 'xkcd:goldenrod'


def get_cache_dir():
    """
    Return the local cache directory for downloaded WorldPop datasets.

    Note
    ----
    You can override the default cache directory by setting the "WORLDPOPPY_CACHE_DIR"
    environment variable.
    """
    cache_dir = os.getenv("WORLDPOPPY_CACHE_DIR", str(DEFAULT_CACHE_DIR))
    cache_dir = Path(cache_dir)
    return cache_dir


def get_max_concurrency():
    """
    Return the maximum concurrency for parallel raster downloads.

    Note
    ----
    You can override the default concurrency limit by setting the "WORLDPOPPY_MAX_CONCURRENCY"
    environment variable.
    """
    num_threads = os.getenv("WORLDPOPPY_MAX_CONCURRENCY", DEFAULT_MAX_CONCURRENCY)
    return int(num_threads)
