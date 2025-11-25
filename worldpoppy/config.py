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
    "ESA_LAND_COVER_DESC_MAP",
    "ESA_LAND_COVER_ALIAS_MAP",
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

with open(ASSET_DIR / 'global_nb_db.json') as file:
    # TODO Document this file asset in README (https://hub.worldpop.org/data/licence.txt)
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


ESA_LAND_COVER_DESC_MAP = {
    # --- 'esaccilc...' aliases ---
    'dst011': 'Cultivated area distance',
    'dst040': 'Woody/tree area distance',
    'dst130': 'Shrub area distance',
    'dst140': 'Herbaceous area distance',
    'dst150': 'Sparse vegetation distance',
    'dst160': 'Aquatic vegetation distance',
    'dst190': 'Artificial surface distance',
    'dst200': 'Bare area distance',

    # ---  'G2_DST_ESA...' aliases ---
    '11': 'Cultivated area distance',
    '40': 'Woody/tree area distance',
    '130': 'Shrub area distance',
    '140': 'Herbaceous area distance',
    '150': 'Sparse vegetation distance',
    '160': 'Aquatic vegetation distance',
    '190': 'Artificial surface distance',
    '200': 'Bare area distance',
    '210': 'Water, snow, and ice distance'
}

ESA_LAND_COVER_ALIAS_MAP = {
    # --- 'esaccilc...' aliases ---
    'dst011': 'cultivated',
    'dst040': 'woody',
    'dst130': 'shrub',
    'dst140': 'herbaceous',
    'dst150': 'sparse_veg',
    'dst160': 'aquatic_veg',
    'dst190': 'artificial',
    'dst200': 'bare_area',

    # ---  'G2_DST_ESA...' aliases ---
    '11': 'cultivated',
    '40': 'woody',
    '130': 'shrub',
    '140': 'herbaceous',
    '150': 'sparse_veg',
    '160': 'aquatic_veg',
    '190': 'artificial',
    '200': 'bare_area',
    '210': 'water_snow_ice'
}