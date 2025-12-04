import json
import os
from multiprocessing import cpu_count
from pathlib import Path

import platformdirs

try:
    import tomllib   # use the standard library tomllib (Python 3.11+)
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # fall back to `tomli` package
    except ModuleNotFoundError:
        print(
            "Error: The 'tomli' package is required. Please install it "
            "using pip, conda, or mamba (e.g., `conda install tomli`)."
        )
        raise

__all__ = [
    "DEBUG",
    "ROOT_DIR",
    "ASSET_DIR",
    "RAW_MANIFEST_CACHE_PATH",
    "METADATA_API_URL",
    "DATA_SERVER_URL",
    "METADATA_API_TIMEOUT",
    "DATA_DOWNLOAD_TIMEOUT",
    "DOWNLOADABLE_ISO3_CODES",
    "WGS84_CRS",
    "PRODUCT_BASE_NAME_MAP",
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
CUSTOM_MAPPING_TOML_PATH = ASSET_DIR / "custom_name_mappings.toml"

METADATA_API_URL = "https://hub.worldpop.org/rest/data"
DATA_SERVER_URL = "https://data.worldpop.org/GIS"
METADATA_API_TIMEOUT = 10.0
DATA_DOWNLOAD_TIMEOUT = 10.0

with open(ASSET_DIR / 'global_nb_db.json') as file:
    # TODO Document this file asset in README (https://hub.worldpop.org/data/licence.txt)
    _nb_dict = json.loads(file.read())
    _iso3_codes = _nb_dict.keys()

DOWNLOADABLE_ISO3_CODES = sorted(_iso3_codes)

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


def _load_mappings_from_toml():
    """Load curated name maps for `worldpoppy` from the mappings.toml file."""
    try:
        with open(CUSTOM_MAPPING_TOML_PATH, "rb") as f:
            mappings = tomllib.load(f)

        # extract the mappings from their TOML sections
        product_map = mappings.get("product_base_name", {})
        desc_map = mappings.get("band_description", {})
        alias_map = mappings.get("band_alias", {})
        product_notes_map_raw = mappings.get("product_notes", {})

        # remove redundant white-space in the product notes
        product_notes_map = {}
        for key, val in product_notes_map_raw.items():
            cleaned_val = ' '.join(val.split())
            product_notes_map[key] = cleaned_val

        return product_map, desc_map, alias_map, product_notes_map

    except FileNotFoundError:
        # This is a critical failure; `worldpoppy` cannot run without this file.
        raise FileNotFoundError(
            f"Fatal: Expected config file not found at {CUSTOM_MAPPING_TOML_PATH}. "
            "Please ensure 'custom_name_mappings.toml' is in the 'assets' directory."
        )
    except Exception as e:
        raise RuntimeError(f"Fatal: Failed to load or parse {CUSTOM_MAPPING_TOML_PATH}: {e}")


PRODUCT_BASE_NAME_MAP, ESA_LAND_COVER_DESC_MAP, ESA_LAND_COVER_ALIAS_MAP, PRODUCT_NOTES_MAP = (
    _load_mappings_from_toml()
)
