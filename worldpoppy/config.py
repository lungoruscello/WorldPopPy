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
    "RAW_MANIFEST_TIMESTAMP_PATH",
    "METADATA_API_URL",
    "DATA_SERVER_URL",
    "METADATA_API_TIMEOUT",
    "DATA_DOWNLOAD_TIMEOUT",
    "DOWNLOADABLE_ISO3_CODES",
    "WGS84_CRS",
    "PRODUCT_BASE_NAME_MAP",
    "ESA_LAND_COVER_DESC_MAP",
    "ESA_LAND_COVER_ALIAS_MAP",
    "PRODUCT_NOTES_MAP",
    "RED",
    "BLUE",
    "GOLDEN",
    "ENABLE_HEAVY_TESTS",
    "get_cache_dir",
    "get_max_concurrency",
]

DEFAULT_CACHE_DIR = Path(platformdirs.user_cache_dir(appname="worldpoppy"))
DEFAULT_MAX_CONCURRENCY = max(1, cpu_count() - 1)
ROOT_DIR = Path(__file__).parent
ASSET_DIR = ROOT_DIR / 'assets'
RAW_MANIFEST_CACHE_PATH = ASSET_DIR / "raw_api_manifest.feather"
RAW_MANIFEST_TIMESTAMP_PATH = ASSET_DIR / "raw_api_manifest_timestamp.txt"
CUSTOM_MAPPING_TOML_PATH = ASSET_DIR / "product_definitions.toml"

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

ENABLE_HEAVY_TESTS = os.getenv("WPY_RUN_HEAVY_TESTS") == "1"
# > Set the above environment variable only if you explicitly want
#   to run API-heavy integration / E2E tests (e.g., performing full
#   API traversals and testing raster URL availability)
DEBUG = False


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
    """
    Load curated product-level settings for `worldpoppy`
    from the product_definitions.toml file.
    """
    try:
        with open(CUSTOM_MAPPING_TOML_PATH, "rb") as f:
            mappings = tomllib.load(f)

        # Extract the mappings from their TOML sections
        product_map = mappings.get("product_base_name", {})
        desc_map = mappings.get("band_description", {})
        alias_map = mappings.get("band_alias", {})
        product_notes_map_raw = mappings.get("product_notes", {})

        # Remove redundant white-space in the product notes
        product_notes_map = {}
        for key, val in product_notes_map_raw.items():
            cleaned_val = ' '.join(val.split())
            product_notes_map[key] = cleaned_val

        return {
            "base_names": product_map,
            "descriptions": desc_map,
            "aliases": alias_map,
            "notes": product_notes_map,
        }

    except FileNotFoundError:
        # This is a critical failure; `worldpoppy` cannot run without the TOML file.
        raise FileNotFoundError(
            f"Fatal: Expected config file not found at {CUSTOM_MAPPING_TOML_PATH}. "
            "Please ensure 'product_definitions.toml' is in the 'assets' directory."
        )
    except Exception as e:
        raise RuntimeError(f"Fatal: Failed to load or parse {CUSTOM_MAPPING_TOML_PATH}: {e}")


# Load once into a private variable
_maps = _load_mappings_from_toml()

# Explicitly assign to the public constants
PRODUCT_BASE_NAME_MAP = _maps["base_names"]
ESA_LAND_COVER_DESC_MAP = _maps["descriptions"]
ESA_LAND_COVER_ALIAS_MAP = _maps["aliases"]
PRODUCT_NOTES_MAP = _maps["notes"]
