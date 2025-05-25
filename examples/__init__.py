import geopandas

from worldpoppy.config import ASSET_DIR


def load_italian_regions():
    return geopandas.read_feather(ASSET_DIR / 'italian_regions_simplified.feather')
