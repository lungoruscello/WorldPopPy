import geopandas

from worldpoppy.config import ASSET_DIR


def load_italian_regions():
    return geopandas.read_feather(ASSET_DIR / 'italian_regions_simplified.feather')


def load_kamchatka_volcano_example():
    return geopandas.read_feather(ASSET_DIR / 'kamchatka_petropavlosk_area.feather')
