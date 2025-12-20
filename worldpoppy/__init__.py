__about__ = "Python client to help you work with WorldPop data for any region on earth"
__version__ = '0.4.0'
__url__ = "https://github.com/lungoruscello/worldpoppy"
__license__ = "MPL-2.0"
__author__ = "S. Langenbach"
__author_email__ = "lungoruscello@gmail.com"

from .config import *
from .download import WorldPopDownloader, purge_cache
from .manifest_loader import (
    show_supported_data_products,
    wp_manifest,
    get_product_info,
    get_all_isos,
    get_static_product_names
)
from .raster import wp_raster, wp_warp, merge_rasters, bbox_from_location
from .borders import load_country_borders
from .func_utils import geolocate_name
from .plot_utils import *
