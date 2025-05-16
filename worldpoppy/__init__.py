__about__ = "Library to help you work with `WorldPop` data for any region on earth"
__version__ = '0.1.0'
__url__ = "https://github.com/lungoruscello/worldpoppy"
__license__ = "Mozilla Public License 2.0 (MPL-2.0)"
__author__ = "S. Langenbach"

from .config import *
from .download import WorldPopDownloader, purge_cache
from .manifest import wp_manifest, get_all_isos, get_annual_product_names, get_static_product_names
from .raster import wp_raster, bbox_from_location
from .borders import load_country_borders
from .utils import *
