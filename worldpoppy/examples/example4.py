"""
Example 4: Check download requirements for a large data request.

Shows how to change the default cache directory and how to preview
download requirements with the `download_dry_run` flag.
"""

import os
from pathlib import Path

from worldpoppy import wp_raster
from worldpoppy.config import get_cache_dir, ROOT_DIR

# set new cache directory
new_cache_dir = ROOT_DIR / 'tmp_cache'
os.environ['WORLDPOPPY_CACHE_DIR'] = str(new_cache_dir)
assert get_cache_dir() == Path(new_cache_dir)

_ = wp_raster(
    product_name='ppp',
    aoi='CAN USA MEX'.split(),
    years='all',
    download_dry_run=True
)
# Setting `download_dry_run=True` will only check download requirements
# (via asynchronous HEAD requests to the WorldPop server). No data will
# actually be downloaded or processed. Also, the return type of `wp_raster`
# will be None.

# revert back to default
del os.environ['WORLDPOPPY_CACHE_DIR']
