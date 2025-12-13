"""
Advanced Example 3: Chile and Argentina (Dask Optimisation).

Illustrates:
1. Explicit manual pre-clipping of a large country raster when the AoI
   is specified using an ISO3-code.
2. Using Dask (`chunks='auto'`) to lazy-load raster data.
3. Manual coarsening and re-chunking of a Dask array before computation.

Note:
    This example visualises population data for Chile and Argentina, using country codes
    to specify the Area of Interest (AoI). Since Chile has sovereignty over the remote
    Pacific Island of Rapa Nui (Easter Island), the raw raster file for Chile consists
    largely of empty pixels in the South Pacific. When specifying the AoI using country
    codes, NO automated pre-clipping of raster data is applied. Users can, however,
    provide an **explicit bounding box** for pre-clipping.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from worldpoppy import (
    wp_raster, wp_warp, clean_axis, plot_country_borders
)

# --- WARNING ---
print("WARNING: This example requires population data for ARG and CHL (~2.4 GB).")
print("If cached files do not exist, they will be downloaded.")
# ---------------

iso_codes = ['CHL', 'ARG']

# Fetch data lazily
# We download the full raster data for Chile and Argentina, but instruct
# `wp_raster` to ignore pixels outside a custom bounding box specified via
# `pre_clip_bbox`.
#
# Since even the area of this bounding box is pretty large, we reduce the
# memory footprint further by providing the `chunks` argument as well.
# Doing so will **lazy-load** all raster data into a Dask array which
# we can then downsample and process in several chunks later.
#
# For more on Dask arrays, see `Dask Array <https://docs.dask.org/en/stable/array.html>`_.
# and specifically `Array Chunks <https://docs.dask.org/en/stable/array-chunks.html>`_.

mainland_box = (-78, -58, -52, -11)  # excludes Easter Island

arg_chl_pop = wp_raster(
    product_name='pop_g1',
    aoi=iso_codes,
    pre_clip_bbox=mainland_box,
    masked=True,
    years=2020,
    chunks='auto'  # Enable Dask
)
print(f"Lazy Dask shape: {arg_chl_pop.shape}")

# Define a downsampled version (lazy operation)
# reducing 100m data to ~2.5km pixels to speed up plotting
arg_chl_pop_coarse = arg_chl_pop.coarsen(x=25, y=25, boundary='trim').sum()

# Re-chunk to optimise graph execution
# Merging countries often fragments Dask chunks; this standardises them.
arg_chl_pop_coarse = arg_chl_pop_coarse.chunk({'x': 2048, 'y': 2048})

# Execute!
# We load into memory now because the array is small enough after coarsening.
arg_chl_pop_loaded = arg_chl_pop_coarse.load()
print(f"Loaded coarse shape: {arg_chl_pop_loaded.shape}")

# Warp for display
# UTM Zone 19S is a good choice for Northern Chile/Andes
utm_19s = "EPSG:32719"

arg_chl_pop_warped = wp_warp(
    arg_chl_pop_loaded,
    to_crs=utm_19s,
    res=2_500,  # target resolution in units of 'to_crs' (here: metres)
    resampling='sum'
)
print(f"Warped shape: {arg_chl_pop_warped.shape}")

# Plot
# We add a small constant (0.1) to allow log-scale plotting of 0 values
(arg_chl_pop_warped + 0.1).plot(norm=LogNorm(), cmap='inferno')

clean_axis(title='Est. Population (2020) - Mainland only', remove_xy_ticks=True)
plot_country_borders(['CHL', 'ARG'], edgecolor='white', to_crs=utm_19s, linewidth=0.5)

plt.show()