"""
Working with Large Rasters, Example 1: Southern Kamchatka (Topography).

Illustrates:
1. Automatic pre-clipping of raster data when the AoI is specified using a GeoDataFrame.
2. Use of more customised plotting options.

Note:
    This example visualises elevation data in Southern Kamchatka (Russian Far East).
    It represents a challenging scenario for network bandwidth, but an easy scenario
    for `worldpoppy`'s memory management. Since the WorldPop project stores data by
    country, you MUST download the full ~2GB elevation raster for Russia. Once this
    data is downloaded, however, `worldpoppy` will only load a small slice of it
    into RAM.
"""

import geopandas
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt

from worldpoppy import wp_raster, wp_warp, clean_axis, plot_location_markers, ASSET_DIR

# --- WARNING ---
print("WARNING: This example requires the elevation raster for Russia (~2 GB).")
print("If a cached file does not exist, it will be downloaded.")
# ---------------

# -- Load the AoI ---
# A polygon for Southern Kamchatka
aoi_gdf = geopandas.read_feather(ASSET_DIR / 'southern_kamchatka.feather')

# --- Fetch Data ---
# The downloader will fetch the full Russia dataset (~2GB) because this is
# how raster files are stored on the server. However, `wp_raster` detects
# that your AoI is small. It will:
#   a) Download the full file to disk (if not cached).
#   b) Open it lazily.
#   c) Ignore all raster pixels located outside a slightly buffered bounding
#      box surrounding your AoI.
# Result: Your RAM usage should stay low, although the source file is huge.
#
# This optimisation draws on the `clip_box` function from `rioxarray`.
# It is used whenever the AoI is specified using a GeoDataFrame or
# bounding box, but can be suppressed with `suppress_pre_clip=True`.
#
kam_topo = wp_raster(
    product_name='srtm_elevation_g1',
    aoi=aoi_gdf,
    masked=True,
)
print(f"Original shape: {kam_topo.shape}")

# --- Warp ---
# Kamchatka is in UTM Zone 57N. We reproject the data to an optimised
# coordinate reference system to minimise distortions.
utm_57n = "EPSG:32657"

kam_topo_warped = wp_warp(
    kam_topo,
    to_crs=utm_57n,
    res=500,  # target resolution in units of 'to_crs' (here: metres)
    resampling='mean'
)
print(f"Warped shape: {kam_topo_warped.shape}")

# --- Plot ---
kam_topo_warped.plot(
    cmap='gist_earth',
    vmin=0, vmax=2_000, alpha=0.95,
    # The colorbar *is* informative, but we need to save space in the 2x2 "repo gallery"
    add_colorbar=False
)

# Annotate
location = (160.642, 56.057, 'Klyuchevskoy Volcano')
plot_location_markers(
    location,
    xytext=(-90, 15),
    to_crs=utm_57n,
    color='white',  # Fill colour for the scatter point (also used as default text colour)
    s=10,  # Size of scatter point
    edgecolors='black', # Border colour for scatter point
    other_annotate_kwargs=dict(  # Additional text options for "pro" look
        weight="bold",
        fontsize=9,
        path_effects=[pe.withStroke(linewidth=1.5, foreground='black')]
    ),
)

# Clean plot
clean_axis(title='Topography of Southern Kamchatka')

if __name__ == "__main__":
    plt.show()
