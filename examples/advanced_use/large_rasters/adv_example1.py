"""
Advanced Example 1: Koryaksky Volcano (Southern Kamchatka).

Illustrates automatic pre-clipping of raster data when the AoI is specified using a GeoDataFrame.

Note:
    This example visualises elevation data around Koryaksky Volcano, a natural landmark in
    Southern Kamchatka (Russian Far East). The example demonstrates a "Worst Case Scenario"
    for bandwidth but a "Best Case Scenario" for `worldpoppy`'s memory management. Since
    the WorldPop project stores data by country, you MUST download the full ~2GB elevation
    raster for the Russian Federation. Once this data is downloaded, however, `worldpoppy`
    will only load a tiny raster slice into RAM.
"""

import matplotlib.pyplot as plt
from worldpoppy import wp_raster, wp_warp, clean_axis, plot_location_markers
from examples import load_kamchatka_volcano_example

# --- WARNING ---
print("WARNING: This example requires the elevation raster for Russia (~2 GB).")
print("If a cached file does not exist, it will be downloaded.")
# ---------------

# Load AoI (a polygon in Southern Kamchatka)
eg_gdf = load_kamchatka_volcano_example()

# Fetch data
# The downloader will fetch the full Russia dataset (~2GB) because that is
# how raster files are stored on the server. However, `wp_raster` detects
# that your AoI is tiny. It will:
#   a) Download the file to disk (if not cached).
#   b) Open it lazily.
#   c) Read ONLY the pixels inside the bounding box of your AoI.
# Result: Your RAM usage stays low, even though the source file is huge.
volcano_topo = wp_raster(
    product_name='srtm_elevation_g1',
    aoi=eg_gdf,
    masked=True,
)
print(f"Original shape: {volcano_topo.shape}")

# Reproject
# Kamchatka is in UTM Zone 57N. We warp the data to an optimised coordinate
# reference system to minimise distortions.
utm_57n = "EPSG:32657"

volcano_topo_warped = wp_warp(
    volcano_topo,
    to_crs=utm_57n,
    res=100,  # target resolution in units of 'to_crs' (here: metres)
    resampling='mean'
)
print(f"Warped shape: {volcano_topo_warped.shape}")

# Plot
volcano_topo_warped.plot(
    cmap='gist_earth',
    vmin=0, vmax=3_000, alpha=0.95,
)

clean_axis(title='Elevation (m) - Southern Kamchatka', remove_xy_ticks=True)

# Annotate
plot_location_markers(
    [(158.701, 53.322, 'Koryaksky Volcano')],
    xytext=(-45, 12),
    color='white',
    to_crs=utm_57n
)

plt.show()