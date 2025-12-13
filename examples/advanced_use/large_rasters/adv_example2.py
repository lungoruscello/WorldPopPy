"""
Advanced Example 2: Chile-Argentina-Bolivia Tri-point.

Illustrates automatic pre-clipping of raster data when the AoI is specified using a bounding box.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from worldpoppy import (
    wp_raster, wp_warp, bbox_from_location,
    clean_axis, plot_country_borders, plot_location_markers
)

# --- WARNING ---
print("WARNING: This example requires population data for ARG, BOL, and CHL (~3 GB).")
# ---------------

# Define AoI: The tri-point on the border between Chile, Argentina, and Bolivia
aoi = bbox_from_location(
    (-67.173, -22.807),
    width_km=800
)

# Fetch data
# `wp_raster` will detect the intersection with 3 countries, download them,
# merge them, and clip to the bounding box.
tri_area_pop = wp_raster(
    product_name='pop_g1',
    aoi=aoi,
    masked=True,
    years=2020,
)
print(f"Original shape: {tri_area_pop.shape}")

# Reproject and Downsample
# UTM Zone 19S is a good choice for Northern Chile/Andes
utm_19s = "EPSG:32719"

tri_area_pop_warped = wp_warp(
    tri_area_pop,
    to_crs=utm_19s,
    res=500,  # target resolution in units of 'to_crs' (here: metres)
    resampling='sum'
)
print(f"Warped shape: {tri_area_pop_warped.shape}")

# Plot
# Add small constant for LogNorm stability
(tri_area_pop_warped + 0.1).plot(norm=LogNorm(), cmap='inferno')
clean_axis(title='Est. Population (2020)', remove_xy_ticks=True)

# Visual references
plot_country_borders(['ARG', 'CHL', 'BOL'], edgecolor='white', linewidth=0.5, to_crs=utm_19s)

plot_location_markers(
    [
        (-64.726, -21.517, 'Tarija'),
        (-70.381, -23.615, 'Antofagasta'),
        (-65.419, -24.779, 'Salta'),
        (-65.221, -26.820, 'San Miguel'),
    ],
    color='white',
    to_crs=utm_19s
)

plt.show()