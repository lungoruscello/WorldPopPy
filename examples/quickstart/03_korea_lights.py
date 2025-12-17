"""
Quickstart 3: Visualise Night-Light Data for the Korean Peninsula

Simply an iconic example.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, wp_warp, clean_axis, plot_country_borders

# 1. Fetch & Merge Data
# Iconic example: night lights on the Korean Peninsula
ntl_data = wp_raster(
    product_name="ntl_viirs_g2",  # Night lights from "Global 2" series
    aoi=["PRK", "KOR"],
    years=2023,
    masked=True,
)

# 2. Reproject and Downsample
# Korea is in UTM Zone 52N. We reproject the data to an optimised
# coordinate reference system to minimise distortions.
utm_52n = "EPSG:32652"
ntl_data_warped = wp_warp(
    ntl_data,
    to_crs=utm_52n,
    res=1_000,         # Target resolution in units of 'to_crs' (here: metres)
    resampling='mean'  # Average the data when resampling
)

# 3. Plot (Log-scale)
# We add a small constant (+0.1) rather than +1 to better preserve some of
# the order-of-magnitude differences between the dark North and bright South.
(ntl_data_warped + 0.1).plot(cmap="inferno", norm=LogNorm(), add_colorbar=False)
clean_axis(title="Night Lights,\nKorean Peninsula (2023)")

if __name__ == "__main__":
    plt.show()
