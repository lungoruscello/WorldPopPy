"""
Example 2: Visualise night-light emissions for the Korean Peninsula.

Illustrates WorldPop data selection using simple country codes.
"""
# TODO update to new manifest

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, clean_axis

# Fetch night-light (NTL) data for the Korean Peninsula. Note: Calling
# `wp-raster()` returns an `xarray.DataArray` ready for analysis and plotting.
viirs_data = wp_raster(
    product_name='ntl_viirs_g2',  # curated worldpoppy product name (here: NTL data from the Global 2 series)
    aoi=['PRK', 'KOR'],  # three-letter country codes for North and South Korea
    years=2023,  # one or more years of interest
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value),
)

# Downsample the data to speed-up plotting
lowres = viirs_data.coarsen(x=5, y=5, boundary='trim').mean()

# Add small constant for log-scale plotting
lowres = lowres + 0.01

# Plot
lowres.plot(cmap='inferno', norm=LogNorm())
clean_axis(title='Night Lights (2023)\nKorean Peninsula')

plt.show()
