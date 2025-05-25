"""
Example 2: Visualise night-light emissions for the Korean Peninsula.

Illustrates WorldPop data selection using simple country codes.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, clean_axis

# Fetch night-light data for the Korean Peninsula. Note: Calling
# `wp-raster()` returns an `xarray.DataArray` ready for analysis and plotting
viirs_data = wp_raster(
    product_name='viirs_100m',  # name of WorldPop's night-light product
    aoi=['PRK', 'KOR'],  # three-letter country codes for North and South Korea
    years=2015,  # one or more years of interest
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value),
)

# Downsample the data to speed-up plotting
lowres = viirs_data.coarsen(x=5, y=5, boundary='trim').mean()

# Plot
lowres.plot(vmin=0.1, cmap='inferno', norm=LogNorm())
clean_axis(title='Night Lights (2015)\nKorean Peninsula')

plt.show()
