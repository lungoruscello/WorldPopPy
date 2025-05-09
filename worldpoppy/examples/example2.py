"""
Example 2: Visualise night-light emissions for the Korean Peninsula.

Illustrates WorldPop data selection using simple country codes.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import *

# Fetch data
viirs_data = wp_raster(
    product_name='viirs_100m',
    aoi=['PRK', 'KOR'],
    years=2015,
    masked=True,
)

# PLOT
lowres = viirs_data.coarsen(x=5, y=5, boundary='trim').mean()
lowres.plot(vmin=0.1, cmap='inferno', norm=LogNorm())
clean_axis(title='Night Lights (2015)\nKorean Peninsula')

plt.show()
