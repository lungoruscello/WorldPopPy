"""
Example 1: Visualise population changes around Accra and Lomé from 2000 to 2020.

Demonstrates multi-year WorldPop data selection using a bounding box across
countries as well as handling of different Coordinate Reference Systems.
"""

import matplotlib.pyplot as plt
import numpy as np

from worldpoppy import *

# Define target CRS (optional)
aeqa_africa = "ESRI:102022"  # Africa Albers Equal Area Conic

# Define the area of interest (runs a 'Nomatim' query under the hood)
aoi_box = bbox_from_location('Accra', width_km=500)  # returns (min_lon, min_lat, max_lon, max_lat)

# Fetch the data
pop_data = wp_raster(  # returns xarray.DataArray
    product_name='ppp',  # name of the WorldPop product (here: est. no. of people per grid-cell)
    aoi=aoi_box,  # passing a GeoDataFrame or one or more country codes would also work
    years=[2000, 2020],  # the years of interest (must be None for static raster products)
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value)
    to_crs=aeqa_africa  # if not provided, the CRS of the source data is kept
)

# Compute population changes on spatially downsampled data
lowres = pop_data.coarsen(x=10, y=10, year=1, boundary='trim').reduce(np.sum)  # will propagate NaNs
pop_change = lowres.sel(year=2020) - lowres.sel(year=2000)

# PLOT
pop_change.plot(cmap='coolwarm', vmin=-1_000, vmax=1_000, cbar_kwargs=dict(shrink=0.85))
clean_axis(title='Est. population change (2020 - 2000)', remove_xy_ticks=True)

# Add visual references
plot_country_borders(['GHA', 'TOG', 'BEN'], edgecolor='white', to_crs=aeqa_africa)
plot_location_markers(['Accra', 'Kumasi', 'Lomé'], to_crs=aeqa_africa)

plt.show()
