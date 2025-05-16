"""
Example 1: Visualise population changes around Accra and Lomé from 2000 to 2020.

Illustrates WorldPop data selection using a bounding box across countries, support
for multi-year requests, and re-projection to a user-provided Coordinate Reference System.
"""

import matplotlib.pyplot as plt
import numpy as np

from worldpoppy import *

# Define the area of interest
# Note: `bbox_from_location` runs a `Nomatim` query under the hood
aoi_box = bbox_from_location('Accra', width_km=500)  # returns (min_lon, min_lat, max_lon, max_lat)

# Define the target CRS (optional)
aeqa_africa = "ESRI:102022"  # an Albers Equal Area projection optimised for Africa

# Fetch the population data
pop_data = wp_raster(
    product_name='ppp',  # name of the WorldPop product (here: # of people per raster cell)
    aoi=aoi_box,  # you could also pass a GeoDataFrame or official country codes
    years=[2000, 2020],  # the years of interest (for annual WorldPop products only)
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value)
    to_crs=aeqa_africa  # if None is provided, CRS of the source data will be kept (EPSG:4326)
)

# Compute population changes on downsampled data
lowres = pop_data.coarsen(x=10, y=10, year=1, boundary='trim').reduce(np.sum)  # will propagate NaNs
pop_change = lowres.sel(year=2020) - lowres.sel(year=2000)

# Plot
pop_change.plot(cmap='coolwarm', vmax=1_000, cbar_kwargs=dict(shrink=0.85))
clean_axis(title='Estimated population change (2000 to 2020)')

# Add visual references
plot_country_borders(['GHA', 'TOG', 'BEN'], edgecolor='white', to_crs=aeqa_africa)
plot_location_markers(['Accra', 'Kumasi', 'Lomé'], to_crs=aeqa_africa)

plt.show()
