"""
Example 1: Compute and plot population changes around Accra and Lomé.
"""
import numpy as np
import matplotlib.pyplot as plt

from worldpoppy import *

# Define the area of interest (AOI) with a bbox
aoi_box = bbox_from_location('Accra', width_km=500)

# Fetch the data
data = wp_raster(  # returns xarray.DataArray
    product_name='ppp',  # name of the WorldPop product (here: est. no. of people per grid-cell)
    aoi=aoi_box,  # passing a GeoDataFrame or country codes would also work
    years=[2000, 2020],  # the years of interest (must be None for static raster products)
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value)
    download_dry_run=False  # setting this to True would check download requirements only
)

# Compute population changes on spatially downsampled data
lowres = data.coarsen(x=10, y=10, year=1, boundary='trim').reduce(np.sum)  # will propagate NaNs
change = lowres.sel(year=2020) - lowres.sel(year=2000)

# PLOT
fig, ax = plt.subplots(1, 1, figsize=(6, 3))
change.plot(cmap='coolwarm', vmin=-1_000, vmax=1_000)
clean_axis(title='Est. population change (2020 - 2000)')

# Add country borders
borders = load_country_borders()  # heavily simplified and for visual reference only
gdf = borders[borders.iso3.isin(['GHA', 'TOG', 'BEN'])]
gdf.plot(color='None', edgecolor='white', ax=ax)

# Add city markers
for city in ['Accra', 'Kumasi', 'Lomé']:
    lon_lat = geolocate_name(city)
    ax.scatter(*lon_lat, color='k', s=5)
    ax.annotate(city, lon_lat, textcoords="offset points", xytext=(7, -7), ha='left')

plt.show()
