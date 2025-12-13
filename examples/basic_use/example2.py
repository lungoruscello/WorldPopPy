"""
Example 2: Visualise population changes around Accra and Lomé from 2000 to 2020.

Illustrates:
1. WorldPop data selection using a bounding box across countries.
2. Support for multi-year requests.
3. Reprojection to a user-provided Coordinate Reference System.
"""

import matplotlib.pyplot as plt

from worldpoppy import *

# Define the area of interest
# Note: `bbox_from_location` runs a `Nominatim` query under the hood
aoi_box = bbox_from_location('Accra', width_km=500)  # returns (min_lon, min_lat, max_lon, max_lat)

# Fetch the population data
pop_data = wp_raster(  # returns xarray.DataArray
    product_name='pop_g1',  # curated worldpoppy product name (here: population data from the Global 1 series)
    aoi=aoi_box,  # passing a GeoDataFrame or one or more country codes would also work
    years=[2000, 2020],  # the years of interest
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value)
    chunks=None  # do *not* load data with Dask (default)
)

# Define target CRS and target resolution
aeqa_africa = "ESRI:102022"   # Africa Albers Equal Area Conic
out_res = 500  # resolution must be in units of the target CRS (here: metres)

# Warp the raster data
pop_data_agg = wp_warp(
    pop_data,
    to_crs=aeqa_africa,
    res=out_res,
    resampling='sum'  # Crucial: population counts should be summed
)

# Compute population changes on the warped (reprojected and downsampled) data
pop_change = pop_data_agg.sel(year=2020) - pop_data_agg.sel(year=2000)

# Plot
pop_change.plot(cmap='coolwarm', vmax=3_000, cbar_kwargs=dict(shrink=0.85))
clean_axis(title='Est. population change (2000 to 2020)', remove_xy_ticks=True)

# Add visual references
plot_country_borders(['GHA', 'TOG', 'BEN'], edgecolor='white', to_crs=aeqa_africa)
plot_location_markers(['Accra', 'Kumasi', 'Lomé'], to_crs=aeqa_africa)

plt.show()
