"""
Example 3: Visualise night-light emissions for two regions in Northern Italy.

Illustrates WorldPop data selection using a GeoDataFrame.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, clean_axis
from worldpoppy.examples import load_italian_regions

# Load shapes for two regions in Northern Italy
italy = load_italian_regions()
tuscany_emilia = italy[italy.name.isin(['Toscana', 'Emilia-Romagna'])]

# Fetch night-light data
viirs_data = wp_raster(
    product_name='viirs_100m',
    aoi=tuscany_emilia,
    years=2015,
    masked=True,
)

# Slightly downsample
lowres = viirs_data.coarsen(x=2, y=2, boundary='trim').mean()

# Plot
lowres.plot(vmin=1, cmap='inferno', norm=LogNorm(), cbar_kwargs=dict(shrink=0.875))
clean_axis(title='Night Lights (2015)\nTuscany & Emilia-Romagna ')

plt.show()
