"""
Example 5: Visualise elevation data for the Ethiopian Great Rift

Illustrates usage of a static WorldPop product.
"""

import matplotlib.pyplot as plt

from worldpoppy import *

# Define the area of interest
aoi_box = bbox_from_location((41, 9), width_degrees=7)  # box around the Ethiopian Great Rift

topo_data = wp_raster(
    product_name='srtm_topo_100m',
    aoi=aoi_box,
    years=None,  # for static data products, the 'years' argument must always be None (default)
    masked=True,
)

# Downsample the data to speed-up plotting
lowres = topo_data.coarsen(x=10, y=10, boundary='trim').mean()

# Plot
lowres.plot(cmap='gist_earth', vmin=0, vmax=3_000, alpha=0.95)  # elevation data is in metres
clean_axis(title='Elevation in metres')

# Add visual reference
plot_location_markers(['Addis Abeba', 'Dire Dawa'])

plt.show()
