"""
Quickstart 2: Visualise Growth of Night-Light Emissions Around Sihanoukville (Cambodia)

Illustrates `worldpoppy`'s time-series support and Xarray's powerful built-in plotting functions.

Download Requirements:
    This example requires night-light rasters for KHM (~9 MB).
    If cached files do not exist, they will be downloaded.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, bbox_from_location, clean_axes

# Fetch Time-Series: Night-light data around Sihanoukville
# Different years will be stacked along the 'year' dimension of the DataArray.
ntl_data = wp_raster(
    product_name="ntl_viirs_g2",  # Night lights from "Global 2" series
    aoi=bbox_from_location("Sihanoukville Special Economic Zone", width_km=45),
    years=['first', 'last'],  # Request first & last available year
)

# Plot Both Years Side-by-Side (Log-scale)
# We use Xarray's built-in plotting to create a facet grid by year.
p = (ntl_data + 1).plot(
    col="year",
    subplot_kws={"facecolor": "#c8d3dc"},  # sea (no data) in light grey-blue
    cmap="inferno",
    vmax=50,
    norm=LogNorm(),
    figsize=(10, 5),
    add_colorbar=False  # Remove since radiance units are not intuitive
)

p.fig.suptitle('Night-light Growth in Sihanoukville', fontsize=12, fontweight='bold')
p.fig.subplots_adjust(top=0.875)
clean_axes(p)

if __name__ == "__main__":
    plt.show()
