"""
Quickstart 2: Visualise Growth of Night-Light Emissions Around Sihanoukville (Cambodia)

Illustrates the time-series support of `worldpoppy` and Xarray's strong plotting functions.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, bbox_from_location, clean_axis

# 1. Fetch Time-Series: Get night-light data for Sihanoukville (Cambodia)
# Different years will be stacked along the 'year' dimension of the DataArray.
ntl_data = wp_raster(
    product_name="ntl_viirs_g2",  # Night lights from "Global 2" series
    aoi=bbox_from_location("Preah Sihanouk", width_km=100),
    years=[2015, 2023],
    masked=True
)

# 2. Plot: Side-by-side comparison
# We use Xarray's built-in plotting to create a facet grid by year.
p = (ntl_data + 1).plot(
    col="year",
    cmap="inferno",
    vmax=50,
    norm=LogNorm(),
    figsize=(10, 5),
    add_colorbar=False  # Remove since raw radiance units are rarely intuitive
)

# Make space for a "super" title
p.fig.subplots_adjust(top=0.875)

# Add the title in the resulting gap
p.fig.suptitle(
    'Night-light Growth in Sihanoukville',
    fontsize=12,
    fontweight='bold',
)


for ax in p.axs.flat:
    clean_axis(ax)

if __name__ == "__main__":
    plt.show()
