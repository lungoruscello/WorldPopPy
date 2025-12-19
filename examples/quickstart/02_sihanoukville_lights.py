"""
Quickstart 2: Visualise Growth of Night-Light Emissions Around Sihanoukville (Cambodia)

Illustrates the time-series support of `worldpoppy` and Xarray's strong plotting functions.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, bbox_from_location, clean_axes

# Fetch Time-Series: Night-light data for Sihanoukville
# Different years will be stacked along the 'year' dimension of the DataArray.
ntl_data = wp_raster(
    product_name="ntl_viirs_g2",  # Night lights from "Global 2" series
    aoi=bbox_from_location("Preah Sihanouk", width_km=100),
    years=[2015, 2023],
)

# Plot 2015 vs 2023 (Log-scale)
# We use Xarray's built-in plotting to create a facet grid by year.
p = (ntl_data + 1).plot(
    col="year",
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
