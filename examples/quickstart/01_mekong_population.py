"""
Quickstart 1: Visualise Population Data for the Lower Mekong Region

Illustrates the ease of data fetching and merging with `worldpoppy`.
"""

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, clean_axis, plot_country_borders

# 1. Fetch & Merge Population Data
# `wp_raster` returns an xarray.DataArray ready for plotting.
countries = ['THA', 'KHM', 'LAO', 'VNM']
pop_data = wp_raster(
    product_name='pop_g2_1km_r25a',  # Low-res. population from "Global 2" series
    aoi=countries, years=2024, masked=True,
)

# 2. Plot on the Log-scale
# We use fillna(0) to represent areas without population and +1 to avoid log(0).
(pop_data.fillna(0) + 1).plot(norm=LogNorm(), cmap='inferno', size=6)

# Add borders & clean up
plot_country_borders(countries, edgecolor='white', linewidth=0.5)
clean_axis(
    title=f"Lower Mekong Region (2024):\n {pop_data.sum() / 1e6:.1f} Million People",
)

if __name__ == "__main__":
    plt.show()
