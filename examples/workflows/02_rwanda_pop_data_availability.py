"""
Workflow Example 2: Plot All Available Population Data Products for One Country.

Rwanda is a good example because it is a contiguous state (as opposed to an
archipelago or city-state), but geographically small. This ensures the raster
files are lightweight enough for a quick demo while still offering a rich,
country-wide spatial distribution.

We plot the raster data for whatever available year is closest to 2020.

Download Requirements:
    This example requires population rasters totalling ~80 MB in size.
    If cached files do not exist, they will be downloaded.
"""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm
from tqdm.autonotebook import tqdm
from worldpoppy import *

aoi = 'RWA'
TARGET_YEAR = 2020  # the specific year we want to approximate
AEQA_AFRICA = "ESRI:102022"  # Africa Albers Equal Area Conic projection

# --- Discover Available 'pop' Data ---
# We search the manifest for all products available for 'RWA' that
# also contain the keyword 'pop'.
print(f"Searching for population data available for {aoi}...")
matches = wp_manifest(iso3_codes=aoi, keywords='pop')

if matches.empty:
    raise ValueError("No matching data found.")

# --- Find the Year Closest to TARGET_YEAR ---
# We group the results by product name and select the best matching year.
products_to_plot = []
unique_names = matches['product_name'].unique()

for name in unique_names:
    # Get all entries for this specific product
    prod_entries = matches[matches['product_name'] == name]

    # Get the years available
    valid_years = prod_entries['year'].dropna()

    if not valid_years.empty:
        # Find the year minimising the distance to TARGET_YEAR
        # valid_years is a pandas Series; we find the index of the minimum absolute difference
        closest_idx = (valid_years - TARGET_YEAR).abs().idxmin()
        plot_year = int(valid_years.loc[closest_idx])

        products_to_plot.append((name, plot_year))

# Sort by name for consistent plotting order
products_to_plot.sort()
print(f"Found {len(products_to_plot)} products. Fetching years closest to {TARGET_YEAR}: {products_to_plot}")

# --- Plot Grid Setup ---
num_cols = 3
num_rows = -(-len(products_to_plot) // num_cols)  # ceiling division
fig, axarr = plt.subplots(num_rows, num_cols, figsize=(6, 6), layout='compressed')
axarr = np.array(axarr).flatten()  # flatten array for easy iteration

# One shared colour scale for all products. The products differ in resolution
# (100 m and 1 km cells), so we compare population densities (people per km²)
# rather than counts per cell.
norm = LogNorm(vmin=10, vmax=20_000)

# --- Iterate and Plot ---
for ax, (name, year) in zip(axarr, tqdm(products_to_plot, leave=False)):
    pop_data = wp_raster(product_name=name, aoi=aoi, years=year)

    # Convert counts per cell to people per km² (cells shrink towards the poles)
    res_x, res_y = pop_data.rio.resolution()
    cell_km2 = abs(res_x * res_y) * 111.32 ** 2 * np.cos(np.deg2rad(pop_data.y))
    density = pop_data / cell_km2

    # Add 1 to allow log-scale plotting of 0 values. We do NOT fill missing
    # values (shown in white), to highlight that different WorldPop data products
    # handle unpopulated areas differently (zero vs. no estimate).
    img = (density + 1).plot(ax=ax, norm=norm, cmap='inferno', add_colorbar=False)
    plot_country_borders(aoi, ax=ax, linewidth=0.5)
    clean_axes(ax, title=f"{name} ({year})", fontsize=7)

# Hide any unused subplots (if products do not fill the last row)
for ax in axarr[len(products_to_plot):]:
    ax.axis('off')

fig.colorbar(img, ax=axarr, orientation='horizontal', shrink=0.6, label='People per km²')
fig.suptitle(f"Population Data Products for Rwanda (~{TARGET_YEAR})", fontweight='bold')

if __name__ == "__main__":
    plt.show()
