"""
Workflow Example 2: Plot All Available Population Data Products for One Country.

Rwanda is a good example because it is a contiguous state (as opposed to an
archipelago or city-state), but geographically small. This ensures the raster
files are lightweight enough for a quick demo while still offering a rich,
country-wide spatial distribution.

We plot the raster data for whatever available year is closest to 2020.
"""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm
from tqdm.autonotebook import tqdm
from worldpoppy import *

# --- WARNING ---
print("WARNING: This example requires downloading approx. 60 MB of data.")
# ---------------

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

# --- Plot Grid Setup---
num_prods = len(products_to_plot)
num_cols = 3
num_rows = -(-num_prods // num_cols)  # ceiling division

fig, axarr = plt.subplots(
    num_rows, num_cols,
    figsize=(12, 3.5 * num_rows),
    constrained_layout=True
)
axarr = np.array(axarr).flatten()  # flatten array for easy iteration

# --- Iterate and Plot ---
for i, (name, year) in enumerate(tqdm(products_to_plot, leave=False)):
    ax = axarr[i]

    # Fetch data
    pop_data = wp_raster(
        product_name=name,
        aoi=aoi,
        years=year,
        masked=True
    )

    # Plot
    # Add 1 to allow log-scale plotting of 0 values.
    # Crucial: We do NOT fill NaN values in this example to highlight that
    # different WorldPop data products handle unpopulated areas differently.
    (pop_data + 1).plot(
        ax=ax,
        norm=LogNorm(),
        cmap='inferno',
        cbar_kwargs={'label': 'Population count'}
    )

    clean_axis(ax, title=f"{name}\n({year})")
    plot_country_borders(aoi, ax=ax, linewidth=0.75)

# Hide any unused subplots (if products don't fill the last row)
for j in range(i + 1, len(axarr)):
    axarr[j].axis('off')

plt.suptitle(f"Population Data Comparison: {aoi} (~{TARGET_YEAR})", fontsize=15)

if __name__ == "__main__":
    plt.show()

