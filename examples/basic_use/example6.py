"""
Example 6: Plot available population datasets for Rwanda.

Rwanda is chosen as an example because it is a contiguous state (as opposed
to an archipelago or city-state), but geographically small. This ensures the
raster files are lightweight enough for a quick demo while still offering
a rich, country-wide spatial distribution.

We plot the raster data for the year 2020 or (if not available) for whatever
available year is closest to 2020.
"""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm
from tqdm.autonotebook import tqdm
from worldpoppy import wp_manifest, wp_raster, clean_axis, plot_country_borders

# --- WARNING ---
print("WARNING: This example requires downloading approx. 60 MB of data.")
# ---------------

aoi = 'RWA'
TARGET_YEAR = 2020  # the specific year we want to approximate

# --- Discover available 'population' data ---
# We search the manifest for all products available for 'RWA' that
# also contain the keyword 'pop'.
print(f"Searching for population data available for {aoi}...")
matches = wp_manifest(iso3_codes=aoi, keywords='pop')

if matches.empty:
    raise ValueError("No matching data found.")

# --- Find the year closest to TARGET_YEAR ---
# We group the results by product name and select the best matching year.
products_to_plot = []
unique_names = matches['product_name'].unique()

for name in unique_names:
    # Get all entries for this specific product
    prod_entries = matches[matches['product_name'] == name]

    # Get the years available
    valid_years = prod_entries['year'].dropna()

    if not valid_years.empty:
        # Find the year minimizing the distance to TARGET_YEAR
        # valid_years is a pandas Series; we find the index of the minimum absolute difference
        closest_idx = (valid_years - TARGET_YEAR).abs().idxmin()
        plot_year = int(valid_years.loc[closest_idx])

        products_to_plot.append((name, plot_year))

# Sort by name for consistent plotting order
products_to_plot.sort()
print(f"Found {len(products_to_plot)} products. Fetching years closest to {TARGET_YEAR}: {products_to_plot}")

# --- Setup the plot grid ---
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

    print(f"Fetching {name} ({year})...")

    # Fetch data
    pop_data = wp_raster(
        product_name=name,
        aoi=aoi,
        years=year,
        masked=True
    )

    # Plot
    # Add small constant (0.1) to allow log-scale plotting of 0 values
    # (For simplicity, we do not re-project the raster data but keep
    # it in lat/lon degrees)
    (pop_data + 0.1).plot(
        ax=ax,
        norm=LogNorm(),
        cmap='inferno',
        cbar_kwargs={'label': 'Population count', 'shrink': 0.9}
    )

    clean_axis(ax, title=f"{name}\n({year})", remove_xy_ticks=True)
    plot_country_borders(aoi, ax=ax, linewidth=0.75)

# Hide any unused subplots (if products don't fill the last row)
for j in range(i + 1, len(axarr)):
    axarr[j].axis('off')

plt.suptitle(f"Population Data Comparison: {aoi} (~{TARGET_YEAR})", fontsize=15)
plt.show()
