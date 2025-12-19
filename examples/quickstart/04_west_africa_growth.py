"""
Quickstart 4: Visualise Population Growth in West Africa

Illustrates raster reprojection (warping) and additional plotting options.
"""

import matplotlib.pyplot as plt
from worldpoppy import *

# 1. Fetch & Merge Population Data
# We grab a large 1,000km slice of the West African coast.
pop_data = wp_raster(
    product_name='pop_g2_1km_r25a',  # Low-res. population from "Global 2" series
    aoi=bbox_from_location('Lomé', width_km=1_000),
    years=[2015, 2024],
    masked=True,
)

# 2. Reproject and Downsample
# We warp to an Equal Area projection for accurate math.
aeqa_africa = "ESRI:102022"  # Africa Albers Equal Area Conic
pop_warped = wp_warp(
    pop_data,
    to_crs=aeqa_africa,
    res=1_000,        # Target resolution in units of 'to_crs' (here: metres)
    resampling='sum'  # Crucial: Sum population when resampling, do not average it
)

# 3. Compute Change
pop_change = pop_warped.sel(year=2024) - pop_warped.sel(year=2015)

# 4. Visualise the Change Raster
# Make a standard canvas for the "repo gallery".
fig, ax = plt.subplots(figsize=(6, 6), layout='compressed')

# vmax=2000 clips the data to highlight the general urban spread.
# We disable the colorbar only to save space in the gallery.
pop_change.plot(
    cmap='coolwarm', ax=ax,
    vmax=2_000, add_colorbar=False,
)
clean_axes(
    title="The Abidjan-Lagos Corridor:\nPopulation Boom (2015-2024)",
    fontweight='bold'
)

# 5. Plot Context Information
# We plot the three central countries only to avoid double-drawing shared borders.
plot_country_borders(['GHA', 'TOG', 'BEN'], to_crs=aeqa_africa)
plot_location_markers(
    ['Abidjan', 'Accra', 'Lomé', 'Lagos'],
    to_crs=aeqa_africa, s=10,
)

if __name__ == "__main__":
    plt.show()
