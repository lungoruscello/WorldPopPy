"""
Working with Large Rasters, Example 2: Chile (Climatic Profile).

Illustrates:
1. Explicit, manual pre-clipping of large country rasters when the AoI
   is specified using **country codes**.
2. Using Dask (`chunks='auto'`) to lazy-load raster data.
3. Pre-coarsening raster data before further processing.
4. Managing the degree of parallelism, and thus the peak memory load,
   by explicitly setting the number of Dask workers.

Note:
    Specifying the Area of Interest (AoI) using a country code can lead to
    issues when a country's territory includes remote outlying islands.
    For instance, Chile includes Rapa Nui (Easter Island), resulting in
    WorldPop rasters that span thousands of kilometres of empty ocean.
    To address this issue, `wp_raster` provides the `pre_clip_bbox`
    argument. This forces a lazy pre-clip of the source raster to a
    specific region (e.g., the Chilean mainland), avoiding the processing
    of irrelevant data.
"""

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from dask.distributed import Client

from worldpoppy import (
    wp_raster,
    wp_warp,
    get_max_concurrency,
    plot_country_borders,
    clean_axes
)

# --- WARNING ---
print("WARNING: This example requires temperature & precipitation data for CHL (~600MB).")
print("If cached files do not exist, they will be downloaded.\n")
# ---------------

# --- Constants ---
ISO_CODE = 'CHL'
CHL_MAINLAND_BOX = (-76, -57, -66, -16)
# > Chile includes Rapa Nui (Easter Island), which extends the country's raster
#   grid thousands of km into the Pacific. The above bounding box allows us
#   to clip empty ocean space immediately after raster ingestion.

YEAR = 2023
ZERO_C_IN_KELVIN = 273.15
CHILE_CRS = "EPSG:20049"  # SIRGAS-Chile 2021 / UTM zone 19S


def get_processed_chile_data(product_name):
    """
    Fetch, coarsen, and warp one set of raster data for the Chile example.
    """

    # --- Lazy Data Fetch ---
    da = wp_raster(
        product_name=product_name,
        aoi=ISO_CODE,
        pre_clip_bbox=CHL_MAINLAND_BOX,  # Remove Rapa Nui and empty ocean space
        masked=True,
        years=YEAR,
        chunks='auto',  # Enable Dask -> Lazy load the pre-clipped data
    )

    # --- Clean Units ---
    if 'temp' in product_name:
        # Kelvin -> Celsius
        da = da - ZERO_C_IN_KELVIN

    # --- Define Coarsened Data (Lazy) ---
    # We "pre-coarsen" the raster data before warping it
    # to further reduce the memory footprint.
    da_coarse = da.coarsen(x=10, y=10, boundary='trim').mean()

    # --- Graph Execution ---
    # Trigger the computation, thus loading the processed raster into RAM
    da_coarse = da_coarse.load()

    # --- Warping ---
    # Now that downsampled data is in memory, warping is fast.
    da_warped = wp_warp(
        da_coarse,
        to_crs=CHILE_CRS,
        res=2_000,          # 2km target resolution
        resampling='mean',  # Average the weather/distance data
    )
    return da_warped


def make_plot():
    """
    Orchestrate the data fetching and plotting for the Chilean weather example.

    Returns a matplotlib Figure object.
    """

    # --- Configuration ---
    # We create a list of configs to iterate over.
    # Format: (Product Name, Plot Title, Year, Colormap_Func)
    plot_configs = [
        ("temp_mean_g2", f"Mean Temperature", get_temp_cmap),
        ("precip_mean_g2", f"Annual Precipitation", get_rain_cmap),
    ]

    # --- Dask Client Setup ---
    # We explicitly start a Dask Client so we can *manually* control the
    # number of workers. Fewer workers -> lower memory footprint.
    n_workers = min(4, get_max_concurrency())

    # Using a context manager ensures the client closes cleanly.
    with Client(n_workers=n_workers, threads_per_worker=1) as client:
        print(f"Dask Dashboard active at: {client.dashboard_link}")

        # --- Get Processed Data ---
        rasters = []
        for pname, title, _ in plot_configs:
            print(f"Fetching and processing '{pname}' ...")
            raster = get_processed_chile_data(pname)
            rasters.append(raster)

    # --- Plot Side-by-Side: Temperature & Precipitation ---
    # Make a standard canvas for the "repo gallery".
    fig, axarr = plt.subplots(1, len(rasters), figsize=(6, 6), layout='compressed')

    # 1. Adjust vertical padding
    # Since `subplots_adjust` does not work with the 'compressed'
    # layout, we talk to the layout engine directly
    fig.get_layout_engine().set(wspace=0.1)

    # 2. Set the "super" title
    fig.suptitle(f'Climatic Zones of Mainland Chile', fontsize=12, fontweight='bold')

    zipped = zip(axarr, rasters, plot_configs)
    for ax, raster, (pname, title, cmap_gen) in zipped:
        # A. Plot the Weather Raster
        # We disable the colorbar only to save space in the repo gallery
        cmap, norm = cmap_gen()
        raster.plot(
            ax=ax,
            cmap=cmap,
            norm=norm,
            add_colorbar=False)


        # B. Add country borders & clean up
        plot_country_borders('CHL', ax, CHILE_CRS, linewidth=0.3, edgecolor='black', alpha=0.6)
        clean_axes(title=title, ax=ax, fontsize=10)

    return fig


# --- Colormap Utilities ---

def get_rain_cmap():
    # Blue/Green for rain
    levels = [0, 10, 50, 100, 500, 1000, 2000, 4000, 6000]
    cmap = plt.get_cmap('YlGnBu', len(levels))
    norm = mcolors.BoundaryNorm(levels, ncolors=cmap.N, extend='max')
    return cmap, norm

def get_temp_cmap():
    # Red/Blue for temperature
    levels = [-5, 0, 5, 10, 15, 20, 25, 30]
    cmap = plt.get_cmap('RdYlBu_r', len(levels) + 1)
    norm = mcolors.BoundaryNorm(levels, ncolors=cmap.N, extend='both')
    return cmap, norm


if __name__ == "__main__":
    fig = make_plot()
    plt.show()
