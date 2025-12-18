"""
Working with Large Rasters, Example 2: Chile (Weather Data).

Illustrates:
1. Explicit, manual pre-clipping of large country rasters when the AoI
   is specified using **country codes**.
2. Using Dask (`chunks='auto'`) to lazy-load raster data.
3. Managing the degree of parallelism, and thus the peak memory load,
   by explicitly setting the number of Dask workers.

Note:
    Specifying the Area of Interest (AoI) using a country code can lead to
    issues when a country's territory includes remote outlying islands.
    This is the case for Chile, which enjoys sovereignty over Rapa Nui
    (Easter Island), which lies thousands of kilometres from the Chilean
    "mainland". WorldPop rasters for Chile are therefore very large and
    cover extensive areas in the South Pacific that will not be of interest
    in a typical analysis. To handle such cases, `wp_raster` provides
    the `pre_clip_bbox` argument by which users can force explicit, lazy
    pre-clipping of a country raster. We illustrate this below using
    data on average temperature and precipitation for Chile.
"""

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from dask.distributed import Client

from worldpoppy import (
    wp_raster,
    wp_warp,
    get_max_concurrency,
    plot_country_borders,
    clean_axis
)

# --- Constants ---

ISO_CODE = 'CHL'
CHL_MAINLAND_BOX = (-76, -57, -66, -16)
# > Chile includes Rapa Nui (Easter Island), which extends the country's raster
#   grid thousands of km into the Pacific. The above bounding box allows us
#   to clip empty ocean space immediately after raster ingestion.

YEAR = 2023
ZERO_C_IN_KELVIN = 273.15
UTM_19S = "EPSG:32719"  # Chile is located in UTM Zone 19S


# --- Main Plot Orchestration ---

def make_plot():
    """
    Orchestrate the data fetching and plotting for the Chilean weather example.

    Returns a matplotlib Figure object.
    """
    # --- WARNING ---
    print("WARNING: This example requires temperature & precipitation data for CHL (~600MB).")
    print("If cached files do not exist, they will be downloaded.\n")
    # ---------------

    # --- Setup ---
    product_names = ["temp_mean_g2", "precip_mean_g2"]
    titles = ['Mean Temp.', 'Precipitation']

    # Generate custom colorbars
    cbar_opts = (get_temp_cbar(), get_rain_cbar())

    # --- Dask Client Setup ---
    # We explicitly start a Dask Client so we can *manually* control the
    # number of workers. Limiting that number reduces memory footprint during
    # the heavy re-projection steps.
    n_workers = min(4, get_max_concurrency())

    # We use a context manager to ensure the client closes cleanly
    with Client(n_workers=n_workers, threads_per_worker=1) as client:
        print(f"Dask Dashboard active at: {client.dashboard_link}")

        # --- Get Weather Data ---
        weather_rasters = []
        for pname in product_names:
            print(f"Fetching and processing data for '{pname}'...")
            raster = get_processed_chile_data(pname)

            # Convert Kelvin to Celsius for the temperature raster
            if 'temp' in pname:
                raster -= ZERO_C_IN_KELVIN

            weather_rasters.append(raster)

    # --- Plot Rasters Side-by-Side ---
    fig, axarr = plt.subplots(1, 2, figsize=(6, 6), layout='compressed')

    # 1. Adjust vertical padding
    # Since `subplots_adjust` does not work with the 'compressed'
    # layout, we talk the layout engine directly
    fig.get_layout_engine().set(wspace=0.1)

    # 2. Set the "super" title
    fig.suptitle('Weather, Mainland Chile (2023)', fontsize=12, fontweight='bold')

    zipped = zip(axarr, weather_rasters, titles, cbar_opts)
    for ax, raster, title, (cmap, cnorm) in zipped:

        # A. Plot the Weather Raster
        # We disable the colorbar to save space in the 2x2 "repo gallery".
        raster.plot(
            ax=ax,
            cmap=cmap,
            norm=cnorm,
            add_colorbar=False
        )

        # B. Add country borders & clean up
        plot_country_borders('CHL', ax, UTM_19S, linewidth=0.3, edgecolor='black', alpha=0.6)
        clean_axis(title=title, ax=ax, fontsize=10)

    return fig


# --- Data Fetch Helper ---

def get_processed_chile_data(product_name):
    """
    Fetch, coarsen, and warp one set of raster data for the Chile example.
    """
    # --- Lazy Data Fetch ---
    da = wp_raster(
        product_name=product_name,
        aoi=ISO_CODE,
        pre_clip_bbox=CHL_MAINLAND_BOX,
        masked=True,
        years=YEAR,
        chunks='auto',  # Enable Dask --> Lazy load the pre-clipped data
    )

    # --- Lazy Coarsening ---
    # We "pre-coarsen" the data before re-projecting is to
    # reduce the memory footprint further.
    da_coarse = da.coarsen(x=10, y=10, boundary='trim').mean()

    # --- Graph Execution ---
    # Trigger the computation, load the processed raster into RAM
    da_coarse = da_coarse.load()

    # --- Warping ---
    # Now that downsampled data is in memory, warping is fast.
    da_warped = wp_warp(
        da_coarse,
        to_crs=UTM_19S,
        res=2_000,          # 2km target resolution
        resampling='mean',  # Average the weather data
    )
    return da_warped


# --- Colormap Utilities ---

def get_rain_cbar():
    """
    Generate a custom colormap and BoundaryNorm for precipitation.
    Uses 'YlGnBu' to map Dry (Yellow) onto Wet (Blue).
    """
    # Discrete intervals suitable for Chile's extreme range
    levels = [0, 5, 10, 25, 50, 100, 250, 500, 750, 1000, 2000, 4000, 6000]

    # We need exactly len(levels) colours because extend='max' adds one extra bin
    cmap = plt.get_cmap('YlGnBu', len(levels))

    norm = mcolors.BoundaryNorm(levels, ncolors=cmap.N, extend='max')
    return cmap, norm


def get_temp_cbar():
    """
    Generate a custom colormap and BoundaryNorm for temperature.
    Uses 'RdYlBu_r' to map Cold (Blue) onto Hot (Red).
    """
    # 5-degree intervals covering the full climatic range
    levels = [-10, -5, 0, 5, 10, 15, 20, 25, 30]

    # We need (len(levels) + 1) colours because extend='both' adds TWO extra bins
    cmap = plt.get_cmap('RdYlBu_r', len(levels) + 1)

    norm = mcolors.BoundaryNorm(levels, ncolors=cmap.N, extend='both')
    return cmap, norm


if __name__ == "__main__":
    fig = make_plot()
    plt.show()
