"""
Working with Large Rasters, Example 2: Chile and Argentina (OSM Major Road Distance).

Illustrates:
1. Explicit, manual pre-clipping of a large country raster when the AoI
   is specified using **country codes**.
2. Using Dask (`chunks='auto'`) to lazy-load raster data.
3. Managing the degree of parallelism, and thus the peak memory load,
   by explicitly setting the number of Dask workers.

Note:
    This example visualises data on distance to the nearest major road for Chile
    and Argentina. The Area of Interest (AoI) is specified using country codes.
    When doing so, `worldpoppy` applies NO automated pre-clipping to the raster
    data. This is an issue for data from Chile since the country's territory
    includes the remote Pacific Island of Rapa Nui (Easter Island). As such,
    the raw raster file for Chile is very large and comprises extensive areas
    in the South Pacific that are not of interest in a typical analysis. When
    the AoI with country codes, users can therefore provide an **explicit bounding
    box** for pre-clipping as illustrated below.
"""

import matplotlib.pyplot as plt
from dask.distributed import Client
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, wp_warp, clean_axis


def make_plot():
    # --- WARNING ---
    print("WARNING: This example requires road-distance data for ARG and CHL (~1.5 GB).")
    print("If cached files do not exist, they will be downloaded.\n")

    print("WARNING: During testing, this example required up to 11 GB of free system memory.")
    # ---------------

    # --- Dask Client Setup---
    # We explicitly start a Client to control resources.
    # n_workers=4: Limits concurrency. We only process 4 raster chunks at a time.
    #              While this reduces memory load, the "base load" is still very
    #              for this example
    with Client(n_workers=4, threads_per_worker=1) as client:
        print(f"Dask Dashboard active at: {client.dashboard_link}")

        # --- Define AoI and Clip-box ---
        iso_codes = ['CHL', 'ARG']

        # Chile includes Rapa Nui (Easter Island), which extends the raster grid
        # thousands of km into the Pacific. We use a bounding box to clip this
        # empty ocean space immediately after ingestion.
        mainland_box = (-78, -58, -52, -11)

        # --- Lazy Data Fetch ---
        # We use explicit chunks (4096) rather than 'auto'.
        # 1. chunks='auto' often picks chunks that misalign with our coarsening factor.
        # 2. 4096 is divisible by 16 (our coarsening factor), allowing Dask to
        #    process chunks cleanly without loading too many neighbours.
        arg_chl_roads = wp_raster(
            product_name='dist_roads_g2',
            aoi=iso_codes,
            pre_clip_bbox=mainland_box,
            masked=True,
            chunks=4096  # Explicit chunks
        )
        print(f"1. Lazy Dask Grid (before coarsening): {arg_chl_roads.shape}")

        # --- Lazy Coarsening ---
        # We downsample the data by a factor of 16.
        # Since 4096 / 16 = 256, our new chunks will be small (256x256).
        arg_chl_roads_coarse = arg_chl_roads.coarsen(x=16, y=16, boundary='trim').mean()
        print(f"2. Lazy Dask Grid (after coarsening): {arg_chl_roads_coarse.shape}")

        # --- Graph Execution ---
        # We trigger the computation now. Dask will:
        #   1. Read raw chunks (throttled to 4 at a time).
        #   2. Coarsen them.
        #   3. Return a small, manageable NumPy array to memory.

        print("2. Loading coarsened data into memory (Check Dashboard for progress)...")
        arg_chl_roads_loaded = arg_chl_roads_coarse.load()
        print(f"   Load complete. In-memory shape: {arg_chl_roads_loaded.shape}")

        # --- Warping ---
        # Now that downsampled data is in memory, warping becomes fast and cheap.
        # UTM Zone 19S (EPSG:32719) provides low distortion for Chile/Argentina.
        print("3. Warping to UTM Zone 19S...")
        arg_chl_roads_warped = wp_warp(
            arg_chl_roads_loaded,
            to_crs="EPSG:32719",
            res=2_500,  # 2.5km target resolution
            resampling='mean'
        )

        # --- PLOT ---
        # We set a small 'vmin' to avoid log(0) errors.
        (arg_chl_roads_warped).plot(
            norm=LogNorm(),
            cmap='magma_r',
            size=6,
            vmin=0.1,
            # The colorbar *is* informative, but we need to save space in the 2x2 "repo gallery"
            add_colorbar=False,
            #cbar_kwargs={"label": "Distance to nearest road (km)"},
        )
        clean_axis(title='Argentina & Chile:\nDistance to Major Roads, 2023')

        # Return the figure so the caller can save it
        return plt.gcf()


if __name__ == "__main__":
    fig = make_plot()
    plt.show()
