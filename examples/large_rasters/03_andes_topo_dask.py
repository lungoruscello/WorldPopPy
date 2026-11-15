"""
Working with Large Rasters, Example 3: The Andes (Topography Across Six Countries).

Illustrates:
1. Merging the rasters of several large countries lazily with Dask (`chunks`).
2. Explicit, manual pre-clipping (`pre_clip_bbox`) to drop remote islands.
3. Coarsening lazily before computing, with chunks that are a multiple of the
   coarsening window.
4. Limiting GDAL's block cache to reduce memory use.

Note:
    The elevation rasters of the six Andean countries form a merged grid of
    about 34,000 x 83,000 cells at 3 arcseconds, i.e. about 11 GB of 32-bit
    floats. With `chunks`, `wp_raster` merges the country rasters lazily, as
    one virtual mosaic that Dask reads chunk by chunk. Coarsening the data
    before computing it then keeps memory use far below the size of the
    merged grid.
"""

import os

# GDAL's block cache defaults to 5% of the computer's memory. A small cache
# (here: 64 MB) lowers memory use when reading large rasters chunk by chunk.
# The variable must be set before GDAL reads any data.
os.environ.setdefault('GDAL_CACHEMAX', '64')

import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

from worldpoppy import (  # noqa: E402
    wp_raster,
    wp_warp,
    plot_country_borders,
    plot_location_markers,
    clean_axes,
)

# --- Constants ---
ANDEAN_COUNTRIES = ['COL', 'ECU', 'PER', 'BOL', 'CHL', 'ARG']
SOUTH_AMERICA_BOX = (-81.5, -56, -53, 13)
# > Chile's raster includes Rapa Nui (Easter Island), and Ecuador's the
#   Galápagos Islands, which would extend the merged grid thousands of km into
#   the Pacific. The above bounding box keeps the South American mainland only.

COARSENING_FACTOR = 12  # 3 arcseconds -> 36 arcseconds (about 1 km)
CHUNK_SIZE = 4092  # a multiple of COARSENING_FACTOR, so Dask need not re-chunk
SOUTH_AMERICA_CRS = 'ESRI:102033'  # South America Albers Equal Area Conic


def get_processed_andes_data():
    """
    Fetch, coarsen, and warp the elevation data for the Andes example.
    """

    # --- Lazy Data Fetch ---
    da = wp_raster(
        product_name='srtm_elevation_g1',
        aoi=ANDEAN_COUNTRIES,
        pre_clip_bbox=SOUTH_AMERICA_BOX,  # Remove Rapa Nui, the Galápagos and empty ocean
        chunks=CHUNK_SIZE,  # Enable Dask -> merge the country rasters lazily
    )
    print(f"Merged grid (lazy): {da.sizes['y']:,} x {da.sizes['x']:,} cells")

    # --- Define Coarsened Data (Lazy) ---
    da_coarse = da.coarsen(
        x=COARSENING_FACTOR, y=COARSENING_FACTOR, boundary='trim'
    ).mean()

    # --- Graph Execution ---
    # Trigger the computation, thus loading only the coarsened raster into RAM
    da_coarse = da_coarse.load()
    print(f"Coarsened grid: {da_coarse.sizes['y']:,} x {da_coarse.sizes['x']:,} cells")

    # --- Warping ---
    # The Andes span several UTM zones; an equal-area projection for South
    # America keeps distortions small across the whole range.
    da_warped = wp_warp(
        da_coarse,
        to_crs=SOUTH_AMERICA_CRS,
        res=2_500,  # Target resolution in units of 'to_crs' (here: metres)
        resampling='mean',  # Average the elevation
    )
    return da_warped


def make_plot():
    """
    Orchestrate the data fetching and plotting for the Andes example.

    Returns a matplotlib Figure object.
    """
    andes_topo = get_processed_andes_data()

    # --- Plot ---
    # Make a standard canvas for the "repo gallery".
    fig, ax = plt.subplots(figsize=(6, 6), layout='compressed')

    # We disable the colorbar merely to save space in the gallery. 'terrain'
    # turns green at about a quarter of its range, so vmin is chosen to put
    # sea level there (lowlands green, high mountains brown and white).
    andes_topo.plot(
        cmap='terrain', ax=ax,
        vmin=-1_700, vmax=5_000,
        add_colorbar=False,
    )
    plot_country_borders(
        ANDEAN_COUNTRIES, ax, SOUTH_AMERICA_CRS, linewidth=0.3, edgecolor='black', alpha=0.6
    )

    # Annotate
    locations = [
        (-78.817, -1.469, 'Chimborazo'),
        (-77.604, -9.122, 'Huascarán'),
        (-70.011, -32.653, 'Aconcagua'),
    ]
    plot_location_markers(
        locations,
        ax=ax,
        xytext=(8, 0),
        to_crs=SOUTH_AMERICA_CRS,
        color='white',
        s=10,
        edgecolors='black',
        fontsize=8,
        fontweight='bold',
        other_annotate_kwargs=dict(
            path_effects=[pe.withStroke(linewidth=1.5, foreground='black')]
        ),
    )

    clean_axes(title='The Andes: Topography Across Six Countries', ax=ax, fontweight='bold')
    return fig


if __name__ == "__main__":
    # --- WARNING ---
    print("WARNING: This example requires the elevation rasters of six countries (~800 MB).")
    print("If cached files do not exist, they will be downloaded.\n")
    # ---------------

    fig = make_plot()
    plt.show()
