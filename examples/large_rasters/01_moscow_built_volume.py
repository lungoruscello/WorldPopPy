"""
Working with Large Rasters, Example 1: Moscow (Built-up Volume).

Illustrates:
1. A shapely geometry as the AoI (here: a circle with a radius of 60 km).
2. Automatic pre-clipping of a large country raster to a small AoI.
3. A logarithmic colour scale and a scale bar.

Note:
    WorldPop stores its data by country, so this example needs the full
    built-up volume raster for Russia (~420 MB). `wp_raster` then reads only
    the part of it around the AoI into memory.

    WorldPop derived this product from the built-up volume layer of the Global
    Human Settlement Layer (GHSL) of the European Commission's Joint Research
    Centre, combined with building footprints from Microsoft, Google and the
    World Settlement Footprint. Each cell holds the built-up volume in cubic
    metres.
"""

import geopandas
import matplotlib.colors as mcolors
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.axes_grid1.anchored_artists import AnchoredSizeBar
from shapely.geometry import Point

from worldpoppy import wp_raster, wp_warp, clean_axes, plot_location_markers

# --- Constants ---
KREMLIN = (37.6176, 55.7520)  # (lon, lat)
RADIUS_KM = 60
SCALE_BAR_KM = 20
UTM_37N = 'EPSG:32637'  # UTM zone 37N covers Moscow


def make_circle_aoi(centre, radius_km):
    """
    Return a circle on the ground around `centre` as a shapely polygon in WGS84.
    """
    # A circle drawn in degrees would be an ellipse on the ground. Instead, the
    # circle is drawn in metres, in an azimuthal equidistant projection centred
    # on `centre`, and then converted to WGS84.
    lon, lat = centre
    local_crs = f'+proj=aeqd +lat_0={lat} +lon_0={lon} +units=m'
    circle = geopandas.GeoSeries([Point(0, 0)], crs=local_crs).buffer(
        radius_km * 1_000,
        quad_segs=64,  # Enough points for a smooth outline at this size
    )
    return circle.to_crs('EPSG:4326').iloc[0]


def get_processed_moscow_data(aoi):
    """
    Fetch and warp the built-up volume data for the Moscow example.
    """

    # --- Fetch Data ---
    # The downloader must fetch the full raster for Russia, because this is how
    # WorldPop stores its data. When reading it, `wp_raster` detects that the
    # AoI is small and keeps only the cells within a slightly enlarged bounding
    # box around it, so memory use stays low.
    volume = wp_raster('built_volume_g2', aoi=aoi, years=2020)
    print(f"Original shape: {volume.shape}")

    # --- Warp ---
    # In UTM zone 37N, distances on this map are true to within 0.04%, so the
    # scale bar is valid everywhere on it.
    volume_warped = wp_warp(
        volume,
        to_crs=UTM_37N,
        res=100,  # Target resolution in units of 'to_crs' (here: metres)
        resampling='mean',
    )
    print(f"Warped shape: {volume_warped.shape}")
    return volume_warped


def make_plot():
    """
    Orchestrate the data fetching and plotting for the Moscow example.

    Returns a matplotlib Figure object.
    """
    aoi = make_circle_aoi(KREMLIN, RADIUS_KM)
    volume = get_processed_moscow_data(aoi)

    # --- Plot ---
    # Make a standard canvas for the "repo gallery".
    fig, ax = plt.subplots(figsize=(6, 6), layout='compressed')

    # Built-up volume spans several orders of magnitude, from scattered houses
    # to the city centre, so the colour scale is logarithmic. Cells without
    # buildings stay black. The upper limit (99.9th percentile) keeps a few
    # extreme cells from washing out the rest. We disable the colorbar merely
    # to save space in the gallery.
    built_up = volume.where(volume > 0)
    vmax = float(np.nanquantile(built_up.values, 0.999))
    built_up.plot(
        cmap='magma', ax=ax,
        norm=mcolors.LogNorm(vmin=1_000, vmax=vmax),
        add_colorbar=False,
    )
    ax.set_facecolor('black')

    # Outline of the AoI
    geopandas.GeoSeries([aoi], crs='EPSG:4326').to_crs(UTM_37N).boundary.plot(
        ax=ax, color='grey', linewidth=0.6
    )

    # Scale bar
    ax.add_artist(AnchoredSizeBar(
        ax.transData,
        SCALE_BAR_KM * 1_000,  # Length in units of the map (here: metres)
        f'{SCALE_BAR_KM} km',
        loc='lower left',
        pad=0.8,
        color='white',
        frameon=False,
        size_vertical=300,  # Thickness of the bar in metres
        fontproperties={'size': 8},
    ))

    # Annotate
    locations = [
        (*KREMLIN, 'Kremlin'),
        (37.2140, 55.9910, 'Zelenograd'),
    ]
    plot_location_markers(
        locations,
        ax=ax,
        xytext=(8, 0),
        to_crs=UTM_37N,
        color='white',
        s=10,
        edgecolors='black',
        fontsize=8,
        fontweight='bold',
        other_annotate_kwargs=dict(
            path_effects=[pe.withStroke(linewidth=1.5, foreground='black')]
        ),
    )

    clean_axes(title='Moscow: Built-up Volume (2020)', ax=ax, fontweight='bold')
    return fig


if __name__ == "__main__":
    # --- WARNING ---
    print("WARNING: This example requires the built-up volume raster for Russia (~420 MB).")
    print("If a cached file does not exist, it will be downloaded.\n")
    # ---------------

    fig = make_plot()
    plt.show()
