"""
Collection of various helper functions.
"""

from functools import lru_cache
from typing import Tuple

import backoff
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from matplotlib import pyplot as plt
from pyproj import Transformer

from worldpoppy.config import WGS84_CRS
from worldpoppy.manifest import get_all_isos

__all__ = [
    "geolocate_name",
    "clean_axis",
    "plot_country_borders",
    "plot_location_markers",
    "module_available"
]


@lru_cache(maxsize=1024)
@backoff.on_exception(backoff.expo, GeocoderTimedOut, max_tries=5)
def geolocate_name(nomatim_query, to_crs=None):
    """
    Return the geo-coordinate associated with a given location name,
    based on search results from OSM's 'Nominatim' service.

    Parameters
    ----------
    nomatim_query : str
        A location name to be geocoded.
    to_crs : pyproj.CRS or str, optional
        If specified, transforms the returned coordinate from (lon, lat)
        to this CRS.

    Returns
    -------
    Tuple[float, float]
        The (x, y) coordinate in the target CRS, or (lon, lat) in WGS84
        if `to_crs` is None.

    Raises
    ------
    RuntimeError
        If the Nominatim query has returned None.
    """
    geolocator = Nominatim(user_agent="MyLocationCacher", timeout=2)
    located = geolocator.geocode(nomatim_query)

    if located is None:
        raise RuntimeError(f"Nomatim search for location name '{nomatim_query}' returned no hit.")

    lon, lat = located.point.longitude, located.point.latitude
    if to_crs is None:
        return lon, lat

    transformer = Transformer.from_crs(WGS84_CRS, to_crs, always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y


def plot_country_borders(iso3_codes, ax=None, to_crs=None, **kwargs):
    """
    Plot country borders on a matplotlib axis.

    Parameters
    ----------
    iso3_codes : str or list of str
        One or more ISO3 country codes, or the 'all' keyword.
    ax : matplotlib.axes.Axes, optional
        Axis on which to plot. If None, uses current axis.
    to_crs : pyproj.CRS or str, optional
        If specified, projects the country borders from WGS84 to this CRS.
    **kwargs :
        Additional keywords passed to `GeoDataFrame.plot`.
    """
    from worldpoppy.borders import load_country_borders

    if isinstance(iso3_codes, str):
        iso3_codes = get_all_isos() if iso3_codes == "all" else [iso3_codes]

    ax = plt.gca() if ax is None else ax

    user_kwargs = dict() if kwargs is None else kwargs
    kwargs = dict(color='None', edgecolor='black', linewidth=1)
    kwargs.update(**user_kwargs)

    world = load_country_borders()
    gdf = world[world.iso3.isin(iso3_codes)]
    if to_crs is not None:
        gdf = gdf.to_crs(to_crs)
    gdf.plot(ax=ax, **kwargs)


def plot_location_markers(
        locations,
        ax=None,
        annotate=True,
        textcoords="offset points",
        xytext=(7, -7),
        ha='left',
        va='center',
        other_annotate_kwargs=None,
        to_crs=None,
        **scatter_kwargs
):
    """
    Plot markers for geolocated place names on a matplotlib axis.
    Optionally annotate the location markers as well.

    Parameters
    ----------
    locations : str or list of str
        Location name(s) to geolocate and plot.
    ax : matplotlib.axes.Axes, optional
        Axis on which to plot. If None, uses current axis.
    annotate : bool, default=True
        Whether to annotate points with their names.
    textcoords : str, default="offset points"
        Coordinate system for annotation positioning.
    xytext : tuple of int, default=(7, -7)
        Offset of annotation text from the marker.
    ha : str, default='left'
        Horizontal alignment of the annotation text.
    va : str, default='center'
        Vertical alignment of the annotation text.
    other_annotate_kwargs : dict, optional
        Additional keyword arguments passed to `annotate`.
    to_crs : pyproj.CRS or str, optional
        If specified, projects the geo-coordinate from WGS84 to this CRS.
    **scatter_kwargs :
        Additional keywords passed to `scatter`.
    """
    ax = plt.gca() if ax is None else ax

    user_scatter_kwargs = dict() if scatter_kwargs is None else scatter_kwargs
    scatter_kwargs = dict(color='k', s=5)
    scatter_kwargs.update(**user_scatter_kwargs)

    other_annotate_kwargs = dict() if other_annotate_kwargs is None else other_annotate_kwargs

    if isinstance(locations, str):
        locations = [locations]

    for name in locations:
        lon_lat = geolocate_name(name, to_crs)
        ax.scatter(*lon_lat, **scatter_kwargs)
        if annotate:
            ax.annotate(
                name,
                lon_lat,
                textcoords=textcoords,
                xytext=xytext,  # noqa
                ha=ha,
                va=va,
                **other_annotate_kwargs,
            )


def clean_axis(ax=None, title=None, remove_xy_ticks=False):
    """
    Clean up a matplotlib axis by removing labels and setting equal aspect.

    Parameters
    ----------
    ax : matplotlib.axes.Axes, optional
        Axis to clean. Defaults to current axis.
    title : str, optional
        Title to set on the axis.
    remove_xy_ticks : bool, optional, default=False
        If True, remove both x and y ticks on the axis.
    """
    ax = plt.gca() if ax is None else ax

    if title is not None:
        ax.set_title(title)

    ax.set_aspect('equal')
    ax.set_xlabel('')
    ax.set_ylabel('')

    if remove_xy_ticks:
        ax.set_xticks([])
        ax.set_yticks([])


def module_available(module_name):
    """Check if a named Python module is available for import."""
    try:
        exec(f"import {module_name}")
    except ModuleNotFoundError:
        return False
    else:
        return True
