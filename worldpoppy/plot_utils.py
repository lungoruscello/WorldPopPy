"""
Collection of various plotting utility functions.
"""
import logging
from matplotlib import pyplot as plt
from pyproj import Transformer

from worldpoppy.config import WGS84_CRS
from worldpoppy.func_utils import cached_nominatim_query, NominatimSearchEmptyError
from worldpoppy.manifest_loader import get_all_isos

logger = logging.getLogger(__name__)

__all__ = [
    "clean_axis",
    "plot_country_borders",
    "plot_location_markers",
]


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
    kwargs = dict(color='None', edgecolor='black', linewidth=0.5)
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
        color='k',
        textcoords="offset points",
        xytext=(0, -10),
        ha='left',
        va='center',
        other_annotate_kwargs=None,
        to_crs=None,
        **scatter_kwargs
):
    """
    Plot markers for geolocated place names or raw coordinates on a matplotlib axis.
    Optionally annotate the location markers as well.

    Parameters
    ----------
    locations : str, tuple, or list of (str or tuple)
        The locations to plot. Can be:
        - A location name string (e.g., "Nairobi").
        - A tuple of (location_name, display_label) to search for "location_name"
          but annotate with "display_label".
        - A coordinate tuple (longitude, latitude) in WGS84.
        - A coordinate tuple with a label (longitude, latitude, label).
        - A mixed list of strings and tuples.
    ax : matplotlib.axes.Axes, optional
        Axis on which to plot. If None, uses current axis.
    annotate : bool, default=True
        Whether to annotate points with their names (or coordinates).
    color : str, default='k'
        Colour to use for both the scatter marker and the annotation text.
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

    Notes
    -----
    **Geocoding Reliability Warning**
    When users pass a location name, this function tries to resolve
    it into a GPS coordinate via OpenStreetMap's Nominatim service.
    Nominatim may occasionally return coordinates for a different
    location than intended (e.g., an administrative region instead
    of a city centre, or a city with the same name in a different
    country) due to search ambiguities.

    For precise control over the plotted location, it is strongly
    recommended to pass explicit GPS coordinates as tuples:
    ``(longitude, latitude, label)`` or ``(longitude, latitude)``.

    """

    ax = plt.gca() if ax is None else ax

    # Prepare scatter kwargs
    # Set the default size and the shared colour
    final_scatter_kwargs = dict(s=5, color=color)
    # If users passed extra scatter args (like s=100 or marker='x'), update them here
    if scatter_kwargs:
        final_scatter_kwargs.update(scatter_kwargs)

    # Prepare annotation kwargs
    # Copy to avoid modifying a dict passed by the user in the outer scope
    final_annotate_kwargs = (
        dict() if other_annotate_kwargs is None else other_annotate_kwargs.copy()
    )

    # Apply the shared colour to the text, unless the user explicitly
    # passed a specific colour in other_annotate_kwargs
    if 'color' not in final_annotate_kwargs:
        final_annotate_kwargs['color'] = color

    # Standardise input to a list
    if isinstance(locations, (str, tuple)):
        locations = [locations]

    # Initialise a re-usable transformer if needed
    transformer = None
    if to_crs is not None:
        transformer = Transformer.from_crs(WGS84_CRS, to_crs, always_xy=True)

    for item in locations:
        try:
            if isinstance(item, str):
                # Case 1: Simple location name string
                lon, lat = cached_nominatim_query(query=item)
                label = item

            elif isinstance(item, tuple):
                # Check the type of the first element to distinguish between
                # ("Query", "Label") and (Lon, Lat, ...)
                first_element = item[0]

                if isinstance(first_element, str):
                    # Case 2: (Location Name, Display Label)
                    if len(item) != 2:
                        raise ValueError(
                            f"Invalid string tuple format: {item}. "
                            "Expected (query_name, display_label)."
                        )
                    query_name, custom_label = item
                    lon, lat = cached_nominatim_query(query=query_name)
                    label = custom_label

                else:
                    # Case 3: Coordinate tuples
                    # These do not require geocoding, but we keep them inside the loop flow
                    if len(item) == 3:
                        # Tuple with explicit label
                        lon, lat, custom_label = item
                        label = custom_label
                    elif len(item) == 2:
                        # Tuple with simple coords
                        lon, lat = item
                        label = f"{lon:.2f}, {lat:.2f}"
                    else:
                        raise ValueError(
                            f"Invalid numeric tuple format: {item}. "
                            "Expected (lon, lat) or (lon, lat, label)."
                        )
            else:
                raise TypeError(
                    f"Location item must be a string or tuple, got {type(item)}."
                )

        except NominatimSearchEmptyError:
            # Determine the name we were trying to find for the log message
            failed_query = item[0] if isinstance(item, tuple) else item
            logger.warning(
                f"Nominatim returned no results for location '{failed_query}'. "
                "Skipping plotting for this location."
            )
            continue

        # --- Transformation & Plotting ---
        if transformer is not None:
            xy = transformer.transform(lon, lat)
        else:
            xy = (lon, lat)

        ax.scatter(*xy, **final_scatter_kwargs)

        if annotate:
            ax.annotate(
                label,
                xy,
                textcoords=textcoords,
                xytext=xytext,  # noqa
                ha=ha,
                va=va,
                **final_annotate_kwargs,
            )


def clean_axis(ax=None, title=None, remove_xy_ticks=True, **title_kwargs):
    """
    Clean up a matplotlib axis by setting equal aspect and removing labels.

    Parameters
    ----------
    ax : matplotlib.axes.Axes, optional
        Axis to clean. Defaults to current axis.
    title : str, optional
        Title to set on the axis.
    remove_xy_ticks : bool, optional, default=True
        If True, remove both x and y ticks on the axis.
    **title_kwargs :
        Additional keyword arguments passed to ax.set_title() (e.g., fontsize, loc, color).
    """
    ax = plt.gca() if ax is None else ax

    if title is not None:
        ax.set_title(title, **title_kwargs)

    ax.set_aspect('equal')
    ax.set_xlabel('')
    ax.set_ylabel('')

    if remove_xy_ticks:
        ax.set_xticks([])
        ax.set_yticks([])
