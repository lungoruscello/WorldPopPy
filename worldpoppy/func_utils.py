"""
Collection of various helper functions.

Note: Plotting utilities are located in a separate module.
"""
import io
import logging
from contextlib import contextmanager, redirect_stdout
from functools import lru_cache
from typing import Tuple

import backoff
import geopandas as gpd
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from pyproj import Transformer, CRS
from shapely.geometry import box

from worldpoppy.config import WGS84_CRS

__all__ = [
    "BboxInvalidError",
    "geolocate_name",
    "module_available",
    "log_info_context",
    "validate_bbox_wgs84",
    "get_buffered_bounds"
]

logger = logging.getLogger(__name__)


class BboxInvalidError(Exception):
    """
    Raised when the bounds for a purported bounding box are invalid,
    assuming the bounds are specified using the WGS84 CRS.
    """

    pass


@lru_cache(maxsize=1024)
@backoff.on_exception(
    backoff.expo, GeocoderTimedOut, max_tries=5, jitter=backoff.full_jitter
)
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


def module_available(module_name):
    """Check if a named Python module is available for import."""
    try:
        exec(f"import {module_name}")
    except ModuleNotFoundError:
        return False
    else:
        return True


@contextmanager
def log_info_context(logger):
    """
    Context manager to optionally redirect `print` statements to a logger.

    If the logger's effective level is WARNING or higher (default),
    `print()` statements execute normally. On lower logging levels,
    `print()` outputs are captured and sent to logger.info() instead.

    Parameters
    ----------
    logger : logging.Logger
        The logger instance to use (e.g., from `logging.getLogger(__name__)`).
    """
    effective_level = logger.getEffectiveLevel()

    if effective_level <= logging.INFO:
        string_buffer = io.StringIO()

        try:
            # use the thread-safe stdout redirector
            with redirect_stdout(string_buffer):
                yield  # user's `print()` runs here
        finally:
            # after the block, get the captured text
            captured_message = string_buffer.getvalue().strip()
            if captured_message:
                # log the captured text instead of printing
                logger.info(captured_message)

    else:
        # logger is not set to INFO, so we don't interfere
        try:
            yield
        finally:
            pass  # nothing to clean up


def validate_bbox_wgs84(bounds):
    """
    Validate a bounding box in the format (min_lon, min_lat, max_lon, max_lat).

    Raises
    ------
    BboxInvalidError
        If the bounding box is invalid.
    """
    # --- Input Type & Format Checks ---
    if not isinstance(bounds, (list, tuple)):
        raise BboxInvalidError(
            f"Bounding box must be a list or tuple, got {type(bounds)}."
        )

    if len(bounds) != 4:
        raise BboxInvalidError(
            f"Bounding box must contain exactly four values, got {len(bounds)}."
        )

    if not all(isinstance(x, (int, float)) for x in bounds):
        raise BboxInvalidError("Bounding box values must be numeric.")

    min_lon, min_lat, max_lon, max_lat = bounds

    # --- Latitude / Pole Checks ---
    # Check for physical impossibility
    if min_lat < -90 or max_lat > 90:
        raise BboxInvalidError(
            f"Latitude out of bounds ({min_lat}, {max_lat}). "
            "Values beyond +/-90 suggest that the AOI crosses a pole. "
        )

    # Check for logical consistency
    if min_lat > max_lat:
        raise BboxInvalidError(
            f"Invalid latitude range: min_lat ({min_lat}) is greater than "
            f"max_lat ({max_lat})."
        )

    # --- Longitude / Anti-Meridian Checks ---
    # Check for projection wrap-around artifacts
    if min_lon < -180 or max_lon > 180:
        raise BboxInvalidError(
            f"Longitude out of bounds ({min_lon}, {max_lon}). "
            "Values outside +/-180 suggest that the AOI crosses "
            "the Anti-Meridian (Date Line)."
        )

    # Check for geometric crossing (e.g., min=179.0, max=-179.0)
    if min_lon > max_lon:
        raise BboxInvalidError(
            f"Invalid longitude range: min_lon ({min_lon}) is greater than "
            f"max_lon ({max_lon}). This could indicate a crossing of the "
            f"Anti-Meridian (Date Line)."
        )

