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


def get_buffered_bounds(clipping_gdf, raster_crs, buffer_deg):
    """
    Calculate a bounding box for the AOI in the target raster CRS,
    with a fixed safety buffer applied in WGS84.

    Parameters
    ----------
    clipping_gdf : geopandas.GeoDataFrame
        The clipping geometry.
    raster_crs : CRS (string or object)
        The Coordinate Reference System of the source raster we intend to clip.
    buffer_deg : float
        The buffer size in Degrees. Default 0.05 (approx 5.5km).

    Returns
    -------
    tuple
        (minx, miny, maxx, maxy) in the units of `raster_crs`.
    """

    # Standardise the CRS so our buffer is consistent
    if clipping_gdf.crs != WGS84_CRS:
        gdf_84 = clipping_gdf.to_crs(WGS84_CRS)
    else:
        gdf_84 = clipping_gdf

    # Calculate bounds in degrees
    minx, miny, maxx, maxy = gdf_84.total_bounds

    # Apply the buffer (in degrees)
    buff_minx = max(minx - buffer_deg, -180.0)
    buff_miny = max(miny - buffer_deg, -90.0)
    buff_maxx = min(maxx + buffer_deg, 180.0)
    buff_maxy = min(maxy + buffer_deg, 90.0)

    bounds = buff_minx, buff_miny, buff_maxx, buff_maxy
    validate_bbox_wgs84(bounds)  # just for safety

    # If the raster is also in WGS84, we are done.
    if CRS(raster_crs) == CRS(WGS84_CRS):
        return bounds

    # If the raster is NOT in WGS84, we reproject the bounds.
    # Note: We expect *all* WorldPop rasters to be in WGS84,
    # so this is just safety logic.
    buffered_box = box(*bounds)
    box_gdf_wgs84 = gpd.GeoDataFrame(geometry=[buffered_box], crs=WGS84_CRS)
    box_gdf_tgt = box_gdf_wgs84.to_crs(raster_crs)

    # Return the bounds of the reprojected box.
    # This might be slightly larger than the original due to rotation/skew,
    # which is desirable for a safety buffer.
    return tuple(box_gdf_tgt.total_bounds)