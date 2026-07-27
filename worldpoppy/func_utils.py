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
import numpy as np
import xarray as xr
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from pyproj import Transformer, CRS
from shapely.geometry import box

from worldpoppy.config import WGS84_CRS

__all__ = [
    "BboxInvalidError",
    "NominatimSearchEmptyError",
    "geolocate_name",
    "cached_nominatim_query",
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


class NominatimSearchEmptyError(Exception):
    """Raised when a Nominatim search returns no result."""

    pass


@lru_cache(maxsize=1024)
@backoff.on_exception(
    backoff.expo, GeocoderTimedOut, max_tries=5, jitter=backoff.full_jitter
)
def geolocate_name(nominatim_query, to_crs=None):
    """
    Return the geo-coordinate associated with a given location name,
    based on search results from OSM's 'Nominatim' service.

    Parameters
    ----------
    nominatim_query : str
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
    NominatimSearchEmptyError
        If the Nominatim query crashed or returned None.
    """
    lon, lat = cached_nominatim_query(nominatim_query)
    if to_crs is None:
        return lon, lat

    transformer = Transformer.from_crs(WGS84_CRS, to_crs, always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y


@lru_cache(maxsize=None)
@backoff.on_exception(
    backoff.expo, GeocoderTimedOut, max_tries=5, jitter=backoff.full_jitter
)
def cached_nominatim_query(query):
    """
    Return the lon/lat coordinate pair associated with a given location name,
    based on search results from OSM's 'Nominatim' service.

    Parameters
    ----------
    query : str
        A location name to be geocoded.

    Returns
    -------
    Tuple[float, float]
        The (x, y) coordinate in the target CRS, or (lon, lat) in WGS84.

    Raises
    ------
    NominatimSearchEmptyError
        If the Nominatim query crashed or returned None.
    """
    try:
        geolocator = Nominatim(user_agent="MyLocationCacher", timeout=2)
        located = geolocator.geocode(query)
    except Exception as e:
        # We re-raise *any* error as the same custom exception
        # since this simplifies error handling in callees like
        # `plot_utils.plot_location_markers`.
        raise NominatimSearchEmptyError(f"Nominatim search failed with error: {e}")

    if located is None:
        raise NominatimSearchEmptyError(f"No hit returned for location '{query}'.")

    lon, lat = located.point.longitude, located.point.latitude
    return lon, lat


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
            "Values beyond +/-90 suggest that the AoI crosses a pole. "
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
            "Values outside +/-180 suggest that the AoI crosses "
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
    Calculate a bounding box for the AoI in the target raster CRS,
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

    # If raster is NOT in WGS84, we reproject the buffered bounds.
    # Note: We actually expect all WorldPop rasters to be in WGS84,
    # so this is a just defensive safety logic.
    buffered_box = box(*bounds)
    box_gdf_wgs84 = gpd.GeoDataFrame(geometry=[buffered_box], crs=WGS84_CRS)
    box_gdf_tgt = box_gdf_wgs84.to_crs(raster_crs)

    # Return the bounds of the reprojected box.
    # This might be slightly larger than the original due to rotation/skew,
    # which is desirable for a safety buffer.
    return tuple(box_gdf_tgt.total_bounds)


def calculate_pixel_areas(da):
    """
    Calculate the area (in square metres) of every pixel in a WGS84 raster.

    This uses the 'Solid Angle' method on the WGS84 Authalic Sphere.
    An authalic sphere is a hypothetical sphere that has exactly the same
    surface area as a reference ellipsoid (here: WGS84), ensuring that
    global area bias is minimised.

    Parameters
    ----------
    da : xarray.DataArray
        Input raster in EPSG:4326 (degrees).

    Returns
    -------
    xarray.DataArray
        A 2D array of the same shape as `da`, containing the area
        of each pixel in square metres.
    """
    # --- Validation ---
    # Ensure we are in lat/lon. If not, this math is wrong.
    if da.rio.crs is None or not da.rio.crs.is_geographic:
        raise ValueError("Input raster must be in geographic coordinates (e.g. EPSG:4326).")

    # --- Extract Resolution (in degrees) ---
    # We assume a regular grid (affine transform).
    # transform[0] is pixel width (x), transform[4] is pixel height (y, usually negative)
    transform = da.rio.transform()
    res_x_deg = abs(transform[0])
    res_y_deg = abs(transform[4])

    # --- Define Earth Model ---
    R_METRES = 6371007.2  # https://en.wikipedia.org/wiki/Earth_radius#Authalic_radius

    # --- Calculate Latitude Bounds for every row ---
    # Get the centre latitudes (Y coordinates)
    lat_centres = da.y.values

    # --- Calculate the top and bottom latitude of each pixel in RADIANS ---
    # Note: We rely on the Y-vector, so this works regardless of X (Anti-Meridian safe)
    lat_top_rad = np.radians(lat_centres + (res_y_deg / 2))
    lat_bottom_rad = np.radians(lat_centres - (res_y_deg / 2))

    # --- Calculate Area using Solid Angle formula ---
    # Area = R^2 * (sin(lat_top) - sin(lat_bottom)) * (lon_width_rad)
    # This formula is exact for a spherical segment.

    # Compute the vertical component (vectorised by latitude)
    vertical_term = np.abs(np.sin(lat_top_rad) - np.sin(lat_bottom_rad))

    # Compute the horizontal component (scalar constant)
    horizontal_term = np.radians(res_x_deg)

    # Combine
    # Result is a 1D array of areas corresponding to latitudes
    row_areas_m2 = (R_METRES ** 2) * vertical_term * horizontal_term

    # --- Broadcast to full shape ---
    # Create a DataArray matching the input's Y coordinates
    area_da = xr.DataArray(
        row_areas_m2,
        coords={"y": da.y},
        dims="y",
        name="pixel_area"
    )

    # Broadcast against X to create the full 2D grid
    # This is virtually instant and memory efficient until materialised
    return area_da.broadcast_like(da)
