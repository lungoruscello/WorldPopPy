"""
Provides functions to obtain raster data from WorldPop, with several
alternative ways to specify the areas of interest and multi-year support.

Note
----
    The present implementation draws on the "raster.py" module from the
    `blackmarblepy` package by Gabriel Stefanini Vicente and Robert Marty.
    `blackmarblepy` is licensed under the Mozilla Public License (MPL-2.0),
    as is the present Python module.

    Links:
    - https://github.com/worldbank/blackmarblepy
    - https://github.com/worldbank/blackmarblepy/blob/main/LICENSE


Main user-facing methods
------------------------
wp_raster
    Retrieve WorldPop data for arbitrary areas of interest (AOIs) and
    multiple years (for annual data products only).
bbox_from_location
    Generate a bounding box from a location name or GPS coordinate.
    The result can be used specify the AOI for `wp_raster`.
geolocate_name
    Find the GPS location for a location name through a `Nomatim` query.

"""
import logging
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Tuple

import backoff
import geopandas as gpd
import rioxarray
import shapely
import xarray as xr
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from rioxarray.merge import merge_arrays
from tqdm.auto import tqdm

from worldpoppy.config import *
from worldpoppy.download import WorldPopDownloader
from worldpoppy.manifest import extract_year
from worldpoppy.utils import module_available

logger = logging.getLogger(__name__)

__all__ = ["wp_raster", "bbox_from_location", "geolocate_name"]


def wp_raster(
        product_name,
        aoi,
        years=None,
        *,
        cache_downloads=True,
        skip_download_if_exists=True,
        masked=False,
        mask_and_scale=False,
        other_read_kwargs=None,
        res=None,
        download_dry_run=False,
        **merge_kwargs
):
    """
    Return WorldPop data for the specified area of interest (AOI) and the
    specified years (for annual raster products only).

    Parameters
    ----------
    product_name : str
        The name of the WorldPop data product of interest.
    aoi : str, List[str], List[float], Tuple[float], or geopandas.GeoDataFrame

        The area of interest (AOI) for which to obtain the raster data. Users can specify
        this area using:
            - one or more three-letter country codes (alpha-3 IS0 codes);
            - a GeoDataFrame with one or more polygonal geometries; or
            - a bounding box of the format (min_lon, min_lat, max_lon, max_lat).
            In the latter two cases, WorldPop data is first downloaded and merged for
            all countries that intersect the area of interest, regardless of how large
            this intersection is. Subsequently, the merged raster is then clipped using
            the AOI.

    years : int or List[int], optional
        For annual data products, one or more years of interest. For static data products,
        this argument must be None (default).
    cache_downloads: bool, optional, default=True
        Whether to cache downloaded source rasters.
    skip_download_if_exists : bool, optional, default=True
        Whether to skip downloading source rasters that already exist in the local cache.
    masked: bool, optional, default=False
        If True, read the mask of all input rasters and set masked
        values to NaN. This argument is passed to `rioxarray.open_rasterio`
        when reading input rasters.
    mask_and_scale: bool, default=False
        Lazily scale (using the `scales` and `offsets` from rasterio) all
        input rasters and mask them. If the _Unsigned attribute is present
        treat integer arrays as unsigned. This argument is passed to
        `rioxarray.open_rasterio` when reading input rasters.
    other_read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to
        `rioxarray.open_rasterio` when reading input rasters (e.g., `lock`
        or `band_as_variable`).
    res: tuple, optional
        Output resolution for the final merged raster in units of coordinate
        reference system. If not set, the resolution of the first source
        raster is used. If a single value is passed, output pixels will be
        square. This argument is passed to `rioxarray.merge.merge_arrays`.
    download_dry_run : bool, optional, default=False
        TODO
    **merge_kwargs : keyword arguments
        Additional arguments passed to `rioxarray.merge.merge_arrays`,
        which give more control over how input rasters should be
        merged (e.g., `method` or `bounds`).

    Returns
    -------
    xr.Dataset or None
        The combined raster data for several countries and years (where applicable)
        if `download_dry_run` is False. None otherwise.

    """
    other_read_kwargs = {} if other_read_kwargs is None else other_read_kwargs

    aoi = _format_aoi(aoi)
    clipping_gdf = aoi if isinstance(aoi, gpd.GeoDataFrame) else None
    shared_opts = dict(
        masked=masked,
        mask_and_scale=mask_and_scale,
        other_read_kwargs=other_read_kwargs,
        res=res,
        clipping_gdf=clipping_gdf,
    )
    shared_opts.update(**merge_kwargs)

    if not cache_downloads and skip_download_if_exists:
        skip_download_if_exists = False
        logger.warning(
            "'skip_download_if_exists' has no effect is 'cache_downloads' is set to False'."
        )

    with (TemporaryDirectory() if not cache_downloads else get_cache_dir() as d):
        # download all rasters
        all_raster_paths = WorldPopDownloader(directory=d).download(
            product_name,
            aoi,
            years,
            skip_download_if_exists,
            dry_run=download_dry_run
        )

        if download_dry_run:
            return None

        if years is None:
            # static product: merge only once
            merged = _merge_rasters(all_raster_paths, **shared_opts)
            return merged.squeeze()

        # annual product: split raster paths by year
        paths_by_year = defaultdict(list)
        for path in all_raster_paths:
            year = extract_year(path.name)
            paths_by_year[year].append(path)

        # merge rasters separately by year
        annual_rasters = []
        pbar = tqdm(
            paths_by_year.items(),
            total=len(paths_by_year),
            desc="Processing years..."
        )
        for year, year_paths in pbar:
            merged = _merge_rasters(year_paths, **shared_opts)
            merged['year'] = year
            annual_rasters.append(merged)

        # stack years
        time_series = _concat_with_warning(
            annual_rasters,
            dim='year',
            combine_attrs='drop_conflicts'
        )
        return time_series.squeeze()


def bbox_from_location(location, width_degrees=None, width_km=None):
    """
    Create a bounding box around a named location or GPS coordinate.

    Parameters
    ----------
    location : str or tuple(float, float)
        Either a human-readable location name (e.g., "Nairobi, Kenya")
        or a tuple of (longitude, latitude).
    width_degrees : float, optional
        Width/height of the bounding box in decimal degrees.
    width_km : float, optional
        Width/height of the bounding box in kilometers. If provided, this
        is converted into degrees assuming ~111 km/degree at the equator.
        Must be None if `width_degrees` is specified.

    Returns
    -------
    Tuple[float, float, float, float]
        GPS coordinates of the bounding box using the format
        (min_lon, min_lat, max_lon, max_lat).

    Raises
    ------
    ValueError
        If either both or neither of `width_degrees` and `width_km` are specified.
    """

    # handle location
    if isinstance(location, str):
        lon, lat = geolocate_name(location)
    elif isinstance(location, tuple) and len(location) == 2:
        lon, lat = location
    else:
        raise ValueError("Location must be a string or a (lon, lat) tuple.")

    # handle bbox width
    num_provided = (width_degrees is None) + (width_km is None)
    if num_provided != 1:
        raise ValueError("You must specify exactly one of 'width_degrees' or 'width_km'.")

    # handle bbox width
    if width_degrees is not None:
        half_width = width_degrees / 2
    else:
        half_width = (width_km / 111.11) / 2

    # build bbox
    min_x, min_y = lon - half_width, lat - half_width
    max_x, max_y = lon + half_width, lat + half_width

    # TODO Make anti-meridian safe
    return min_x, min_y, max_x, max_y


@lru_cache(maxsize=1024)
@backoff.on_exception(backoff.expo, GeocoderTimedOut, max_tries=5)
def geolocate_name(nomatim_query):
    """
    Return the point coordinate (lon, lat) associated with a given location
    name, based on search results from OSM's 'Nominatim' service.

    Returns
    -------
    Tuple[float, float]
        Longitude and latitude of the geolocated location name.

    Raises
    ------
    Raises
        If the Nominatim query has returned None.
    """
    geolocator = Nominatim(user_agent="MyLocationCacher", timeout=2)
    located = geolocator.geocode(nomatim_query)

    if located is None:
        raise RuntimeError(f"Nomatim search for location name '{nomatim_query}' returned no hit.")

    return located.point.longitude, located.point.latitude


def _merge_rasters(
        raster_fpaths,
        masked=False,
        mask_and_scale=False,
        other_read_kwargs=None,
        clipping_gdf=None,
        **merge_kwargs
):
    """
    Merge multiple raster files, and optionally clip the result, using `rioxarray`.

    Parameters
    ----------
    raster_fpaths : List[Path] or List[str]
        List of paths to the input raster files that are to be merged.
    masked: bool, optional, default=False
        If True, read the mask of all input rasters and set masked
        values to NaN. This argument is passed to `rioxarray.open_rasterio`
        when reading input rasters.
    mask_and_scale: bool, default=False
        Lazily scale (using the `scales` and `offsets` from rasterio) all
        input rasters and mask them. If the _Unsigned attribute is present
        treat integer arrays as unsigned. This argument is passed to
        `rioxarray.open_rasterio` when reading input rasters.
    other_read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to
        `rioxarray.open_rasterio` when reading input rasters (e.g., `lock`
        or `band_as_variable`).
    clipping_gdf : geopandas.GeoDataFrame, optional
        GeoDataFrame with geometries used to clip the merged raster.
    **merge_kwargs : keyword arguments
        Additional arguments passed to `rioxarray.merge.merge_arrays`,
        which give more control over how input rasters should be merged
        (e.g., `method` or `bounds`).

    Returns
    -------
    xarray.DataArray
        The merged and optionally clipped raster.

    Raises
    ------
    ValueError
        If input rasters have mismatched`_FillValue` or `scale_factor` attributes.
    """

    # read country rasters into a list
    rasters = []
    fill_val = None
    scale_factor = None
    for i, path in enumerate(raster_fpaths):
        try:
            da = rioxarray.open_rasterio(
                path,
                masked=masked,
                mask_and_scale=mask_and_scale,
                **other_read_kwargs
            )
        except Exception as e:
            raise ValueError(
                f"Failed to read raster file at {path}. Error: {e}\n"
                "If you suspect a corrupted cache, please try to delete the affected "
                "file and trigger the download again."
            )

        # ensure masking and scaling attributes are aligned
        if '_FillValue' in da.attrs:
            if fill_val is None:
                fill_val = da.attrs['_FillValue']
            else:
                if da.attrs['_FillValue'] != fill_val:
                    raise ValueError(
                        "Country rasters do not use the same '_FillValue'. Try calling "
                        "this function again by setting 'mask_and_scale' to True."
                    )

        if 'scale_factor' in da.attrs:
            if scale_factor is None:
                scale_factor = da.attrs['scale_factor']
            else:
                if da.attrs['scale_factor'] != scale_factor:
                    raise ValueError(
                        "Country rasters do not use the same 'scale_factor'. Try calling "
                        "this function again by setting 'mask_and_scale' to True."
                    )

        rasters.append(da)

    da = merge_arrays(rasters, **merge_kwargs)

    if clipping_gdf is not None:
        geoms = clipping_gdf.geometry.apply(shapely.geometry.mapping)
        da = da.rio.clip(geoms, clipping_gdf.crs, drop=True, all_touched=True)
    return da


def _concat_with_warning(objs, **kwargs):
    """
    Thin wrapper for `xarray.concat` which logs a warning if the optional
    `bottleneck` library is not available.

    Parameters
    ----------
    objs : List[xarray.DataArray or xarray.Dataset]
        List of xarray objects to concatenate.
    **kwargs : keyword arguments
        Additional arguments passed to `xarray.concat`.
    """
    if not module_available("bottleneck"):
        logger.warning(
            "Installing the optional `bottleneck` module will accelerate "
            "`xarray` concatenation. (pip install bottleneck)"
        )
    return xr.concat(objs, **kwargs)


def _format_aoi(aoi):
    """
    Standardise a user-specified area of interest (AOI).

    Parameters
    ----------
    aoi : str, list, tuple, or geopandas.GeoDataFrame
        The area of interest for which to obtain WorldPop data.

    Returns
    -------
    geopandas.GeoDataFrame or original value
        A standardised AOI representation suitable for use with `wp_raster`.
    """
    if isinstance(aoi, gpd.GeoDataFrame):
        # handle GeoDataFrame
        if aoi.crs != WGS84_CRS:
            aoi = aoi.to_crs(WGS84_CRS)  # ensure proper CRS

    elif isinstance(aoi, (list, tuple)):
        # handle bounding box
        if not isinstance(aoi[0], str):
            _validate_bbox(aoi)  # check range of lat/lon values
            box_poly = shapely.box(*aoi)
            aoi = gpd.GeoDataFrame(geometry=[box_poly], crs=WGS84_CRS)  # convert to GeoDataFrame

    else:
        pass  # ISO3 string(s) — handled elsewhere

    return aoi


def _validate_bbox(bbox):
    """
    Validate a bounding box in the format (min_lon, min_lat, max_lon, max_lat).

    Raises
    ------
    ValueError
        If the bounding box is invalid.
    """
    if not isinstance(bbox, (list, tuple)):
        raise ValueError("Bounding box must be a list or tuple.")

    if len(bbox) != 4 or not all([isinstance(x, (int, float)) for x in bbox]):
        raise ValueError(
            "Bounding box must contain exactly four numeric values: "
            "(min_lon, min_lat, max_lon, max_lat)."
        )

    min_lon, min_lat, max_lon, max_lat = bbox

    if min_lon >= max_lon:
        raise ValueError("Bad bounding box. min_lon must be less than max_lon.")
    if min_lat >= max_lat:
        raise ValueError("Bad bounding box. min_lat must be less than max_lat.")

    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError("Bad bounding box. Longitude must be between -180 and 180 degrees.")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("Bad bounding box. Latitude must be between -90 and 90 degrees.")
