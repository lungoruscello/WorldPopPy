"""
This is the main module of `WorldPopPy`. It provides logic to fetch raster
data from `WorldPop <https://www.worldpop.org/>`_ through several alternative
specifications for the geographic area of interest.

Note
----
    The implementation of this module draws on the "raster.py" module from the
    `blackmarblepy <https://github.com/worldbank/blackmarblepy>`_ package by
    Gabriel Stefanini Vicente and Robert Marty. `blackmarblepy` is licensed
    under the Mozilla Public License (MPL-2.0), as is `WorldPopPy`.


Main methods
------------------------
    - :func:`wp_raster`
        Retrieve WorldPop data for arbitrary geographical areas and
        multiple years (where applicable).
    - :func:`merge_rasters`
        Merge multiple raster files and optionally clip the result.
    - :func:`bbox_from_location`
        Generate a bounding box from a location name or GPS coordinate.
        The result can be used specify the AOI for `wp_raster`.

"""
import logging
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Tuple

import geopandas as gpd
import rioxarray
import shapely
import xarray as xr
from pyproj import CRS, Transformer
from rioxarray.merge import merge_arrays
from tqdm.auto import tqdm

from worldpoppy.borders import load_country_borders
from worldpoppy.config import *
from worldpoppy.download import WorldPopDownloader
from worldpoppy.func_utils import module_available, geolocate_name

logger = logging.getLogger(__name__)

__all__ = [
    "RasterReadError",
    "IncompatibleRasterError",
    "wp_raster",
    "bbox_from_location",
    "merge_rasters",
]


class RasterReadError(Exception):
    """Raised when reading a WorldPop source raster fails."""

    pass


class IncompatibleRasterError(Exception):
    """Raised when trying to merge incompatible WorldPop source rasters."""

    pass


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
    download_chunk_size=1024**2,
    download_dry_run=False,
    to_crs=None,
    **merge_kwargs,
):
    """
    Return WorldPop data for the user-defined area of interest (AOI) and the
    specified years (where applicable).

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

    years : int or List[int] or str, optional
        For annual data products, one or more years of interest or the 'all' keyword
        (str). For static data products, this argument must be None (default).
    cache_downloads: bool, optional, default=True
        Whether to cache downloaded source rasters.
    skip_download_if_exists : bool, optional, default=True
        Whether to skip downloading source rasters that already exist in the local cache.
    masked: bool, optional, default=False
        If True, read the mask of all input rasters and set masked
        values to NaN. This argument is passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`_
        when reading input rasters.
    mask_and_scale: bool, default=False
        Lazily scale (using the `scales` and `offsets` from rasterio) all
        input rasters and mask them. If the _Unsigned attribute is present
        treat integer arrays as unsigned. This argument is passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`_
        when reading input rasters.
    other_read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`_
        when reading input rasters (e.g., `lock` or `band_as_variable`).
    res: tuple, optional
        Output resolution for the final merged raster in units of coordinate
        reference system. If not set, the resolution of the first source
        raster is used. If a single value is passed, output pixels will be
        square. This argument is passed to
        `rioxarray.merge.merge_arrays <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray.merge.merge_arrays>`_.
    download_chunk_size : int, optional, default=1MB
        The size (in bytes) of chunks to read/write during raster downloads.
        Larger chunks may improve performance, especially on systems with
        real-time file scanning (e.g., antivirus).
    download_dry_run : bool, optional, default=False
        If True, only check how many raster files would need to be downloaded
        from WorldPop if `download_dry_run` was False. Report the number and
        size of required file downloads, but do not actually fetch or process
        any files.
    to_crs : str or pyproj.CRS, optional
        Coordinate reference system (CRS) to reproject the merged raster into.
        Re-projection is applied *after* merging (and clipping, if requested).
        If `to_crs` is not provided, raster data remains in the source CRS.
    **merge_kwargs : keyword arguments
        Additional arguments passed to
        `rioxarray.merge.merge_arrays <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray.merge.merge_arrays>`_,
        which give more control over how input rasters should be merged
        (e.g., `method` or `bounds`).

    Returns
    -------
    xr.Dataset or None
        The combined raster data for several countries and years (where applicable),
        or None if `download_dry_run` is True.

    """
    other_read_kwargs = {} if other_read_kwargs is None else other_read_kwargs

    # --- Parse the area of interest ---
    # (This logic is correct and unchanged)
    if isinstance(aoi, (list, tuple)):
        if not isinstance(aoi[0], str):
            _validate_bbox(aoi)
            box_poly = shapely.box(*aoi)
            aoi = gpd.GeoDataFrame(geometry=[box_poly], crs=WGS84_CRS)
    if isinstance(aoi, gpd.GeoDataFrame):
        world = load_country_borders()
        joined = gpd.sjoin(
            world, aoi.to_crs(WGS84_CRS), predicate='intersects', how='right'
        )
        iso3_codes = sorted(joined.iso3.unique())
    else:
        if isinstance(aoi, str):
            iso3_codes = [aoi]
        else:
            if not isinstance(aoi[0], str):
                raise ValueError(
                    "Cannot parse 'aoi'. Please pass one or more country codes..."
                )
            iso3_codes = aoi

    # --- Check other user args ---
    if not cache_downloads and skip_download_if_exists:
        skip_download_if_exists = False
        logger.warning(
            "'skip_download_if_exists' has no effect is 'cache_downloads' is set to False'."
        )

    # --- Prepare shared merge arguments ---
    merge_options = merge_kwargs.copy()
    if res is not None:
        merge_options['res'] = res

    clipping_gdf = aoi if isinstance(aoi, gpd.GeoDataFrame) else None
    shared_merge_kwargs = dict(
        masked=masked,
        mask_and_scale=mask_and_scale,
        other_read_kwargs=other_read_kwargs,
        clipping_gdf=clipping_gdf,
        to_crs=to_crs,
        merge_options=merge_options,
    )

    with TemporaryDirectory() if not cache_downloads else get_cache_dir() as d:
        # --- Trigger raster download where needed ---
        all_raster_paths, filtered_mdf = WorldPopDownloader(directory=d).download(
            product_name,
            iso3_codes,
            years,
            skip_download_if_exists,
            dry_run=download_dry_run,
            chunk_size=download_chunk_size,
        )

        if download_dry_run:
            return None

        # --- Static product ---
        # In this case, meta-data validation is performed solely *within*
        # the stand-alone `merge_rasters` function (which
        if years is None:
            # `merge_rasters` validates, merges, and builds its own "flat"
            # metadata. It correctly consumes or preserves the attributes
            # based on the `masked` flags we pass it.
            merged = merge_rasters(all_raster_paths, **shared_merge_kwargs)
            return merged.squeeze()

        # --- Multi-year product  ---
        paths_by_year = defaultdict(list)
        for path, mrow in zip(all_raster_paths, filtered_mdf.itertuples()):
            year = int(mrow.year)  # convert from numpy type
            paths_by_year[year].append(path)

        # In this case, we must validate raster meta-data for *all years*
        # in one go (to catch inconsistencies across different years).
        logger.info("Validating meta-data consistency across all years...")
        try:
            all_src_metadata = [_read_raster_metadata(str(p)) for p in all_raster_paths]
        except RasterReadError as e:
            logger.error(f"A raster file is unreadable. Aborting. Error: {e}")
            raise e

        # This catches cross-year mismatches (e.g., _FillValue)
        global_safe_attrs = _validate_raster_metadata(all_src_metadata)

        # `global_safe_attrs` now holds what *exists* in the source files.
        # But if the user *requested* masking/scaling, those attributes
        # will be "consumed" by the data-loading path. We *must*
        # remove them here to prevent lying about the final data's properties.

        # If masking was requested, the _FillValue was consumed.
        if (masked or mask_and_scale) and '_FillValue' in global_safe_attrs:
            del global_safe_attrs['_FillValue']

        # If scaling was requested, the scaling attrs were consumed.
        if mask_and_scale:
            if 'scale_factor' in global_safe_attrs:
                del global_safe_attrs['scale_factor']
            if 'add_offset' in global_safe_attrs:
                del global_safe_attrs['add_offset']

        # Merge the actual rasters separately by year
        annual_rasters = []
        annual_history = {}
        annual_source_meta = {}

        pbar = tqdm(
            paths_by_year.items(),
            total=len(paths_by_year),
            desc="Processing years...",
            leave=False,
        )
        for year, year_paths in pbar:
            # Note: The repeated metadata check inside `merge_rasters`
            # will be instantaneous because the cache is already warm.
            merged = merge_rasters(year_paths, **shared_merge_kwargs)
            merged['year'] = year
            annual_rasters.append(merged)

            # Save the (flat) metadata from each year's merge
            # *before* xr.concat can destroy it.
            annual_history[year] = merged.attrs.get('history', 'N/A')
            annual_source_meta[year] = merged.attrs.get('source_metadata', {})

        # Stack years via `xr.concat`
        time_series = _concat_with_info(
            annual_rasters,
            dim='year',
            combine_attrs='drop_conflicts',
        )

        # Build final, nested meta-data
        # `xr.concat` has (correctly) dropped the conflicting 'history'
        # and 'source_metadata' attributes for annual merged rasters.
        # We now re-build them from our "collected" dictionaries.

        time_series.attrs = {}  # nuke the empty/dropped attrs
        time_series.attrs.update(global_safe_attrs)  # re-apply safe attrs

        # Apply the *nested* metadata we collected
        time_series.attrs['history'] = annual_history
        time_series.attrs['source_metadata'] = annual_source_meta

        return time_series.squeeze()


def merge_rasters(
    raster_fpaths,
    masked=False,
    mask_and_scale=False,
    other_read_kwargs=None,
    clipping_gdf=None,
    to_crs=None,
    merge_options=None
):
    """
    Merge multiple raster files, and optionally clip the result, using `rioxarray`.

    This function is a "smart" wrapper around `rioxarray.merge.merge_arrays`.
    It validates that all input rasters share the same critical metadata (CRS,
    FillValue, etc.) and then creates a new, synthetic set of metadata for the
    final merged raster, including a 'history' and a 'source_metadata' attribute
    for full provenance.

    Parameters
    ----------
    raster_fpaths : List[Path] or List[str]
        List of paths to the input raster files that are to be merged.
    masked: bool, optional, default=False
        If True, read the mask of all input rasters and set masked
        values to NaN. This argument is passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`_
        when reading input rasters.
    mask_and_scale: bool, default=False
        Lazily scale (using the `scales` and `offsets` from rasterio) all
        input rasters and mask them. If the _Unsigned attribute is present
        treat integer arrays as unsigned. This argument is passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`_
        when reading input rasters.
    other_read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`_
        when reading input rasters (e.g., `lock`
        or `band_as_variable`).
    clipping_gdf : geopandas.GeoDataFrame, optional
        GeoDataFrame with geometries used to clip the merged raster.
    to_crs : str or pyproj.CRS, optional
        Coordinate reference system (CRS) to reproject the merged raster into.
        Re-projection is applied *after* merging (and clipping, if requested).
        If `to_crs` is not provided, raster data remains in the source CRS.
    merge_options : dict, optional
        A dictionary of keyword arguments passed directly to
        `rioxarray.merge.merge_arrays`, which give more control over how input
        rasters should be merged (e.g., `{'method': 'first'}`).

    Returns
    -------
    xarray.DataArray
        The merged and optionally clipped raster.

    Raises
    ------
    RasterReadError
        If reading an input raster fails.

    IncompatibleRasterError
        - If input rasters have mismatched Coordinate Reference Systems.
        - If input rasters have mismatched `_FillValue` or `scale_factor` attributes.
    """

    # --- Get all metadata (cheaply, from cache or lazy-read) ---
    try:
        metadata_list = [_read_raster_metadata(str(p)) for p in raster_fpaths]
    except RasterReadError as e:
        # catch read errors during the metadata-gathering phase
        raise e

    # --- Validate metadata (cheaply, in-memory) ---
    safe_attrs = _validate_raster_metadata(metadata_list)

    # --- Open *actual* rasters for merging ---
    # validation passed, so now we open the files for real
    rasters_to_merge = []
    if other_read_kwargs is None:
        other_read_kwargs = {}

    for path in raster_fpaths:
        try:
            da = rioxarray.open_rasterio(
                path,
                masked=masked,
                mask_and_scale=mask_and_scale,
                **other_read_kwargs
            )
            rasters_to_merge.append(da)
        except Exception as e:
            # This is a safety catch, but _get_raster_metadata
            # should have caught most read errors.
            raise RasterReadError(f"Failed to read raster file at {path}. Error: {e}\n")

    # --- Merge rasters using `rioxarray` ---
    if merge_options is None:
        merge_options = {}
    da = merge_arrays(rasters_to_merge, **merge_options)

    # --- Clean-up and create final metadata ---
    da.attrs = {}
    da.attrs.update(safe_attrs)

    # Build 'history' field
    fnames = [Path(meta['path']).name for meta in metadata_list]
    da.attrs['history'] = (
        f"Merged from {len(fnames)} files by worldpoppy "
        f"on {datetime.now().isoformat()}. "
        f"Source files: {', '.join(fnames)}"
    )

    # Build `source_metadata` field (wherein we keep
    # track of all input raster's original meta-data)
    source_metadata = {
        Path(meta['path']).name: meta['all_attrs'] for meta in metadata_list
    }
    da.attrs['source_metadata'] = source_metadata

    # --- Clip the merged rasters (optional) ---
    if clipping_gdf is not None:
        geoms = clipping_gdf.geometry.apply(shapely.geometry.mapping)
        da = da.rio.clip(geoms, clipping_gdf.crs, drop=True, all_touched=True)

    # --- Re-project (optional) ---
    if to_crs is not None:
        to_crs = CRS(to_crs)  # force format errors
        da = da.rio.reproject(to_crs)

    return da


def bbox_from_location(centre, width_degrees=None, width_km=None):
    """
    Construct a bounding box centered on a given geographic location.

    The `centre` argument can be either a place name (which is geocoded
    using `geolocate_name`) or a (longitude, latitude) coordinate pair.

    If `width_km` is specified, the bounding box is computed in a local
    Azimuthal Equidistant projection centered on the specified location,
    and then reprojected back to WGS84 longitude/latitude coordinates.
    This method may produce unexpected results near the poles or across
    the anti-meridian (180° longitude).

    Parameters
    ----------
    centre : str or Tuple(float, float)
        Either a human-readable location name (e.g., "Nairobi, Kenya")
        or a tuple of (longitude, latitude).
    width_degrees : float, optional
        Width/height of the bounding box in decimal degrees. Must be
        None if `width_km` is specified.
    width_km : float, optional
        Width/height of the bounding box in kilometers. Must be None if
        `width_degrees` is specified.

    Returns
    -------
    Tuple[float, float, float, float]
        Geo-coordinates of the bounding box using the format
        (min_lon, min_lat, max_lon, max_lat) [WGS84].

    Raises
    ------
    ValueError
        If either both or neither of `width_degrees` and `width_km` are specified.
    """

    # handle location
    if isinstance(centre, str):
        lon, lat = geolocate_name(centre)
    elif isinstance(centre, tuple) and len(centre) == 2:
        lon, lat = centre
    else:
        raise ValueError("Location must be a string or a (lon, lat) tuple.")

    # handle bbox width
    num_provided = (width_degrees is None) + (width_km is None)
    if num_provided != 1:
        raise ValueError(
            "You must specify exactly one of 'width_degrees' or 'width_km'."
        )

    if width_degrees is not None:
        # distance specified in degrees
        half_width = width_degrees / 2
        return (
            lon - half_width, lat - half_width,
            lon + half_width, lat + half_width
        )

    # define a local Azimuthal Equidistant projection
    proj4_str = (
        f"+proj=aeqd +lon_0={lon} +lat_0={lat} +x_0=0 +y_0=0 +datum=WGS84 +units=m"
    )
    local_aeqd_crs = CRS(proj4_str)

    # Compute box corners in kilometres
    # Note: Under our Azimuthal CRS, the centre point always
    # has the coordinate (0, 0). The bounding box is thus trivial.
    half_width_m = (width_km * 1_000) / 2
    x_min, y_min = -half_width_m, -half_width_m
    x_max, y_max = half_width_m, half_width_m

    # transform corners back to lon/lat
    from_proj = Transformer.from_crs(local_aeqd_crs, WGS84_CRS, always_xy=True)
    min_lon, min_lat = from_proj.transform(x_min, y_min)
    max_lon, max_lat = from_proj.transform(x_max, y_max)

    return min_lon, min_lat, max_lon, max_lat


@lru_cache(maxsize=4096)
def _read_raster_metadata(path):
    """
    Read critical metadata from a single raster file.

    This function is cached and opens the file lazily, i.e. does *not*
    read the full raster data into memory. It immediately closes the
    file handle after extracting the metadata.

    Parameters
    ----------
    path : str
        The file path to the raster.

    Returns
    -------
    dict
        A dictionary containing the critical metadata.
    """
    logger.debug(f"Cache MISS: Reading metadata for {path}")
    try:
        # Note: `xr.open_dataarray` is lazy by default. Crucially,
        # we *must* set mask_and_scale=False. If True (the default),
        # xarray/rioxarray would "consume" the _FillValue and
        # scale_factor attributes, and we would not be able to read them.
        with xr.open_dataarray(path, engine="rasterio", mask_and_scale=False) as da:

            # store CRS as a string (WKT) to ensure it is hashable
            # for the cache and comparable
            crs_str = da.rio.crs.to_wkt() if da.rio.crs else None

            # Read all three critical attributes
            nodata_val = da.attrs.get('_FillValue')
            scale_factor = da.attrs.get('scale_factor')
            add_offset = da.attrs.get('add_offset')

            # store a full copy of the original attributes
            # for the 'source_metadata' dict downstream.
            all_attrs = da.attrs.copy()

            return {
                'path': path,
                'crs': crs_str,
                'nodata': nodata_val,
                'scale_factor': scale_factor,
                'add_offset': add_offset,
                'all_attrs': all_attrs,
            }
    except Exception as e:
        logger.error(f"Failed to read metadata for {path}: {e}")
        # ee-raise as a known error type
        raise RasterReadError(
            f"Failed to read/parse metadata for {path}. Error: {e}"
        ) from e


def _validate_raster_metadata(metadata_list):
    """
    Validate a list of raster meta-data dicts for consistency.

    Checks that all rasters share the same CRS, _FillValue,
    scale_factor, and add_offset.

    Parameters
    ----------
    metadata_list : List[dict]
        A list of raster metadata dictionaries, typically from
        `_read_raster_metadata`.

    Returns
    -------
    dict
        A dictionary of the "safe attributes" that are
        consistent across all raster files.

    Raises
    ------
    IncompatibleRasterError
        If any critical metadata attributes are mismatched.
    """
    if not metadata_list:
        return {}

    # Use the first raster's metadata as the reference
    ref = metadata_list[0]

    # Define the checks we need to run as a list of tuples:
    # (key_in_metadata_dict, user_facing_attribute_name_for_error)
    CHECKS_TO_RUN = [
        ('crs', 'CRS'),
        ('nodata', '_FillValue'),
        ('scale_factor', 'scale_factor'),
        ('add_offset', 'add_offset'),
    ]

    # Loop through the rest of the rasters
    for meta in metadata_list[1:]:
        for key, attr_name in CHECKS_TO_RUN:
            if meta[key] != ref[key]:
                raise IncompatibleRasterError(
                    f"Input rasters do not share the same '{attr_name}'. "
                    f"{Path(ref['path']).name} has '{ref[key]}' but "
                    f"{Path(meta['path']).name} has '{meta[key]}'."
                )

    # All checks passed. Return the single, consistent set of safe attrs.
    # This logic is unchanged and correct.
    safe_attrs = {}
    if ref['nodata'] is not None:
        safe_attrs['_FillValue'] = ref['nodata']
    if ref['scale_factor'] is not None:
        safe_attrs['scale_factor'] = ref['scale_factor']
    if ref['add_offset'] is not None:
        safe_attrs['add_offset'] = ref['add_offset']

    return safe_attrs


def _concat_with_info(objs, **kwargs):
    """
    Thin wrapper for `xarray.concat` which logs an info message if the optional
    `bottleneck` library is not available.

    Parameters
    ----------
    objs : List[xarray.DataArray or xarray.Dataset]
        List of xarray objects to concatenate.
    **kwargs : keyword arguments
        Additional arguments passed to `xarray.concat`.
    """
    if not module_available("bottleneck"):
        logger.info(
            "Installing the optional `bottleneck` module may accelerate "
            "`xarray` concatenation. (pip install bottleneck)"
        )
    return xr.concat(objs, **kwargs)


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
        raise ValueError(
            "Bad bounding box. Longitude must be between -180 and 180 degrees."
        )
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError(
            "Bad bounding box. Latitude must be between -90 and 90 degrees."
        )
