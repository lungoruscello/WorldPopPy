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
from worldpoppy.manifest import extract_year
from worldpoppy.utils import module_available, geolocate_name

logger = logging.getLogger(__name__)

__all__ = [
    "RasterReadError",
    "IncompatibleRasterError",
    "wp_raster",
    "bbox_from_location",
    "merge_rasters"
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
        download_dry_run=False,
        to_crs=None,
        **merge_kwargs
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

    # parse the area of interest
    if isinstance(aoi, (list, tuple)):
        if not isinstance(aoi[0], str):
            # convert bounding box to GeoDataFrame
            _validate_bbox(aoi)
            box_poly = shapely.box(*aoi)
            aoi = gpd.GeoDataFrame(geometry=[box_poly], crs=WGS84_CRS)

    if isinstance(aoi, gpd.GeoDataFrame):
        # find the ISO codes of countries intersecting the GeoDataFrame
        world = load_country_borders()
        joined = gpd.sjoin(
            world,
            aoi.to_crs(WGS84_CRS),
            predicate='intersects',
            how='right'
        )
        iso3_codes = sorted(joined.iso3.unique())
    else:
        # ensure that country codes were passed
        if isinstance(aoi, str):
            iso3_codes = [aoi]
        else:
            if not isinstance(aoi[0], str):
                raise ValueError(
                    "Cannot parse 'aoi'. Please pass one or more country codes, "
                    "a GeoDataFrame with one or more polygons, or a bounding box "
                    "specifying (min_lon, min_lat, max_lon, max_lat)."
                )
            iso3_codes = aoi

    if not cache_downloads and skip_download_if_exists:
        skip_download_if_exists = False
        logger.warning(
            "'skip_download_if_exists' has no effect is 'cache_downloads' is set to False'."
        )

    # prepare shared merge arguments
    clipping_gdf = aoi if isinstance(aoi, gpd.GeoDataFrame) else None
    shared_merge_opts = dict(
        masked=masked,
        mask_and_scale=mask_and_scale,
        other_read_kwargs=other_read_kwargs,
        res=res,
        clipping_gdf=clipping_gdf,
        to_crs=to_crs
    )
    shared_merge_opts.update(**merge_kwargs)

    with (TemporaryDirectory() if not cache_downloads else get_cache_dir() as d):
        # download all required rasters
        all_raster_paths = WorldPopDownloader(directory=d).download(
            product_name,
            iso3_codes,
            years,
            skip_download_if_exists,
            dry_run=download_dry_run
        )

        if download_dry_run:
            return None

        if years is None:
            # static product: merge only once
            merged = merge_rasters(all_raster_paths, **shared_merge_opts)
            return merged.squeeze()

        # annual product
        # > split raster paths by year
        paths_by_year = defaultdict(list)
        for path in all_raster_paths:
            year = extract_year(path.name)
            paths_by_year[year].append(path)

        # > merge rasters separately by year
        annual_rasters = []
        pbar = tqdm(
            paths_by_year.items(),
            total=len(paths_by_year),
            desc="Processing years...",
            leave=False,
        )
        for year, year_paths in pbar:
            merged = merge_rasters(year_paths, **shared_merge_opts)
            merged['year'] = year
            annual_rasters.append(merged)

        # > stack years
        time_series = _concat_with_info(
            annual_rasters,
            dim='year',
            combine_attrs='drop_conflicts'
        )
        return time_series.squeeze()


def merge_rasters(
        raster_fpaths,
        masked=False,
        mask_and_scale=False,
        other_read_kwargs=None,
        clipping_gdf=None,
        to_crs=None,
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
    RasterReadError
        If reading an input raster fails.

    IncompatibleRasterError
        - If input rasters have mismatched Coordinate Reference Systems.
        - If input rasters have mismatched `_FillValue` or `scale_factor` attributes.
    """

    # read country rasters into a list
    rasters = []
    fill_val_ref = None
    scaling_ref = None
    crs_ref = None

    for i, path in enumerate(raster_fpaths):
        try:
            da = rioxarray.open_rasterio(
                path,
                masked=masked,
                mask_and_scale=mask_and_scale,
                **other_read_kwargs
            )
        except Exception as e:
            raise RasterReadError(
                f"Failed to read raster file at {path}. Error: {e}\n"
                "If you suspect a corrupted cache, please try to delete the affected "
                "file and trigger the download again."
            )

        # ensure consistent CRS
        this_crs = da.rio.crs
        if crs_ref is None:
            crs_ref = this_crs
        elif this_crs != crs_ref:
            raise IncompatibleRasterError(
                f"Input rasters do not share the same CRS. Found mismatch: {this_crs} != {crs_ref}.\n"
                "Ensure all rasters have the same projection before merging."
            )

        # ensure consistent _FillValue (if any)
        if '_FillValue' in da.attrs:
            if fill_val_ref is None:
                fill_val_ref = da.attrs['_FillValue']
            else:
                if da.attrs['_FillValue'] != fill_val_ref:
                    raise IncompatibleRasterError(
                        "Country rasters do not use the same '_FillValue'. Please try again "
                        "with either the 'masked' or 'mask_and_scale' argument set to True."
                    )

        # ensure consistent scale_factor (if any)
        if 'scale_factor' in da.attrs:
            if scaling_ref is None:
                scaling_ref = da.attrs['scale_factor']
            else:
                if da.attrs['scale_factor'] != scaling_ref:
                    raise IncompatibleRasterError(
                        "Country rasters do not use the same 'scale_factor'. Please try again "
                        "with the 'mask_and_scale' argument set to True."
                    )

        rasters.append(da)

    da = merge_arrays(rasters, **merge_kwargs)

    # optional clipping
    if clipping_gdf is not None:
        geoms = clipping_gdf.geometry.apply(shapely.geometry.mapping)
        da = da.rio.clip(geoms, clipping_gdf.crs, drop=True, all_touched=True)

    # optional re-projection
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
        raise ValueError("You must specify exactly one of 'width_degrees' or 'width_km'.")

    if width_degrees is not None:
        # distance specified in degrees
        half_width = width_degrees / 2
        return (
            lon - half_width, lat - half_width,
            lon + half_width, lat + half_width
        )

    # define a local Azimuthal Equidistant projection
    proj4_str = f"+proj=aeqd +lon_0={lon} +lat_0={lat} +x_0=0 +y_0=0 +datum=WGS84 +units=m"
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
        raise ValueError("Bad bounding box. Longitude must be between -180 and 180 degrees.")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("Bad bounding box. Latitude must be between -90 and 90 degrees.")
