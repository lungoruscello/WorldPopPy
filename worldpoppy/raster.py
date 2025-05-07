"""
Functions to combine WorldPop rasters for several countries and years.

Note:
    This module is a port of the "raster.py" module from the `blackmarblepy`
    package by Gabriel Stefanini Vicente and Robert Marty. `blackmarblepy` is
    licensed under the Mozilla Public License (MPL-2.0), as is the present
    modified version.

    Links:
    - https://github.com/worldbank/blackmarblepy
    - https://github.com/worldbank/blackmarblepy/blob/main/LICENSE
"""

import logging
from collections import defaultdict
from tempfile import TemporaryDirectory
from typing import List

import geopandas as gpd
import rioxarray
import xarray as xr
from rioxarray.merge import merge_arrays
from shapely import box
from shapely.geometry import mapping
from tqdm.auto import tqdm

from worldpoppy.config import *
from worldpoppy.download import WorldPopDownloader
from worldpoppy.manifest import extract_year
from worldpoppy.utils import validate_bbox

logger = logging.getLogger(__name__)


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
        **merge_kwargs
):
    """
    Merge a collection of country-specific rasters and stack different years (if any)
    along the time dimension.

    Parameters
    ----------
    product_name : str
        The name of the WorldPop data product of interest.
    aoi : str or List[str] or geopandas.GeoDataFrame

        The area of interest (AOI) for which to obtain the raster data. Users can specify
        this area using:
            - one or more three-letter country codes (alpha-3 IS0 codes);
            - a GeoDataFrame with one or more polygonal geometries; or
            - a bounding box of the format (min_lon, min_lat, max_lon, max_lat).
            In the latter two cases, WorldPop data is first downloaded and merged for all
            countries that intersect the area of interest, even if this intersection is
            only small. Subsequently, the merged raster is clipped using the AOI.

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
     **merge_kwargs : keyword arguments
        Additional arguments passed to `rioxarray.merge.merge_arrays`,
        which give more control over how input rasters should be
        merged (e.g., `method` or `bounds`).

    Returns
    -------
    xr.Dataset
        The combined raster data for several countries and years (where applicable).
    """
    other_read_kwargs = {} if other_read_kwargs is None else other_read_kwargs

    aoi = _format_user_aoi(aoi)
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
            skip_download_if_exists
        )

        if years is None:
            # static product: merge only once
            merged = _merge_rasters(all_raster_paths, **shared_opts)
            return merged.squeeze()

        # annual product: split raster paths by year
        paths_by_year = defaultdict(list)
        for path in all_raster_paths:
            year = extract_year(path.name)
            paths_by_year[year].append(path)

        # merge years separately
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
        geoms = clipping_gdf.geometry.apply(mapping)
        da = da.rio.clip(geoms, clipping_gdf.crs, drop=True)

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
    if not _module_available("bottleneck"):
        logger.warning(
            "Installing the optional `bottleneck` module will accelerate "
            "`xarray` concatenation. (pip install bottleneck)"
        )
    return xr.concat(objs, **kwargs)


def _format_user_aoi(aoi):
    """ TODO """
    if isinstance(aoi, gpd.GeoDataFrame):
        # GeoDataFrame was passed
        if aoi.crs != WGS84_CRS:
            aoi = aoi.to_crs(WGS84_CRS)  # ensure proper CRS

    elif isinstance(aoi, (list, tuple)):
        # bounding box was passed
        if not isinstance(aoi[0], str):
            validate_bbox(aoi)  # check lat/lon values are in range
            aoi = gpd.GeoDataFrame(geometry=[box(*aoi)], crs=WGS84_CRS)  # convert to GeoDataFrame

    else:
        # nothing to do
        pass

    return aoi


def _module_available(module_name):
    """Check if a Python module is available for import."""
    try:
        exec(f"import {module_name}")
    except ModuleNotFoundError:
        return False
    else:
        return True
