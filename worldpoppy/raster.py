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
import tempfile
from collections import defaultdict
from typing import List

import rioxarray
import xarray as xr
from rioxarray.merge import merge_arrays
from tqdm.auto import tqdm

from worldpoppy.config import *
from worldpoppy.download import WorldPopDownloader
from worldpoppy.manifest import extract_year

logger = logging.getLogger(__name__)


def wp_raster(
        product_name,
        iso3_codes,
        years=None,
        *,
        cache_downloads=True,
        skip_download_if_exists=True,
        read_kwargs=None,
        **merge_kwargs
):
    """
    Merge a collection of country-specific rasters and stack different years (if any)
    along the time dimension.

    Parameters
    ----------
    product_name : str
        The name of the WorldPop product of interest.
    iso3_codes : str or List[str]
        The three-letter ISO code (or ISO codes) of the country (or countries) of interest.
    years : int or List[int], optional
        For annual data products, the year (or years) of interest. For static data products,
        this argument must be None (default).
    cache_downloads: bool, optional, default=True
        Whether to cache downloaded source rasters.
    skip_download_if_exists : bool, optional, default=True
        Whether to skip downloading source rasters that already exist in the local cache.
    read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to `rioxarray.open_rasterio`
        when reading individual raster files.

    Returns
    -------
    xr.Dataset
        The combined raster data for several countries and years (where applicable).
    """
    read_kwargs = {} if read_kwargs is None else read_kwargs

    if not cache_downloads and skip_download_if_exists:
        skip_download_if_exists = False
        logger.warning(
            "'skip_download_if_exists' has no effect is 'cache_downloads' is set to False'."
        )

    with (tempfile.TemporaryDirectory() if not cache_downloads else get_cache_dir() as d):
        # download all rasters with max concurrency
        downloader = WorldPopDownloader(directory=d)
        all_fpaths = downloader.download(
            product_name,
            iso3_codes,
            years,
            skip_download_if_exists
        )

        if years is None:
            # for a static product: merge once and return
            merged = _merge_rasters(all_fpaths, read_kwargs, **merge_kwargs)
            return merged.squeeze()

        # for an annual product: split paths by year
        grpd = defaultdict(list)
        for fpath in all_fpaths:
            year = extract_year(fpath.name)
            grpd[year].append(fpath)

        # merge years separately
        annual_rasters = []
        pbar = tqdm(grpd.items(), total=len(grpd), desc="COLLATING YEARS | Processing...")
        for year, year_paths in pbar:
            merged = _merge_rasters(year_paths, read_kwargs, **merge_kwargs)
            merged['year'] = year
            annual_rasters.append(merged)

        # return stacked years
        time_series = _concat_with_warning(
            annual_rasters,
            dim='year',
            combine_attrs='drop_conflicts'
        )
        return time_series.squeeze()


def _merge_rasters(raster_fpaths, read_kwargs, **merge_kwargs):
    """
    Merge multiple raster files using `rioxarray.merge.merge_arrays`.

    Parameters
    ----------
    raster_fpaths : List[Path] or List[str]
        List of paths to the input raster files that are to be merged.
    read_kwargs : dict
        Dictionary with keyword arguments passed to `rioxarray.open_rasterio`
        These arguments control how input rasters are read.
    **merge_kwargs : keyword arguments
        Additional arguments passed to `rioxarray.merge.merge_arrays`.
        These arguments define how input rasters should be merged.

    Returns
    -------
    xarray.DataArray
        The merged raster

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
            da = rioxarray.open_rasterio(path, **read_kwargs)
        except Exception as e:
            raise ValueError(
                f"Failed to read raster file at {path}. Error: {e}\n"
                "If you suspect a corrupted cache, please try to delete the affected "
                "file and trigger the download again."
            )

        # ensure masking and scaling meta-data is aligned
        if '_FillValue' in da.attrs:
            if fill_val is None:
                fill_val = da.attrs['_FillValue']
            else:
                if da.attrs['_FillValue'] != fill_val:
                    raise ValueError(
                        "Country rasters do not use the same '_FillValue'. Try calling "
                        "this function again by passing the 'mask_and_scale' flag to "
                        "`read_kwargs`."
                    )

        if 'scale_factor' in da.attrs:
            if scale_factor is None:
                scale_factor = da.attrs['scale_factor']
            else:
                if da.attrs['scale_factor'] != scale_factor:
                    raise ValueError(
                        "Country rasters do not use the same 'scale_factor'. Try calling "
                        "this function again by passing the 'mask_and_scale' flag to "
                        "`read_kwargs`."
                    )

        rasters.append(da)

    return merge_arrays(rasters, **merge_kwargs)


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


def _module_available(module_name):
    """Check if a Python module is available for import."""
    try:
        exec(f"import {module_name}")
    except ModuleNotFoundError:
        return False
    else:
        return True
