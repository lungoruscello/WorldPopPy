"""
This is the main module of `WorldPopPy`. It provides logic to fetch raster
data from `WorldPop <https://www.worldpop.org/>`__ through several alternative
specifications for the geographic area of interest.

Main methods
------------------------
    - :func:`wp_raster`
        Retrieve WorldPop data for arbitrary geographical areas and
        multiple years (where applicable).
    - :func:`wp_warp`
        Reproject or resample a WorldPop raster.
    - :func:`merge_rasters`
        Merge multiple raster files and optionally clip the result.
    - :func:`bbox_from_location`
        Generate a bounding box from a location name or GPS coordinate.
        The result can be used to specify the AoI for `wp_raster`.

"""
import logging
import re
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Tuple
from contextlib import nullcontext

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray
import shapely
import xarray as xr
from pyproj import CRS, Transformer
from rasterio.enums import Resampling
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from rioxarray.exceptions import NoDataInBounds
from rioxarray.merge import merge_arrays
from worldpoppy.borders import load_country_borders
from worldpoppy.config import WGS84_CRS, get_cache_dir
from worldpoppy.country_names import resolve_country_name
from worldpoppy.download import WorldPopDownloader
from worldpoppy.func_utils import (
    BboxInvalidError,
    module_available,
    geolocate_name,
    standardise_iso3_codes,
    validate_bbox,
    validate_bbox_wgs84,
    get_buffered_bounds,
)
from worldpoppy.manifest_loader import _get_active_manifest_info, resolve_product_years
from worldpoppy import __url__ as wpy_url, __version__ as wpy_version

logger = logging.getLogger(__name__)

__all__ = [
    "RasterReadError",
    "IncompatibleRasterError",
    "wp_raster",
    "wp_warp",
    "bbox_from_location",
    "merge_rasters",
]

# Map config strings to Rasterio Enums for Reprojection/Warping
RESAMPLING_MAP = {
    'sum': Resampling.sum,
    'mean': Resampling.average,
    'max': Resampling.max,
    'min': Resampling.min,
    'median': Resampling.med,
    'nearest': Resampling.nearest,
    'bilinear': Resampling.bilinear,
    'cubic': Resampling.cubic
}

# --- Metadata of results (see docs/metadata.md) ---
# Fixed attribute values. The creator of the data is WorldPop; WorldPopPy
# provides no data (only its curated product names and notes are its own).
CREATOR_NAME = "WorldPop"
CREATOR_URL = "https://www.worldpop.org"
SOURCE_DESCRIPTION = (
    "WorldPop gridded datasets (see 'references'), processed with WorldPopPy (see 'history')"
)
RESULT_COMMENT = (
    f"Retrieved and processed with WorldPopPy ({wpy_url}), an independent client "
    "not affiliated with WorldPop. Please cite the original datasets (see "
    "'references'). The summary URLs in 'references' are inferred by WorldPopPy "
    "and may occasionally not resolve; the official dataset titles help to find "
    "the pages."
)

# Appended to error messages about unusable `aoi` arguments
AOI_INPUT_HINT = (
    "Please pass one or more three-letter country codes or country names, a bounding box "
    "(min_lon, min_lat, max_lon, max_lat), a GeoDataFrame, a GeoSeries, a "
    "shapely geometry, or the path to a vector file."
)


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
    name=None,
    chunks=None,
    pre_clip_bbox=None,
    cache_downloads=True,
    skip_download_if_exists=None,
    masked=True,
    mask_and_scale=None,
    other_read_kwargs=None,
    suppress_pre_clip=False,
    download_chunk_size=1024*1024*4,
    download_dry_run=False,
):
    """
    Return WorldPop data for the user-defined area of interest (AoI) and the
    specified years (where applicable).

    Note that WorldPop organises its raster files by country. If the AoI spans
    multiple countries, this function will automatically merge all corresponding
    raster files. If multiple years are requested, the raster data is stacked
    along a new 'year' dimension.

    By default, this function returns a regular `xarray.DataArray`. If users
    provide the `chunks` argument, a lazy-loaded  *Dask* array is returned
    instead.

    This function implements several optimisation techniques to minimise
    the memory footprint involved when working with raster data from large
    countries.

    Parameters
    ----------
    product_name : str
        The name of the WorldPop data product of interest.
    aoi : str, List[str], Tuple[float], GeoDataFrame, GeoSeries, shapely geometry or Path
        The area of interest (AoI) for which to obtain the raster data. Users can specify
        this area using:

        - one or more three-letter country codes (alpha-3 IS0 codes) or country
          names. A name is converted into its code (with a warning) only if it
          matches exactly one country; use
          :func:`~worldpoppy.country_names.find_country_code` to look
          up codes;
        - a bounding box of the format (min_lon, min_lat, max_lon, max_lat);
        - a GeoDataFrame or GeoSeries with one or more geometries (in any CRS);
        - a shapely geometry, whose coordinates are assumed to be longitude and
          latitude (WGS84), as for bounding boxes; or
        - the path to a vector file (e.g., GeoPackage, GeoJSON or Shapefile) that
          `geopandas.read_file` can read. A string counts as a path if it contains
          a path separator or ends in a file suffix (e.g., 'districts.gpkg'). To
          read a specific layer or a subset of a file, read it with
          `geopandas.read_file` yourself and pass the resulting GeoDataFrame.

        Country codes and names are used as they are. For all other AoI types, WorldPop data
        is first downloaded and merged for all countries that intersect the area of
        interest, regardless of how large this intersection is. Subsequently, the
        merged raster is then clipped using the AoI. Every cell that the AoI touches
        is kept in full (`all_touched=True` in `rioxarray.clip`), including cells on
        its boundary.

        The AoI's CRS does not affect the CRS of the result (see Returns).
    name : str, optional
        A custom name for the returned DataArray. This is useful for plotting
        (it appears as the title/label) or when converting to a Dataset.
        Defaults to `product_name` if not provided.
    chunks : str, int, dict or None, optional, default=None
        If chunks is provided, the raster data is loaded into a *Dask* array
        for better memory management.

        - If 'auto', Dask chooses the chunk size.
        - If int K, the data is loaded in chunks of size (K, K).
          Equivalent to passing {'x': K, 'y': K}.
        - If dict (e.g., {'x': 1024, 'y': 1024}), that specific chunking is used.

        Lazy reading requires `masked=True` and `cache_downloads=True`.
    years : int or List[Union[int, str]] or str or None, optional
        One or more years of interest or a keyword string.
        For static data products, this argument is usually None (default).

        * 'all' Retrieve all available data for the specified product.
            * For **multi-year** products, this returns a 3D array stacked
              along the `year` dimension (unless only one year exists,
              in which case it returns a 2D array).
            * For **static** products, returns the single available raster.
        * 'first': Retrieve data for the earliest available year.
        * 'last': Retrieve data for the most recent available year.
        * List: A list containing integers and/or keywords (e.g.,
          ``[2010, 'last']``).
        * None: (Default) for static data products.

    pre_clip_bbox : Tuple[float, float, float, float], optional
        A bounding box (min_lon, min_lat, max_lon, max_lat) to which
        input rasters will *immediately* be clipped after loading them
        from disk. This is the **manual** pre-clipping boundary.
        If provided, this overrides the **automatic** buffered pre-clipping
        mechanism (which is applied by default when users pass the AoI as
        either a GeoDataFrame or BBOx). Manual pre-clipping is useful when
        working with country-code AoIs like Chile, where remote outlying
        islands result in a merged raster that is largely empty, causing
        RAM explosions. Mutually exclusive with `suppress_pre_clip`.
    cache_downloads: bool, optional, default=True
        Whether to cache downloaded source rasters. Without caching, the downloads
        are deleted when this function returns, so lazy reading (`chunks`) is not
        possible.
    skip_download_if_exists : bool, optional
        Whether to skip downloading source rasters that already exist in the local cache.
        Defaults to True if `cache_downloads` is True. Has no effect (and defaults to
        False) if `cache_downloads` is False, since no local cache is used then.
    masked: bool, optional, default=True
        If True, set the nodata values of all input rasters to NaN and apply
        their scale and offset, if any (`mask_and_scale=True` in
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`__).
        The result is then floating-point data. If False, read the raw values
        in the source files' data type, with their nodata value. (WorldPop
        rasters carry no scale or offset, so masking does not change any
        valid value.)
    mask_and_scale: bool, optional
        Follows `masked` and only exists for compatibility. If given, it must
        equal `masked`: reading masked but unscaled data is not supported (use
        `rioxarray.open_rasterio` directly for this).
    other_read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`__
        when reading input rasters (e.g., `lock` or `band_as_variable`).
        Note that `chunks` passed here will be ignored in favour of the
        explicit `chunks` argument. The read settings (`masked`,
        `mask_and_scale`) cannot be passed here.
    suppress_pre_clip : bool, optional, default=False
        If True, no **automatic** or **manual** pre-clipping is ever applied when
        loading input rasters. Mutually exclusive with `pre_clip_bbox`.
    download_chunk_size : int, optional, default=4MB
        The size (in bytes) of chunks to read/write during raster downloads.
        The large, default chunk size aims to improve performance on systems
        with real-time file scanning (e.g., antivirus).
    download_dry_run : bool, optional, default=False
        If True, only check how many raster files would need to be downloaded
        from WorldPop if `download_dry_run` was False. Report the number and
        size of required file downloads, but do not actually fetch or process
        any files.

    Returns
    -------
    xarray.DataArray or None
        The combined raster data. The result is always in the CRS of the WorldPop
        source rasters (WGS84, EPSG:4326), whatever the AoI's CRS. To reproject it,
        use :func:`wp_warp` (with `resampling='sum'` for counts such as population).

        - For static products, dimensions are ``(y, x)``.
        - For multi-years products, dimensions are likewise ``(y, x)`` IF
          users only request a single year.
        - If multiple years are requested, dimensions are ``(year, y, x)``.

        Returns None if `download_dry_run` is True.

        The attributes of the result credit WorldPop as the creator of the data
        (with the recommended citations and summary pages of the source datasets)
        and record how the result was produced. They are described on the
        documentation page "Metadata of WorldPopPy results" (`docs/metadata.md`).

    Raises
    -------
    ValueError
        If `mask_and_scale` is given and differs from `masked`, if `chunks`
        is combined with `masked=False` or `cache_downloads=False`, or if
        `other_read_kwargs` contains `masked` or `mask_and_scale`.

    BboxInvalidError
        If `pre_clip_bbox` (or a bounding-box AoI) is not a valid longitude/latitude
        box, e.g. because a value is not finite or a minimum exceeds its maximum.

    RasterReadError
        If reading an input raster fails, or if `pre_clip_bbox` cannot be applied
        to it.

    IncompatibleRasterError
        This function validates input-raster attributes before merging.

        - `crs` is *always* validated.
        - `_FillValue`, `scale_factor` and `add_offset` are validated *only*
          for raw reads (`masked=False`).

        (This function thus trusts `rioxarray` to correctly normalise
         masked input rasters, even if the underlying source files have
         different `_FillValue`, `scale_factor` or `add_offset` attributes.)
    """

    # --- Resolve read settings ---
    masked, mask_and_scale = _resolve_read_settings(masked, mask_and_scale, chunks, other_read_kwargs)
    if chunks is not None and not cache_downloads:
        raise ValueError(
            "Lazy reading (`chunks`) requires `cache_downloads=True`: a lazy result reads "
            "its source files only when it is computed, but without caching, the "
            "downloaded files are deleted when `wp_raster` returns."
        )

    if skip_download_if_exists is None:
        skip_download_if_exists = cache_downloads
    elif skip_download_if_exists and not cache_downloads:
        # Only warn if the user explicitly asked for a contradictory setting
        skip_download_if_exists = False
        logger.warning(
            "'skip_download_if_exists' has no effect if "
            "'cache_downloads' is set to False."
        )

    # --- Resolve 'all' into concrete years/None BEFORE processing ---
    # This ensures the rest of the pipeline sees only concrete Lists or None.
    years = resolve_product_years(product_name, years)

    if suppress_pre_clip and pre_clip_bbox is not None:
        raise ValueError(
            "Cannot provide `pre_clip_bbox` when `suppress_pre_clip` is True."
        )

    # --- Standardise Chunks ---
    if isinstance(chunks, int):
        chunks = {'x': chunks, 'y': chunks}

    # --- Process the AoI ---
    # The output 'aoi' variable holds either a GeoDataFrame or the original ISO codes.
    aoi, iso3_codes, orig_aoi_type = _standardise_aoi(aoi)

    # --- Validate/Process pre_clip_bbox based on AoI type ---
    # The pre_clip_bbox is only allowed for ISO codes.
    if pre_clip_bbox is not None:
        validate_bbox_wgs84(pre_clip_bbox)

        if orig_aoi_type == 'bbox':
            pre_clip_bbox = None
            logger.warning(
                'Ignoring `pre_clip_bbox` since `aoi` is a bounding box itself. '
                "Relying on this box for automatic pre-clip instead."
            )
        elif orig_aoi_type == 'gdf':
            pre_clip_bbox = None
            logger.warning(
                'Ignoring `pre_clip_bbox` for geometry AoI. Relying on '
                "the AoI's bounding box for automatic pre-clip instead."
            )

    # --- Default Naming ---
    if name is None:
        name = product_name

    # --- Prepare Shared Raster-processing Arguments ---
    # `clipping_gdf` is used for the final precise clip AND for the
    # automatically inferred pre-clip bounds.
    clipping_gdf = aoi if isinstance(aoi, gpd.GeoDataFrame) else None

    shared_processing_kwargs = dict(
        chunks=chunks,
        masked=masked,
        mask_and_scale=mask_and_scale,
        other_read_kwargs=other_read_kwargs,
        pre_clip_bbox=pre_clip_bbox,
        clipping_gdf=clipping_gdf,
        suppress_pre_clip=suppress_pre_clip,
    )

    with TemporaryDirectory() if not cache_downloads else nullcontext(get_cache_dir()) as d:
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

        if years is None:
            # --- Static Product ---
            # Meta-data validation for file-paths passed to `_merge_rasters`
            # is *always* performed within that function.
            merged, used_paths, safe_attrs = _merge_rasters(
                all_raster_paths, **shared_processing_kwargs
            )
            result = merged.squeeze("band")

        else:
            # --- Multi-year Product ---
            paths_by_year = defaultdict(list)
            for path, mrow in zip(all_raster_paths, filtered_mdf.itertuples()):
                year = int(mrow.year)  # convert from numpy type
                paths_by_year[year].append(path)

            # In the multi-year case, we must validate raster meta-data for
            # raster files from *all years* in one go (to catch inconsistencies
            # across years).
            safe_attrs = _validate_raster_attrs(all_raster_paths, masked, mask_and_scale)

            # Merge the actual rasters separately by year
            annual_rasters, used_paths = [], []
            for year, year_paths in paths_by_year.items():
                merged, used_year_paths, _ = _merge_rasters(year_paths, **shared_processing_kwargs)
                merged['year'] = year
                annual_rasters.append(merged)
                used_paths.extend(used_year_paths)

            # Stack years via `xr.concat` (attributes are set below). A single
            # year is dropped as a dimension (its year stays a coordinate).
            result = _concat_with_info(annual_rasters, dim='year', combine_attrs='drop').squeeze("band")
            if result.sizes['year'] == 1:
                result = result.squeeze('year')

        # --- Metadata (see docs/metadata.md) ---
        result.name = name
        used = {str(p) for p in used_paths}
        used_rows = filtered_mdf[[str(p) in used for p in all_raster_paths]]

        steps = [f"merged {len(used_rows)} files"]
        if pre_clip_bbox is not None:
            steps.append("pre-clipped to bbox")
        if clipping_gdf is not None:
            steps.append("clipped to AoI")
        call = f"wp_raster({product_name!r}" + (f", years={years!r})" if years is not None else ")")

        attrs = _provenance_attrs(product_name, used_rows, _describe_aoi(aoi, iso3_codes, orig_aoi_type))
        attrs.update(_processing_attrs(
            call, steps,
            source_files=list(used_rows.remote_name),
            masked=masked,
            mask_and_scale=mask_and_scale,
            pre_clip_bbox=pre_clip_bbox,
        ))
        return _set_result_metadata(result, attrs, masked, mask_and_scale, safe_attrs)


def wp_warp(
    da,
    to_crs=None,
    res=None,
    resampling=None,
    **kwargs
):
    """
    Reproject or resample a raster.

    This is a convenience wrapper around `rioxarray.reproject` that handles
    Nodata values, memory materialisation (handling Dask vs Eager), and
    Enum-based "resampling" arguments.

    WARNING: This function is EAGER. It triggers immediate computation.
    If 'da' is a lazy Dask array, it will be loaded into memory (materialised)
    before processing.

    It supports three modes:
    1. Reprojection: Change CRS (provide `to_crs`).
    2. Resampling: Change resolution in current CRS (provide `res`, leave `to_crs=None`).
    3. Both: Change both CRS *and* resolution.

    Parameters
    ----------
    da : xarray.DataArray
        The input raster data (usually from `wp_raster`).
    to_crs : str or pyproj.CRS, optional
        The target Coordinate Reference System (e.g., "EPSG:3035").
        If None, the raster's current CRS is used (enabling pure resampling).
    res : tuple or float, optional
        Target resolution in the units of `to_crs` (if provided) or
        in the units of the source CRS (if `to_crs` is not provided).
        If a single float is provided, it is used for both X and Y axes.
        If None, the resolution is determined automatically by rioxarray.
    resampling : str or rasterio.enums.Resampling, optional
        The resampling method to use (e.g., 'nearest', 'bilinear', 'sum').
        If None, defaults to 'nearest'.
    **kwargs : dict
        Additional keyword arguments passed directly to
        `rioxarray.reproject <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray.raster_array.RasterArray.reproject>`__.
        Useful for passing specific GDAL warp options.

    Returns
    -------
    xarray.DataArray
        The warped raster.

    Raises
    ------
    ValueError
        If neither `to_crs` nor `res` is given (nothing to do), if `resampling`
        is an unknown name, or if the input has no CRS and `to_crs` is not given.
    """

    # --- Check that there is something to do ---
    if to_crs is None and res is None:
        raise ValueError(
            "`wp_warp` has nothing to do: pass `to_crs` (to reproject) and/or `res` "
            "(to resample)."
        )

    # --- Resolve Resampling ---
    if resampling is None:
        resampling_enum = Resampling.nearest
    elif isinstance(resampling, str):
        try:
            resampling_enum = RESAMPLING_MAP[resampling]
        except KeyError:
            valid = list(RESAMPLING_MAP.keys())
            raise ValueError(
                f"Unknown resampling method '{resampling}'. Valid: {valid}"
            )
    else:
        resampling_enum = resampling

    # --- Resolve Target CRS ---
    # If to_crs is None, we stay in the current CRS (Pure Resampling)
    target_crs = to_crs if to_crs is not None else da.rio.crs
    if target_crs is None:
        raise ValueError("Input raster has no CRS and 'to_crs' was not provided.")

    # --- Standardise Nodata ---
    # (on a copy: the input must never be modified; the data itself is shared)
    da = _standardise_rio_nodata_meta(da.copy())
    fill_value = da.rio.nodata

    if fill_value is None:
        logger.warning(
            "Warping integer raster without a defined nodata value. "
            "Padding areas may default to 0, which may be indistinguishable "
            "from valid data."
        )

    # --- Handle Dask Arrays (Explicit Eager Load) ---
    # rioxarray.reproject is not truly lazy and often triggers computation implicitly.
    # We make this explicit here to avoid "hidden" memory spikes.
    if da.chunks is not None:
        logger.warning(
            "wp_warp: Input is a lazy Dask array. Materialising into memory "
            "before warping."
        )
        da = da.compute()  # unlike `load()`, this leaves the (lazy) input as it is

    # --- Execute Warp ---
    reproject_kwargs = {'resampling': resampling_enum}
    if res is not None:
        reproject_kwargs['resolution'] = res

    # We explicitly pass nodata to reproject to ensure the *output*
    # (including new padding areas) uses this value.
    if fill_value is not None:
        reproject_kwargs['nodata'] = fill_value

    # Merge explicit args with user-provided kwargs
    # (kwargs overwrite defaults if conflicts exist, though unlikely given keys)
    reproject_kwargs.update(kwargs)

    warped = da.rio.reproject(target_crs, **reproject_kwargs)

    # --- History (all other attributes are kept) ---
    call = f"wp_warp(to_crs={to_crs!r}, res={res!r}, resampling={resampling!r})"
    _append_history(warped, _history_line(call))

    return warped


def merge_rasters(
    raster_fpaths,
    *,
    name=None,
    chunks=None,
    masked=True,
    mask_and_scale=None,
    other_read_kwargs=None,
    pre_clip_bbox=None,
    clipping_gdf=None,
    suppress_pre_clip=False,
):
    """
    Merge multiple rasters.

    This function validates that all input rasters share the same
    critical metadata (CRS, FillValue, etc.) and then creates a new,
    synthetic set of metadata for the final merged raster.


    Parameters
    ----------
    raster_fpaths : List[Path] or List[str]
        List of paths to the input raster files that are to be merged.
    name : str, optional
        A custom name for the returned DataArray.
    chunks : str, int, dict or None, optional, default=None
        If chunks is provided, the raster data is loaded into a *Dask* array
        for better memory management. Lazy reading requires `masked=True`.

        * If 'auto', Dask chooses the chunk size.
        * If int K, the data is loaded in chunks of size (K, K).
          Equivalent to passing {'x': K, 'y': K}.
        * If dict (e.g., {'x': 1024, 'y': 1024}), that specific chunking is used.
        * If None (default), data loading with Dask is *disabled*.

    masked: bool, optional, default=True
        If True, set the nodata values of all input rasters to NaN and apply
        their scale and offset, if any (`mask_and_scale=True` in
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`__).
        The result is then floating-point data. If False, read the raw values
        in the source files' data type, with their nodata value.
    mask_and_scale: bool, optional
        Follows `masked` and only exists for compatibility. If given, it must
        equal `masked`: reading masked but unscaled data is not supported (use
        `rioxarray.open_rasterio` directly for this).
    other_read_kwargs : dict, optional
        Dictionary with additional keyword arguments that are passed to
        `rioxarray.open_rasterio <https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray-open-rasterio>`__
        when reading input rasters (e.g., `lock`
        or `band_as_variable`). Note that `chunks` passed here will be ignored
        in favour of the explicit `chunks` argument. The read settings
        (`masked`, `mask_and_scale`) cannot be passed here.
    pre_clip_bbox : Tuple[float, float, float, float], optional
        An explicit bounding box to use for pre-clipping the input rasters.
        If provided, this overrides the default buffered pre-clipping.
    clipping_gdf : geopandas.GeoDataFrame, optional
        GeoDataFrame with geometries used to clip the merged raster. This is
        used for the **final precise clip**, which keeps every cell that the
        geometries touch (`all_touched=True` in `rioxarray.clip`).
    suppress_pre_clip : bool, optional, default=False
        If True, disables all pre-clipping optimisations.

    Returns
    -------
    xarray.DataArray
        The merged and optionally clipped raster. Its attributes record how it
        was produced (see "Metadata of WorldPopPy results", `docs/metadata.md`);
        since the input files may come from anywhere, they include no WorldPop
        provenance.

    Raises
    ------
    ValueError
        If `mask_and_scale` is given and differs from `masked`, if `chunks`
        is combined with `masked=False`, or if `other_read_kwargs` contains
        `masked` or `mask_and_scale`.

    BboxInvalidError
        If `pre_clip_bbox` is not four finite numbers with each minimum not
        above its maximum.

    RasterReadError
        If reading an input raster fails, or if `pre_clip_bbox` cannot be applied
        to it.

    IncompatibleRasterError
        This function validates input-raster attributes before merging.

        - `crs` is *always* validated.
        - `_FillValue`, `scale_factor` and `add_offset` are validated *only*
          for raw reads (`masked=False`).

        (This function thus trusts `rioxarray` to correctly normalise
         masked input rasters, even if the underlying source files have
         different `_FillValue`, `scale_factor` or `add_offset` attributes.)

    Notes
    -----
    **Performance Warning:**
    This function uses `xarray.combine_first` iteratively to merge rasters
    lazily. While this preserves memory, it builds a nested Dask task graph
    whose depth is proportional to the number of input files.

    Merging a large number of files may result in a `RecursionError` or
    significant overhead during graph construction. If you are processing
    a large number of raster tiles, consider merging them in smaller batches
    first.
    """

    masked, mask_and_scale = _resolve_read_settings(masked, mask_and_scale, chunks, other_read_kwargs)
    if pre_clip_bbox is not None:
        validate_bbox(pre_clip_bbox)  # in the CRS of the rasters (any CRS)

    da, used_paths, safe_attrs = _merge_rasters(
        raster_fpaths,
        chunks=chunks,
        masked=masked,
        mask_and_scale=mask_and_scale,
        other_read_kwargs=other_read_kwargs,
        pre_clip_bbox=pre_clip_bbox,
        clipping_gdf=clipping_gdf,
        suppress_pre_clip=suppress_pre_clip,
    )

    # --- Metadata (see docs/metadata.md) ---
    # The files may come from anywhere, so no WorldPop provenance is recorded
    steps = [f"merged {len(used_paths)} files"]
    if pre_clip_bbox is not None:
        steps.append("pre-clipped to bbox")
    if clipping_gdf is not None:
        steps.append("clipped to AoI")

    da.name = name if name is not None else "wpy_data"
    attrs = _processing_attrs(
        "merge_rasters()", steps,
        source_files=[Path(p).name for p in used_paths],
        masked=masked,
        mask_and_scale=mask_and_scale,
        pre_clip_bbox=pre_clip_bbox,
    )
    return _set_result_metadata(da, attrs, masked, mask_and_scale, safe_attrs)


def _merge_rasters(
    raster_fpaths,
    *,
    chunks,
    masked,
    mask_and_scale,
    other_read_kwargs,
    pre_clip_bbox,
    clipping_gdf,
    suppress_pre_clip,
):
    """
    Merge multiple rasters and optionally clip the result (see `merge_rasters`
    for the parameters). Attributes are left to the caller.

    Returns
    -------
    tuple
        The merged DataArray, the paths of the input rasters that contributed
        to it (rasters without overlap with the pre-clip box are skipped), and
        the validated attributes shared by all input rasters (see
        `_validate_raster_attrs`).
    """
    use_dask = chunks is not None

    # --- Argument Validation ---
    if suppress_pre_clip and pre_clip_bbox is not None:
        raise ValueError(
            "`pre_clip_bbox` must be None when `suppress_pre_clip` is True."
        )

    # --- Standardise Chunks ---
    if isinstance(chunks, int):
        chunks = {'x': chunks, 'y': chunks}

    # --- Metadata Validation ---
    # This ensures all input rasters share the same CRS, _FillValue, etc.
    safe_attrs = _validate_raster_attrs(raster_fpaths, masked, mask_and_scale)

    # --- Consolidate Read Options ---
    # The explicit 'chunks' arg takes priority over anything in 'other_read_kwargs'
    if other_read_kwargs is None:
        read_options = {}
    else:
        read_options = other_read_kwargs.copy()
        if 'chunks' in read_options:
            read_options.pop('chunks')

    read_options['chunks'] = chunks
    read_options['masked'] = masked
    read_options['mask_and_scale'] = mask_and_scale

    # ------------------------------------------------------------------
    # --- Prepare Pre-Clipping Geometry ---
    # ------------------------------------------------------------------
    # We calculate the generic pre-clipping box *once* before the loop
    # that lazy-loads country rasters. Note that `_validate_raster_attrs`
    # guarantees consistency of key attrs (incl. the CRS, which we need
    # to compute the pre-clip geometry).

    processing_clip_box = None

    if not suppress_pre_clip:
        if pre_clip_bbox is not None:
            # Case 1: Pre-clipping BBox *manually* provided
            processing_clip_box = pre_clip_bbox

        elif clipping_gdf is not None:
            # Case 2: Pre-clipping BBox *automatically* inferred
            # We need the raster CRS to calculate the buffer. Peek at first file.
            try:
                processing_clip_box = get_buffered_bounds(
                    clipping_gdf, raster_crs=safe_attrs['crs'], buffer_deg=0.1
                )
                logger.debug(
                    f"Calculated automatic pre-clip bounds: {processing_clip_box}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to calculate automatic buffer from first raster. "
                    f"Pre-clipping disabled. Reason: {e}"
                )

    # ------------------------------------------------------------------
    # --- Open Input Rasters ---
    # ------------------------------------------------------------------
    rasters_to_merge, used_paths = [], []

    for path in raster_fpaths:
        try:
            # --- Open ---
            # We fix a missing 'nodata' attribute from .rio immediately
            # upon load to avoid metada ambiguity
            da = rioxarray.open_rasterio(path, **read_options)
            da = _standardise_rio_nodata_meta(da, masked, mask_and_scale)

            # --- Apply Pre-Clipping Slice (Lazy with Dask) ---
            if processing_clip_box is not None:
                try:
                    # A raster that overlaps the box by a single row or column
                    # still contributes that strip
                    da = da.rio.clip_box(*processing_clip_box, allow_one_dimensional_raster=True)
                except NoDataInBounds:
                    # No overlap with the pre-clip box: the raster contributes
                    # nothing (rather than the full raster being used)
                    da = None
                except Exception as e:
                    if pre_clip_bbox is not None:
                        # A user-provided box is applied or the call fails, so
                        # that the result agrees with its history
                        raise RasterReadError(
                            f"Failed to apply `pre_clip_bbox` {tuple(pre_clip_bbox)} to "
                            f"{Path(path).name}: {e}"
                        ) from e
                    # The automatic pre-clip only saves memory: fall back
                    logger.warning(
                        f"Pre-clipping optimization failed for {Path(path).name}. "
                        f"Proceeding with full raster load. Reason: {e}"
                    )

            if da is None or da.sizes['x'] == 0 or da.sizes['y'] == 0:
                logger.info(f"Skipping {Path(path).name}: no overlap with the pre-clip box.")
                continue

            # -- Round Coordinates ---
            # To avoid floating-point issues in a downstream merge.
            da = da.assign_coords({"x": da.x.round(5), "y": da.y.round(5)})

            # Assign fixed variable name for mosaicking
            # (needed by _lazy_merge_helper)
            da.name = "wpy_data"

            rasters_to_merge.append(da)
            used_paths.append(path)

        except RasterReadError:
            raise
        except Exception as e:
            raise RasterReadError(f"Failed to read {path}: {e}")

    if not rasters_to_merge:
        raise ValueError(
            "No raster data found intersecting the buffered AoI. "
            "Check your AoI coordinates."
        )

    # --- Merge! ---
    if use_dask:
        # We use a custom helper function to ensure
        # that merging different input rasters does *not*
        # trigger execution of the Dask graph.
        da = _lazy_merge_helper(rasters_to_merge)
    else:
        da = merge_arrays(rasters_to_merge)

    # --- Final Precise Clipping ---
    if clipping_gdf is not None:
        geoms = clipping_gdf.geometry.apply(shapely.geometry.mapping)
        da = da.rio.clip(geoms, clipping_gdf.crs, drop=True, all_touched=True)

    return da, used_paths, safe_attrs


def bbox_from_location(centre, width_degrees=None, width_km=None):
    """
    Construct a bounding box centered on a given geographic location.

    The `centre` argument can be either a place name (which is geocoded
    using `geolocate_name`) or a (longitude, latitude) coordinate pair.

    If `width_km` is specified, the bounding box is computed in a local
    Azimuthal Equidistant projection centered on the specified location,
    and then reprojected back to WGS84 longitude/latitude coordinates.

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

    # --- Handle location ---
    if isinstance(centre, str):
        lon, lat = geolocate_name(centre)
    elif isinstance(centre, tuple) and len(centre) == 2:
        lon, lat = centre
    else:
        raise ValueError("Location must be a string or a (lon, lat) tuple.")

    # --- Handle BBox width ---
    num_provided = (width_degrees is None) + (width_km is None)
    if num_provided != 1:
        raise ValueError(
            "You must specify exactly one of 'width_degrees' or 'width_km'."
        )

    if width_km is not None and width_km > 1000 and abs(lat) >= 70:
        warnings.warn(
            "Box generation is near its geometric limit. Width (>1,000km) "
            "combined with high latitude (>=70°) risks significant projection "
            "skew.",
            UserWarning,
            stacklevel=2,
        )

    if width_degrees is not None:
        # TRIVIAL CASE: distance specified in degrees
        half_width = width_degrees / 2
        bounds = (
            lon - half_width, lat - half_width,
            lon + half_width, lat + half_width
        )
        validate_bbox_wgs84(bounds)
        return bounds

    # HARDER CASE: distance specified in kms
    # 1. Define a local Azimuthal Equidistant projection
    proj4_str = (
        f"+proj=aeqd +lon_0={lon} +lat_0={lat} +x_0=0 +y_0=0 +datum=WGS84 +units=m"
    )
    local_aeqd_crs = CRS(proj4_str)

    # 2. Compute box corners in kilometres
    # Note: Under our Azimuthal CRS, the centre point always
    # has the coordinate (0, 0). The bounding box is thus trivial.
    half_width_m = (width_km * 1_000) / 2
    x_min, y_min = -half_width_m, -half_width_m
    x_max, y_max = half_width_m, half_width_m

    # 3. Transform corners back to lon/lat
    from_proj = Transformer.from_crs(local_aeqd_crs, WGS84_CRS, always_xy=True)
    min_lon, min_lat = from_proj.transform(x_min, y_min)
    max_lon, max_lat = from_proj.transform(x_max, y_max)

    bounds = min_lon, min_lat, max_lon, max_lat
    validate_bbox_wgs84(bounds)
    return bounds


def _resolve_read_settings(masked, mask_and_scale, chunks, other_read_kwargs):
    """
    Return the effective read settings `(masked, mask_and_scale)`, which are
    always equal: `mask_and_scale` follows `masked` unless given.

    Raises
    ------
    ValueError
        If `mask_and_scale` is given and differs from `masked` (masked but
        unscaled reads are not supported), if lazy reading (`chunks`) is
        requested for raw data (the lazy merge marks missing data with NaN),
        or if `other_read_kwargs` contains a read setting (which would bypass
        these checks).
    """
    if other_read_kwargs and {'masked', 'mask_and_scale'} & set(other_read_kwargs):
        raise ValueError(
            "`other_read_kwargs` must not contain `masked` or `mask_and_scale`. "
            "Use the `masked` argument instead."
        )

    masked = bool(masked)
    if mask_and_scale is not None and bool(mask_and_scale) != masked:
        if masked:
            raise ValueError(
                "Reading masked but unscaled data (`masked=True`, `mask_and_scale=False`) "
                "is not supported. Use `rioxarray.open_rasterio` directly to read such data."
            )
        raise ValueError(
            "`mask_and_scale=True` requires `masked=True`, since scaling also masks the "
            "data. For raw values, pass `masked=False` (and leave out `mask_and_scale`)."
        )

    if chunks is not None and not masked:
        raise ValueError(
            "Lazy reading (`chunks`) requires `masked=True`, since lazy merging marks "
            "missing data with NaN. For raw values, read eagerly (`chunks=None`)."
        )
    return masked, masked


def _standardise_aoi(aoi):
    """
    Parse the various AoI input formats.

    Country codes and names are used as they are, without any geometry
    (WorldPop data is organised by country anyway). Three-letter strings are
    country codes; longer strings are resolved as country names, unless they
    look like a file path. Only geometry-like inputs are converted into a
    GeoDataFrame, which is later used to clip the merged raster:

    - a bounding box (min_lon, min_lat, max_lon, max_lat) in WGS84;
    - a shapely geometry, assumed to be in WGS84 (it carries no CRS);
    - a GeoSeries or GeoDataFrame in its own CRS; or
    - the path to a vector file (a `pathlib.Path`, or a string with a path
      separator or a file suffix).

    Returns
    -------
    tuple
        The AoI (a GeoDataFrame, or the original country codes or names), the
        list of ISO3 codes of the countries to fetch, and the type of AoI the
        user passed ('iso', 'bbox' or 'gdf', where 'iso' stands for country
        codes and names, and 'gdf' for all geometry-like inputs other than a
        bounding box).

    Raises
    ------
    ValueError
        If the AoI is empty, has no CRS, or intersects no supported country,
        or if a country name is unknown or ambiguous.
    BboxInvalidError
        If a bounding box or shapely geometry has invalid WGS84 coordinates.
    FileNotFoundError
        If a path (or a string that is no country code) does not exist.
    TypeError
        If the AoI is of an unsupported type.
    """
    if isinstance(aoi, (list, tuple)) and len(aoi) == 0:
        raise ValueError(f"`aoi` is empty. {AOI_INPUT_HINT}")

    # --- Country codes and names: used as they are ---
    if _is_country_aoi(aoi):
        return aoi, _country_codes_from_aoi(aoi), 'iso'

    # --- Geometry-like inputs: converted into a GeoDataFrame ---
    orig_aoi_type = 'gdf'
    if isinstance(aoi, (list, tuple)):
        validate_bbox_wgs84(aoi)
        aoi = gpd.GeoDataFrame(geometry=[box(*aoi)], crs=WGS84_CRS)
        orig_aoi_type = 'bbox'
    elif isinstance(aoi, (str, Path)):
        aoi = _read_aoi_file(aoi)
    elif isinstance(aoi, BaseGeometry):
        aoi = _wgs84_geometry_to_gdf(aoi)
    elif isinstance(aoi, gpd.GeoSeries):
        aoi = gpd.GeoDataFrame(geometry=aoi)
    elif not isinstance(aoi, gpd.GeoDataFrame):
        raise TypeError(f"Cannot parse `aoi` of type {type(aoi).__name__}. {AOI_INPUT_HINT}")

    return aoi, _iso3_codes_intersecting(aoi), orig_aoi_type


def _is_country_aoi(aoi):
    """
    Return True if the AoI is given as country codes and/or names: a string
    that does not look like a file path, or a non-empty list or tuple of
    strings.
    """
    if isinstance(aoi, str):
        return not _looks_like_path(aoi)
    return isinstance(aoi, (list, tuple)) and isinstance(aoi[0], str)


def _looks_like_path(text):
    """
    Return True if a string looks like a file path rather than a country code
    or name: it contains a path separator, or it ends in a file suffix (e.g.,
    '.gpkg'). Country names with abbreviations (e.g., 'St. Lucia') do not.
    """
    return bool(re.search(r'[/\\]|\.[A-Za-z0-9_]+$', text.strip()))


def _is_iso3_code(text):
    """
    Return True for a three-letter string (any case, surrounding whitespace
    allowed).
    """
    if not isinstance(text, str):
        return False
    code = text.strip()
    return len(code) == 3 and code.isalpha()


def _country_codes_from_aoi(aoi):
    """
    Return the standardised country codes for an AoI given as one or more
    country codes and/or names. Three-letter strings are taken to be country
    codes; all other strings are resolved as country names (see
    `country_names.resolve_country_name`).
    """
    entries = [aoi] if isinstance(aoi, str) else aoi
    codes = [e if _is_iso3_code(e) else resolve_country_name(e) for e in entries]
    return standardise_iso3_codes(codes)


def _read_aoi_file(path):
    """
    Read an AoI from a vector file, in any format `geopandas.read_file`
    supports.

    Raises
    ------
    FileNotFoundError
        If the path does not exist.
    """
    path = Path(path).expanduser()
    if not path.exists():
        raise FileNotFoundError(
            f"Cannot find the AoI file '{path}'. (`aoi` strings with a path "
            "separator or a file suffix are read as paths to vector files.)"
        )
    return gpd.read_file(path)


def _wgs84_geometry_to_gdf(geom):
    """
    Wrap a shapely geometry in a GeoDataFrame. A shapely geometry carries no
    CRS, so its coordinates are taken to be WGS84 longitude and latitude (as
    for bounding boxes).

    Raises
    ------
    ValueError
        If the geometry is empty.
    BboxInvalidError
        If the geometry's bounds are not valid WGS84 coordinates (e.g., because
        it uses a projected CRS).
    """
    if geom.is_empty:
        raise ValueError("The AoI geometry is empty.")

    try:
        validate_bbox_wgs84(geom.bounds)
    except BboxInvalidError as e:
        raise BboxInvalidError(
            "A shapely geometry AoI must use longitude/latitude coordinates (WGS84), "
            f"but its bounds {geom.bounds} are invalid. If the geometry uses another "
            "CRS, pass it as a GeoSeries with that CRS instead, e.g., "
            "`geopandas.GeoSeries([geom], crs=...)`."
        ) from e

    return gpd.GeoDataFrame(geometry=[geom], crs=WGS84_CRS)


def _iso3_codes_intersecting(aoi_gdf):
    """
    Return the sorted ISO3 codes of all countries that intersect at least
    one geometry of the AoI GeoDataFrame.

    AoI geometries that intersect no country (e.g., because they lie at sea)
    contribute no codes; a warning reports how many there are.

    Raises
    ------
    ValueError
        If the AoI GeoDataFrame is empty, has no CRS, or if no AoI geometry
        intersects any country.
    """
    if aoi_gdf.empty:
        raise ValueError("The AoI GeoDataFrame is empty (it contains no geometries).")
    if aoi_gdf.crs is None:
        raise ValueError(
            "The AoI has no coordinate reference system (CRS). Please set one, e.g., "
            "with `.set_crs('EPSG:4326')` for longitude/latitude coordinates. (For a "
            "vector file without a CRS, read it with `geopandas.read_file`, set the "
            "CRS, and pass the resulting GeoDataFrame.)"
        )

    # Join only the border geometries and their codes, under a private
    # column name. This avoids name clashes with the user's own columns
    # (e.g., an 'iso3' column, which `sjoin` would rename to 'iso3_left').
    world = load_country_borders()
    world = world[['iso3', 'geometry']].rename(columns={'iso3': '_wpy_iso3'})
    joined = gpd.sjoin(
        world, aoi_gdf.to_crs(WGS84_CRS), predicate='intersects', how='right'
    )

    # With a right join, AoI geometries without any country get a NaN code
    unmatched = joined['_wpy_iso3'].isna()
    iso3_codes = sorted(joined.loc[~unmatched, '_wpy_iso3'].unique())

    if not iso3_codes:
        raise ValueError(
            "The area of interest does not intersect any country supported by "
            "WorldPopPy (e.g., because it lies entirely at sea)."
        )

    # Each unmatched AoI geometry appears exactly once in a right join, so
    # counting rows is exact (counting index labels is not, if the user's
    # GeoDataFrame has duplicate index values)
    if num_unmatched := int(unmatched.sum()):
        logger.warning(
            f"{num_unmatched} of {len(aoi_gdf)} AoI geometries do not intersect any "
            "supported country (e.g., because they lie at sea) and contribute no data."
        )

    return iso3_codes


def _validate_raster_attrs(raster_fpaths, masked, mask_and_scale):
    """
    Validate critical meta-data for a list of raster files.

    Implementation Note ("Smart Skip" Validation)
    --------------------------------------------------
    This function's logic depends on the `masked` and `mask_and_scale` flags.

    1.  It calls `_read_raster_attrs`, which *also* receives these flags.
    2.  `_read_raster_attrs` then calls `rioxarray.open_rasterio` with
        those flags.
    3.  If `mask_and_scale=True`, `rioxarray` consumes the scaling
        attributes (`scale_factor`, `add_offset`) and `_FillValue`
        from the lazy-loaded DataArray.
    4.  `_read_raster_attrs` therefore (correctly) reads these
        attributes as `None`.
    5.  This function's validation (e.g., comparing `None == None`)
        will then (correctly) pass, cleanly skipping the validation
        for attributes that `rioxarray` is about to handle anyway.
    6.  The one exception is `crs`, which `rioxarray` does not
        "consume" and which would cause a fatal error on merge.
        Therefore, `crs` is always validated, regardless of flags.

    Raises
    ------
    RasterReadError
        If reading an input raster fails.
    IncompatibleRasterError
        If any critical metadata attributes are mismatched.
    """
    try:
        metadata_list = []
        for p in raster_fpaths:
            meta = _read_raster_attrs(str(p), masked, mask_and_scale)
            metadata_list.append(meta)
    except RasterReadError as e:
        logger.error(f"A raster file is unreadable. Aborting. Error: {e}")
        raise e

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
            if not _same_value(meta[key], ref[key]):
                raise IncompatibleRasterError(
                    f"Input rasters do not share the same '{attr_name}'. "
                    f"{Path(ref['path']).name} has '{ref[key]}' but "
                    f"{Path(meta['path']).name} has '{meta[key]}'."
                )

    # All checks passed. Return the single, consistent set of safe attrs.
    # TODO Consider simplifying
    safe_attrs = {'crs': ref['crs']}
    if ref['nodata'] is not None:
        safe_attrs['_FillValue'] = ref['nodata']
    if ref['scale_factor'] is not None:
        safe_attrs['scale_factor'] = ref['scale_factor']
    if ref['add_offset'] is not None:
        safe_attrs['add_offset'] = ref['add_offset']

    return safe_attrs


def _same_value(a, b):
    """
    Return True if two raster attribute values are equal, treating NaN as
    equal to NaN (a common nodata value).
    """
    try:
        if np.isnan(a) and np.isnan(b):
            return True
    except TypeError:
        pass  # not a number (e.g., a CRS string or None)
    return a == b


@lru_cache(maxsize=4096)
def _read_raster_attrs(path, masked, mask_and_scale):
    """
    Read critical meta-data from a single raster file.

    This function is cached and opens the file lazily, i.e. does *not*
    read the full raster data into memory. It immediately closes the
    file handle after extracting the metadata.
    """

    try:
        # The meta-data read should be lazy even without 'chunks={}'
        # since we never ask for any actual raster data. We still
        # set 'chunks={}' as an added safety measure.
        with rioxarray.open_rasterio(
            path,
            masked=masked,
            mask_and_scale=mask_and_scale,
            chunks={}
        ) as da:

            # Store CRS as a string (WKT) to ensure it is hashable
            # for the cache and comparable
            crs_str = da.rio.crs.to_wkt() if da.rio.crs else None

            # Read all three critical attributes
            nodata_val = da.attrs.get('_FillValue')
            scale_factor = da.attrs.get('scale_factor')
            add_offset = da.attrs.get('add_offset')

            return {
                'path': path,
                'crs': crs_str,
                'nodata': nodata_val,
                'scale_factor': scale_factor,
                'add_offset': add_offset,
            }
    except Exception as e:
        logger.error(f"Failed to read metadata for {path}: {e}")
        # Re-raise as a known error type
        raise RasterReadError(
            f"Failed to read/parse metadata for {path}. Error: {e}"
        ) from e


def _standardise_rio_nodata_meta(da, masked=False, mask_and_scale=False):
    """
    Standardise the .rio 'nodata' metadata attribute for floating-point rasters.

    This function resolves the ambiguity where floating-point arrays (either
    implicitly float or explicitly masked) rely on `NaN` to represent missing
    data, but lack the specific metadata attribute telling downstream tools
    that `NaN` is indeed the nodata value.

    Behavior:
    1. If `masked` or `mask_and_scale` is True:
       The array is treated as Float/NaN. We strictly enforce `nodata=np.nan`.
    2. If Raw Data (`masked=False`):
       - If the array is Float and `nodata` is missing, we default it to `np.nan`.
       - If the array is Integer OR already has a valid `nodata` (e.g., -9999),
         we leave it *unchanged* to avoid corrupting raw data.
    """
    # Case A: Explicit Masking requested
    # The data IS Float/NaN, so we label it as such.
    if masked or mask_and_scale:
        # We check if the metadata is missing OR set to something other than NaN
        # (though usually it is just None)
        if da.rio.nodata is None or not np.isnan(da.rio.nodata):
            da.rio.write_nodata(np.nan, encoded=True, inplace=True)
        return da

    # Case B: Implicit Float
    fill_value = da.rio.nodata
    if fill_value is None and np.issubdtype(da.dtype, np.floating):
        da.rio.write_nodata(np.nan, encoded=True, inplace=True)

    return da


def _set_result_metadata(da, attrs, masked, mask_and_scale, safe_attrs):
    """
    Set the attributes and the nodata metadata of a new result, following
    `docs/metadata.md`. Besides `_append_history` (for later processing
    steps), this is the only place where attributes are written.

    Parameters
    ----------
    da : xarray.DataArray
        The result (it is modified and returned).
    attrs : dict
        The attributes (see `_provenance_attrs` and `_processing_attrs`).
    masked, mask_and_scale : bool
        The read settings.
    safe_attrs : dict
        The validated attributes shared by all input rasters (see
        `_validate_raster_attrs`), e.g. the `_FillValue` of unmasked data.
    """
    # The CRS is left to rioxarray (in the `spatial_ref` coordinate)
    da.attrs = {key: value for key, value in safe_attrs.items() if key != 'crs'}
    da.attrs.update(attrs)

    if 'year' in da.dims:
        # Band names, which `rio.to_raster` writes as band descriptions
        # (rioxarray's convention; the one non-scalar attribute)
        da.attrs['long_name'] = tuple(str(int(year)) for year in da.year.values)

    if masked or mask_and_scale:
        # Masked data is floating point with NaN as nodata. Drop any encoding
        # inherited from the source files (e.g., their data type and fill
        # value), so that eager and lazy results are saved alike.
        da.encoding = {key: value for key, value in da.encoding.items() if key == 'grid_mapping'}
        da.rio.write_nodata(np.nan, encoded=True, inplace=True)
    elif (fill_value := safe_attrs.get('_FillValue')) is not None:
        # Raw data keeps the source files' nodata value (validated to be the
        # same for all files). Earlier steps (e.g., stacking years) may have
        # left a different fill value in the encoding, which would take
        # precedence over the one in the attributes.
        da.encoding.pop('_FillValue', None)
        da.rio.write_nodata(fill_value, inplace=True)

    return da


def _provenance_attrs(product_name, rows, aoi_description):
    """
    Return the attributes that credit WorldPop and describe the data of a
    `wp_raster` result.

    Parameters
    ----------
    product_name : str
        The (curated) WorldPopPy product name.
    rows : pandas.DataFrame
        The manifest rows of the datasets that contributed to the result.
    aoi_description : str
        A short description of the AoI (see `_describe_aoi`).
    """
    title = f"{product_name} (curated WorldPopPy product name)"
    if notes := _unique(rows['product_notes']):
        title += f": {notes[0]}"

    institution = "; ".join(_unique(rows['api_source']))
    citations = _unique(rows['citation']) if 'citation' in rows else []
    summary_pages = _unique(
        f"{title_}: {url}"
        for title_, url in zip(rows['api_entry_title'], rows['summary_url'])
        if pd.notna(url)
    )
    attrs = {
        'title': title,
        'creator_name': CREATOR_NAME,
        'creator_url': CREATOR_URL,
        'creator_institution': institution,
        'institution': institution,
        'source': SOURCE_DESCRIPTION,
        'references': "\n".join(citations + summary_pages),
        'comment': RESULT_COMMENT,
    }
    if licences := (_unique(rows['license']) if 'license' in rows else []):
        attrs['license'] = "\n".join(licences)

    attrs['wpy_product_name'] = product_name
    attrs['wpy_product_type'] = "multi-year" if rows['multi_year'].any() else "static"
    attrs['wpy_manifest_built_at'] = _format_build_time(_get_active_manifest_info().built_at)
    attrs['wpy_countries'] = " ".join(sorted(_unique(rows['iso3'])))
    if years := sorted(int(year) for year in _unique(rows['year'])):
        attrs['wpy_years'] = " ".join(str(year) for year in years)
    attrs['wpy_aoi'] = aoi_description
    return attrs


def _processing_attrs(call, steps, *, source_files, masked, mask_and_scale, pre_clip_bbox):
    """
    Return the attributes that describe how a result was produced: one
    `history` line for the function call, the source files, and settings.
    """
    attrs = {
        'history': _history_line(call, steps),
        'wpy_version': wpy_version,
        'wpy_source_files': "\n".join(source_files),
        'wpy_read_masked': "true" if masked else "false",
        'wpy_read_mask_and_scale': "true" if mask_and_scale else "false",
    }
    if pre_clip_bbox is not None:
        attrs['wpy_pre_clip_bbox'] = _format_numbers(pre_clip_bbox)
    return attrs


def _describe_aoi(aoi, iso3_codes, orig_aoi_type):
    """
    Return a short description of the AoI for the `wpy_aoi` attribute, e.g.,
    'countries: KHM VNM' or 'bbox: 103.0 10.0 105.0 12.0'.
    """
    if orig_aoi_type == 'iso':
        return "countries: " + " ".join(iso3_codes)

    bounds = _format_numbers(aoi.to_crs(WGS84_CRS).total_bounds)
    if orig_aoi_type == 'bbox':
        return f"bbox: {bounds}"
    num = len(aoi)
    return f"geometry: {num} feature{'' if num == 1 else 's'}, bounds {bounds}"


def _history_line(call, steps=()):
    """
    Return one `history` line: the UTC time, the worldpoppy version, the
    function call and the processing steps applied.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{timestamp} worldpoppy {wpy_version}: {call}"
    if steps:
        line += ": " + "; ".join(steps)
    return line


def _append_history(da, line):
    """
    Append a line to the `history` attribute of a result (in place). Besides
    `_set_result_metadata`, this is the only place where attributes are written.
    """
    history = da.attrs.get('history')
    da.attrs['history'] = f"{history}\n{line}" if history else line


def _format_build_time(built_at):
    """Format the manifest's build time (as recorded in the manifest)."""
    if built_at is None:
        return "unknown"
    if built_at.tzinfo is not None:
        return built_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return built_at.isoformat(timespec="seconds")


def _format_numbers(values):
    """Format numbers as a space-separated string, e.g. '103.0 10.0 105.0 12.0'."""
    return " ".join(str(float(value)) for value in values)


def _unique(values):
    """Return the distinct, non-missing values in order of appearance."""
    return list(dict.fromkeys(value for value in values if pd.notna(value)))


def _concat_with_info(objs, **kwargs):
    """
    Thin wrapper for `xarray.concat` which logs an info message if the optional
    `bottleneck` library is not available.
    """
    if not module_available("bottleneck"):
        logger.info(
            "Installing the optional `bottleneck` module may accelerate "
            "`xarray` concatenation. (pip install bottleneck)"
        )

    da = xr.concat(objs, **kwargs)

    # We again restore .rio's 'nodata' metadata attribute since
    # `concat` will likely have dropped it
    da = _standardise_rio_nodata_meta(da)

    return da


def _lazy_merge_helper(das):
    """
    Lazily merge a list of DataArrays using a 'Painter's Algorithm' with
    Tree Reduction to minimise Dask graph depth.

    Strategy:
        Instead of merging linearly (A+B)+C+D... which creates a deep graph (Depth ~N),
        we merge pairwise ((A+B) + (C+D))... which creates a shallower tree (Depth ~log2 N).
        This is helps avoid recursion errors during graph construction for continental-scale
        merges (e.g., 50+ countries).

    Where rasters overlap, the first raster wins, as in eager merges with
    `rioxarray.merge.merge_arrays`.

    Note:
        This helper is ONLY used when lazy loading (via Dask) is active
        (i.e., `chunks` is not None), which requires masked data: missing
        values are NaN, since `combine_first` only fills NaN. It is NOT the
        default merge path for eager execution (which uses
        `rioxarray.merge.merge_arrays` instead).
    """
    # TODO: Document that lazy-merging country rasters can create very deep Dask graphs.
    #  In many workflows, it likely is better to downsample individual country
    #  rasters first and merge them afterwards.

    if not das:
        raise ValueError("Cannot merge empty list of rasters.")

    # --- No-op Check ---
    if len(das) == 1:
        return das[0]

    # --- WARNING ---
    warnings.warn(
        "Merging multiple country rasters lazily via Dask is an "
        "experimental feature. You should only use lazy merging "
        "if eager merging (via rioxarray) is impossible due to "
        "memory constraints. Also, please inspect your results carefully.",
        UserWarning,
        stacklevel=2
    )

    # --- Establish Z-order ---
    # The first element in 'current_layer' stays on top, since `combine_first`
    # prioritises the caller (self) and the pairwise merging keeps the order.
    current_layer = list(das)

    # --- Pairwise Merging ---
    # Iterative Tree Reduction
    # We loop until only one merged array remains.
    while len(current_layer) > 1:
        next_layer = []

        # Step through the list in pairs
        for i in range(0, len(current_layer), 2):
            left = current_layer[i]

            # If there is a right partner, merge them
            if i + 1 < len(current_layer):
                right = current_layer[i + 1]
                # 'left' is strictly above 'right' in Z-order list
                merged = left.combine_first(right)
                next_layer.append(merged)
            else:
                # Orphan element at the end, carry it over to the next round
                next_layer.append(left)

        current_layer = next_layer

    combined = current_layer[0]

    # --- Fix Coordinate Flip ---
    # Check if y coordinate is ascending (increasing)
    if combined.y[-1] > combined.y[0]:
        # Flip it to match standard Rasterio/GDAL convention (Top-Left origin)
        combined = combined.sortby("y", ascending=False)

    # --- Clean the Metadata ---
    # Masked data is floating point with NaN as nodata, which `combine_first`
    # does not keep in the metadata
    combined.rio.write_nodata(np.nan, encoded=True, inplace=True)

    # Recover CRS & name
    combined.rio.write_crs(das[0].rio.crs, inplace=True)
    if das[0].name:
        combined.name = das[0].name

    return combined
