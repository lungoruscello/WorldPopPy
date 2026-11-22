# Working with results

This guide describes what `wp_raster` returns, and how to save, reproject, and combine results. It also explains
how to keep memory use low for large areas.

## The result

`wp_raster` returns an `xarray.DataArray` with geospatial metadata (handled by
[`rioxarray`](https://vylor.github.io/rioxarray/)). Results can therefore be analysed with xarray:

- For static products, and for a single year of a multi-year product, the dimensions are `(y, x)`.
- If several years are requested, the years are stacked along a `year` dimension: `(year, y, x)`.
- By default (`masked=True`), cells without data are NaN.

Every result carries WorldPop's recommended citations, the licence, and a processing history in its attributes.
Importantly, these attributes describe the result as returned by **WorldPopPy**. After you select data or compute new
values with xarray, some attributes no longer match the data (e.g. after selecting one year of a multi-year result,
`wpy_years` still lists all years). If you save such results, e.g. to pass the files on, update or drop these
attributes first. The [metadata guide](metadata.md) describes all attributes, and the
[metadata example](https://github.com/lungoruscello/WorldPopPy/blob/master/examples/workflows/03_working_with_metadata.py)
shows how to handle them.

## Saving

Save a result as a GeoTIFF with rioxarray:

```python
pop_data.rio.to_raster("mekong_pop_2024.tif")
```

A multi-year result is saved with one band per year. The [metadata guide](metadata.md) shows how to label the bands
with their years.

## Reprojecting

Results are always in the coordinate reference system of WorldPop's source rasters (WGS84, EPSG:4326), whatever the
CRS of the area of interest. To reproject a result, use `wp_warp`. For counts such as population, use
`resampling='sum'` to approximately preserve totals:

```python
from worldpoppy import wp_warp

pop_warped = wp_warp(pop_data, to_crs="EPSG:32647", res=1_000, resampling="sum")
```

## Combining results

To combine results for different products or areas of interest (e.g. population and elevation), align one result to
the other first:

```python
elevation = elevation.rio.reproject_match(pop_data)
```

The cell coordinates of such results can differ by tiny floating-point amounts, and xarray, which matches cells by
exactly equal coordinates, would silently drop the cells that differ. Note that `reproject_match` loads lazy (Dask)
results into memory, and that it resamples with nearest neighbour by default: where the resolutions differ, pass
`resampling=` (e.g. `Resampling.average` from `rasterio.enums`, or `Resampling.sum` to aggregate counts such as
population to coarser cells).

## Large areas and memory

Country rasters can be very large: Russia's built-up volume raster, for instance, takes about 84 GB in memory. Without
pre-clipping, an eager read (the default) holds the full merged grid in memory, and can briefly need several times
the size of the result.
**WorldPopPy** offers three ways to keep memory use low:

1. **Automatic pre-clipping.** For bounding-box and geometry AoIs, only the part of each country raster near the AoI
   is read. See the [Moscow example](https://github.com/lungoruscello/WorldPopPy/blob/master/examples/large_rasters/01_moscow_built_volume.py).
2. **Manual pre-clipping.** For AoIs given as country codes or names, `pre_clip_bbox` clips each country raster to a
   box right after reading it, e.g. to leave out remote islands. See the
   [Chile example](https://github.com/lungoruscello/WorldPopPy/blob/master/examples/large_rasters/02_chile_climate_dask.py).
3. **Lazy reading with Dask.** With `chunks`, `wp_raster` returns a lazy result that is read chunk by chunk when it
   is computed. Several country rasters are then merged as one virtual mosaic, a small VRT file in the cache
   directory. Lazy reading requires `masked=True` and `cache_downloads=True`. See the
   [Andes example](https://github.com/lungoruscello/WorldPopPy/blob/master/examples/large_rasters/03_andes_topo_dask.py).

To check the size of a request before downloading, use `download_dry_run=True` (see
[Cache and downloads](cache_and_downloads.md)). The Notes of `merge_rasters` in the API reference describe further
settings that affect the memory use of lazy merges.
