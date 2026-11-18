# Metadata of WorldPopPy results

Results of **WorldPopPy** (from version 0.5.0) carry metadata in their attributes: who created the data
and how to cite it, which WorldPop files were used, and what WorldPopPy did with them. This guide
describes these attributes, how they behave when you process results further, and how to keep them
when saving. A worked example is
[`examples/workflows/03_working_with_metadata.py`](https://github.com/lungoruscello/WorldPopPy/blob/master/examples/workflows/03_working_with_metadata.py).

## 1. Principles

Metadata handling in WorldPopPy is designed to follow these principles: 

1. **Credit the creator.** Every result of `wp_raster` names WorldPop as the creator of the data,
   lists the official dataset titles, and links the summary pages with the recommended citations.
   WorldPopPy provides no data; only the curated product names and notes are its own, and they
   are marked as such.
2. **Standard names first.** Attributes follow the CF conventions and the Attribute Convention for
   Data Discovery (ACDD) where a standard name exists, with the meaning these conventions define
   (e.g., CF's `source` describes how a file was produced, not who provides the data; the creator
   of the data is named by ACDD's `creator_*` attributes). WorldPopPy-specific attributes start
   with `wpy_`.
3. **Saveable values.** Attribute values are strings or numbers only (no `None`, no lists, no
   booleans), so that they survive saving to GeoTIFF and netCDF unchanged. Several values are
   separated by newlines (file names, URLs) or single spaces (country codes, years).
4. **A history of the processing.** Each WorldPopPy function that processes a result appends one
   line to `history`; earlier lines are kept as they are.
5. **Inputs stay unchanged.** WorldPopPy functions do not change the attributes, encoding or data of
   an array passed to them.
6. **Eager and lazy alike.** Results with and without `chunks=` carry the same metadata (apart from
   timestamps).

## 2. Attributes of `wp_raster` results

Example: `wp_raster('pop_g2_1km_r25a', aoi=bbox, years=[2015, 2024])`.

| Attribute | Content | Example | Present |
|---|---|---|---|
| `title` | WorldPopPy's curated product name and notes, marked as such (the official dataset titles are in `references`) | `pop_g2_1km_r25a (curated WorldPopPy product name): Estimated total number of people per grid-cell. 2025 alpha release (R2025A).` | always |
| `creator_name` (ACDD) | the creator of the data | `WorldPop` | always |
| `creator_url` (ACDD) | the creator's website | `https://www.worldpop.org` | always |
| `creator_institution` (ACDD) | the institution that created the data, as given by WorldPop | `WorldPop, University of Southampton, UK` | always |
| `institution` (CF: where the original data was produced) | the same as `creator_institution` | `WorldPop, University of Southampton, UK` | always |
| `source` (CF: how this file was produced) | fixed text; the individual processing steps are in `history` | `WorldPop gridded datasets (see 'references'), processed with WorldPopPy (see 'history')` | always |
| `references` | WorldPop's recommended citations of the source datasets (usually one per data series, incl. the DOI), followed by the official dataset titles with their summary URLs. One entry per line, without duplicates. See "Citing the data" below. | `WorldPop (www.worldpop.org - School of Geography and Environmental Science, University of Southampton; ...) (2018). Global High Resolution Population Denominators Project ... https://dx.doi.org/10.5258/SOTON/WP00645` and `The spatial distribution of population in 2000, Afghanistan: https://hub.worldpop.org/geodata/summary?id=1352` | always (citations only where WorldPop's metadata has them) |
| `license` | the datasets' licence, as given by WorldPop (a link to the licence text) | `https://hub.worldpop.org/data/licence.txt` | always (where WorldPop's metadata has it) |
| `comment` | fixed text: independent client, cite the original datasets, what is inferred | `Retrieved and processed with WorldPopPy (https://github.com/lungoruscello/WorldPopPy), an independent client not affiliated with WorldPop. Please cite the original datasets (see 'references'). The summary URLs in 'references' are inferred by WorldPopPy and may occasionally not resolve; the official dataset titles help to find the pages. The licence and the recommended citations are those of WorldPop's metadata for one sample dataset per data series.` | always |
| `history` | the processing steps, one line each (see "Processing history" below) | | always |
| `wpy_version` | WorldPopPy version | `0.5.0` | always |
| `wpy_product_name` | WorldPopPy's curated product name | `pop_g2_1km_r25a` | always |
| `wpy_manifest_built_at` | build time of the data manifest used (WorldPopPy's catalogue of WorldPop's files), in UTC | `2026-11-17T19:51:06Z` | always (`unknown` if the manifest does not record it) |
| `wpy_source_files` | WorldPop file names that contributed data, one per line | `khm_pop_2015_CN_1km_R2025A_UA_v1.tif` | always |
| `wpy_countries` | ISO3 codes of the source files, space-separated | `KHM VNM` | always |
| `wpy_product_type` | whether the product is static (one dataset per country) or multi-year, in WorldPopPy's terms | `static` or `multi-year` | always |
| `wpy_years` | the years of the data, space-separated. Static results have no `year` dimension, so for them this is the only record of the year. | `2015 2024` (multi-year); `2024` (static product such as `pop_g2_alt`) | whenever the datasets have a year; absent for static products without one |
| `wpy_aoi` | the AoI, in short | `countries: KHM VNM`, `bbox: 103.0 10.0 105.0 12.0`, `geometry: 3 features, bounds 103.0 10.0 105.0 12.0` | always |
| `wpy_pre_clip_bbox` | the pre-clip box passed by the user (`pre_clip_bbox`). The automatic pre-clipping for bbox and geometry AoIs only speeds up processing and is not recorded. | `97.5 18.0 99.0 20.0` | only if the user passed a pre-clip box |
| `wpy_read_masked`, `wpy_read_mask_and_scale` | the effective settings used to read the source files (the `wp_raster` parameters `masked` and `mask_and_scale`, which are always equal) | `true` / `false` | always (both) |

`wpy_source_files`, `wpy_countries` and `references` describe only datasets that contributed data
to the result: files that do not overlap the pre-clip box (the user's `pre_clip_bbox`, or the AoI's
slightly enlarged bounding box) are left out, and a log message reports them. (A file that overlaps
this box but not the AoI itself is still listed; detecting that exactly would need a pixel-level
check.)

## 3. Citing the data

WorldPopPy provides no data of its own. `references` lists WorldPop's recommended citations (with
DOIs), followed by the official titles and summary pages of the datasets used. Cite these in
publications, and respect the licence given in `license`; WorldPop's licence text names the
Creative Commons Attribution 4.0 International License.

Some of this information is inferred rather than read for each dataset. To keep the load on
WorldPop's servers low, WorldPopPy infers the summary URLs from URL patterns, and takes the licence
and the recommended citations from WorldPop's metadata for one sample dataset per data series (as
`comment` notes). If a summary link does not work, the official dataset title helps to find the
page on WorldPop's website. Read the summary page before using the data: WorldPop's datasets are
often the result of complex modelling.

## 4. Data values, nodata and coordinates

| Aspect | Behaviour |
|---|---|
| Masked results (`masked=True`, the default) | floating-point data; empty cells are NaN; `rio.nodata` is NaN (also after saving) |
| Unmasked results | the source files' data type; `rio.nodata` is the source files' nodata value. If the source files have none: NaN for floating-point data, none for integer data. |
| CRS | in the `spatial_ref` coordinate (EPSG:4326 for all WorldPop data), not in the attributes |
| Name | the product name, or the `name` passed by the user |
| Results with several years | a `year` dimension with the years as coordinate values. No `long_name` is set: band labels held in an attribute would no longer match the bands once a year is selected, and `rio.to_raster` would then refuse to save the result. Saved GeoTIFF bands are therefore labelled with the result's name; "Saving" below shows how to label them with their years. A single year of a multi-year product has no `year` dimension; its year is in `wpy_years`. |
| Scale and offset | `_FillValue`, `scale_factor`, `add_offset` only for raw reads (`masked=False`) |

## 5. Processing history

`history` holds one line per WorldPopPy function call, oldest first, in UTC:

```
2026-11-14T15:50:08Z worldpoppy 0.5.0: wp_raster('pop_g2_1km_r25a', years=[2015, 2024]): merged 4 files; clipped to AoI
2026-11-14T15:52:31Z worldpoppy 0.5.0: wp_warp(to_crs='EPSG:3857', res=1000, resampling='sum')
```

- `wp_raster` adds **one** line for the whole call, also for multi-year products. Internal merging
  steps do not add lines of their own.
- The line names the function, the key arguments, and every step applied: merging, pre-clipping
  (only with a user-provided `pre_clip_bbox`), and clipping to the AoI. Details such as the full AoI
  are in the `wpy_` attributes, not in `history`.

## 6. `merge_rasters` and `wp_warp`

| Function | Metadata |
|---|---|
| `merge_rasters` (public, low-level; works on local files without WorldPopPy's catalogue) | as described under "Data values, nodata and coordinates"; one `history` line; `wpy_version`, `wpy_read_masked`, `wpy_read_mask_and_scale`, `wpy_pre_clip_bbox` (if passed) and `wpy_source_files` (local file names). No WorldPop provenance (`title`, `references`, ...), since the files may come from anywhere. Default name `'wpy_data'`; pass `name=` for a meaningful one. |
| `wp_warp` | keeps all attributes of its input and appends one `history` line; data values, nodata and coordinates as described above. Each year of a multi-year result gives the same result as when warped alone. |

## 7. Your own processing

The attributes of a WorldPopPy result describe the result as returned by WorldPopPy. Your own
selections and arithmetic with xarray (e.g., `da.sel(year=2024)`, `da + 1`) do not update them.
Whether xarray keeps the attributes at all depends on the operation and the xarray version (with
xarray 2026.9, both examples keep them). After `da.sel(year=2024)`, for example, `wpy_years` and
`wpy_source_files` still list all years, and `history` has no line for the selection. Arithmetic on
masked results also loses the nodata value (`rio.nodata` becomes None); on raw reads
(`masked=False`), the nodata value stays, but no longer marks the empty cells.

Stale raster attributes matter once you save a result, e.g. to pass the file on. In that case, the
saved metadata should describe what the file contains. **If you save a result after your own
selections or arithmetic, update or drop its attributes first**, and restore the nodata value (see
"Saving" below). For results you only use in memory, this is not needed. The provenance attributes
(`references`, `license`, `comment`, ...) remain valid.

To ensure consistency, update those attributes that describe the raster data (e.g. `title`,
`wpy_years`, `wpy_source_files`), and record your own steps in `history`. For one year on its own,
request it from `wp_raster` instead: all its attributes then describe that year.

## 8. Saving

- **GeoTIFF** (`da.rio.to_raster`): all attributes are written as GDAL metadata tags. The bands are
  labelled with the result's name (the product name). To label the bands of a multi-year result with
  their years, set `long_name` on the array you save, just before saving:

  ```python
  years = tuple(str(year) for year in da.year.values)
  da.assign_attrs(long_name=years).rio.to_raster("result.tif")
  ```

  Results of arithmetic have no nodata value (see "Your own processing"); restore it before saving,
  so that the GeoTIFF marks empty cells as such: `result = result.rio.write_nodata(np.nan)` (for
  masked results).
- **netCDF** (`da.to_netcdf`): xarray writes the attributes as *variable* attributes, while CF
  expects `title`, `history` and similar as *global* (file-level) attributes, where tools such as
  Panoply look for them. WorldPopPy provides no saving helper; this recipe copies the attributes to
  the file level as well:

  ```python
  ds = da.to_dataset()       # one variable, named after the product
  ds.attrs.update(da.attrs)  # copy the attributes to file level as well
  ds.to_netcdf("result.nc", engine="netcdf4")
  ```

  The recipe uses the `netCDF4` library (netCDF-4 format); xarray's netCDF-3 fallback (`scipy`,
  `engine="scipy"`) works as well. Multi-year results keep their years as the `year` coordinate.
  WorldPopPy itself depends on no netCDF library.

## 9. Changes compared with 0.4.x

- `history`: new format, and now complete for multi-year results.
- `input_files` (local cache names) is replaced by `wpy_source_files` (WorldPop file names, only
  contributing files).
- `wpy_masked` / `wpy_mask_and_scale` are renamed to `wpy_read_masked` / `wpy_read_mask_and_scale`,
  with the values `true` / `false` (previously `'True'` / `'False'`); both are always present.
- New: `title`, `license`, `creator_name`, `creator_url`, `creator_institution`, `institution`, `source`,
  `references`, `comment`, and further `wpy_` attributes.
- Static results now carry a nodata value.
