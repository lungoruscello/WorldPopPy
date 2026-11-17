# Metadata of WorldPopPy results

> **DRAFT (2026-11-14).** This document defines the metadata that results of **WorldPopPy**
> carry. It is the target for the 0.5.0 redesign (TODO 2.4, incl. bugs 1.14-1.18). All open points
> were decided on 2026-11-14. Each rule below becomes a test.

## 1. Principles

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
   One deliberate exception: `long_name` on results with a `year` dimension is a tuple of band
   names (one year per band), following rioxarray's convention for band descriptions (see
   section 3).
4. **History records every processing step.** Each public function that processes data appends
   exactly one line to `history`; nothing removes or rewrites earlier lines.
5. **Attributes are only written by the metadata helpers:** `_set_result_metadata` for new
   results, `_append_history` for later steps. No other code assigns attributes.
6. **Inputs are never modified.** No function changes the attributes, encoding or data of an
   array passed in by the user.
7. **Eager equals lazy.** Results with and without `chunks=` carry the same metadata (apart from
   timestamps).

## 2. Attributes of `wp_raster` results

Example: `wp_raster('pop_g2_1km_r25a', aoi=bbox, years=[2015, 2024])`.

| Attribute | Content | Example | Present |
|---|---|---|---|
| `title` | WorldPopPy's curated product name and notes, marked as such (the official dataset titles are in `references`) | `pop_g2_1km_r25a (curated WorldPopPy product name): Estimated total number of people per grid-cell. 2025 alpha release (R2025A).` | always |
| `creator_name` (ACDD) | the creator of the data | `WorldPop` | always |
| `creator_url` (ACDD) | the creator's website | `https://www.worldpop.org` | always |
| `creator_institution` (ACDD) | from the manifest (`api_source`) | `WorldPop, University of Southampton, UK` | always |
| `institution` (CF: where the original data was produced) | the same as `creator_institution` | `WorldPop, University of Southampton, UK` | always |
| `source` (CF: how this file was produced) | fixed text; the individual processing steps are in `history` | `WorldPop gridded datasets (see 'references'), processed with WorldPopPy (see 'history')` | always |
| `references` | WorldPop's recommended citations of the source datasets (from the manifest, deduplicated; usually one per data series, incl. the DOI link), followed by the official dataset titles with their summary URLs (deduplicated). One entry per line. | `WorldPop (www.worldpop.org - School of Geography and Environmental Science, University of Southampton; ...) (2018). Global High Resolution Population Denominators Project ... https://dx.doi.org/10.5258/SOTON/WP00645` and `The spatial distribution of population in 2000, Afghanistan: https://hub.worldpop.org/geodata/summary?id=1352` | always (citations only where the manifest has them) |
| `license` | the datasets' licence, as given by WorldPop (a link to the licence text), deduplicated | `https://hub.worldpop.org/data/licence.txt` | always (where the manifest has it) |
| `comment` | fixed text: independent client, cite the original datasets, URLs are inferred | `Retrieved and processed with WorldPopPy (https://github.com/lungoruscello/WorldPopPy), an independent client not affiliated with WorldPop. Please cite the original datasets (see 'references'). The summary URLs in 'references' are inferred by WorldPopPy and may occasionally not resolve; the official dataset titles help to find the pages. The licence and the recommended citations are those of WorldPop's metadata for one sample dataset per data series.` | always |
| `history` | see section 4 | | always |
| `wpy_version` | WorldPopPy version | `0.5.0` | always |
| `wpy_product_name` | WorldPopPy's curated product name | `pop_g2_1km_r25a` | always |
| `wpy_manifest_built_at` | build time of the data manifest used, as recorded in the manifest (in UTC, marked `Z`, if the manifest records a time zone) | `2025-12-08T19:24:34` | always (`unknown` if the manifest does not record it) |
| `wpy_source_files` | WorldPop file names that contributed data, one per line | `khm_pop_2015_CN_1km_R2025A_UA_v1.tif` | always |
| `wpy_countries` | ISO3 codes of the source files, space-separated | `KHM VNM` | always |
| `wpy_product_type` | whether the product is static (one dataset per country) or multi-year, in the package's terms | `static` or `multi-year` | always |
| `wpy_years` | the years of the data, space-separated. Static results have no `year` dimension, so for them this is the only record of the year. | `2015 2024` (multi-year); `2024` (static product such as `pop_g2_alt`) | whenever the datasets have a year; absent for static products without one |
| `wpy_aoi` | the AoI as passed (in short) | `countries: KHM VNM`, `bbox: 103.0 10.0 105.0 12.0`, `geometry: 3 features, bounds 103.0 10.0 105.0 12.0` | always |
| `wpy_pre_clip_bbox` | the pre-clip box passed by the user (`pre_clip_bbox`). The automatic pre-clipping for bbox and geometry AoIs only speeds up processing and is not recorded. | `97.5 18.0 99.0 20.0` | only if the user passed a pre-clip box |
| `wpy_read_masked`, `wpy_read_mask_and_scale` | the effective settings used to read the source files (the `wp_raster` parameters `masked` and `mask_and_scale`, which are always equal) | `true` / `false` | always (both) |

`wpy_source_files`, `wpy_countries` and `references` describe only datasets that contributed data
to the result: files that do not overlap the pre-clip box (the user's `pre_clip_bbox`, or the AoI's
slightly enlarged bounding box) are left out, and a log message reports them. (A file that overlaps
this box but not the AoI itself is still listed; detecting that exactly would need a pixel-level
check.)

**Manifest requirement.** `license` and the citations in `references` come from WorldPop's metadata
API: each Details Call entry has `license`, `citation` and `doi` fields (checked on 2026-11-14 for
`pop/wpgp?iso3=AFG`). The manifest builder already makes one such call per data series (the
"sample") and stores further series-level fields from it; it will also store `license`, `citation`
and `doi`. This needs a manifest rebuild for 0.5.0. Guard: if a sampled citation mentions the
sample country, it may be country-specific, so it is not stored for the series (and a warning is
logged).

## 3. Data, nodata and coordinates

| Aspect | Rule |
|---|---|
| Masked results (`masked=True`, the default) | floating-point data; missing values are NaN; `rio.nodata` is NaN (also after saving) |
| Unmasked results | the source files' data type; `rio.nodata` is the source files' nodata value. If the source files have none: NaN for floating-point data, none for integer data. |
| CRS | in the `spatial_ref` coordinate (EPSG:4326 for all WorldPop data), not in the attributes |
| Name | the product name, or the `name` passed by the user |
| Results with several years | a `year` dimension with the years as coordinate values, and `long_name` set to the years as a tuple of strings, e.g. `('2015', '2024')`. This is rioxarray's convention for band descriptions: `rio.to_raster` labels each GeoTIFF band with its year, and `rioxarray.open_rasterio` restores the tuple when reading the file back. (Without it, every band would be named after the product, and the years would be lost in saved GeoTIFFs.) A single year of a multi-year product has no `year` dimension and no `long_name`; its year is in `wpy_years`. |
| Scale and offset | `_FillValue`, `scale_factor`, `add_offset` only for raw reads (`masked=False`) |

## 4. `history`

Format: one line per public function call, oldest first, in UTC:

```
2026-11-14T15:50:08Z worldpoppy 0.5.0: wp_raster('pop_g2_1km_r25a', years=[2015, 2024]): merged 4 files; clipped to AoI
2026-11-14T15:52:31Z worldpoppy 0.5.0: wp_warp(to_crs='EPSG:3857', res=1000, resampling='sum')
```

- `wp_raster` adds **one** line for the whole call, also for multi-year products. Internal merging
  steps do not add lines of their own.
- The line names the function, the key arguments, and every step applied: merging, pre-clipping
  (only with a user-provided `pre_clip_bbox`), and clipping to the AoI. Details such as the full AoI
  are in the `wpy_` attributes, not in `history`.

## 5. Other functions

| Function | Metadata |
|---|---|
| `merge_rasters` (public, low-level; works on local files without manifest information) | the rules of section 3; one `history` line; `wpy_version`, `wpy_read_masked`, `wpy_read_mask_and_scale`, `wpy_pre_clip_bbox` (if applied) and `wpy_source_files` (local file names). No WorldPop provenance (`title`, `references`, ...), since the files may come from anywhere. Default name `'wpy_data'` (unchanged); pass `name=` for a meaningful one. |
| `wp_warp` | keeps all attributes of its input; appends one `history` line; follows the rules of section 3; never modifies its input |
| xarray operations (e.g. `da + 1`) | outside WorldPopPy's control: xarray drops attributes in most arithmetic unless `xr.set_options(keep_attrs=True)` is set. To be documented. |

## 6. Saving

- **GeoTIFF** (`da.rio.to_raster`): all attributes are written as GDAL metadata tags and read back
  unchanged (as strings). For multi-year results, `long_name` becomes the band descriptions, so the
  bands are labelled with their years.
- **netCDF** (`da.to_netcdf`): xarray writes the attributes as *variable* attributes, while CF
  expects `title`, `history` and similar as *global* (file-level) attributes, where tools such as
  Panoply look for them. WorldPopPy provides no saving helper; the documentation gives this recipe:

  ```python
  ds = da.to_dataset()       # one variable, named after the product
  ds.attrs.update(da.attrs)  # copy the attributes to file level as well
  ds.to_netcdf("result.nc", engine="netcdf4")
  ```

  The `netCDF4` library (netCDF-4 format) is recommended: xarray's netCDF-3 fallback (`scipy`)
  cannot store the tuple-valued `long_name` of multi-year results and fails with an unhelpful
  `KeyError` (checked 2026-11-14). WorldPopPy itself depends on no netCDF library.

## 7. Testable invariants

Each becomes at least one test, for every relevant path (static and multi-year; country codes,
bbox and geometry AoIs; eager and lazy; after `wp_warp`; after saving and reloading):

1. All attributes of section 2 marked "always" are present, with the documented format.
2. All attribute values are strings or numbers.
3. `references` contains the summary URL of every contributing dataset.
4. `history` has exactly one line per public call, in order; earlier lines are unchanged.
5. Masked results have `rio.nodata` NaN, also after saving and reloading (bug 1.14).
6. Eager and lazy results carry the same attributes (apart from timestamps).
7. Inputs to `wp_warp` (and `merge_rasters`) are unchanged after the call (bug 1.15).
8. `wpy_read_masked` and `wpy_read_mask_and_scale` reflect the effective settings (bug 1.17).
9. `pre_clip_bbox` skips files without overlap (bug 1.16); `wpy_source_files` lists only
   contributing files.
10. Files whose nodata is NaN are accepted as compatible (bug 1.18).
11. Saved GeoTIFFs keep all attributes; multi-year bands carry their years (and reading the file
    back with rioxarray restores `long_name`).

## 8. Changes compared with 0.4.x

- `history`: new format, and now complete for multi-year results.
- `input_files` (local cache names) is replaced by `wpy_source_files` (WorldPop file names, only
  contributing files).
- `wpy_masked` / `wpy_mask_and_scale` are renamed to `wpy_read_masked` / `wpy_read_mask_and_scale`,
  with the values `true` / `false` (previously `'True'` / `'False'`); both are always present.
- New: `title`, `license`, `creator_name`, `creator_url`, `creator_institution`, `institution`, `source`,
  `references`, `comment`, and further `wpy_` attributes.
- Static results now carry a nodata value (bug 1.14).
- Multi-year results carry `long_name` (the years), so saved GeoTIFF bands are labelled with their
  years instead of the product name.
