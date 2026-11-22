# Notes on `manifest_builder.py`

This document elaborates on the strategy and terminology used by the `manifest_builder` 
module, which generates a raw data manifest for the `worldpoppy` library.

## Discovering "Leaf Nodes"

Raster data from **WorldPop** is organised hierarchically. Available data can be discovered using a 
[public meta-data API](https://www.worldpop.org/sdi/introapi/). 

**Branch Nodes** of the API represent broader categories (e.g. [/covariates](https://hub.worldpop.org/rest/data/covariates/)) 
that lead to more specific categories, or API **Leaf Nodes** (e.g. [/covariates/G2_NT_lights](https://hub.worldpop.org/rest/data/covariates/G2_NT_lights)). 
A Leaf Node is the endpoint for a single conceptual data series (like "Nighttime Lights"). Our crawler traverses this
meta-data API until all Leaf Nodes have been identified.

## URL templating

Each API Leaf Node returns a **Coverage Index**, listing the countries or country-years for which the 
corresponding data series is available. However, that index does not contain download URLs for actual 
raster files.

To get file listings, we need to make a separate **Details Call** to the API, which is always made 
for one specific country (e.g. [/covariates/G2_NT_lights?iso3=AFG](https://hub.worldpop.org/rest/data/covariates/G2_NT_lights?iso3=AFG)). 
Fetching file listings for each country (and Leaf Node) would require thousands of API calls. To avoid 
this, our crawler implements a "sampling" strategy: We only request a file listing for the first country
in a given Coverage Index. Based on the **Sample Payload** returned by the API, we infer the general 
pattern of raster filenames and download URLs. That pattern can then be extended to all other countries 
(or country-years) listed in the Coverage Index.

The same strategy yields a link to each dataset's summary page on WorldPop. The Sample Payload contains 
the summary URL of the sample dataset (e.g. `https://hub.worldpop.org/geodata/summary?id=1325`). Replacing 
the sample's ID with a placeholder gives a URL template, which is then filled in with the ID of each entry 
in the Coverage Index. For data series that group several years or bands under one entry per country (see 
below), all datasets of a country therefore share one summary page.

**Download and summary URLs are thus inferred from a sample, not taken from a file listing for each dataset.** 
The test suite spot-checks a random sample of both (in an opt-in, heavy test), but does not verify each individual 
URL. Download URLs of the Global 1 series are additionally checked against a file list (see "Checking inferred URLs" 
below).

## Where WorldPop lists its files

WorldPop describes its raster data in three places, which differ in coverage:

- The **meta-data API** lists all data series, and, for each series, the countries or country-years 
  covered (what we call the Coverage Index above). File names are only returned by what we call a Details 
  Call, for one specific country.
- A **file list** for the Global 1 series (2000-2020), `assets/wpgpDatasets.csv` on WorldPop's file 
  servers, gives the path of each raster file in that particular series (97,261 files as of November 2026). 
  We refer to that list as the **Global 1 File List**.
- A **STAC catalogue** (`api.stac.worldpop.org`) lists individual files for some recent Global 2 products.

We have not yet found a complete file list for the Global 2 series (2015-2030).

On WorldPop's file servers, the folder layout differs between data series. Within a series, files of different 
countries can also be stored in different folders: for example, the constrained population estimates for 
2020 (Global 1) are stored in one of two folders, depending on the country.

## Checking inferred URLs

Since download URLs are inferred from a single Sample Payload per data series, they are checked against 
the Global 1 File List where possible. For each dataset of the Global 1 series:

- If the inferred URL is in the Global 1 File List, it is **confirmed**.
- If the file name appears in the Global 1 File List under a different path, the URL is **corrected** to 
  that path. (File names in the Global 1 File List are unique.)
- If the file name does not appear in the Global 1 File List, the URL is kept as inferred and marked as 
  **unconfirmed**. The Global 1 File List does not cover all Global 1 datasets that the meta-data API 
  lists, so such datasets are not dropped.

Download URLs of the Global 2 series are not checked (marked as **not checked**). The result of the 
check is recorded for each dataset in the raw manifest, and counted per data series in the build report.

## File organisation schemes

WorldPop organises the files of a data series in one of three ways. Our crawler infers the scheme from 
the Sample Payload:

- **Flat:** Each country-year is listed as a distinct entry in the Coverage Index, and each entry is 
  associated with one raster file (e.g. the /pop/wpgp series).
- **Multi-year:** All available years for one country are grouped together under a single entry, with 
  one raster file per year (e.g. the /covariates/G2_NT_lights series).
- **Multi-band:** A single entry per country holds several raster files for the same year, one per 
  thematic class or "band" (e.g. the distances to different land-cover classes in the 
  /covariates/G2_DST_ESA_2015 series).

Grouped data is always flattened, so that our data manifest records each dataset (one country-year, 
or one country-year-band) in its own row.

## Unsupported data series

The crawler skips data series in the following cases:

- The data is not stored as GeoTIFF files (e.g. as 7z archives). This is detected from the Sample Payload.
- The data series covers the whole globe or a continent rather than individual countries, or uses an 
  unrecognised country code. This is detected from the country code of the first entry in the Coverage 
  Index (e.g. 'WCD' for global data).
- The file organisation cannot be parsed reliably: multi-year files without a year in their names, or 
  with duplicate or non-consecutive years, and multi-band files without identifiable bands.
- The data series belongs to the `age_structures` branch of the API, which is skipped entirely because 
  its filenames cannot yet be parsed into bands.
