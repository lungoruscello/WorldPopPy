# Notes on `manifest_builder.py`

This document elaborates on the strategy and terminology used by the `manifest_builder` 
module, which generates a raw data manifest for the `worldpoppy` library.

## Discovering "Leaf Nodes"

Raster data from **WorldPop** is organised hierarchically. Available data can be discovered using a 
[public meta-data API](https://www.worldpop.org/sdi/introapi/). 

**Branch Nodes** of the API represent broader categories (e.g., [/covariates](https://hub.worldpop.org/rest/data/covariates/)) 
that lead to more specific categories, or API **Leaf Nodes** (e.g., [/covariates/G2_NT_lights](https://hub.worldpop.org/rest/data/covariates/G2_NT_lights)). 
A Leaf Node is the endpoint for a single conceptual data series (like "Nighttime Lights"). Our crawler traverses this
meta-data API until all Leaf Nodes have been identified.

## URL templating

Each API Leaf Node returns a **Coverage Index**, listing the countries or country-years for which the 
corresponding data series is available. However, that index does not contain download URLs for actual 
raster files.

To get file listings, we need to make a separate **Details Call** to the API, which is always made 
for one specific country (e.g., [/covariates/G2_NT_lights?iso3=AFG](https://hub.worldpop.org/rest/data/covariates/G2_NT_lights?iso3=AFG)). 
Fetching file listings for each country (and Leaf Node) would require thousands of API calls. To avoid 
this, our crawler implements a "sampling" strategy: We only request a file listing for the first country
in a given Coverage Index. Based on the **Sample Payload** returned by the API, we infer the general 
pattern of raster filenames and download URLs. That pattern can then be extended to all other countries 
(or country-years) listed in the Coverage Index.

## Further parsing & filtering

Further complexity arises from the way the WorldPop project organises multi-year datasets. Depending on 
the conceptual data series, WorldPop uses either a flat or nested file structure. In the former case, 
each country-year is listed as a distinct entry in the Coverage Index (e.g., the /pop/wpgp series).
Each such entry is then associated with one raster file. In the latter case, all available years for 
one country are "grouped" together under a single entry (e.g., the /covariates/G2_NT_lights series).

Our crawler infers this file-organisation scheme from the Sample Payload. When "grouped" multi-year 
data is encountered, we always flattened this to ensure that our own data manifest always records 
each country-year-specific dataset in one row.

## Unsupported datasets

Unsupported data series are likewise identified and filtered through analysis of the Sample Payload. 
Notably, `worldpoppy` does currently not support data that WorldPop hosts in formats other than 
Geotiff. (e.g., 7z.)
