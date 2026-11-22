# Finding data

WorldPop's datasets are typically distributed as individual raster files per country. This guide shows how to find the data products that **WorldPopPy** supports, which years and countries they cover, and how to
trace a product back to WorldPop's official datasets.

## Data products

To simplify data discovery, **WorldPopPy** organises WorldPop's raw files into curated data products with a
consistent naming scheme. For example, `pop_g2_r25a` denotes population counts (`pop`) from WorldPop's Global 2
series (`g2`), 2025 alpha release (`r25a`). Products cover population counts, night lights, elevation, land cover,
and climate variables, among others, and many are available for several years.

Use `show_supported_data_products` for an overview, with a short description, the spatial resolution, and whether a
data product covers several years or is static. The overview can be filtered by keyword, country, or year:

```python
from worldpoppy import show_supported_data_products

# Print data products related to "population" from the Global 2 series 
show_supported_data_products(keywords=["population", "global2"])

# Print static (single-year) data products available for Brazil
show_supported_data_products(static_only=True, iso3_codes="BRA")

# Print data products with data for 2015 or 2023 (or both)
show_supported_data_products(years=[2015, 2023])
```

A product passes the year filter if it has data for at least one of the given years.

## The data manifest

Alternatively, you can also get the library's full data manifest as a pandas DataFrame, with one row per raster
file:

```python
from worldpoppy import wp_manifest

mdf = wp_manifest()
mdf.head()
```

`wp_manifest` accepts the same filters, and a `product_name` to select one product.

To see which years and countries a data product covers, use `get_product_info`:

```python
from worldpoppy import get_product_info

info = get_product_info("ntl_viirs_g2")
sorted(info["years"])  # -> [2015, 2016, ..., 2023]
```

## Countries

WorldPop organises its data by country, and **WorldPopPy** identifies countries by their three-letter ISO codes
(ISO 3166-1 alpha-3, e.g. "KEN" for Kenya). These codes select countries in `wp_raster`, `wp_manifest`,
`show_supported_data_products`, and `plot_country_borders`. To look up a country's code, search for (part of) its name.
The result lists all matching countries, exact matches first:

```python
from worldpoppy import find_country_code

find_country_code("ivory")
#   iso3           name matched_name
# 0  CIV  Côte d'Ivoire  Ivory Coast
```

`wp_raster` also accepts country names (e.g. `aoi="Kenya"`) if they match exactly one country, and reports the code 
it used. Ambiguous names such as "Congo" raise an error that lists the candidates.

## Tracing a product to WorldPop's datasets

The product names used by this library (e.g. `pop_g1_alt` or `pop_g2_alt`) are specific to **WorldPopPy**; WorldPop
does not use them. **When citing data, please refer to WorldPop's official dataset titles.** The manifest records them
for every raster file, together with a link to the dataset's summary page:

```python
from worldpoppy import wp_manifest

# Select country entries for one "product" using its curated WorldPopPy alias
mdf = wp_manifest(product_name='pop_g2_alt', iso3_codes='AFG')

# Inspect the raw metadata for one raster file (sourced from the WorldPop API)
row = mdf.iloc[0]

print(f"Source File Name:       {row.dataset_name}")
print(f"Official Dataset Title: {row.api_entry_title}")
print(f"Official Data Category: {row.api_series_category}")
print(f"Dataset Summary:        {row.summary_url}")  # Read this before using data!
print("-----")

# > The internal fields below are for data discovery in WorldPopPy
print(f"Library Product Name:   {row.product_name}")
print(f"Multi-year Product?     {row.multi_year}")
print(f"Library Product Notes:  {row.product_notes}")
```

WorldPop datasets are often the result of complex modelling. Always check the `summary_url` provided in the manifest
for details and further notes. To keep the load on WorldPop's servers low, **WorldPopPy** does not request the file
listing of every raster dataset when building the manifest. Instead, we infer download and summary URLs from URL
patterns, based on one sample dataset per data series, and only spot-check the results. Download URLs of WorldPop's
Global 1 series (2000-2020) are additionally checked against WorldPop's file list for that series. If a summary link
does not work, the official dataset title (`api_entry_title`) helps to find the page on WorldPop's website.

Results of `wp_raster` carry the same titles and summary pages, and WorldPop's recommended citations, in their
attributes (see [Metadata of WorldPopPy results](metadata.md)).
