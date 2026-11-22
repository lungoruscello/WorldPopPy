# Finding data

WorldPop's datasets are typically distributed as individual raster files per country. This guide shows how to find the data products that **WorldPopPy** supports, which years and countries they cover, and how to
trace a product back to WorldPop's official datasets.

## Data products

To simplify data discovery, **WorldPopPy** organises WorldPop's raw files into curated data products with a
consistent naming scheme. For example, `pop_g2_r25a` denotes population counts (`pop`) from WorldPop's Global 2
series (`g2`), 2025 alpha release (`r25a`). Products cover population counts, night lights, elevation, distances to
land-cover classes, and climate variables, among others, and many are available for several years.

Other parts of the product names:

- `g1`, `g2`: WorldPop's Global 1 series (2000-2020) and Global 2 series (2015-2030).
- `r24b`, `r25a`: releases of the Global 2 series (2024 beta release R2024B, 2025 alpha release R2025A).
- `1km`: cells of about 1 km at the equator (30 arc seconds); most products have cells of about 100 m (3 arc seconds).
- `uc`: unconstrained population estimates (`UC` in WorldPop's file names).
- `unadj`: population estimates whose country totals were adjusted to match the corresponding estimates of the UN
  Population Division (`UNadj` in WorldPop's file names). The name does not mean "unadjusted".
- `alt`: alternative single-year datasets (`pop_g1_alt` for 2020; `pop_g2_alt` and `pop_uc_g2_alt` for 2024).

The population products of the Global 2 series cover the years 2015 to 2030, so that `years='last'` returns 2030. For
the 2025 alpha release (R2025A), WorldPop's release statement explains that "[f]or each country, the two timepoints of
population data were interpolated and projected, providing a full set of annual population projections for the period
2010-2030", and that "the uncertainty will increase for population datasets representing reference years further away
from the input population dataset timepoints"
([Global2 Release Statement R2025A](https://data.worldpop.org/repo/prj/Global_2015_2030/R2025A/doc/Global2_Release_Statement_R2025A_v1.pdf),
pp. 3 and 7).

Population products are either constrained or unconstrained, as their product notes say. WorldPop explains the two
approaches on its page
[Top-down estimation modelling: Constrained vs Unconstrained](https://www.worldpop.org/methods/top_down_constrained_vs_unconstrained/):
unconstrained modelling makes "predictions about population numbers for all 100x100m land grid cells", while
constrained modelling disaggregates population "to only those grid cells identified as containing residential
buildings/built settlement". Read this page before choosing a population product. WorldPop's guide
[Choosing the right WorldPop population data for you](https://www.worldpop.org/choosing-the-right-worldpop-population-data-for-you/)
helps to choose between its population datasets, including some that WorldPopPy does not cover.

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
