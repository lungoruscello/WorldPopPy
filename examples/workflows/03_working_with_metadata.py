# %% [markdown]
# # Workflow Example 3: Working with Metadata
#
# Illustrates:
# 1. The provenance a result carries: citations, licence, source files and processing history.
# 2. How WorldPopPy records its own processing steps.
# 3. Why attributes need updating after your own selections or arithmetic.
# 4. Saving results as GeoTIFF (with bands labelled by year) and as netCDF.
#
# The example downloads less than 1 MB (population of Bahrain, 2015 and 2023). The rules
# behind it are described in the metadata guide (`docs/metadata.md`).
#
# This file runs as a script (`python 03_working_with_metadata.py`). Its cells (marked `# %%`)
# can also be run one by one in editors that support them, e.g. VS Code. To use it as a
# Jupyter notebook, convert it with `jupytext --to notebook 03_working_with_metadata.py`, or
# open it in Jupyter with the `jupytext` extension installed.

# %%
import importlib.util
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
import xarray as xr

from worldpoppy import wp_raster, wp_warp

out_dir = Path(tempfile.mkdtemp())  # for the saved files; removed in the last cell

# %% [markdown]
# ## 1. A result and its attributes

# %%
pop = wp_raster("pop_g2_r25a", aoi="BHR", years=[2015, 2023])
print(list(pop.attrs))

# %% [markdown]
# ## 2. Provenance and citation
#
# WorldPopPy provides no data of its own. `references` lists WorldPop's recommended citations
# (with DOIs), followed by the official titles and summary pages of the datasets used.
# Cite these in publications, and respect the licence given in `license`.
#
# Some of this information is inferred rather than read for each dataset. To keep the load on
# WorldPop's servers low, WorldPopPy infers the summary URLs from URL patterns, and takes the
# licence and the recommended citations from WorldPop's metadata for one sample dataset per
# data series (as `comment` notes). If a summary link does not work, the official dataset title
# helps to find the page on WorldPop's website. Read the summary page before using the data:
# WorldPop's datasets are often the result of complex modelling.

# %%
print(pop.attrs["references"])
print(pop.attrs["license"])
print(pop.attrs["comment"])

# %% [markdown]
# `wpy_source_files` names the WorldPop files that contributed data, and `history` records
# what WorldPopPy did with them.

# %%
print(pop.attrs["wpy_source_files"])
print(pop.attrs["history"])

# %% [markdown]
# ## 3. WorldPopPy records its own processing steps
#
# Every WorldPopPy function that processes a result appends one line to `history`. Here,
# `wp_warp` reprojects the result to a metric grid (UTM zone 39N) with 100 m cells.
# `resampling="sum"` approximately preserves the population totals.

# %%
pop_utm = wp_warp(pop, to_crs="EPSG:32639", res=100, resampling="sum")
print(pop_utm.attrs["history"])

for year in pop.year.values:
    before = pop.sel(year=year).sum(dtype="float64").item()
    after = pop_utm.sel(year=year).sum(dtype="float64").item()
    print(f"{year}: {before:,.0f} people before warping, {after:,.0f} after")

# %% [markdown]
# ## 4. Your own processing
#
# The attributes of a WorldPopPy result (e.g. `wpy_years`, `wpy_source_files` and `history`)
# describe the result as returned by WorldPopPy. **Your own selections and arithmetic
# with xarray do not update raster attributes.** In the example at hand, after
# selecting one year, `wpy_years` and `wpy_source_files` still describe both years,
# and `history` has no line for the selection.

# %%
print(pop.sel(year=2023).attrs["wpy_years"])  # still '2015 2023'

# %% [markdown]
# A common next step is to compute the change between two years, here the change in
# population from 2015 to 2023. This needs care with empty cells. The population product used
# in this example, `pop_g2_r25a`, is "constrained": in each year, WorldPop places people only
# in areas mapped as settled, and leaves all other cells empty (NaN). Areas first settled after
# 2015 are therefore empty in 2015, and a plain difference (2023 minus 2015) would be empty
# there as well. Counting empty cells as zero people keeps them in the result; cells empty in
# both years (e.g., the sea) stay empty. (Unconstrained products have the same empty cells in
# every year, so a plain difference suffices for them.)

# %%
before, after = pop.sel(year=2015), pop.sel(year=2023)
growth = (after.fillna(0) - before.fillna(0)).where(before.notnull() | after.notnull())
print(f"{int((before.isnull() & after.notnull()).sum()):,} cells were first settled after 2015")

# %% [markdown]
# Arithmetic also loses the nodata value, and a difference of two years has no `year`
# coordinate.

# %%
print(growth.rio.nodata)        # None
print("year" in growth.coords)  # False
print(growth.attrs["history"])  # no line for the subtraction

# %% [markdown]
# ## 5. Updating attributes before saving
#
# Stale raster attributes matter once you save a result, e.g. to pass the file on. In that
# case, the saved metadata should describe what the file contains. **If you save a result
# after your own selections or arithmetic, update or drop its attributes first.** (For results
# you only use in memory, this is not needed.) The provenance attributes (`references`,
# `license`, `comment`, ...) remain valid.
#
# To ensure consistency, update those attributes that describe the raster data (e.g. `title`,
# `wpy_years`, `wpy_source_files`), and record your own steps in `history`, here in
# WorldPopPy's format, marked as yours. For one year on its own, request it from `wp_raster`
# instead: all its attributes then describe that year.


# %%
def with_history(da, step):
    """Return `da` with one line for your own processing step added to `history`."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return da.assign_attrs(history=f"{da.attrs['history']}\n{now} user: {step}")


# One year on its own: ask `wp_raster` for it, so that all its attributes
# describe that year (the file is already in the cache, nothing is downloaded)
pop_2023 = wp_raster("pop_g2_r25a", aoi="BHR", years=2023)

# The difference: a name and title of its own (the name becomes the band label
# of a saved GeoTIFF), a history line, and the nodata value restored. Its
# references (the datasets of both years) remain valid.
growth = growth.rename("pop_growth_2015_2023")
growth = growth.assign_attrs(
    title="Change in the estimated number of people per grid cell, 2015 to 2023 "
          "(derived from pop_g2_r25a)",
)
growth = with_history(growth, "difference 2023 - 2015, empty cells counted as 0 people")
growth = growth.rio.write_nodata(np.nan)

print(pop_2023.attrs["wpy_years"], pop_2023.attrs["wpy_source_files"])
print(growth.attrs["title"])
print(growth.attrs["history"])

# %% [markdown]
# ## 6. Saving as GeoTIFF
#
# All attributes are saved as GeoTIFF metadata tags. The bands are labelled with the array's
# name; to label the bands of a multi-year result with their years, set `long_name` on the
# array you save, just before saving.

# %%
years = tuple(str(year) for year in pop.year.values)
pop.assign_attrs(long_name=years).rio.to_raster(out_dir / "pop_2015_2023.tif")
pop_2023.rio.to_raster(out_dir / "pop_2023.tif")
growth.rio.to_raster(out_dir / "pop_growth.tif")

for name in ["pop_2015_2023.tif", "pop_2023.tif", "pop_growth.tif"]:
    with rasterio.open(out_dir / name) as src:
        print(f"{name}: bands {src.descriptions}, nodata {src.nodata}, "
              f"wpy_years {src.tags()['wpy_years']!r}")

# %% [markdown]
# ## 7. Saving as netCDF
#
# netCDF keeps the years as a coordinate. xarray writes the attributes to the variable;
# copying them to the file level as well is where tools following the CF conventions look for
# them. This part needs the `netCDF4` library, which WorldPopPy does not install.

# %%
if importlib.util.find_spec("netCDF4") is None:
    print("Skipping the netCDF part: it needs the netCDF4 library (pip install netCDF4).")
else:
    ds = pop.to_dataset()       # one variable, named after the product
    ds.attrs.update(pop.attrs)  # copy the attributes to file level as well
    ds.to_netcdf(out_dir / "pop_2015_2023.nc", engine="netcdf4")

    with xr.open_dataset(out_dir / "pop_2015_2023.nc", engine="netcdf4") as saved:
        print(f"years {saved.year.values}, wpy_years {saved.attrs['wpy_years']!r}")

# %%
shutil.rmtree(out_dir)
