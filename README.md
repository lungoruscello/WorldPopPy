# WorldPopPy

[![PyPI Latest Release](https://img.shields.io/pypi/v/WorldPopPy.svg)](https://pypi.org/project/WorldPopPy/)
[![License](https://img.shields.io/pypi/l/WorldPopPy.svg)](https://github.com/lungoruscello/WorldPopPy/blob/master/LICENSE.txt)

**WorldPopPy** is a Python package that makes it easy for you to work with data from the [WorldPop](https://www.worldpop.org/)
project. WorldPop offers a variety of [high-resolution, global geo-datasets](https://www.worldpop.org/datacatalog/) on 
population dynamics, night-light emissions, land-cover features, and more. With WorldPopPy, you can easily download, 
combine, and clean WorldPop datasets for different geographic regions and years.

## Key Features

* Fetch data for any area of interest by passing country codes, bounding boxes, GeoDataFrames, or location names.
* Easy handling of annual time-series through integration with [`xarray`](https://docs.xarray.dev/en/stable/) (a powerful 
Python library for multi-dimensional raster datasets).
* Parallel downloads for faster data retrieval, including retry mechanism and 'dry runs' to preview required download sizes.
* Auto-updating manifest file so you stay up-to-date with WorldPop’s latest available datasets.

## Installation

**WorldPopPy** is available on [PyPI](https://pypi.org/project/WorldPopPy/) and can be 
installed using `pip`:

`pip install worldpoppy`

## Quickstart
```python
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from worldpoppy import wp_raster, clean_axis

# Fetch Night-Light data for the Korean Peninsula  
viirs_data = wp_raster(  # returns an `xarray.DataArray` ready for analysis and plotting 
    product_name='viirs_100m',
    aoi=['PRK', 'KOR'],  # three-letter country codes for North and South Korea  
    years=2015,
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value),
)

# Downsample
lowres = viirs_data.coarsen(x=5, y=5, boundary='trim').mean()

# Plot
lowres.plot(vmin=0.1, cmap='inferno', norm=LogNorm())
clean_axis(title='Night Lights (2015)\nKorean Peninsula')

plt.show()
```
<img src="worldpoppy/assets/korea_viirs.png" alt="Night Lights Korea" width="250"/> 


## More detailed example

Below, we visualise **population growth** in a patch of West Africa from 2000 to 2020. The geographic 
area of interest is selected using helper function that converts location names into a bounding box. 
We also illustrate how to re-project WorldPop data into a user-defined Coordinate Reference System (CRS).

```python
import numpy as np
import matplotlib.pyplot as plt
from worldpoppy import *

# Define the area of interest (runs a `Nomatim` query under the hood) 
aoi_box = bbox_from_location('Accra', width_km=500)  # returns (min_lon, min_lat, max_lon, max_lat)

# Define the target CRS (optional)
aeqa_africa = "ESRI:102022"  # Albers Equal Area projection optimised for Africa

# Fetch the population data
pop_data = wp_raster(
    product_name='ppp',  # name of the WorldPop product (here: est. no. of people per raster cell)
    aoi=aoi_box,  # you can also pass a GeoDataFrame or official country codes here
    years=[2000, 2020],  # the years of interest (for annual WorldPop products only)
    masked=True,  # mask missing values with NaN (instead of WorldPop's default fill value),
    to_crs=aeqa_africa  # if None is provided, the CRS of the source data will be kept
)

# Compute population changes on downsampled data
lowres = pop_data.coarsen(x=10, y=10, year=1, boundary='trim').reduce(np.sum)  # will propagate NaNs
pop_change = lowres.sel(year=2020) - lowres.sel(year=2000)

# Plot
pop_change.plot(cmap='coolwarm', vmax=1_000, cbar_kwargs=dict(shrink=0.85))
clean_axis(title='Est. population change (2020 - 2000)')

# Add visual references
plot_country_borders(['GHA', 'TOG', 'BEN'], edgecolor='white', to_crs=aeqa_africa)
plot_location_markers(['Accra', 'Kumasi', 'Lomé'], to_crs=aeqa_africa)

plt.show()
```
<img src="worldpoppy/assets/accra_pop.png" alt="Night Lights Korea" width="350"/> 

## Further information

### Examples

More detailed examples are provided in the accompanying [Jupyter notebook](./worldpoppy/examples/eg.ipynb).

### Full API Reference
For a full reference of all available functions and their parameters, please refer to the official documentation.
(Coming soon)

## Feedback and Issues

If you have any feedback, encounter issues, or want to suggest improvements, please [open an issue](https://github.com/lungoruscello/WorldPopPy/issues).

**Note**: WorldPopPy has been developed and tested on Linux. Issues encountered on other platforms  
may take longer to address.

## License
This projects is licensed under the [Mozilla Public License](https://www.mozilla.org/en-US/MPL/2.0/). 
See [LICENSE.txt](LICENSE.txt)  for details.
