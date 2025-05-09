# Data assets

### level0_500m_2000_2020_simplified_world.feather

This file contains a GeoDataFrame with simplified country borders for the whole world. The
border data is not intended for display or any geo-data analysis. Its sole purpose is to 
translate *user-specified* areas of interest into a list of ISO-codes for which WorldPop 
rasters need to be downloaded and merged (for details, see `WorldPopDownloader`). 

Simplified country borders were extracted from WorldPop's [*level0_100m*](https://hub.worldpop.org/geodata/listing?id=62) rasters, 
after down-sampling these by a factor of 10. The full data-processing code can be found 
in `worldpoppy.borders`. The original WorldPop *level0_100m* rasters are licenced under the 
[Creative Commons Attribution 4.0 International License](https://hub.worldpop.org/data/licence.txt). 
