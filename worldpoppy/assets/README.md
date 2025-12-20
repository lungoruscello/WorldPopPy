# Additional Data Assets

### level0_500m_2000_2020_simplified_world.feather

This file contains a GeoDataFrame with simplified country borders for the whole world. 
The border data is not intended for geo-data analysis, but serves two other purposes in `worldpoppy`:
1.  Translating *user-specified* areas of interest into a list of ISO-codes for which 
    WorldPop rasters need to be downloaded and merged.
2.  Providing a lightweight geometry source for the `plot_country_borders` visualisation utility.

Simplified country borders were extracted from WorldPop's [*level0_100m*](https://hub.worldpop.org/geodata/listing?id=62) 
rasters, after down-sampling these by a factor of 5. The full data-processing code can be found in `worldpoppy.borders`. 
The original WorldPop *level0_100m* rasters are licenced under the [Creative Commons Attribution 4.0 International License](https://hub.worldpop.org/data/licence.txt).

### global_nb_db.json

This file contains ISO3 country codes supported by the WorldPop project. The file is provided by WorldPop itself and 
likewise licenced under the Creative Commons Attribution 4.0 International License.  

### southern_kamchatka.feather

This file contains a GeoDataFrame with a *highly* simplified polygon for Southern Kamchatka. The polygon was 
drawn by hand using QGIS 3.22. It is provided solely for illustrative purposes in one of the worked examples
in this repository (`./examples/large_rasters/01_kamchatka_topo_eager.py`)
