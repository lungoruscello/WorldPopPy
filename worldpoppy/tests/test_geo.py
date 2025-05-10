import numpy as np
from shapely.geometry import box
import geopandas as gpd


def test_bbox_area_km():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 1_000

    for lat in np.linspace(-60, 60, 10):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 percent
