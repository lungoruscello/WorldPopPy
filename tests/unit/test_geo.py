import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import box


def test_bbox_area_km_moderate_lats():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 2_000

    for lat in np.linspace(-60, 60, 50):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)


def test_bbox_area_km_high_lats():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 1_000

    for lat in np.linspace(-70, 70, 50):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 percent


def test_bbox_area_km_extreme_lats():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.config import WGS84_CRS

    world_eqa = "ESRI:54034"  # Cylindrical Equal Area, World (units: metre)
    width_km = 500

    for lat in np.linspace(-80, 80, 50):
        corners = bbox_from_location((0, lat), width_km=width_km)
        box_gdf_wgs84 = gpd.GeoDataFrame(
            geometry=[box(*corners)],  # noqa
            crs=WGS84_CRS
        )
        box_gdf_eqa = box_gdf_wgs84.to_crs(world_eqa)
        ref_area_km = box_gdf_eqa.area.iloc[0] / 1e6
        assert np.isclose(ref_area_km, width_km ** 2, rtol=0.05)  # within 5 percent


def test_bbox_date_line_crash():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.func_utils import BboxInvalidError

    for lat in np.linspace(-80, 80, 20):
        with pytest.raises(BboxInvalidError):
            bbox_from_location((180.0, lat), width_degrees=2.0)


def test_bbox_pole_crash_latitude():
    from worldpoppy.raster import bbox_from_location
    from worldpoppy.func_utils import BboxInvalidError

    for lon in np.linspace(-180, 180, 20):
        with pytest.raises(BboxInvalidError):
            bbox_from_location((lon, 89.0), width_degrees=4.0)