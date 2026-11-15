import numpy as np
import pytest
import rasterio
import rioxarray
from rioxarray.merge import merge_arrays

from worldpoppy.mosaic import build_vrt, write_vrt

NODATA = -9999.0


# --- Helpers ---

def _read_mosaic(vrt_fpath):
    """Read a mosaic with rasterio: values (nodata as NaN), transform, shape."""
    with rasterio.open(vrt_fpath) as src:
        values = src.read(1, masked=True).astype("float64").filled(np.nan)
        return values, src.transform, src.nodata


def _filled(value, shape=(3, 3)):
    return np.full(shape, value, dtype="float32")


# --- Tests: Placement and Priority ---

def test_first_raster_wins_where_rasters_overlap(mock_raster_factory, tmp_path):
    """
    Where two rasters overlap, the mosaic shows the first raster given, as
    `rioxarray.merge.merge_arrays` does.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    # b overlaps the two right-hand columns of a
    b = mock_raster_factory("b.tif", _filled(2.0), origin=(1, 3), nodata=NODATA)

    values_ab, _, _ = _read_mosaic(write_vrt([a, b], tmp_path / "ab.vrt"))
    values_ba, _, _ = _read_mosaic(write_vrt([b, a], tmp_path / "ba.vrt"))

    assert values_ab.shape == (3, 4)
    np.testing.assert_array_equal(values_ab[0], [1.0, 1.0, 1.0, 2.0])
    np.testing.assert_array_equal(values_ba[0], [1.0, 2.0, 2.0, 2.0])


def test_nodata_cells_do_not_cover_valid_data(mock_raster_factory, tmp_path):
    """
    A nodata cell of the raster on top lets the valid cell underneath show.
    """
    top_data = _filled(1.0)
    top_data[0, 2] = NODATA  # hole in the overlap
    bottom_data = _filled(2.0)
    top = mock_raster_factory("top.tif", top_data, origin=(0, 3), nodata=NODATA)
    bottom = mock_raster_factory("bottom.tif", bottom_data, origin=(1, 3), nodata=NODATA)

    values, _, nodata = _read_mosaic(write_vrt([top, bottom], tmp_path / "m.vrt"))

    np.testing.assert_array_equal(values[0], [1.0, 1.0, 2.0, 2.0])
    np.testing.assert_array_equal(values[1], [1.0, 1.0, 1.0, 2.0])
    assert nodata == NODATA


def test_mosaic_grid_matches_eager_merge(mock_raster_factory, tmp_path):
    """
    For rasters with a gap between them, the mosaic has the grid, the values
    and the nodata cells of `rioxarray.merge.merge_arrays`.
    """
    data_a = np.arange(6, dtype="float32").reshape(2, 3)
    data_b = np.arange(4, dtype="float32").reshape(2, 2) + 10
    a = mock_raster_factory("a.tif", data_a, origin=(0, 2), res=0.5, nodata=NODATA)
    b = mock_raster_factory("b.tif", data_b, origin=(2.5, 1.5), res=0.5, nodata=NODATA)

    vrt = write_vrt([a, b], tmp_path / "m.vrt")
    values, transform, _ = _read_mosaic(vrt)
    eager = merge_arrays([rioxarray.open_rasterio(p, masked=True) for p in (a, b)]).squeeze("band")

    assert values.shape == eager.shape == (3, 7)
    assert transform == eager.rio.transform()
    np.testing.assert_array_equal(values, eager.values)
    assert np.isnan(values[0, 3:]).all()  # gap between the rasters


def test_sub_pixel_offsets_are_rounded_to_whole_pixels(mock_raster_factory, tmp_path):
    """
    A raster shifted by a fraction of a pixel is placed at the nearest
    whole-pixel offset of the first raster's grid (rounded, not truncated:
    2.96 pixels become 3).
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", _filled(2.0), origin=(2.96, 2.96), nodata=NODATA)

    values, transform, _ = _read_mosaic(write_vrt([a, b], tmp_path / "m.vrt"))

    assert values.shape == (3, 6)
    assert transform.c == 0 and transform.f == 3 and transform.a == 1
    np.testing.assert_array_equal(values[:, 3:], 2.0)


def test_raster_rounded_outwards_is_not_cut(mock_raster_factory, tmp_path):
    """
    The grid holds every raster entirely, also when rounding its offset moves
    it one pixel beyond the extent of the others.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", _filled(2.0, (3, 2)), origin=(1.6, 3), nodata=NODATA)

    values, _, _ = _read_mosaic(write_vrt([a, b], tmp_path / "m.vrt"))

    assert values.shape == (3, 4)  # b at offset 2 (1.6 rounded), 2 columns wide
    np.testing.assert_array_equal(values[0], [1.0, 1.0, 1.0, 2.0])


def test_nan_nodata(mock_raster_factory, tmp_path):
    """
    Rasters whose nodata value is NaN keep their NaN cells from covering valid
    data, and the mosaic declares NaN as its nodata value.
    """
    top_data = _filled(1.0)
    top_data[0, 2] = np.nan
    top = mock_raster_factory("top.tif", top_data, origin=(0, 3), nodata=np.nan)
    bottom = mock_raster_factory("bottom.tif", _filled(2.0), origin=(1, 3), nodata=np.nan)

    values, _, nodata = _read_mosaic(write_vrt([top, bottom], tmp_path / "m.vrt"))

    np.testing.assert_array_equal(values[0], [1.0, 1.0, 2.0, 2.0])
    assert np.isnan(nodata)


def test_lazy_read_equals_eager_read(mock_raster_factory, tmp_path):
    """
    Read lazily with rioxarray (small chunks), the mosaic gives the same values
    as read at once, and missing cells are NaN in a masked read.
    """
    rng = np.random.default_rng(0)
    data_a, data_b = rng.random((5, 4), dtype="float32"), rng.random((3, 6), dtype="float32")
    a = mock_raster_factory("a.tif", data_a, origin=(0, 5), nodata=NODATA)
    b = mock_raster_factory("b.tif", data_b, origin=(3, 2), nodata=NODATA)
    vrt = write_vrt([a, b], tmp_path / "m.vrt")

    eager = rioxarray.open_rasterio(vrt, masked=True).load()
    lazy = rioxarray.open_rasterio(vrt, masked=True, chunks=2)

    assert lazy.chunks is not None
    np.testing.assert_array_equal(lazy.compute().values, eager.values)
    assert np.isnan(eager.values).any()


# --- Tests: Files and Paths ---

def test_write_vrt_handles_non_ascii_paths(mock_raster_factory, tmp_path):
    """
    Paths with non-ASCII characters are written in UTF-8 and can be read back.
    """
    folder = tmp_path / "Zürich_données"
    folder.mkdir()
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    moved = folder / "ä.tif"
    a.rename(moved)

    vrt = write_vrt([moved], folder / "mosaïc.vrt")

    assert "Zürich_données" in vrt.read_text(encoding="utf-8")
    values, _, _ = _read_mosaic(vrt)
    np.testing.assert_array_equal(values, 1.0)


def test_build_vrt_returns_xml_with_absolute_paths(mock_raster_factory):
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)

    xml = build_vrt([a])

    assert xml.startswith("<VRTDataset")
    assert f'relativeToVRT="0">{a.resolve()}<' in xml


# --- Tests: Rejected Inputs ---

def test_empty_list_raises():
    with pytest.raises(ValueError, match="empty"):
        build_vrt([])


@pytest.mark.parametrize("change, message", [
    (dict(crs="EPSG:3857"), "different CRS"),
    (dict(res=0.5), "different resolution"),
    (dict(nodata=-1.0), "different nodata value"),
    (dict(nodata=np.nan), "different nodata value"),
    (dict(nodata=None), "different nodata value"),
    (dict(dtype="float64"), "different data type"),
])
def test_incompatible_rasters_raise(mock_raster_factory, change, message):
    """
    Rasters that differ from the first one in CRS, resolution, nodata value or
    data type are rejected, naming the raster.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    kwargs = dict(origin=(3, 3), nodata=NODATA)
    kwargs.update({k: v for k, v in change.items() if k != "dtype"})
    data = _filled(2.0).astype(change.get("dtype", "float32"))
    b = mock_raster_factory("b.tif", data, **kwargs)

    with pytest.raises(ValueError, match=f"b.tif has a {message}"):
        build_vrt([a, b])


def test_multi_band_raster_raises(tmp_path):
    path = tmp_path / "two_bands.tif"
    profile = dict(driver="GTiff", width=2, height=2, count=2, dtype="float32",
                   crs="EPSG:4326", transform=rasterio.transform.from_origin(0, 2, 1, 1))
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.ones((2, 2, 2), dtype="float32"))

    with pytest.raises(ValueError, match="2 bands"):
        build_vrt([path])
