import numpy as np
import pytest
import rasterio
import rioxarray
from affine import Affine
from rioxarray.merge import merge_arrays

from worldpoppy.mosaic import build_vrt, check_same_grid, write_vrt

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


def test_grid_noise_is_snapped_onto_the_first_grid(mock_raster_factory, tmp_path):
    """
    A raster whose origin is off the first raster's grid by floating-point
    noise is placed at the whole-pixel offset, and the mosaic lies exactly on
    the first raster's grid, also when another raster lies further left.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(3, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", _filled(2.0), origin=(1e-7, 3 - 1e-7), nodata=NODATA)

    values, transform, _ = _read_mosaic(write_vrt([a, b], tmp_path / "m.vrt"))

    assert values.shape == (3, 6)
    assert (transform.c, transform.f, transform.a) == (0.0, 3.0, 1.0)  # exactly a's grid
    np.testing.assert_array_equal(values[:, :3], 2.0)
    np.testing.assert_array_equal(values[:, 3:], 1.0)


@pytest.mark.parametrize("shift", [0.02, 0.5, 0.96])
def test_offsets_beyond_noise_are_rejected(mock_raster_factory, shift):
    """
    A raster whose origin is off the first raster's grid by more than
    MAX_GRID_NOISE pixels (e.g. a half-pixel shift) is rejected, not rounded
    to the nearest pixel.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", _filled(2.0), origin=(3 + shift, 3), nodata=NODATA)

    with pytest.raises(ValueError, match="b.tif lies .* pixels off the grid of a.tif"):
        build_vrt([a, b])


def test_check_same_grid_returns_whole_pixel_offsets():
    """
    Offsets are counted in whole pixels of the first raster (columns to the
    right, rows downwards), also across a large extent with noise in the
    pixel size and origin.
    """
    res = 1 / 1200
    first = Affine(res, 0, 10.0, 0, -res, 50.0)
    noisy = Affine(res * (1 + 4e-9), 0, 10.0 - 25_000 * res + 1e-10, 0, -res, 50.0 + 7 * res)

    offsets = check_same_grid([(first, 100, 100), (noisy, 100, 100)])

    assert offsets == [(0, 0), (-25_000, -7)]


def test_pixel_size_noise_counts_across_each_raster_only():
    """
    Noise in a raster's pixel size counts only across that raster: a tiny
    raster with a relatively noisy pixel size (e.g. Vatican City's `pop_g1`
    file: 14 x 8 pixels, relative noise 2.9e-8) is accepted next to a raster
    432,000 pixels wide (e.g. Fiji's, which spans 360 degrees). In the reverse
    order, the wide raster would be snapped onto the tiny raster's noisy pixel
    size, and its far cells would move by 0.0125 pixels, so it is rejected.
    """
    res = 1 / 1200
    noisy_res = res * (1 + 2.9e-8)
    wide = (Affine(res, 0, -180.0, 0, -res, -12.0), 432_000, 10_000)
    tiny = (Affine(noisy_res, 0, 12.0, 0, -noisy_res, 42.0), 14, 8)

    offsets = check_same_grid([wide, tiny], names=["wide", "tiny"])

    assert offsets == [(0, 0), (230_400, -64_800)]
    with pytest.raises(ValueError, match="wide has a different resolution than tiny"):
        check_same_grid([tiny, wide], names=["tiny", "wide"])


def test_origin_offset_and_pixel_size_drift_add_up():
    """
    A raster whose origin offset (0.006 pixels) and pixel-size drift across its
    width (0.006 pixels) are each within MAX_GRID_NOISE, but together exceed
    it, is rejected: its far cells would move by 0.012 pixels.
    """
    first = (Affine(1.0, 0, 0.0, 0, -1.0, 100.0), 100, 100)
    offset_only = (Affine(1.0, 0, 50.006, 0, -1.0, 100.0), 100, 100)
    drift_only = (Affine(1 + 6e-5, 0, 50.0, 0, -1.0, 100.0), 100, 100)
    both = (Affine(1 + 6e-5, 0, 50.006, 0, -1.0, 100.0), 100, 100)

    check_same_grid([first, offset_only])
    check_same_grid([first, drift_only])
    with pytest.raises(ValueError, match="lies 0.012 pixels off the grid"):
        check_same_grid([first, both])


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


def test_mosaic_has_the_data_type_of_the_first_raster(mock_raster_factory, tmp_path):
    """
    Rasters of different data types are accepted: the mosaic has the data type
    of the first raster, and the values of the others are converted to it, as
    in `rioxarray.merge.merge_arrays`. (Fractional values converted to integers
    are rounded by GDAL, whereas `merge_arrays` truncates them.)
    """
    int_data = np.ones((3, 3), dtype="int16")
    float_data = np.array([[2.0, 2.4, 2.6]] * 3)  # float64
    a = mock_raster_factory("a.tif", int_data, origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", float_data, origin=(3, 3), nodata=NODATA)

    vrt = write_vrt([a, b], tmp_path / "m.vrt")
    with rasterio.open(vrt) as src:
        dtype, values = src.dtypes[0], src.read(1)
    eager = merge_arrays([rioxarray.open_rasterio(p) for p in (a, b)]).squeeze("band")

    assert dtype == eager.dtype == "int16"
    np.testing.assert_array_equal(values[:, :4], eager.values[:, :4])  # whole values
    np.testing.assert_array_equal(values[0, 4:], [2, 3])  # 2.4 and 2.6, rounded


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


def test_resolution_noise_is_accepted(mock_raster_factory, tmp_path):
    """
    Pixel sizes that differ by floating-point noise (as in real WorldPop files)
    count as the same resolution, and the mosaic uses the first raster's.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", _filled(2.0), origin=(3, 3), res=1 + 1e-7, nodata=NODATA)

    values, transform, _ = _read_mosaic(write_vrt([a, b], tmp_path / "m.vrt"))

    assert transform.a == 1.0
    np.testing.assert_array_equal(values[:, 3:], 2.0)


# --- Tests: Rejected Inputs ---

def test_resolution_drift_across_a_raster_is_rejected(mock_raster_factory):
    """
    A difference in pixel size that is tiny in relative terms is still rejected
    if it adds up to more than MAX_GRID_NOISE pixels across the raster itself
    (here: 1e-4 over 200 pixels = 0.02 pixels).
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory(
        "b.tif", _filled(2.0, shape=(3, 200)), origin=(3, 3), res=1 + 1e-4, nodata=NODATA
    )

    with pytest.raises(ValueError, match="b.tif has a different resolution"):
        build_vrt([a, b])

def test_empty_list_raises():
    with pytest.raises(ValueError, match="empty"):
        build_vrt([])


@pytest.mark.parametrize("change, message", [
    (dict(crs="EPSG:3857"), "different CRS"),
    (dict(res=0.5), "different resolution"),
    (dict(nodata=-1.0), "different nodata value"),
    (dict(nodata=np.nan), "different nodata value"),
    (dict(nodata=None), "different nodata value"),
])
def test_incompatible_rasters_raise(mock_raster_factory, change, message):
    """
    Rasters that differ from the first one in CRS, resolution or nodata value
    are rejected, naming the raster.
    """
    a = mock_raster_factory("a.tif", _filled(1.0), origin=(0, 3), nodata=NODATA)
    b = mock_raster_factory("b.tif", _filled(2.0), **{"origin": (3, 3), "nodata": NODATA, **change})

    with pytest.raises(ValueError, match=f"b.tif has a {message}"):
        build_vrt([a, b])


@pytest.mark.parametrize("transform, message", [
    (Affine(1, 0, 0, 0, 1, 0), "is stored south-up"),
    (Affine(-1, 0, 5, 0, -1, 5), "runs from east to west"),
    (Affine(1, 0.1, 0, 0, -1, 5), "is rotated"),
], ids=["south-up", "east-to-west", "rotated"])
def test_rasters_that_are_not_north_up_are_rejected(transform, message):
    """
    A raster that is not north-up is rejected with a message naming the cause,
    whether it is the only raster, the first or a later one (before, a
    south-up raster first gave all-NaN eager merges, and a later one a
    misleading "different resolution").
    """
    north_up = (Affine(1, 0, 0, 0, -1, 5), 5, 5)
    other = (transform, 5, 5)

    for grids, names in (([other], ["b"]), ([north_up, other], ["a", "b"]), ([other, north_up], ["b", "a"])):
        with pytest.raises(ValueError, match=f"b {message}"):
            check_same_grid(grids, names=names)


def test_multi_band_raster_raises(tmp_path):
    path = tmp_path / "two_bands.tif"
    profile = dict(driver="GTiff", width=2, height=2, count=2, dtype="float32",
                   crs="EPSG:4326", transform=rasterio.transform.from_origin(0, 2, 1, 1))
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.ones((2, 2, 2), dtype="float32"))

    with pytest.raises(ValueError, match="2 bands"):
        build_vrt([path])
