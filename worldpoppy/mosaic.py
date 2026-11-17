"""
This module builds GDAL virtual mosaics (VRT files) of single-band rasters that
share a coordinate reference system and a resolution, using `rasterio` only (no
GDAL Python bindings).

A VRT file is a short XML description of a mosaic: it names the source rasters
and the pixel rectangle that each one fills. Opened with
`rioxarray.open_rasterio(..., chunks=...)`, a mosaic behaves like a single
raster that is read chunk by chunk, so merging rasters lazily takes little more
memory than reading them. GDAL, including the copy that the `rasterio` wheels bundle,
can read the VRT format natively.

Where rasters overlap, the first raster wins (as in
`rioxarray.merge.merge_arrays`), and nodata cells never cover valid data of
another raster. All rasters must lie on one grid, up to floating-point noise
(see `check_same_grid`), and the mosaic uses the grid of the first raster
exactly. The mosaic has the data type of the first raster; GDAL converts the values of the other
rasters to it when reading (as `rioxarray.merge.merge_arrays` does, except that
GDAL rounds fractional values converted to integers, which `merge_arrays`
truncates).

This module does not depend on the rest of `worldpoppy`. The approach was
informed by a review of rio-vrt (https://github.com/12rambau/rio-vrt, MIT
licence); its code is not reused here.

Main methods
------------------------
    - :func:`build_vrt`
        Return the XML of a mosaic of several rasters.
    - :func:`write_vrt`
        Write the XML of a mosaic of several rasters to a file.
    - :func:`check_same_grid`
        Check that rasters lie on one grid, up to floating-point noise.
    - :func:`mosaic_grid`
        Return the grid of the mosaic of several rasters.
"""

import math
import xml.etree.ElementTree as ET
from pathlib import Path

import rasterio

__all__ = [
    "MAX_GRID_NOISE",
    "VRT_FORMAT_VERSION",
    "build_vrt",
    "check_same_grid",
    "mosaic_grid",
    "write_vrt",
]

# Version of the XML that `build_vrt` writes. It changes whenever that XML
# changes, so that callers who keep VRT files can rebuild outdated ones.
# (Raise it with every change to the XML: VRT files built by earlier code are
# otherwise reused, e.g. from worldpoppy's cache.)
VRT_FORMAT_VERSION = 2

# GDAL's names of the data types that rasterio reports
GDAL_DATA_TYPES = {
    "uint8": "Byte",
    "int8": "Int8",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint32": "UInt32",
    "int32": "Int32",
    "uint64": "UInt64",
    "int64": "Int64",
    "float32": "Float32",
    "float64": "Float64",
}

# Largest difference between two grids (in pixels) that counts as
# floating-point noise in the files' geotransforms, see `check_same_grid`.
# Measured on 2026-11-15 for 524 cached WorldPop rasters (28 products, 3 and
# 30 arcseconds; rasterio 1.5.2, GDAL 3.12.2): pixel-size noise adds up to at
# most 0.0017 pixels across a raster (nearly 360 degrees wide), and origins of
# files of one product differ from whole pixels by at most 0.00034 pixels;
# real differences between WorldPop grids are half a pixel. The limit leaves a
# margin of 5 above the noise and of 50 below a real shift.
MAX_GRID_NOISE = 0.01


# --- Public Functions ---

def build_vrt(raster_fpaths):
    """
    Return the XML of a virtual mosaic (VRT) of several rasters.

    Where rasters overlap, the first raster in `raster_fpaths` wins, and nodata
    cells never cover valid data of another raster. The mosaic uses the grid
    of the first raster exactly. The mosaic has the data type of the first
    raster; GDAL converts the values of the other rasters to it when reading.

    Parameters
    ----------
    raster_fpaths : List[str or Path]
        Paths to single-band rasters with the same CRS and nodata value, on
        one grid up to floating-point noise (see `check_same_grid`). The VRT
        refers to the rasters by their absolute paths.

    Returns
    -------
    str
        The XML of the VRT.

    Raises
    ------
    ValueError
        If `raster_fpaths` is empty, if a raster has more than one band, is
        rotated or has an unsupported data type, if the rasters differ in CRS or
        nodata value, or if they do not lie on one grid (see `check_same_grid`).
    """
    if not raster_fpaths:
        raise ValueError("`raster_fpaths` is empty: a mosaic needs at least one raster.")

    rasters = [_read_raster_info(path) for path in raster_fpaths]
    _check_compatible(rasters)
    return _mosaic_xml(rasters)


def write_vrt(raster_fpaths, vrt_fpath):
    """
    Write the XML of a virtual mosaic (VRT) of several rasters to a file.

    See `build_vrt` for the parameters and the rules of the mosaic. The file is
    written in UTF-8, as GDAL expects.

    Parameters
    ----------
    raster_fpaths : List[str or Path]
        Paths to the rasters (see `build_vrt`).
    vrt_fpath : str or Path
        Path of the VRT file to write.

    Returns
    -------
    Path
        The path of the VRT file.

    Raises
    ------
    ValueError
        See `build_vrt`.
    """
    xml = build_vrt(raster_fpaths)
    vrt_fpath = Path(vrt_fpath)
    vrt_fpath.write_text(xml, encoding="utf-8")
    return vrt_fpath


def check_same_grid(grids, names=None):
    """
    Check that north-up rasters lie on one grid, up to floating-point noise,
    and return their whole-pixel offsets from the first raster.

    A raster lies on the first raster's grid up to noise if snapping it onto
    that grid moves none of its cells by more than MAX_GRID_NOISE pixels: the
    offset of its origin from a whole number of pixels, plus the difference in
    pixel size added up across the raster's own width (or height), must not
    exceed MAX_GRID_NOISE. A tiny raster whose stored pixel size is relatively
    noisy is therefore accepted next to a very wide one. (The reverse order can
    fail: the very wide raster would then be snapped onto the tiny raster's
    noisy pixel size.) Real differences (e.g. a half-pixel shift, or another
    resolution) are rejected.

    Parameters
    ----------
    grids : List[Tuple[affine.Affine, int, int]]
        The transform, width and height of each raster (in this order).
    names : List[str], optional
        Names of the rasters for error messages. Defaults to 'raster 1',
        'raster 2', and so on.

    Returns
    -------
    List[Tuple[int, int]]
        The offset (columns, rows) of each raster from the first one, in
        whole pixels of the first raster (positive: to the right, downwards).

    Raises
    ------
    ValueError
        If a raster does not lie on the grid of the first raster.
    """
    if names is None:
        names = [f"raster {i + 1}" for i in range(len(grids))]
    first_transform = grids[0][0]
    xres, yres = first_transform.a, -first_transform.e

    offsets = []
    for (transform, width, height), name in zip(grids, names):
        col = (transform.c - first_transform.c) / xres
        row = (first_transform.f - transform.f) / yres
        largest_move = 0.0
        for res, ref_res, num_pixels, offset in (
            (transform.a, xres, width, col), (-transform.e, yres, height, row)
        ):
            # How far the raster's cells drift from the first raster's grid
            # across the raster itself, in pixels
            drift = abs(res - ref_res) / ref_res * num_pixels
            if drift > MAX_GRID_NOISE:
                raise ValueError(
                    f"{name} has a different resolution than {names[0]} ({res!r} vs "
                    f"{ref_res!r}): across its {num_pixels} pixels, the difference adds up "
                    f"to {drift:.3g} pixels (at most {MAX_GRID_NOISE} pixels are accepted as "
                    f"floating-point noise)."
                )
            largest_move = max(largest_move, abs(offset - round(offset)) + drift)
        if largest_move > MAX_GRID_NOISE:
            raise ValueError(
                f"{name} lies {largest_move:.3g} pixels off the grid of {names[0]} "
                f"(at most {MAX_GRID_NOISE} pixels are accepted as floating-point noise)."
            )
        offsets.append((round(col), round(row)))
    return offsets


def mosaic_grid(grids, names=None):
    """
    Return the grid of the mosaic of north-up rasters: the first raster's
    grid, extended to hold every raster entirely.

    Parameters
    ----------
    grids : List[Tuple[affine.Affine, int, int]]
        The transform, width and height of each raster (in this order).
    names : List[str], optional
        Names of the rasters for error messages (see `check_same_grid`).

    Returns
    -------
    Tuple[affine.Affine, int, int, List[Tuple[int, int]]]
        The transform, width and height of the mosaic, and the offset
        (columns, rows) of each raster within it, in whole pixels.

    Raises
    ------
    ValueError
        If the rasters do not lie on one grid (see `check_same_grid`).
    """
    offsets = check_same_grid(grids, names=names)
    first = grids[0][0]

    # The mosaic starts at the top-left raster, on the first raster's grid
    min_col = min(col for col, _ in offsets)
    min_row = min(row for _, row in offsets)
    offsets = [(col - min_col, row - min_row) for col, row in offsets]
    width = max(col + w for (col, _), (_, w, _) in zip(offsets, grids))
    height = max(row + h for (_, row), (_, _, h) in zip(offsets, grids))
    transform = rasterio.Affine(
        first.a, 0.0, first.c + min_col * first.a, 0.0, first.e, first.f + min_row * first.e
    )
    return transform, width, height, offsets


# --- Reading and Checking the Rasters ---

def _read_raster_info(path):
    """
    Read the properties of a raster that the mosaic needs.
    """
    path = Path(path).resolve()
    with rasterio.open(path) as src:
        if src.count != 1:
            raise ValueError(
                f"{path.name} has {src.count} bands; a mosaic needs single-band rasters."
            )
        transform = src.transform
        if transform.b != 0 or transform.d != 0:
            raise ValueError(f"{path.name} is rotated; a mosaic needs north-up rasters.")
        dtype = src.dtypes[0]
        if dtype not in GDAL_DATA_TYPES:
            raise ValueError(f"{path.name} has the unsupported data type {dtype}.")
        block_height, block_width = src.block_shapes[0]

        return dict(
            path=path,
            transform=transform,
            width=src.width,
            height=src.height,
            crs=src.crs,
            dtype=dtype,
            nodata=src.nodata,
            block_width=block_width,
            block_height=block_height,
        )


def _check_compatible(rasters):
    """
    Check that all rasters share the CRS and nodata value of the first one and
    lie on its grid (see `check_same_grid`). (Their data types may differ, see
    `build_vrt`.)
    """
    first = rasters[0]
    for raster in rasters[1:]:
        name, first_name = raster['path'].name, first['path'].name
        if raster['crs'] != first['crs']:
            raise ValueError(f"{name} has a different CRS than {first_name}.")
        if not _same_nodata(raster['nodata'], first['nodata']):
            raise ValueError(
                f"{name} has a different nodata value ({raster['nodata']}) than "
                f"{first_name} ({first['nodata']})."
            )
    check_same_grid(
        [(r['transform'], r['width'], r['height']) for r in rasters],
        names=[r['path'].name for r in rasters],
    )


def _same_nodata(a, b):
    """
    Compare two nodata values; NaN equals NaN, and None (no nodata) only None.
    """
    if a is None or b is None:
        return a is None and b is None
    return (math.isnan(a) and math.isnan(b)) or a == b


# --- Building the Mosaic ---

def _mosaic_xml(rasters):
    """
    Place the rasters on the grid of the mosaic (see `mosaic_grid`) and return
    the XML of the VRT.
    """
    transform, width, height, offsets = mosaic_grid(
        [(r['transform'], r['width'], r['height']) for r in rasters],
        names=[r['path'].name for r in rasters],
    )
    first = rasters[0]

    data_type = GDAL_DATA_TYPES[first['dtype']]
    nodata = first['nodata']

    root = ET.Element("VRTDataset", rasterXSize=str(width), rasterYSize=str(height))
    ET.SubElement(root, "SRS").text = first['crs'].to_wkt()
    ET.SubElement(root, "GeoTransform").text = ", ".join(
        repr(float(value)) for value in transform.to_gdal()
    )
    band = ET.SubElement(root, "VRTRasterBand", dataType=data_type, band="1")
    if nodata is not None:
        ET.SubElement(band, "NoDataValue").text = repr(float(nodata))

    # GDAL draws later sources on top, so the first raster is written last
    for raster, (xoff, yoff) in reversed(list(zip(rasters, offsets))):
        band.append(_source_element(raster, xoff, yoff, nodata))

    return ET.tostring(root, encoding="unicode")


def _source_element(raster, xoff, yoff, nodata):
    """
    Return the `ComplexSource` element of one raster. Its own NODATA value
    keeps its nodata cells from covering valid data underneath.
    """
    source = ET.Element("ComplexSource")
    ET.SubElement(source, "SourceFilename", relativeToVRT="0").text = str(raster['path'])
    ET.SubElement(source, "SourceBand").text = "1"
    ET.SubElement(
        source, "SourceProperties",
        RasterXSize=str(raster['width']), RasterYSize=str(raster['height']),
        DataType=GDAL_DATA_TYPES[raster['dtype']],
        BlockXSize=str(raster['block_width']), BlockYSize=str(raster['block_height']),
    )
    size = dict(xSize=str(raster['width']), ySize=str(raster['height']))
    ET.SubElement(source, "SrcRect", xOff="0", yOff="0", **size)
    ET.SubElement(source, "DstRect", xOff=str(xoff), yOff=str(yoff), **size)
    if nodata is not None:
        ET.SubElement(source, "NODATA").text = repr(float(nodata))
    return source
