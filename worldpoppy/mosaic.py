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
another raster. Each raster is placed at the nearest whole-pixel offset of the
mosaic grid, which uses the exact resolution of the first raster. The mosaic
has the data type of the first raster; GDAL converts the values of the other
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
"""

import math
import xml.etree.ElementTree as ET
from pathlib import Path

import rasterio

__all__ = [
    "VRT_FORMAT_VERSION",
    "build_vrt",
    "write_vrt",
]

# Version of the XML that `build_vrt` writes. It changes whenever that XML
# changes, so that callers who keep VRT files can rebuild outdated ones.
VRT_FORMAT_VERSION = 1

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

# The mosaic uses the resolution of the first raster. A raster whose pixel
# size differs slightly (floating-point noise in the files' geotransforms)
# drifts against that grid across the mosaic; it counts as having the same
# resolution if this drift stays below MAX_RESOLUTION_DRIFT pixels.
MAX_RESOLUTION_DRIFT = 0.01


# --- Public Functions ---

def build_vrt(raster_fpaths):
    """
    Return the XML of a virtual mosaic (VRT) of several rasters.

    Where rasters overlap, the first raster in `raster_fpaths` wins, and nodata
    cells never cover valid data of another raster. Each raster is placed at
    the nearest whole-pixel offset of the mosaic grid, which uses the exact
    resolution of the first raster. The mosaic has the data type of the first
    raster; GDAL converts the values of the other rasters to it when reading.

    Parameters
    ----------
    raster_fpaths : List[str or Path]
        Paths to single-band rasters with the same CRS, resolution and nodata
        value. Pixel sizes may differ by floating-point noise: by so little
        that the drift across the mosaic stays below MAX_RESOLUTION_DRIFT
        pixels. The VRT refers to the rasters by their absolute paths.

    Returns
    -------
    str
        The XML of the VRT.

    Raises
    ------
    ValueError
        If `raster_fpaths` is empty, if a raster has more than one band, is
        rotated or has an unsupported data type, or if the rasters differ in
        CRS, resolution or nodata value.
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
            width=src.width,
            height=src.height,
            left=transform.c,
            top=transform.f,
            xres=transform.a,
            yres=-transform.e,
            crs=src.crs,
            dtype=dtype,
            nodata=src.nodata,
            block_width=block_width,
            block_height=block_height,
        )


def _check_compatible(rasters):
    """
    Check that all rasters share the CRS, resolution and nodata value of the
    first one. (Their data types may differ, see `build_vrt`.)
    """
    first = rasters[0]
    # Extent of the mosaic in pixels of the first raster, along x and y
    num_pixels = dict(
        xres=(max(r['left'] + r['width'] * r['xres'] for r in rasters)
              - min(r['left'] for r in rasters)) / first['xres'],
        yres=(max(r['top'] for r in rasters)
              - min(r['top'] - r['height'] * r['yres'] for r in rasters)) / first['yres'],
    )
    for raster in rasters[1:]:
        name, first_name = raster['path'].name, first['path'].name
        if raster['crs'] != first['crs']:
            raise ValueError(f"{name} has a different CRS than {first_name}.")
        for key in ('xres', 'yres'):
            drift = abs(raster[key] - first[key]) / first[key] * num_pixels[key]
            if drift > MAX_RESOLUTION_DRIFT:
                raise ValueError(
                    f"{name} has a different resolution than {first_name} "
                    f"({raster[key]!r} vs {first[key]!r})."
                )
        if not _same_nodata(raster['nodata'], first['nodata']):
            raise ValueError(
                f"{name} has a different nodata value ({raster['nodata']}) than "
                f"{first_name} ({first['nodata']})."
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
    Place the rasters on the mosaic grid and return the XML of the VRT.
    """
    first = rasters[0]
    xres, yres = first['xres'], first['yres']
    left = min(r['left'] for r in rasters)
    top = max(r['top'] for r in rasters)

    # Whole-pixel offsets on the grid of the first raster; the grid is sized to
    # hold every raster entirely, so that no raster is cut at the edge
    offsets = [
        (round((r['left'] - left) / xres), round((top - r['top']) / yres))
        for r in rasters
    ]
    width = max(xoff + r['width'] for r, (xoff, _) in zip(rasters, offsets))
    height = max(yoff + r['height'] for r, (_, yoff) in zip(rasters, offsets))

    data_type = GDAL_DATA_TYPES[first['dtype']]
    nodata = first['nodata']

    root = ET.Element("VRTDataset", rasterXSize=str(width), rasterYSize=str(height))
    ET.SubElement(root, "SRS").text = first['crs'].to_wkt()
    ET.SubElement(root, "GeoTransform").text = ", ".join(
        repr(float(value)) for value in (left, xres, 0.0, top, 0.0, -yres)
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
