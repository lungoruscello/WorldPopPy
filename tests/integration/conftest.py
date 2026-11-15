"""
Fixtures shared by the integration tests.
"""

import pytest


# --- Fixtures ---

@pytest.fixture(autouse=True)
def fresh_mosaic_files(monkeypatch, tmp_path):
    """
    Fixture that writes the VRT files of lazy merges (see `worldpoppy.mosaic`)
    to a temporary directory of each test, instead of the cache directory.

    Integration tests read raster data from the user's cache, but must always
    build their mosaics with the current code: a VRT file cached by an earlier
    run (or by another version of the code) would otherwise be reused, and a
    change to `worldpoppy.mosaic` would go untested. The fixture also keeps the
    tests from leaving VRT files in the user's cache.
    """
    import worldpoppy.raster as raster

    cached_path = raster._mosaic_vrt_path
    mosaic_dir = tmp_path / "mosaics"
    monkeypatch.setattr(
        raster, "_mosaic_vrt_path", lambda raster_fpaths: mosaic_dir / cached_path(raster_fpaths).name
    )
