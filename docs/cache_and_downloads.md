# Cache and downloads

**WorldPopPy** includes helper functions to manage the local cache and download bandwidth.

## Managing the cache

Downloaded rasters are cached locally by default. You can change the location by setting the `WORLDPOPPY_CACHE_DIR` 
environment variable.

```python
from worldpoppy import purge_cache, get_cache_dir

# Print the cache directory
print(get_cache_dir())

# Check the size of the local cache (deletes nothing; see 'total_size_mb')
print(purge_cache(dry_run=True))

# Delete all downloaded rasters (keeps the cached country borders)
purge_cache(dry_run=False)

# Delete everything, incl. the country borders (rebuilt automatically when needed)
purge_cache(dry_run=False, keep_country_borders=False)
```

## Download dry run

To estimate the size of a request before downloading, use the `download_dry_run` flag:

```python
from worldpoppy import wp_raster

# Prints a summary of files to be downloaded without fetching them
wp_raster(
    product_name='pop_g1', 
    aoi=['CAN', 'USA'], 
    years='all', 
    download_dry_run=True
)
```

The dry run accounts for files that are already in the cache.
