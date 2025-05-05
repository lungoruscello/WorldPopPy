"""
Functions to download WorldPop data asynchronously, with logic for automatic
retry and file caching.

Note:
    This module is a port of the "download.py" module from the `blackmarblepy`
    package by Gabriel Stefanini Vicente and Robert Marty. `blackmarblepy` is
    licensed under the Mozilla Public License (MPL-2.0), as is the present
    modified version.

    Links:
    - https://github.com/worldbank/blackmarblepy
    - https://github.com/worldbank/blackmarblepy/blob/main/LICENSE
"""

from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, List

import backoff
import httpx
import nest_asyncio
import pandas as pd
from httpx import HTTPError
from pqdm.threads import pqdm
from tqdm.auto import tqdm

from worldpoppy.config import *
from worldpoppy.manifest import filter_global_manifest

__all__ = ["WorldPopDownloader", "repair_cache", "purge_cache"]


@dataclass
class WorldPopDownloader:
    """
    An HTTP downloader to retrieve country-specific raster data from the
    `WorldPop <https://hub.worldpop.org/>`_ project.

    Attributes
    ----------
    directory: Path
        Local directory to which to download the data.

    Methods
    -------
    download(product_name, iso3_codes, years=None, skip_download_if_exists=True)
        Asynchronously download a collection of country-specific WorldPop rasters.
    """

    URL: ClassVar[str] = "https://data.worldpop.org"

    def __init__(self, directory=None):
        """
        Parameters
        ----------
        directory: Path, optional
            Local directory to which to download WorldPop rasters. If None is
            provided (default), rasters are downloaded into the central cache
            directory (see `get_cache_dir`).
        """
        nest_asyncio.apply()

        self.directory = Path(directory) if directory is not None else get_cache_dir()

    def download(
            self,
            product_name,
            iso3_codes,
            years=None,
            skip_download_if_exists=True
    ):
        """
        Asynchronously download a collection of country-specific WorldPop rasters.

        Parameters
        ----------
        product_name : str
            The name of the WorldPop product of interest.
        iso3_codes : str or List[str]
            The three-letter ISO code (or ISO codes) of the country (or countries) of interest.
        years : int or List[int], optional
            For annual data products, the year (or years) of interest. For static data products,
            this argument must be None (default).
        skip_download_if_exists : bool, optional, default=True
            Whether to skip downloading raster files that already exist locally.


        Returns
        -------
        list of pathlib.Path
            A lexically sorted list of local download paths.
        """

        # delete artefacts from previously interrupted downloads
        repair_cache()

        # fetch the manifest (will validate the user query)
        mdf = filter_global_manifest(product_name, iso3_codes, years)

        # assemble URLs and local paths
        data = mdf[['product_name', 'iso3', 'year']].values
        local_paths = [self.directory / build_local_fname(*tup) for tup in data]
        remote_paths = mdf['remote_path'].tolist()

        # prepare arguments for parallel download
        args = [(remote, local, skip_download_if_exists) for remote, local in zip(remote_paths, local_paths)]

        res = pqdm(
            args,
            self._download_file,
            n_jobs=get_max_concurrency(),
            argument_type="args",
            desc="Downloading...",
            leave=False
        )
        assert len(res) == len(local_paths)

        return sorted(local_paths)

    @backoff.on_exception(backoff.expo, HTTPError)
    def _download_file(
            self,
            remote_path,
            local_path,
            skip_if_exists=True
    ):
        """
        Download a WorldPop raster with automatic retries.

        Parameters
        ----------
        remote_path : str
            The remote path to the WorldPop raster file to be downloaded.
        local_path : Path
            The local file path where the raster will be saved.
        skip_if_exists : bool, optional, default=True
            Whether to skip the download if the file already exists locally.

        Returns
        -------
        None
            This method does not return any value. If the file already exists
            and `skip_if_exists` is True, no action is taken.
        """
        if local_path.is_file() and skip_if_exists:
            # nothing to do
            return None

        remote_url = f"{self.URL}/{remote_path}"
        remote_fname = remote_path.split("/")[-1]
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # download the raster to a temporary path in the same directory
        tmp_path = local_path.with_suffix(local_path.suffix + ".download")

        with open(tmp_path, "wb+") as f:
            with httpx.stream("GET", remote_url) as response:
                total = int(response.headers["Content-Length"])
                with tqdm(
                        total=total,
                        unit="B",
                        unit_scale=True,
                        leave=False
                ) as pbar:
                    pbar.set_description(f"Downloading {remote_fname}...")
                    for chunk in response.iter_raw():
                        f.write(chunk)
                        pbar.update(len(chunk))

        # only after the download has finished do we rename the temporary file to
        # its proper name. In this way, interrupting downloads will not corrupt the
        # local cache.
        tmp_path.rename(local_path)

    @staticmethod
    def _build_local_fname(product_name, iso3, year=None):
        """Return the file name used to store a single downloaded Worldpop raster"""

        if pd.isnull(year):  # catches both None and np.NaN
            fname = f'{product_name}_{iso3}.tif'
        else:
            fname = f'{product_name}_{iso3}_{int(year)}.tif'

        return fname


def repair_cache():
    """
    Delete all files ending on '.download' in the local cache directory and any of its subdirectories.
    """
    cache_dir = get_cache_dir()
    fpaths = list(cache_dir.glob('**/*.download'))

    for path in fpaths:
        try:
            path.unlink()
        except Exception as e:
            print(f"Failed to delete cached file at {path}: {e}")


def purge_cache(dry_run=True):
    """
    Delete all .tif files in the local cache directory and any of its subdirectories.

    Parameters
    ----------
    dry_run : bool, optional
        If True (default), do not delete any files and simply report what would be
        deleted without the `dry_run` flag.

    Returns
    -------
    dict
        Summary of how many files and total size (bytes) would be or were deleted.
    """
    cache_dir = get_cache_dir()
    fpaths = list(cache_dir.glob('**/*.tif'))

    total_size = 0
    actual_deleted = 0
    for path in fpaths:
        total_size += path.stat().st_size

        if not dry_run:
            try:
                path.unlink()
                actual_deleted += 1
            except Exception as e:
                print(f"Failed to delete cached file at {path}: {e}")

    return {
        "dry_run": dry_run,
        "matched_files": len(fpaths),
        "deleted_files": actual_deleted if not dry_run else 0,
        "total_size_mb": round(total_size / 1e6, 2)
    }
