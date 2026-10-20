"""
This module provides logic to download WorldPop data asynchronously, with
support for automatic retry, file caching, and a preview of required download
sizes (dry run).

Note
----
    The implementation of this module draws on the "download.py" module from the
    `blackmarblepy <https://github.com/worldbank/blackmarblepy>`__ package by
    Gabriel Stefanini Vicente and Robert Marty. `blackmarblepy` is licensed
    under the Mozilla Public License (MPL-2.0), as is `WorldPopPy`.


Main classes
------------------------
    - :class:`WorldPopDownloader`
        Asynchronous downloader for WorldPop raster data.

Main methods
------------------------
    - :func:`purge_cache`
        Delete all files in the WorldPop local cache directory, with optional dry-run.

"""
import logging
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Optional, Any

import backoff
import httpx
import nest_asyncio
import pandas as pd
from pqdm.threads import pqdm
from tqdm.auto import tqdm

from worldpoppy.config import (
    DATA_DOWNLOAD_TIMEOUT,
    get_cache_dir,
    get_max_concurrency,
)
from worldpoppy.func_utils import is_fatal_http_error, log_info_context
from worldpoppy.manifest_loader import wp_manifest_constrained

__all__ = [
    "DownloadSizeCheckError",
    "DownloadError",
    "WorldPopDownloader",
    "purge_cache"
]

logger = logging.getLogger(__name__)

# Shared options for all requests to WorldPop's data server. Keeping them
# in one place ensures that downloads and dry-run size checks behave alike.
_DATA_REQUEST_KWARGS = dict(
    follow_redirects=True,
    timeout=DATA_DOWNLOAD_TIMEOUT,
)

# Partial downloads ('*.download') untouched for this long are treated as
# leftovers from crashed runs and deleted by `_repair_cache`. A running download
# updates its file with every chunk it writes (4 MB by default), and it aborts
# if the server sends nothing for DATA_DOWNLOAD_TIMEOUT seconds. So a download
# still in progress only looks stale below roughly 1 KB/s, which leaves a wide
# margin for other processes that are still downloading.
_STALE_DOWNLOAD_AGE_SECONDS = 60 * 60


class DownloadError(Exception):
    """Raised when one or more files fail to download."""

    pass


class DownloadSizeCheckError(DownloadError):
    """Raised when one or more HEAD requests fail during dry-run size checking."""

    pass


@dataclass
class DownloadResult:
    """
    Represents the outcome of a download-related operation, such as downloading
    a WorldPop file or querying the file's size with a HEAD request.

    Attributes
    ----------
    success : bool
        Indicates whether the operation completed successfully.
    value : Any or None
        Optional payload returned by the operation. None if not applicable.
    error : Exception or None
        An exception raised during the operation. None if not applicable.
    """

    success: bool
    value: Optional[Any] = None
    error: Optional[Exception] = None


@dataclass
class WorldPopDownloader:
    """
    An HTTP downloader to retrieve country-specific raster data from the
    `WorldPop <https://hub.worldpop.org/>`__ project.

    Attributes
    ----------
    directory: Path
        Local directory to which to download the data.
    """

    URL: ClassVar[str] = "https://data.worldpop.org"

    def __init__(self, directory=None):
        """
        Create a new WorldPopDownloader instance.

        Parameters
        ----------
        directory: str or Path, optional
            Local directory to which to download WorldPop rasters. If None is
            provided (default), rasters are downloaded into the local cache
            directory (see `get_cache_dir`).
        """
        nest_asyncio.apply()

        self.directory = Path(directory) if directory is not None else get_cache_dir()

    def download(
        self,
        product_name,
        iso3_codes,
        years=None,
        skip_download_if_exists=True,
        dry_run=False,
        chunk_size=1024*1024*4,
    ):
        """
        Asynchronously download a collection of country-specific WorldPop rasters.

        Parameters
        ----------
        product_name : str
            The name of the WorldPop data product of interest.
        iso3_codes : str or List[str]
            One or more three-letter ISO codes, denoting the countries of interest.
        years : int or List[int] or str, optional
            For annual data products, the year (or years) of interest, or the 'all'
            keyword (str) indicating that all available years for the requested data
            product should be downloaded. For static data products, this argument
            must be None (default).
        skip_download_if_exists : bool, optional, default=True
            Whether to skip downloading raster files that already exist locally.
        dry_run : bool, optional, default=False
            If True, only check how many files would need to be downloaded if `dry_run`
            was False. Report the number and size of required file downloads, but do not
            actually fetch or return any data.
        chunk_size : int, optional, default=4MB
            The size (in bytes) of chunks to read/write during download. Larger chunks
            may improve performance, especially on systems with real-time file scanning
            (e.g., antivirus).

        Returns
        -------
        list of pathlib.Path
            A lexically sorted list of local download paths.

        Raises
        ------
        RuntimeError
            If not all requested files were successfully downloaded
        """

        # --- Prepare ---
        # Delete artefacts from previously interrupted downloads
        _repair_cache()

        # Fetch download manifest (will validate user query)
        filtered_mdf = wp_manifest_constrained(product_name, iso3_codes, years)

        # Assemble URLs and local paths
        data = filtered_mdf[['product_name', 'iso3', 'year']].values
        local_paths = [self._build_local_fpath(*tup) for tup in data]
        remote_paths = filtered_mdf['remote_path'].tolist()

        # --- Filter Phase: Identify Actual Work ---
        # We separate the requested files into those we actually need to
        # download (network tasks) and those we can "take" from the cache.
        tasks = []

        for remote, local in zip(remote_paths, local_paths):
            if skip_download_if_exists and local.is_file():
                continue
            # If we are here, we either do not have the file, or we are
            # forcing a re-download.
            tasks.append((remote, local))

        num_cached = len(local_paths) - len(tasks)

        # --- Execution Phase ---
        if dry_run:
            # Case A: Dry Run
            if not tasks:
                print(f"Dry run: All {len(local_paths)} files already exist locally.")
                return local_paths, filtered_mdf

            with log_info_context(logger):
                print("Dry run: calculating number and size of files to download...\n")
                if num_cached > 0:
                    print(f"(Skipping {num_cached} files already in cache)")

                # Prepare arguments for parallel processing
                # We only process the files in the 'tasks' list
                args = [(r, l, skip_download_if_exists) for r, l in tasks]

                res = pqdm(
                    args,
                    self._get_required_file_download_size,  # noqa
                    n_jobs=get_max_concurrency(),
                    argument_type="args",
                    desc="Checking download sizes...",
                    leave=False,
                )

                if errors := [r.error for r in res if not r.success]:
                    formatted = '\n'.join(f"- {e}" for e in errors)
                    raise DownloadSizeCheckError(
                        f"{len(errors)} HEAD request(s) failed. Details:\n{formatted}"
                    )

                # Sum only the tasks we actually checked
                total_size = sum(r.value for r in res if r.success and r.value > 0)

                print(f"No. of files to download: {len(tasks)}")
                print(f"Total est. download size: {round(total_size / 1e6, 2):,} MB")

        else:
            # Case A: Real Download Mode
            if not tasks:
                # No network tasks -> No progress bar (just a clean log message).
                logger.info(f"All {len(local_paths)} requested files found in cache.")
                return local_paths, filtered_mdf

            if num_cached > 0:
                logger.info(
                    f"Found {num_cached} files in cache; downloading {len(tasks)} new files."
                )

            # Prepare arguments for parallel processing
            args = [(r, l, skip_download_if_exists, chunk_size) for r, l in tasks]

            res = pqdm(
                args,
                self._download_file,  # noqa
                n_jobs=get_max_concurrency(),
                argument_type="args",
                desc="Downloading...",
                leave=False,
            )

            if errors := [r.error for r in res if not r.success]:
                formatted = '\n'.join(f"- {e}" for e in errors)
                raise DownloadError(
                    f"{len(errors)} download(s) failed. Details:\n{formatted}"
                )

        return local_paths, filtered_mdf

    def _download_file(
        self,
        remote_url,
        local_path,
        skip_if_exists=True,
        chunk_size=1024*1024*4
    ):
        """
        Download a WorldPop raster with automatic retries.

        Retries happen inside `_stream_to_file`. This wrapper only converts
        the final outcome into a `DownloadResult`, so that one failed file
        does not abort the parallel download of all other files.

        Parameters
        ----------
        remote_url : str
            The URL to the WorldPop raster file to be downloaded.
        local_path : Path
            The local file path where the raster will be saved.
        skip_if_exists : bool, optional, default=True
            Whether to skip the download if the file already exists locally.
        chunk_size : int, optional, default=4MB
            The size (in bytes) of chunks to read/write during download.
            Larger chunks may improve performance, especially on systems
            with real-time file scanning (e.g., antivirus).

        Returns
        -------
        DownloadResult
        """
        if local_path.is_file() and skip_if_exists:
            # nothing to do
            return DownloadResult(success=True)

        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download the raster to a uniquely named temporary file in the same
        # directory, so that concurrent downloads of the same raster (e.g., by
        # parallel jobs) never share, truncate or delete each other's files
        tmp_path = _create_unique_temp_file(local_path)

        try:
            self._stream_to_file(remote_url, tmp_path, chunk_size)

            # Only after the download has finished do we move the temporary
            # file to its proper name. In this way, crashing downloads will
            # not corrupt the local cache. (`replace` is atomic and overwrites
            # a file that a concurrent download of the same raster finished
            # first; both files are complete and identical.)
            tmp_path.replace(local_path)
            return DownloadResult(success=True)

        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(success=False, error=e)

    @backoff.on_exception(
        backoff.expo,
        httpx.HTTPError,  # catch EVERYTHING (Network, Timeout, Status codes)
        max_tries=5,
        jitter=backoff.full_jitter,
        giveup=is_fatal_http_error,  # stop retrying if it's a 404, loop, etc.
    )
    def _stream_to_file(self, remote_url, tmp_path, chunk_size):
        """
        Stream a remote file to `tmp_path`, retrying on network errors and
        retryable HTTP status codes.

        Exceptions must propagate out of this method, since the `backoff`
        decorator can only retry calls that raise. Each attempt re-opens
        (and thus truncates) `tmp_path`, so no partial data is carried over.
        """
        remote_fname = remote_url.split("/")[-1]

        with open(tmp_path, "wb+") as f:
            with httpx.stream("GET", remote_url, **_DATA_REQUEST_KWARGS) as response:
                response.raise_for_status()

                total = int(response.headers.get("Content-Length", 0))
                pbar = tqdm(total=total, unit="B", unit_scale=True, leave=False)
                with pbar:
                    pbar.set_description(f"Downloading {remote_fname}...")
                    for chunk in response.iter_raw(chunk_size=chunk_size):
                        f.write(chunk)
                        pbar.update(len(chunk))

    def _get_required_file_download_size(
            self,
            remote_url,
            local_path,
            skip_download_if_exists=True,
    ):
        """
        Get the required download size for one file in bytes using a HEAD request.

        Returns a size of 0 if the remote file does not need to be downloaded at
        all since a local version exists.

        Parameters
        ----------
        remote_url : str
            The URL to the WorldPop raster file.
        local_path : Path
            The local file path where a cached version of the file may exist.
        skip_download_if_exists : bool, optional, default=True
            Whether to skip downloading files that already exist locally.

        Returns
        -------
        DownloadResult
            The size of the required file download in bytes (int) if the HEAD request
            did not raise a httpx.HTTPStatusError. Otherwise, the error message (str).

        """
        if local_path.exists() and skip_download_if_exists:
            return DownloadResult(success=True, value=0)

        try:
            size = self._fetch_file_size(remote_url)
        except Exception as e:
            return DownloadResult(success=False, error=e)
        else:
            return DownloadResult(success=True, value=size)

    @backoff.on_exception(
        backoff.expo,
        httpx.HTTPError,  # catch EVERYTHING (Network, Timeout, Status codes)
        max_tries=5,
        jitter=backoff.full_jitter,
        giveup=is_fatal_http_error,  # stop retrying if it's a 404, loop, etc.
    )
    def _fetch_file_size(self, remote_url):
        """
        Return the size of a remote file in bytes using a HEAD request,
        retrying on network errors and retryable HTTP status codes.
        """
        response = httpx.head(remote_url, **_DATA_REQUEST_KWARGS)
        response.raise_for_status()
        return int(response.headers.get("Content-Length", 0))

    def _build_local_fpath(self, product_name, iso3, year=None):
        """Return the local file path used to store a single downloaded WorldPop raster"""

        if pd.isnull(year):  # catches both None and np.NaN
            fname = f'{product_name}_{iso3}.tif'
        else:
            fname = f'{product_name}_{iso3}_{int(year)}.tif'

        return self.directory / fname


def purge_cache(dry_run=True, keep_country_borders=True):
    """
    Purge the local cache directory and any of its subdirectories.

    By default, this deletes the downloaded WorldPop rasters but keeps the
    (buffered) country borders, which are derived from an asset shipped with
    `worldpoppy` rather than downloaded.

    Parameters
    ----------
    dry_run : bool, optional
        If True (default), do not delete any files and simply report what would be
        deleted without the `dry_run` flag.
    keep_country_borders : bool, optional, default=True
        If True (default), do not delete any cached data related to country borders.
        This data is assumed to be the only one which includes the 'level0' keyword
        in a file name. If False, the country borders are deleted as well; they are
        rebuilt automatically (offline, in a few seconds) the next time they are needed.

    Returns
    -------
    dict
        Summary of how many files and what total size (in MB) would be or were deleted.
    """
    cache_dir = get_cache_dir()
    fpaths = list(cache_dir.glob('**/*'))

    total_size = num_matched = num_deleted = 0
    for path in fpaths:
        if not path.is_file():
            # Currently, we have no sub-dirs in the cache dir, but this
            # can change in future. For directories, .unlink() would raise
            # an error.
            continue

        if keep_country_borders and 'level0' in path.name:
            continue

        num_matched += 1
        total_size += path.stat().st_size

        if not dry_run:
            try:
                path.unlink()
                num_deleted += 1
            except Exception as e:
                print(f"Failed to delete cached file at {path}: {e}")

    return {
        "dry_run": dry_run,
        "matched_files": num_matched,
        "deleted_files": num_deleted,
        "total_size_mb": round(total_size / 1e6, 2),
    }


def _create_unique_temp_file(local_path):
    """
    Create an empty, uniquely named temporary file next to `local_path`
    (e.g., 'x.tif.3f9a1c2e.download') and return its path.

    Unlike `tempfile.mkstemp`, which creates files readable only by their
    owner, this file gets the usual permissions (subject to the umask). The
    cached raster keeps them after the final move, so that it stays readable
    in shared caches (e.g., a group cache on a cluster).
    """
    while True:
        tmp_path = local_path.with_name(f"{local_path.name}.{secrets.token_hex(4)}.download")
        try:
            open(tmp_path, "xb").close()  # exclusive create: fails if the name is taken
            return tmp_path
        except FileExistsError:
            continue


def _repair_cache():
    """
    Delete stale partial downloads (files ending on '.download') in the local
    cache directory and any of its subdirectories.

    Only files that have not been modified for `_STALE_DOWNLOAD_AGE_SECONDS`
    are deleted. Recent ones may belong to downloads that another process is
    still running.
    """
    cache_dir = get_cache_dir()
    stale_before = time.time() - _STALE_DOWNLOAD_AGE_SECONDS

    for path in cache_dir.glob('**/*.download'):
        try:
            if path.stat().st_mtime < stale_before:
                path.unlink()
        except FileNotFoundError:
            pass  # already finished or removed by its own process
        except Exception as e:
            logger.warning(f"Failed to delete stale partial download at {path}: {e}")
