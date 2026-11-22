"""
Core "engine" for building a raw data manifest for the `worldpoppy` library.

This module contains all the logic for traversing the WorldPop metadata API,
parsing the results, and saving them to a new local cache file.

A build runs in three phases:

    1. Discover the "Leaf Nodes" (data series) of the API hierarchy.
    2. Process each Leaf Node ("Sample -> Analyse -> Parse"), which yields one
       manifest row per raster file.
    3. Check the inferred download URLs of the Global 1 series against
       WorldPop's Global 1 File List.

Main methods
------------------------

    - :func:`build_raw_manifest_from_api`
        Query WorldPop's meta-data API and analyse the results to build a new,
        raw manifest of raster datasets for `worldpoppy`. Note: Users will rarely
        need to import this function directly. Instead, it is called by the separate
        `manifest_loader` module whenever a cached version of the raw manifest does
        not exist.

For a detailed, high-level explanation of this module's API traversal strategy,
related terminology (e.g. "Leaf Node", "Sample Payload"), and data-parsing
logic, please see the `manifest_build_strategy.md` document in the `docs/` folder.

----

**A note on the complexity of this module:**

The module's size is a result of two challenges:

1.  To minimise API calls, this module implements a "sample and infer"
    strategy, in which a general template for download URLs of a raster-data
    series is inferred from only a single example per series. Inferred URLs
    of the Global 1 series are then checked against WorldPop's Global 1 File
    List, and corrected where the list gives a different path.

2.  We must parse and "flatten" three different data organisation schemes
    used by the WorldPop project: 'flat' (1-to-1), 'multi-year' (1-to-N by year),
    and 'multi-band' (1-to-N by class).

When processing operations fail, an entire raster-data series is dropped from the
raw manifest and will hence not be supported by the `worldpoppy` package.

"""

import io
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import floor
from pathlib import Path

import backoff
import httpx
import pyarrow as pa
import pycountry
import pyarrow.feather
from tqdm.autonotebook import tqdm

from worldpoppy import __version__
from worldpoppy.config import (
    DEBUG,
    DATA_DOWNLOAD_TIMEOUT,
    DATA_SERVER_URL,
    GLOBAL1_FILE_LIST_URL,
    METADATA_API_URL,
    METADATA_API_TIMEOUT,
    BUNDLED_MANIFEST_PATH,
    BUNDLED_MANIFEST_TIMESTAMP_PATH,
    DOWNLOADABLE_ISO3_CODES,
    get_user_manifest_path,
)
from worldpoppy.func_utils import is_fatal_http_error
from worldpoppy.manifest_utils import *
from worldpoppy.tracking import api_query_log

logger = logging.getLogger(__name__)

# API paths that the crawler does not request, with the reason (keys in lower
# case). A skipped path does not count as a failure of the build.
SKIPPED_API_PATHS = {
    "age_structures": "not supported (a limitation in `_extract_unique_bands`)",
    "covariates/merit_elevation_200": (
        "not reachable: WorldPop's API answers HTTP 500 for every spelling (the listed "
        "alias ' MERIT_Elevation_200' has a leading space and a truncated name, 'MERIT "
        "Elevation 2007'; checked on 2026-11-17 and 2026-11-22). Re-check at the next rebuild."
    ),
}

# Data series whose recommended citation differs by country (keys in lower case),
# with the reason. The citation and DOI of the sample dataset are not stored for
# them: results then list only the official dataset titles and their summary
# pages, which give each dataset's citation.
COUNTRY_SPECIFIC_CITATION_SERIES = {
    "pop/cic2020_100m": "WorldPop cites different datasets for the 'maxar_v1' and 'BSGM' countries",
    "pop/cic2020_unadj_100m": "WorldPop cites different datasets for the 'maxar_v1' and 'BSGM' countries",
}

# Columns of the raw manifest, i.e. the keys of each record created by
# `_build_dataset_record`. The manifest loader requires all of them.
RAW_MANIFEST_COLUMNS = (
    "wpy_id",
    "iso3",
    "dataset_name",
    "remote_path",
    "api_entry_title",
    "api_path",
    "year",
    "band",
    "remote_name",
    "api_project",
    "api_series_desc",
    "api_series_category",
    "api_source",
    "license",
    "citation",
    "doi",
    "summary_url",
)

# Series-level texts that repeat in every row of a data series. Written
# dictionary-encoded (each distinct text once), so that e.g. the citations add
# about 0 MB to the manifest file instead of about 1 MB (measured on 2026-11-17).
DICTIONARY_ENCODED_COLUMNS = ("license", "citation", "doi")

# The URL Check (Phase 3 of a build) adds this column to the raw manifest, with
# one of the values below per row (see `_check_urls_against_global1_file_list`).
# The manifest loader drops it: it is not part of the user-facing manifest.
URL_CHECK_COLUMN = "url_check"
URL_CONFIRMED = "confirmed"
URL_CORRECTED = "corrected"
URL_UNCONFIRMED = "unconfirmed"
URL_NOT_CHECKED = "not_checked"

# Series folders of the Global 1 series on the data server start with this
# (e.g. 'Global_2000_2020', 'Global_2000_2020_Constrained')
GLOBAL1_FOLDER_PREFIX = "Global_2000_2020"


class APIRequestError(Exception):
    """Raised when an API request fails permanently or after all retries."""

    pass


@dataclass
class ManifestBuildReport:
    """
    Summary of one manifest build, as returned by `build_raw_manifest_from_api`.

    Manifest builds are "robust": API branches or data series that fail are
    logged and skipped, and the manifest is saved with everything else. This
    report makes such gaps visible, so that callers (e.g. release scripts)
    can refuse to ship an incomplete manifest.

    Attributes
    ----------
    num_series : int
        Number of supported data series (API "Leaf Nodes") found by the crawl.
    num_files : int
        Number of raster files in the new manifest.
    failed_branches : list of str
        API paths of branches that could not be crawled. All data series
        below them are missing from the manifest.
    failed_series : list of str
        API paths of data series that could not be processed.
    saved_to : pathlib.Path or None
        Where the manifest was saved, or None if nothing was saved.
    fatal_error : str or None
        Why the build was aborted, if it was.
    kept_existing : bool
        True if the build was incomplete and therefore not saved, so that an
        existing manifest stays in use (instead of being shadowed by a newer
        but incomplete one).
    url_check_counts : dict
        Results of the URL Check per data series: API path -> {result: number
        of raster files} (results as in `_check_urls_against_global1_file_list`).
    url_check_error : str or None
        Why the URL Check could not be run (WorldPop's Global 1 File List could
        not be downloaded or read), if it could not.
    """

    num_series: int = 0
    num_files: int = 0
    failed_branches: list[str] = field(default_factory=list)
    failed_series: list[str] = field(default_factory=list)
    saved_to: Path | None = None
    fatal_error: str | None = None
    kept_existing: bool = False
    url_check_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    url_check_error: str | None = None

    # The summary lists at most this many paths per kind of failure
    # (all paths remain available in `failed_branches` / `failed_series`)
    MAX_PATHS_IN_SUMMARY = 20

    @property
    def has_failures(self):
        """True if an API branch, a data series or the URL Check failed."""
        return bool(self.failed_branches or self.failed_series or self.url_check_error)

    @property
    def is_complete(self):
        """True if the manifest was saved and nothing failed along the way."""
        return self.saved_to is not None and not self.has_failures

    def summary(self):
        """Return a human-readable summary of the build."""
        if self.saved_to is None and not self.kept_existing:
            reason = self.fatal_error or "unknown error"
            return f"Manifest build aborted ({reason}). No manifest was saved."

        num_ok_series = self.num_series - len(self.failed_series)
        files = "raster file" if self.num_files == 1 else "raster files"
        where = "NOT saved" if self.kept_existing else f"saved to {self.saved_to}"
        lines = [
            f"Manifest build finished: {self.num_files:,} {files} from "
            f"{num_ok_series} of {self.num_series} data series, {where}."
        ]
        lines += self._url_check_lines()
        if self.is_complete:
            lines.append("No failures.")
            return "\n".join(lines)

        if self.failed_branches:
            num = len(self.failed_branches)
            branches = "API branch" if num == 1 else "API branches"
            lines.append(
                f"{num} {branches} could not be crawled "
                "(all data series below them are missing):"
            )
            lines += self._format_paths(self.failed_branches)
        if self.failed_series:
            lines.append(f"{len(self.failed_series)} data series could not be processed:")
            lines += self._format_paths(self.failed_series)
        if self.kept_existing:
            lines.append(
                "Because the new manifest is INCOMPLETE, it was not saved: worldpoppy keeps using "
                "the existing one. Such failures are often temporary; consider trying again later."
            )
        else:
            lines.append("The saved manifest is likely INCOMPLETE. See the log above for details.")
        return "\n".join(lines)

    def _url_check_lines(self):
        """
        Return the summary lines on the URL Check: the total per result, and
        the data series with corrected or unconfirmed URLs.
        """
        if self.url_check_error:
            return [
                "URL check failed (WorldPop's Global 1 File List could not be downloaded "
                f"or read: {self.url_check_error}). No download URL was checked."
            ]
        if not self.url_check_counts:
            return []

        labels = {
            URL_CONFIRMED: "confirmed",
            URL_CORRECTED: "corrected",
            URL_UNCONFIRMED: "unconfirmed",
            URL_NOT_CHECKED: "not checked",
        }
        totals = {
            result: sum(counts.get(result, 0) for counts in self.url_check_counts.values())
            for result in labels
        }
        lines = [
            "URL check against WorldPop's Global 1 File List: "
            + ", ".join(f"{totals[result]:,} {label}" for result, label in labels.items())
            + "."
        ]
        noteworthy = []
        for api_path, counts in self.url_check_counts.items():
            parts = [f"{counts[r]:,} {labels[r]}" for r in (URL_CORRECTED, URL_UNCONFIRMED) if counts.get(r)]
            if parts:
                noteworthy.append(f"{api_path}: {', '.join(parts)}")
        if noteworthy:
            lines.append("Data series with corrected or unconfirmed URLs:")
            lines += self._format_paths(noteworthy)
        return lines

    def _format_paths(self, paths):
        """Return one summary line per path, capped at MAX_PATHS_IN_SUMMARY."""
        lines = [f"  - {path}" for path in paths[:self.MAX_PATHS_IN_SUMMARY]]
        if (num_hidden := len(paths) - self.MAX_PATHS_IN_SUMMARY) > 0:
            lines.append(f"  ... and {num_hidden} more")
        return lines


@dataclass
class ManifestInfo:
    """
    Where a raw manifest is stored, and when and by which `worldpoppy`
    version it was built (from metadata embedded in the file).

    Attributes
    ----------
    path : pathlib.Path
        Location of the manifest file.
    source : str
        Either 'bundled' (shipped with the package) or 'user' (updated by
        the user into their cache directory).
    built_at : datetime.datetime or None
        Build time, or None if unknown.
    version : str or None
        The `worldpoppy` version that built the manifest, or None if unknown.
    """

    path: Path
    source: str
    built_at: datetime | None = None
    version: str | None = None


def build_raw_manifest_from_api(force_rebuild=False):
    """
    Query WorldPop's meta-data API and analyse the results to build a new,
    raw manifest of raster datasets for `worldpoppy`. We call this manifest
    "raw" because it will be further checked and filtered (where needed)
    by the `manifest_loader` module.

    This is a SERIAL (single-threaded) implementation.

    Phase 1: Discover "Leaf Nodes" by recursively crawling the API hierarchy
    (using `_discover_leaf_nodes`).

    Phase 2: Processes each discovered "Leaf Node" (using `_process_leaf_node`)
    by applying a "Sample -> Analyse -> Parse" strategy that generates our
    final list of raw manifest rows. This phase is "robust", meaning a
    failure on one Leaf Node will be logged and skipped, allowing the
    raw manifest build to continue.

    Phase 3: Checks the inferred download URLs of the Global 1 series against
    WorldPop's Global 1 File List (using `_run_url_check`), and corrects URLs
    whose file is listed under a different path.

    **For expert users only.** A rebuild sends more than a hundred requests to
    WorldPop's meta-data API, and downloads WorldPop's Global 1 File List (about
    26 MB), so please rebuild sparingly. Most users never need to: `worldpoppy`
    ships with a manifest of all supported data.

    The new manifest is saved to the user's cache directory (see
    `worldpoppy.config.get_user_manifest_path`), never into the installed
    package. From then on, it is used instead of the bundled manifest as long
    as it is the newer of the two and was built by a compatible `worldpoppy`
    version. The manifest loaded in the current session is refreshed as well.
    If some API branches or data series fail during a rebuild, or the URL Check
    cannot be run, the incomplete result is not saved, and the existing manifest
    stays in use.

    Parameters
    ----------
    force_rebuild : bool, optional
        If True, forces a full re-crawl and re-processing of WorldPop's meta-data
        API, even if a usable manifest (bundled or user-updated) already exists.
        Default is False.

    Returns
    -------
    ManifestBuildReport or None
        A summary of the build (also logged at the end), including any API
        branches or data series that failed. Check `report.is_complete` before
        relying on the new manifest. None if no build was needed, because a
        usable manifest already exists (and `force_rebuild` is False).
    """
    existing = _select_manifest()
    if existing is not None and not force_rebuild:
        _warn_if_stale(existing)
        return None

    # An incomplete rebuild must not shadow an existing (complete) manifest.
    # Without any manifest, an incomplete one is still better than none.
    return _build_manifest(get_user_manifest_path(), save_incomplete=existing is None)


def _build_manifest(output_path, save_incomplete=True):
    """
    Crawl WorldPop's meta-data API, save the resulting raw manifest to
    `output_path` (with embedded build metadata), and return a
    `ManifestBuildReport`. See `build_raw_manifest_from_api` for details.

    If `save_incomplete` is False, a build with failed API branches or data
    series is not saved (the report's `kept_existing` is then True).
    """

    report = ManifestBuildReport()
    logger.warning(
        "Traversing WorldPop's meta-data API to index supported data series..."
    )

    try:
        # --- Phase 1 ---
        # Discover candidate data series in one call (serially)
        leaf_nodes = _discover_leaf_nodes(report)
        if not leaf_nodes:
            report.fatal_error = "no supported data series found"
            return _finish_build(report)
        report.num_series = len(leaf_nodes)

        # --- Phase 2 ---
        # Generate well-formatted listings of supported raster files for
        # all Leaf Nodes (serially).
        logger.info(f"Phase 2: Processing {len(leaf_nodes)} API Leaf Nodes (serially)...")

        # This will be a list of lists
        raw_manifest_rows_nested = []

        # NOTE
        # The `try...except Exception` block *inside* this loop is intentional.
        # It ensures that a failure on a single Leaf Node (e.g. an APIRequestError
        # from _get_sample_payload or a parsing error) is logged and skipped.
        # This allows the build to continue for all other nodes.
        #
        # The *outer* `try...except` blocks are for *fatal* errors
        # (like a failure in Phase 1) that must abort the entire build.

        for leaf_node in tqdm(leaf_nodes, desc="Processing API Leaf Nodes"):
            try:
                # We call _process_leaf_node inside its *own* try/except.
                result_list = _process_leaf_node(
                    leaf_node["api_path"],
                    leaf_node["node_name"],
                    leaf_node["coverage_index"],
                )
                raw_manifest_rows_nested.append(result_list)

            except Exception as e:
                # If one Leaf Node fails (e.g. unexpected JSON),
                # we log the *specific* failure but *continue* the loop.
                logger.error(
                    f"Failed to process Leaf Node: {leaf_node.get('api_path')}. "
                    f"Skipping this series. Error: {e}",
                    exc_info=True,
                )
                report.failed_series.append(leaf_node.get("api_path"))
                continue

    except APIRequestError as e:
        # This "outer" block will *only* catch critical failures,
        # such as a network error in `_discover_leaf_nodes`.
        report.fatal_error = f"critical API error: {e}"
        return _finish_build(report)
    except Exception as e:
        # This catches any other unexpected startup error
        logger.error(f"Manifest update failed with unexpected error: {e}", exc_info=True)
        report.fatal_error = f"unexpected error: {e}"
        return _finish_build(report)

    # --- Post-Processing ---
    # Flatten results
    raw_manifest_rows = [row for sublist in raw_manifest_rows_nested for row in sublist]

    if not raw_manifest_rows:
        report.fatal_error = "API traversal returned no data (check API status or logs)"
        return _finish_build(report)

    raw_mdf = pd.DataFrame(raw_manifest_rows)

    # --- Phase 3 ---
    # Check the inferred download URLs against WorldPop's Global 1 File List
    logger.info("Phase 3: Checking download URLs against WorldPop's Global 1 File List...")
    raw_mdf = _run_url_check(raw_mdf, report)

    # Save build results to disk.
    # (This will be our raw, uncleaned data manifest).
    if not save_incomplete and report.has_failures:
        report.num_files = len(raw_mdf)
        report.kept_existing = True
        return _finish_build(report)
    try:
        _write_manifest(raw_mdf, output_path)
        report.num_files = len(raw_mdf)
        report.saved_to = output_path

    except Exception as e:
        report.fatal_error = f"failed to save the manifest to {output_path}: {e}"

    else:
        # Make the running session use the new manifest from now on.
        # (Imported here, since `manifest_loader` imports this module.)
        from worldpoppy.manifest_loader import _clear_manifest_caches
        _clear_manifest_caches()

    return _finish_build(report)


def _finish_build(report):
    """
    Log the summary of a manifest build (as an error if the build was aborted,
    as a warning otherwise, so that it is visible by default) and return the
    report.
    """
    if report.saved_to is None:
        logger.error(report.summary())
    else:
        logger.warning(report.summary())
    return report


def _write_manifest(mdf, path, built_at=None, version=None):
    """
    Save a raw manifest as a Feather file, with its build time and the
    building `worldpoppy` version embedded in the file's schema metadata.

    The file is written to a temporary name first and then moved into place,
    so that readers never see a half-written manifest. (`built_at` and
    `version` default to now, in UTC, and the installed version; tests may set
    them.)
    """
    built_at = built_at or datetime.now(timezone.utc)
    version = version or __version__

    table = pa.Table.from_pandas(mdf, preserve_index=False)
    for name in DICTIONARY_ENCODED_COLUMNS:
        if (i := table.schema.get_field_index(name)) >= 0:
            table = table.set_column(i, name, table.column(name).dictionary_encode())
    metadata = {
        **(table.schema.metadata or {}),
        b"wpy_built_at": built_at.isoformat().encode(),
        b"wpy_version": version.encode(),
    }
    table = table.replace_schema_metadata(metadata)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{secrets.token_hex(4)}.tmp")
    try:
        pyarrow.feather.write_feather(table, tmp_path, compression="zstd")
        tmp_path.replace(path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _read_manifest_info(path, source):
    """
    Return a `ManifestInfo` for the manifest at `path`, or None if there is
    no readable manifest there (an unreadable file is reported in the log).
    Only the file header is read, not the data.
    """
    if not path.is_file():
        return None
    try:
        with pa.memory_map(str(path)) as source_file:
            metadata = pa.ipc.open_file(source_file).schema.metadata or {}
    except (pa.ArrowInvalid, OSError) as e:
        logger.warning(f"Ignoring the unreadable data manifest at {path}: {e}")
        return None

    built_at = metadata.get(b"wpy_built_at")
    version = metadata.get(b"wpy_version")
    try:
        built_at = _parse_build_time(built_at.decode()) if built_at else None
    except ValueError:
        built_at = None  # malformed: treated like an unknown build time
    return ManifestInfo(
        path=path,
        source=source,
        built_at=built_at,
        version=version.decode() if version else None,
    )


def _parse_build_time(text):
    """
    Parse a recorded build time into a time-zone-aware datetime. Manifests
    built before 0.5.0 recorded the local time of the building machine without
    a time zone; such times are read as local time (an approximation for
    manifests built elsewhere), so that all build times can be compared.
    """
    built_at = datetime.fromisoformat(text)
    return built_at if built_at.tzinfo is not None else built_at.astimezone()


def _major_minor(version):
    """Return the (major, minor) version numbers as ints, or None if unparsable."""
    try:
        major, minor = version.split(".")[:2]
        return int(major), int(minor)
    except (AttributeError, ValueError):
        return None


def _is_compatible_version(version):
    """
    Return True if a manifest built by `worldpoppy` `version` can be used by
    the installed version (same major and minor version, e.g. 0.4.x).
    """
    built_by = _major_minor(version)
    return built_by is not None and built_by == _major_minor(__version__)


def _select_manifest():
    """
    Return the `ManifestInfo` of the raw manifest to use, or None if there is
    none at all.

    The bundled manifest is always usable. A user-updated manifest (in the
    cache directory) is used instead if it is newer and was built by a
    compatible `worldpoppy` version; otherwise, it is ignored with a warning.
    """
    bundled = _read_manifest_info(BUNDLED_MANIFEST_PATH, source="bundled")
    if bundled is not None and bundled.built_at is None:
        # Legacy: bundled manifests without embedded metadata ship a sidecar
        bundled.built_at = _read_legacy_build_time(BUNDLED_MANIFEST_TIMESTAMP_PATH)

    user = _read_manifest_info(get_user_manifest_path(), source="user")
    if user is not None and not (user.built_at and _is_compatible_version(user.version)):
        built_by = _major_minor(user.version)
        if built_by is not None and built_by > _major_minor(__version__):
            reason = (
                f"it was built by a newer worldpoppy ({user.version}). Upgrade worldpoppy "
                "to use it"
            )
        elif user.built_at is None:
            reason = "its build time is unknown"
        else:
            version = f"worldpoppy {user.version}" if user.version else "an unknown worldpoppy version"
            reason = f"it was built by {version}, which is not compatible with worldpoppy {__version__}"
        logger.warning(
            f"Ignoring the rebuilt data manifest at {user.path}: {reason}. "
            "Using the bundled manifest instead."
        )
        user = None

    candidates = [info for info in (bundled, user) if info is not None]
    if not candidates:
        return None
    # Prefer the newer manifest (a manifest of unknown age counts as oldest)
    return max(candidates, key=lambda info: info.built_at or datetime.min.replace(tzinfo=timezone.utc))


def _read_legacy_build_time(sidecar_path):
    """Return the build time stored in a legacy sidecar file, or None."""
    try:
        return _parse_build_time(sidecar_path.read_text().strip())
    except (OSError, ValueError):
        return None


def _warn_if_stale(info, max_age_days=365):
    """Suggest an optional update if the manifest in use is older than `max_age_days`."""
    if info.built_at is None:
        return
    age_days = (datetime.now(timezone.utc) - info.built_at).days
    if age_days > max_age_days:
        logger.warning(
            f"worldpoppy's data manifest (its list of available WorldPop files) "
            f"was built {floor(age_days)} days ago. It may lack datasets that "
            "WorldPop has published since; the datasets it lists should still be "
            "available. (Expert users can rebuild the manifest from WorldPop's API; "
            "see `worldpoppy.manifest_builder.build_raw_manifest_from_api`. Please do "
            "so sparingly, as each rebuild sends more than a hundred requests to WorldPop.)"
        )


def _query_metadata_api(url):
    """
    Perform a single, robust GET request to the endpoint of WorldPop's
    metadata API.

    This function is the core HTTP utility for the entire crawler. Retries
    for intermittent network or server errors happen inside
    `_fetch_metadata_json`. This wrapper only converts the final outcome
    into an `APIRequestError`, which callers use to skip failed API nodes.

    Raises
    ------
    APIRequestError
        On a non-retryable error (e.g. 404 Not Found), after all retries
        have failed, or if the response is not valid JSON.
    """
    try:
        return _fetch_metadata_json(url)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP Error for {url}: {e.response.status_code}")
        raise APIRequestError(f"API request failed (HTTP error): {e}") from e
    except httpx.HTTPError as e:
        logger.error(f"Request Error for {url}: {e}")
        raise APIRequestError(f"API request failed (request error): {e}") from e
    except Exception as e:
        logger.error(f"Failed to fetch or parse JSON from {url}: {e}")
        raise APIRequestError(f"Failed to process {url}: {e}") from e


@backoff.on_exception(
    backoff.expo,
    httpx.HTTPError,  # catch all httpx errors
    max_tries=3,
    jitter=backoff.full_jitter,
    logger=logger,
    giveup=is_fatal_http_error,  # stop retrying if it's a 404, loop, etc.
    giveup_log_level=logging.WARNING,  # callers log the final failure as an error
)
def _fetch_metadata_json(url):
    """
    Return the parsed JSON response for a metadata API URL, retrying on
    network errors and retryable HTTP status codes.

    Exceptions must propagate out of this function, since the `backoff`
    decorator can only retry calls that raise.
    """
    with httpx.Client(timeout=METADATA_API_TIMEOUT, follow_redirects=True) as client:
        response = client.get(url)
        api_query_log.log_request(url)
        response.raise_for_status()
        return response.json()


def _discover_leaf_nodes(report):
    """
    Discover all "Leaf Nodes" in the API hierarchy. API branches that cannot
    be crawled are recorded in `report.failed_branches`.

    This function starts the recursive crawl from the API root and traverses
    WorldPop's top-level "Branch Nodes" (e.g. 'pop', 'covariates') serially.

    After the crawl, it filters the results to exclude data series that appear
    to have global coverage (based no the returned meta-data).

    Returns:
        list[dict]: A list of all discovered and *supported* "Leaf Node" dictionaries.

    """
    logger.info("Phase 1: Discovering API Leaf Nodes...")

    # get the top nodes in WorldPop's data hierarchy (e.g. 'pop' or 'covariates')
    try:
        response = _query_metadata_api(f'{METADATA_API_URL}')
        top_nodes = response.get("data", [])
        if not top_nodes:
            raise APIRequestError("API root returned no data.")
    except APIRequestError as e:
        logger.error(f"Failed to fetch the root API node: {e}")
        raise

    # start the recursive API crawl!
    all_leaf_nodes = []
    for node in tqdm(top_nodes, desc="Discovering API Leaf Nodes"):
        if "alias" in node and "name" in node:
            alias = node["alias"]  # noqa
            name = node["name"]  # noqa

            if _is_skipped(alias):
                continue

            if DEBUG and alias.lower() != 'covariates':
                # TODO: Use a top node with fewer data series for debugging
                # reduce number of API calls in debug mode
                continue

            leaf_nodes_from_branch = _traverse_api_node(alias, name, report)
            all_leaf_nodes.extend(leaf_nodes_from_branch)

    logger.info(
        f"Phase 1 crawl complete. Found {len(all_leaf_nodes)} "
        "total API Leaf Nodes. Now filtering..."
    )

    # 2. Filter the results
    leaf_nodes_to_process = []
    for leaf_node in all_leaf_nodes:
        # this key is passed up by the refactored _traverse_api_node
        iso3 = leaf_node.get("representative_iso3")
        api_path = leaf_node["api_path"]  # for logging

        if iso3 is None or iso3 == 'WCD':
            msg = f"Skipping node {api_path}: It (likely) is a global data series."
            logger.info(msg)
            continue

        if iso3 in ['WCA', 'WCB', 'WCT']:
            msg = f"Skipping node {api_path}: It (likely) is a continental data series."
            logger.info(msg)
            continue

        if iso3 not in DOWNLOADABLE_ISO3_CODES:
            if DEBUG:
                print('unsupported ISO3 code:', iso3)
                print('Full Leaf Node:', leaf_node)
            msg = f"Skipping node {api_path}: Type of data series not recognised (iso3={iso3})."
            logger.info(msg)
            continue

        # This node is supported. Clean up the temp key and add it.
        del leaf_node["representative_iso3"]
        leaf_nodes_to_process.append(leaf_node)

    # 3. Log final count
    num_supported = len(leaf_nodes_to_process)
    logger.info(
        f"Phase 1 filtering complete: {num_supported} supported API Leaf Nodes found."
    )

    return leaf_nodes_to_process


def _is_skipped(api_path):
    """
    Return True, and log the reason, if the crawler should not request
    `api_path` (see SKIPPED_API_PATHS).
    """
    reason = SKIPPED_API_PATHS.get(api_path.lower())
    if reason is not None:
        logger.info(f"Skipping API path {api_path}: {reason}")
    return reason is not None


def _traverse_api_node(api_path, node_name, report):
    """
    Recursively traverse a single node in the API hierarchy and return
    all Leaf Nodes found. Branches that cannot be crawled are recorded in
    `report.failed_branches` (a `ManifestBuildReport`).

    Returns:
        list[dict]: A list of all "Leaf Nodes" found under this API path.
    """

    url = f"{METADATA_API_URL}/{api_path}"
    logger.debug(f"Crawling API node: {url}")

    try:
        response = _query_metadata_api(url)
        # Guard against a JSON `null` body (`response.json()` returns None)
        if response is None:
            logger.error(f"Failed to crawl node {api_path}. Skipping this branch.")
            report.failed_branches.append(api_path)
            return []

        returned_entries = response.get("data", [])
        if not returned_entries:
            logger.warning(f"No data found for node: {api_path}")
            return []

        first_item = returned_entries[0]

        if "alias" in first_item:
            # --- Case 1: This is a "Branch Node" ---
            # The 'alias' keyword tells us that the API returned a
            # "Branch Index" so we need to recurse into each child.
            logger.debug(f"Node {api_path} is a Branch Node. Recursing serially...")
            leaf_nodes_to_process = []
            for item in returned_entries:
                alias = item.get("alias", "").strip()
                next_node_name = item.get("name", "N/A")
                if not alias or _is_skipped(f"{api_path}/{alias}"):
                    continue

                # recurse the function (serially)
                child_nodes = _traverse_api_node(
                    api_path=f"{api_path}/{alias}",
                    node_name=next_node_name,
                    report=report,
                )
                leaf_nodes_to_process.extend(child_nodes)

            return leaf_nodes_to_process

        elif "id" in first_item:
            # --- Case 2: This is a "Leaf Node" ---
            # The 'id' keyword tells us this is a Leaf Node. We stop
            # recursing and return this node. Filtering is handled
            # by the caller (`_discover_leaf_nodes`).

            return [
                {
                    "api_path": api_path,
                    "coverage_index": returned_entries,
                    "node_name": node_name,
                    "representative_iso3": first_item.get("iso3"),
                }
            ]
        else:
            logger.warning(f"Unknown data format for node {api_path}.")
            return []

    except APIRequestError as e:
        # This catch is for *fatal* errors raised by _query_metadata_api
        # after all retries have failed.
        logger.error(f"Failed to crawl node {api_path}: {e}")
        report.failed_branches.append(api_path)
        return []


def _process_leaf_node(
    api_path,
    node_name,
    coverage_index
):
    """
    Processes a single "Leaf Node" using the "Sample -> Parse -> Generate" strategy.

        1. Make one "Details Call" for a sample country.
        2. Parse the file-organisation scheme of the "Sample Payload"
           to check whether the data series is supported and, if so,
           which file organisation scheme is used ("flat", "multi-year",
           or "multi-band").
        3. For supported cases, generate manifest rows for the *entire*
           Coverage Index while ensuring a consistent, flat organisation
           of our processed data (even for "multi-year" or "multi-band"
           data.)
    """
    logger.info(f"Phase 2: Processing Leaf Node: {api_path}")

    # --- 1. Get a sample payload (=file listing for one country) ---
    sample_coverage_entry = coverage_index[0]
    sample_iso = sample_coverage_entry["iso3"]
    # This call will raise an APIRequestError if it fails,
    # which is caught by the robust loop in build_raw_manifest_from_api
    sample_details, sample_filenames = _get_sample_payload(api_path, sample_iso)

    # --- 2. Parse the sample file listing ---
    file_pattern, parsed_data = _analyse_sample_payload(
        api_path,
        sample_coverage_entry,
        sample_details,
        sample_filenames
    )

    # --- 3. Generate raw manifest rows for supported cases---
    if file_pattern == "unsupported":
        return []  # skip this Leaf Node

    # --- 3A. Extract *series-level* meta-data ---
    citation, doi = _series_citation_and_doi(sample_details, sample_iso, api_path)
    series_metadata = {
        "desc": sample_details.get("desc"),
        "source": sample_details.get("source"),
        "project": sample_details.get("project"),
        "category": sample_details.get("category").strip(),
        "license": sample_details.get("license") or None,
        "citation": citation,
        "doi": doi,
    }

    summary_url_template = _infer_summary_url_template(sample_details)

    # --- 3C. Build raw manifest rows ---
    manifest_rows = []

    if file_pattern == "flat":
        # Case: 1-to-1 data (easy)
        logger.debug(f"Processing {api_path} as 'flat' (1-to-1) scheme.")

        # Infer template for this specific pattern
        download_url_template = _infer_download_url_template(
            literal_url=parsed_data["url_for_template"],
            sample_iso=sample_iso,
            sample_year=parsed_data["year_for_template"]
        )
        logger.debug(f"Inferred URL template: {download_url_template}")

        for coverage_entry in coverage_index:
            row = _build_dataset_record(
                coverage_entry=coverage_entry,
                download_url_template=download_url_template,
                summary_url_template=summary_url_template,
                id_for_summary=coverage_entry.get("id"),
                api_path=api_path,
                series_metadata=series_metadata,
                node_name=node_name,
                band=None # No band for flat files
            )
            if row:
                manifest_rows.append(row)

    elif file_pattern == "multi-year":
        # Case: 1-to-N, "multi-year" data (=same measurement, different years)
        logger.debug(f"Processing {api_path} as 'multi-year' scheme.")
        years_to_unpack = parsed_data["years"]

        # Infer template for this specific pattern
        download_url_template = _infer_download_url_template(
            literal_url=parsed_data["url_for_template"],
            sample_iso=sample_iso,
            sample_year=parsed_data["year_for_template"]
        )
        logger.debug(f"Inferred URL template: {download_url_template}")

        # 'multi-year' requires {year} placeholder
        _validate_download_url_template(download_url_template, api_path, ["{year}"])

        for base_country_entry in coverage_index:  # outer Loop (countries)
            id_for_summary = base_country_entry.get("id")

            for year in years_to_unpack:  # inner Loop (years)
                synthetic_entry = _create_synthetic_entry_for_year(base_country_entry, year)
                if not synthetic_entry:
                    continue

                row = _build_dataset_record(
                    coverage_entry=synthetic_entry,
                    download_url_template=download_url_template,
                    summary_url_template=summary_url_template,
                    id_for_summary=id_for_summary,
                    api_path=api_path,
                    series_metadata=series_metadata,
                    node_name=node_name,
                    band=None # No band for multi-year files
                )
                if row:
                    manifest_rows.append(row)

    elif file_pattern == "multi-band":
        # Case: 1-to-N, "multi-band" data (=different measurements, same year)
        logger.debug(f"Processing {api_path} as 'multi-band' scheme.")
        bands_to_unpack = parsed_data["bands"]
        static_year = parsed_data["year"]

        # Infer template for this specific pattern
        download_url_template = _infer_download_url_template(
            literal_url=parsed_data["url_for_template"],
            sample_iso=sample_iso,
            sample_year=static_year,
            sample_band=parsed_data["band_for_template"]
        )
        logger.debug(f"Inferred URL template: {download_url_template}")

        # 'multi-band' requires both {year} and {band}
        required_placeholders = ["{year}", "{band}"]
        _validate_download_url_template(download_url_template, api_path, required_placeholders)

        for base_country_entry in coverage_index: # outer Loop (countries)
            id_for_summary = base_country_entry.get("id")

            for band in bands_to_unpack: # inner Loop (bands)
                synthetic_entry = _create_synthetic_entry_for_band(
                    base_country_entry, static_year, band
                )
                if not synthetic_entry:
                    continue

                row = _build_dataset_record(
                    coverage_entry=synthetic_entry,
                    download_url_template=download_url_template,
                    summary_url_template=summary_url_template,
                    id_for_summary=id_for_summary,
                    api_path=api_path,
                    series_metadata=series_metadata,
                    node_name=node_name,
                    band=band
                )
                if row:
                    manifest_rows.append(row)

    return manifest_rows


def _get_sample_payload(
    api_path,
    sample_iso3
):
    """
    Perform the "Details Call" for a sample country.

    Given an API Leaf Node (`api_path`), query the exact same API again
    for *one* concrete example country (e.g. ?iso3=AFG). Then return
    JSON details for the *first* entry of the resulting listing (e.g.
    the first available year). This is what we call a "Sample Payload*,
    which we can then analyse with `_analyse_sample_payload` to check
    whether the current data series is supported or not, as well as to
    infer a download-URL template for *all* other countries covered by
    the series.

    Returns
    -------
    tuple[dict, list]:
        - `sample_details` (dict): The *first* entry from the `data`
          array of the Sample Payload (e.g. `data[0]`).
        - `sample_filenames` (list): The `files` array from that first
          entry (e.g. `data[0].get("files")`).

    Raises
    ------
    APIRequestError
        If the API call fails, or returns a JSON `null` body or empty/invalid data.
    """

    url = f"{METADATA_API_URL}/{api_path}?iso3={sample_iso3}"
    logger.debug(f"Making 'Details Call' for sample country: {url}")
    response = _query_metadata_api(url)

    # --- Robustness Check ---
    # If _query_metadata_api failed (e.g. 404, 500) it will raise an error
    # after all retries. If it returns None (i.e. the API sent a JSON `null`
    # body) or empty data, we raise an error here.
    if response is None:
        raise APIRequestError(
            f"API query for sample payload returned a JSON `null` body "
            f"for {api_path} (iso={sample_iso3})."
        )

    sample_payload_data = response.get("data", [])
    if not sample_payload_data or not isinstance(sample_payload_data, list):
        raise APIRequestError(
            f"Sample call for {api_path} (iso={sample_iso3}) "
            f"returned no valid 'data' array."
        )

    # use the *first* dataset entry (e.g. first year) as our sample
    sample_details = sample_payload_data[0]
    sample_filenames = sample_details.get("files", [])

    if not sample_filenames:
        raise APIRequestError(
            f"Sample call for {api_path} (iso={sample_iso3}) "
            f"returned a valid 'data' array, but no 'files' list."
        )

    return sample_details, sample_filenames

def _analyse_sample_payload(
    api_path,
    sample_coverage_entry,
    sample_details,
    sample_filenames
):
    """
    Analyse the "Sample Payload" to determine the file organisation scheme.

    This function distinguishes between three supported patterns:
    1.  **"flat"**: One file per country-year (e.g. population).
    2.  **"multi-year"**: Multiple files for one country, each representing
        a different year (e.g. DMSP nighttime lights 2000-2011).
    3.  **"multi-band"**: Multiple files for one country-year, each
        representing a different thematic class or band (e.g. land-cover
        classes for 2017).

    Returns
    -------
    tuple[str, dict]:
        - `pattern` (str): One of "flat", "multi-year", "multi-band",
                           or "unsupported".
        - `parsed_data` (dict): Data needed by the parser for templating.
    """

    # filter out unsupported file formats right away
    if not are_all_files_tif(sample_filenames):
        logger.info(
            f"Skipping {api_path}: Format of sample file(s) is not TIF "
            f"(e.g. {sample_filenames[0]})."
        )
        return "unsupported", {}

    num_files = len(sample_filenames)
    sample_url = sample_filenames[0]
    sample_year_from_details = sample_details.get("popyear")
    sample_year_from_coverage = sample_coverage_entry.get("popyear")

    # --- Case 1: "Flat" (1-to-1) Scheme ---
    if num_files == 1:
        logger.debug(f"Recognised {api_path} as 'flat' scheme.")
        return "flat", {
            "url_for_template": sample_url,
            "year_for_template": sample_year_from_details
        }

    # --- Case 2: "Multi-File" (1-to-N) Schemes ---
    if num_files > 1:

        # --- Test A: Is it "multi-year"? ---
        # This is the case in which the Coverage Index entry has `popyear: null`
        # and the files themselves contain the year.
        if sample_year_from_coverage is None:

            year_to_file_map = {}
            for fname in sample_filenames:
                year = extract_year_from_filename(fname)
                if year:
                    if year in year_to_file_map:
                        logger.warning(
                            f"Skipping {api_path}: 'multi-year' data series "
                            f"has duplicate year: {year}. Not supported."
                        )
                        return "unsupported", {}
                    year_to_file_map[year] = fname  # store the year -> file mapping
                else:
                    logger.warning(
                        f"Could not extract year from filename: {fname} in what looked "
                        f"like a 'multi-year' data series: {api_path}. Treating series "
                        "as unsupported."
                    )
                    return "unsupported", {}

            # We must have found a unique year for every file
            if len(year_to_file_map) != num_files:
                logger.warning(
                    f"Skipping {api_path}: 'multi-year' series file count "
                    f"({num_files}) does not match unique year count "
                    f"({len(year_to_file_map)}). Not supported."
                )
                return "unsupported", {}

            sorted_years = sorted(year_to_file_map.keys())

            # Check for consecutive years
            if not are_unique_integers_consecutive(sorted_years):
                logger.warning(
                    f"Skipping {api_path}: 'multi-year' data series "
                    f"has non-consecutive year identifiers: {sorted_years}. "
                    f"This is not currently supported."
                )
                return "unsupported", {}

            # Get the first year, and the *correct* URL for that first year
            first_year = sorted_years[0]
            url_for_first_year = year_to_file_map[first_year]

            logger.debug(
                f"Analysed {api_path} as 'multi-year' scheme. "
                f"Found {len(sorted_years)} unique, consecutive years: "
                f"{sorted_years[0]}-{sorted_years[-1]}"
            )

            return "multi-year", {
                "years": sorted_years,
                "url_for_template": url_for_first_year,  # Pass the *correct* URL
                "year_for_template": first_year,
            }

        # --- Test B: Is it "multi-band"? ---
        # This is the case where the Coverage Index entry has a *single* year
        # (e.g. "2001") and the files contain different "bands" or "classes".
        else: # (sample_year_from_coverage is NOT None)
            bands = extract_unique_bands(sample_filenames)

            if bands and len(bands) == len(sample_filenames):
                logger.debug(
                    f"Analysed {api_path} as 'multi-band' scheme. "
                    f"Found {len(bands)} unique bands for year {sample_year_from_coverage}."
                    f" E.g.: {bands[0]}"
                )
                return "multi-band", {
                    "year": sample_year_from_coverage,
                    "bands": bands,
                    "url_for_template": sample_url,
                    "band_for_template": bands[0] # Pass one for templating
                }
            else:
                logger.warning(
                    f"Skipping {api_path}: Seemed like 'multi-band' but could "
                    f"not extract unique band identifiers from filenames."
                )
                return "unsupported", {}

    # Should be unreachable, but as a fallback
    return "unsupported", {}

def _infer_download_url_template(literal_url, sample_iso, sample_year, sample_band=None):
    """
    Convert a literal download URL into a replaceable template.

    This function handles substitution for ISO codes, years, and (optionally)
    multi-band "class" identifiers.

    It assumes the literal_url from the API does *not*
    contain any literal braces ('{}').
    """

    template = literal_url

    if sample_year:
        # We must cast sample_year to string for replacement
        year_str = str(sample_year)

        # Build a robust pattern to avoid replacing years
        # in YYYY_YYYY ranges.
        # We MUST double-escape the {4} so .format() doesn't parse it.
        pattern = re.compile(r"(?<!\d{{4}}_){}(?!_\d{{4}})".format(re.escape(year_str)))

        # Replace directly with format string
        template = pattern.sub("{year}", template)

    if sample_band:
        # Simple string replacement for the band part
        template = template.replace(sample_band, "{band}")

    # Replace ISOs (must come *after* band/year replacement)
    template = template.replace(sample_iso.lower(), "{iso3_lower}", 1)
    template = template.replace(sample_iso.upper(), "{iso3_upper}", 1)

    return template


def _series_citation_and_doi(sample_details, sample_iso, api_path):
    """
    Return the citation and DOI of a sample dataset, for use for its whole data
    series, or (None, None) if they may not apply to the series' other countries:
    for the series in COUNTRY_SPECIFIC_CITATION_SERIES, and where the citation
    names the sample country (see `_series_citation`).
    """
    reason = COUNTRY_SPECIFIC_CITATION_SERIES.get(api_path.lower())
    if reason is not None:
        logger.info(f"Not storing the citation of {api_path}: {reason}.")
        return None, None
    citation = _series_citation(sample_details, sample_iso, api_path)
    if citation is None and sample_details.get("citation"):
        return None, None  # dropped as country-specific: its DOI may be too
    return citation, sample_details.get("doi") or None


def _series_citation(sample_details, sample_iso, api_path):
    """
    Return the recommended citation of a sample dataset, for use as the citation
    of its whole data series, or None if there is none or if it may be
    country-specific: a citation that names the sample country is not stored
    (with a warning), since it might not apply to the series' other countries.
    (On 2026-11-17, citations were identical across countries and years in
    every series checked, and named no country.)
    """
    citation = sample_details.get("citation") or None
    country = pycountry.countries.get(alpha_3=sample_iso)
    if citation is None or country is None:
        return citation
    names = {getattr(country, attr, None) for attr in ("name", "common_name", "official_name")}
    if any(name and name.lower() in citation.lower() for name in names):
        logger.warning(
            f"Not storing the citation of {api_path}: it names the sample country "
            f"({sample_iso}), so it may not apply to the series' other countries."
        )
        return None
    return citation


def _infer_summary_url_template(sample_details):
    """
    Convert the summary URL for a single WorldPop dataset into
    a more general template.

    This function handles the simple substitution for summary URLs,
    which only requires replacing the dataset ID.

    Parameters
    ----------
    sample_details : dict
        The `sample_details` dictionary from the Sample Payload.

    Returns
    -------
    str | None
        The template (e.g. "...?id={id}") or None if templating fails.
    """
    sample_summary_url = sample_details.get("url_summary")
    sample_id_str = sample_details.get("id")

    if not sample_summary_url or not sample_id_str:
        logger.info(
            f"Could not infer 'url_summary' template: "
            f"Missing sample URL or sample ID."
        )
        return None

    if sample_id_str in sample_summary_url:
        summary_url_template = sample_summary_url.replace(sample_id_str, "{id}")
        logger.debug(f"Inferred summary template: {summary_url_template}")
        return summary_url_template
    else:
        logger.warning(
            f"Could not infer 'url_summary' template: "
            f"Sample ID '{sample_id_str}' not found in sample URL '{sample_summary_url}'."
        )
        return None

def _create_synthetic_entry_for_year(base_country_entry, year):
    """
    Create a synthetic "flat" entry for a "multi-year" dataset.

    It takes a "multi-year" country entry (which has `popyear=null`) and
    a specific `year` and merges them into a "synthetic" entry that
    mimics a "flat" scheme.

    ID becomes: {original_id}_{year}
    """
    try:
        synthetic_entry = base_country_entry.copy()
        original_id = base_country_entry.get("id", "unknown")
        synthetic_entry["popyear"] = int(year)
        synthetic_entry["id"] = f"{original_id}_{year}"
        return synthetic_entry
    except Exception as e:
        logger.error(
            f"Failed to create synthetic entry for "
            f"id={base_country_entry.get('id')} and year={year}. Error: {e}"
        )
        return None

def _create_synthetic_entry_for_band(base_country_entry, year, band):
    """
    Create a synthetic "flat" entry for a "multi-band" dataset.

    It takes a "multi-band" country entry, its single `year`, and a
    specific `band` and merges them into a "synthetic" entry.

    ID becomes: {original_id}_{band}
    """
    try:
        synthetic_entry = base_country_entry.copy()
        original_id = base_country_entry.get("id", "unknown")
        # Set the popyear to the single year this band belongs to
        synthetic_entry["popyear"] = int(year)
        # The unique ID is based on the band
        synthetic_entry["id"] = f"{original_id}_{band}"
        return synthetic_entry
    except Exception as e:
        logger.error(
            f"Failed to create synthetic entry for "
            f"id={base_country_entry.get('id')}, year={year}, band={band}. Error: {e}"
        )
        return None


def _build_dataset_record(
    coverage_entry,
    download_url_template,
    summary_url_template,
    id_for_summary,
    api_path,
    series_metadata,
    node_name,
    band=None,
):
    """
    Builds a final manifest row (dict) for a single Dataset.

    This function takes a (potentially synthetic) coverage entry,
    the inferred URL templates, and series metadata and formats the
    final dictionary that will become a row in the `raw_mdf` DataFrame.
    Its keys must match `RAW_MANIFEST_COLUMNS`.
    """
    try:
        iso_code = coverage_entry["iso3"]
        year = coverage_entry.get("popyear")  # will be None for static data

        # This is `worldpoppy`'s own ID for the manifest, which *must* be unique.
        # For "multi-year" and "multi-band" data, this will be synthetic (e.g. "62514_2020")
        unique_idx = str(coverage_entry["id"])

        # --- 1. Build Download URL ---
        # This is robust: .format() will ignore any placeholders
        # (like {band}) that are not in the template.
        url = download_url_template.format(
            iso3_lower=iso_code.lower(),
            iso3_upper=iso_code.upper(),
            year=year,
            band=band,
        )

        # --- 2. Build Summary URL ---
        summary_url = None
        if summary_url_template and id_for_summary:
            try:
                summary_url = summary_url_template.format(id=id_for_summary)
            except Exception:
                logger.warning(
                    f"Failed to format summary URL for {api_path} "
                    f"(id={id_for_summary})."
                )

        filename = Path(url).name

        return {
            "wpy_id": unique_idx,
            "iso3": iso_code,
            "dataset_name": Path(filename).stem,
            "remote_path": url,
            # use either the dataset-specific 'title' or the generic 'node_name'
            "api_entry_title": coverage_entry.get("title", node_name),
            "api_path": api_path,
            "year": int(year) if year else pd.NA,
            "band": band,
            "remote_name": filename,
            # --- Series-Level Metadata ---
            "api_project": series_metadata.get("project"),
            "api_series_desc": series_metadata.get("desc"),
            "api_series_category": series_metadata.get("category"),
            "api_source": series_metadata.get("source"),
            "license": series_metadata.get("license"),
            "citation": series_metadata.get("citation"),
            "doi": series_metadata.get("doi"),
            # --- Dataset-Level Metadata ---
            "summary_url": summary_url,
        }

    except (KeyError, TypeError, ValueError) as e:
        logger.warning(
            f"Skipping entry for {api_path} ({coverage_entry.get('iso3')}). "
            f"Failed to build from template. Error: {e} (Entry: {coverage_entry})"
        )
        return None

def _validate_download_url_template(template_string, api_path, required_placeholders):
    """
    Validates that a generated URL template contains all required placeholders.

    This is a critical safety check to prevent silent data corruption from
    permissive .format() calls (which ignore extra keys). A missing placeholder
    usually means that WorldPop changed its file naming. The resulting error is
    caught by the series loop in `build_raw_manifest_from_api`, which skips the
    data series and records it as failed in the build report.

    Parameters:
    - template_string (str): The generated URL template (e.g. "...{iso3_lower}_{year}.tif")
    - api_path (str): The API path, for the error message.
    - required_placeholders (list[str]): A list of placeholders (e.g. ["{year}", "{band}"])
                                         that *must* be in the template.

    Raises:
    - ValueError: If any required placeholder is missing.
    """
    missing = [p for p in required_placeholders if p not in template_string]
    if missing:
        raise ValueError(
            f"Template validation FAILED for {api_path}. "
            f"The generated template is missing required placeholder(s): {missing}. "
            f"Template was: '{template_string}'."
        )


def _run_url_check(mdf, report):
    """
    Phase 3 of a manifest build: check the inferred download URLs in `mdf`
    against WorldPop's Global 1 File List, and count the results per data
    series in `report`. Return the checked manifest (see
    `_check_urls_against_global1_file_list`).

    If the Global 1 File List cannot be downloaded or used (e.g. because its
    format changed), every URL is marked as not checked, and the build counts
    as incomplete (`report.url_check_error`).
    """
    try:
        file_list = _fetch_global1_file_list()
        checked = _check_urls_against_global1_file_list(mdf, file_list)
    except Exception as e:
        logger.error(f"Could not check the download URLs against WorldPop's Global 1 File List: {e!r}")
        report.url_check_error = repr(e)
        return mdf.assign(**{URL_CHECK_COLUMN: URL_NOT_CHECKED})

    for api_path, results in checked.groupby("api_path", sort=True)[URL_CHECK_COLUMN]:
        report.url_check_counts[api_path] = results.value_counts().to_dict()
    return checked


@backoff.on_exception(
    backoff.expo,
    httpx.HTTPError,  # catch all httpx errors
    max_tries=3,
    jitter=backoff.full_jitter,
    logger=logger,
    giveup=is_fatal_http_error,  # stop retrying if it's a 404, loop, etc.
    giveup_log_level=logging.WARNING,  # the caller logs the final failure as an error
)
def _fetch_global1_file_list():
    """
    Download WorldPop's Global 1 File List (one CSV file of about 26 MB) and
    return the paths of its raster files on the data server (e.g.
    'GIS/Population/Global_2000_2020/2000/AFG/afg_ppp_2000.tif'). Rows without
    a path are skipped.
    """
    with httpx.Client(timeout=DATA_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
        response = client.get(GLOBAL1_FILE_LIST_URL)
        api_query_log.log_request(GLOBAL1_FILE_LIST_URL)
        response.raise_for_status()
    file_list = pd.read_csv(io.BytesIO(response.content), usecols=["PathToRaster"])
    return file_list["PathToRaster"].dropna().astype(str).tolist()


def _check_urls_against_global1_file_list(mdf, file_list):
    """
    Return a copy of `mdf` in which each download URL (`remote_path`) of the
    Global 1 series has been checked against `file_list`, the paths listed in
    WorldPop's Global 1 File List. The result of each check is recorded in the
    new column `url_check`:

    - 'confirmed': the URL's path is in the list.
    - 'corrected': the URL's file name is in the list under exactly one other
      path, to which the URL is corrected.
    - 'unconfirmed': the file name is not in the list (or is listed under
      several paths). The URL is kept as inferred. The list does not include
      every Global 1 dataset that the meta-data API lists, so the row is kept.
    - 'not_checked': the URL does not belong to the Global 1 series.

    Paths are compared with doubled slashes collapsed.
    """
    listed_paths = {_normalise_data_path(path) for path in file_list}
    path_by_name = {}
    for path in listed_paths:
        name = path.rsplit("/", 1)[-1]
        path_by_name[name] = None if name in path_by_name else path  # None: listed several times

    urls, results = [], []
    for url in mdf["remote_path"]:
        path = _normalise_data_path(url)
        listed_path = path_by_name.get(path.rsplit("/", 1)[-1])
        if not _is_global1_path(path):
            result = URL_NOT_CHECKED
        elif path in listed_paths:
            result = URL_CONFIRMED
        elif listed_path is not None:
            result = URL_CORRECTED
            url = f"{DATA_SERVER_URL}/{listed_path.removeprefix('GIS/')}"
        else:
            result = URL_UNCONFIRMED
        urls.append(url)
        results.append(result)

    return mdf.assign(remote_path=urls, **{URL_CHECK_COLUMN: results})


def _normalise_data_path(url_or_path):
    """
    Return the path of a file on WorldPop's data server from 'GIS/' on, with
    doubled slashes collapsed. Accepts a download URL or a path.
    """
    _, sep, rest = url_or_path.partition("GIS/")
    return re.sub("/{2,}", "/", f"GIS/{rest}" if sep else url_or_path)


def _is_global1_path(path):
    """
    True if `path` (see `_normalise_data_path`) lies in a series folder of the
    Global 1 series, e.g. 'GIS/Population/Global_2000_2020/...' or
    'GIS/Population/Global_2000_2020_Constrained/...'.
    """
    parts = path.split("/")
    return len(parts) > 2 and parts[2].startswith(GLOBAL1_FOLDER_PREFIX)
