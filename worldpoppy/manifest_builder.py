"""
Core "engine" for building a raw data manifest for the `worldpoppy` library.

This module contains all the logic for traversing the WorldPop metadata API,
parsing the results, and saving them to a new local cache file.

Main methods
------------------------

    - :func:`build_raw_manifest_from_api`
        Query WorldPop's meta-data API and analyse the results to build a new,
        raw manifest of raster datasets for `worldpoppy`. Note: Users will rarely
        need to import this function directly. Instead, it is called by the separate
        `manifest_loader` module whenever a cached version of the raw manifest does
        not exist.

For a detailed, high-level explanation of this module's API traversal strategy,
related terminology (e.g., "Leaf Node", "Sample Payload"), and data-parsing
logic, please see the `manifest_build_strategy.md` document in the project root.

----

**A note on the complexity of this module:**

The module's size is a result of two challenges:

1.  To minimise API calls, this module implements a "sample and infer"
    strategy, in which a general template for download URLs of a raster-data
    series is inferred from only a single example per series.

2.  We must parse and "flatten" three different data organisation schemes
    used by the WorldPop project: 'flat' (1-to-1), 'multi-year' (1-to-N by year),
    and 'multi-band' (1-to-N by class).

When processing operations fail, an entire raster-data series is dropped from the
raw manifest and will hence not be supported by the `worldpoppy` package.

"""

from datetime import datetime
from math import floor

import backoff
import httpx
from tqdm.autonotebook import tqdm

from worldpoppy.config import (
    DEBUG,
    METADATA_API_URL,
    METADATA_API_TIMEOUT,
    RAW_MANIFEST_CACHE_PATH,
    RAW_MANIFEST_TIMESTAMP_PATH,
    DOWNLOADABLE_ISO3_CODES,
)
from worldpoppy.manifest_utils import *
from worldpoppy.tracking import api_query_log

logger = logging.getLogger(__name__)


class APIRequestError(Exception):
    """Raised when an API request fails permanently or after all retries."""

    pass


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

    Parameters
    ----------
    force_rebuild : bool, optional
        If True, forces a full re-crawl and re-processing of WorldPop's meta-data
        API, even if cached results from a previous run exist on disk. Default
        is False.
    """

    # Check if we need to run
    if RAW_MANIFEST_CACHE_PATH.is_file() and not force_rebuild:
        age_days = 0

        # 1. Try to read manifest age from the sidecar timestamp file (Preferred)
        if RAW_MANIFEST_TIMESTAMP_PATH.is_file():
            try:
                with open(RAW_MANIFEST_TIMESTAMP_PATH, 'r') as f:
                    ts_str = f.read().strip()
                    last_build_time = datetime.fromisoformat(ts_str)
                    age_days = (datetime.now() - last_build_time).days
            except Exception as e:
                logger.warning(
                    f"Could not read manifest timestamp sidecar: {e}. "
                    "Falling back to file system time."
                )
                # Fallback to system time if read fails
                mtime = RAW_MANIFEST_CACHE_PATH.stat().st_mtime
                age_days = (datetime.now().timestamp() - mtime) / (3600 * 24)

        # 2. Fallback to file system time (Legacy/Dev scenarios)
        else:
            mtime = RAW_MANIFEST_CACHE_PATH.stat().st_mtime
            age_days = (datetime.now().timestamp() - mtime) / (3600 * 24)

        if age_days > 180:
            logger.warning(
                f"worldpoppy's existing data manifest was built {floor(age_days)} "
                f"days ago. Use `build_raw_manifest_from_api(force_rebuild=True)` "
                "to overwrite.\nNote that this will trigger a fresh traversal "
                "of WorldPop's meta-data API."
            )
        return

    logger.warning(
        "Traversing WorldPop's meta-data API to index supported data series..."
    )

    try:
        # --- Phase 1 ---
        # Discover candidate data series in one call (serially)
        leaf_nodes = _discover_leaf_nodes()
        if not leaf_nodes:
            logger.error("Manifest update failed: No supported data series found.")
            return

        # --- Phase 2 ---
        # Generate well-formatted listings of supported raster files for
        # all Leaf Nodes (serially).
        logger.info(f"Phase 2: Processing {len(leaf_nodes)} API Leaf Nodes (serially)...")

        # This will be a list of lists
        raw_manifest_rows_nested = []

        # NOTE
        # The `try...except Exception` block *inside* this loop is intentional.
        # It ensures that a failure on a single Leaf Node (e.g., an APIRequestError
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
                # If one Leaf Node fails (e.g., unexpected JSON),
                # we log the *specific* failure but *continue* the loop.
                logger.error(
                    f"Failed to process Leaf Node: {leaf_node.get('api_path')}. "
                    f"Skipping this series. Error: {e}",
                    exc_info=True,
                )
                continue

    except APIRequestError as e:
        # This "outer" block will *only* catch critical failures,
        # such as a network error in `_discover_leaf_nodes`.
        logger.error(f"Manifest update failed with critical error: {e}")
        return
    except Exception as e:
        # This catches any other unexpected startup error
        logger.error(f"Manifest update failed with unexpected error: {e}", exc_info=True)
        return

    # --- Post-Processing ---
    # Flatten results
    raw_manifest_rows = [row for sublist in raw_manifest_rows_nested for row in sublist]

    if not raw_manifest_rows:
        logger.error("API traversal returned no data. Check API status or logs.")
        return

    # Save build results to disk.
    # (This will be our raw, uncleaned data manifest).
    raw_mdf = pd.DataFrame(raw_manifest_rows)
    try:
        # 1. Save the actual data
        raw_mdf.to_feather(RAW_MANIFEST_CACHE_PATH, compression="zstd")

        # 2. Save the timestamp sidecar
        with open(RAW_MANIFEST_TIMESTAMP_PATH, 'w') as f:
            f.write(datetime.now().isoformat())

        logger.warning(
            f"Updated raw data manifest saved to: {RAW_MANIFEST_CACHE_PATH} "
            f"({len(raw_mdf)} supported remote files found)"
        )

    except Exception as e:
        logger.error(
            f"Failed to save updated raw data manifest to {RAW_MANIFEST_CACHE_PATH}: {e}"
        )


@backoff.on_exception(
    backoff.expo,
    httpx.HTTPError, # catch all httpx errors
    max_tries=3,
    jitter=backoff.full_jitter,
    logger=logger,
    # use giveup to *not* retry on 4xx Client Errors
    giveup=lambda e: not _is_retryable_http_error(e)
)
def _query_metadata_api(url):
    """
    Performs a single, robust GET request to the endpoint of WorldPop's
    metadata API.

    This function is the core HTTP utility for the entire crawler.
    It is wrapped in a backoff/retry mechanism to handle intermittent
    server (5xx) or network errors.

    It will only give up and raise an error on client (4xx) errors
    (e.g., 404 Not Found) or after all retries have failed.
    """
    try:
        with httpx.Client(timeout=METADATA_API_TIMEOUT, follow_redirects=True) as client:
            response = client.get(url)
            api_query_log.log_request(url)
            response.raise_for_status()
            return response.json()

    except httpx.HTTPStatusError as e:
        logger.error(
            f"Permanent HTTP Error for {url}: {e.response.status_code}"
        )
        raise APIRequestError(f"API request failed (client error): {e}") from e
    except (httpx.NetworkError, httpx.TimeoutException) as e:
        logger.error(f"Network/Timeout Error for {url} after all retries: {e}")
        raise APIRequestError(f"API request failed (network error): {e}") from e
    except Exception as e:
        logger.error(f"Failed to fetch or parse JSON from {url}: {e}")
        raise APIRequestError(f"Failed to process {url}: {e}") from e


def _is_retryable_http_error(e):
    """
    Check if an httpx error is retryable (network, timeout, or 5xx)
    """
    if isinstance(e, (httpx.NetworkError, httpx.TimeoutException)):
        return True
    if isinstance(e, httpx.HTTPStatusError):
        # Retry on Server Errors (5xx), not Client Errors (4xx)
        return e.response.status_code >= 500
    return False


def _discover_leaf_nodes():
    """
    Discover all "Leaf Nodes" in the API hierarchy.

    This function starts the recursive crawl from the API root and traverses
    WorldPop's top-level "Branch Nodes" (e.g., 'pop', 'covariates') serially.

    After the crawl, it filters the results to exclude data series that appear
    to have global coverage (based no the returned meta-data).

    Returns:
        list[dict]: A list of all discovered and *supported* "Leaf Node" dictionaries.

    """
    logger.info("Phase 1: Discovering API Leaf Nodes...")

    # get the top nodes in WorldPop's data hierarchy (e.g., 'pop' or 'covariates')
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

            if alias.lower() == 'age_structures':
                # Data along this API branch is currently not supported
                # due to a limitation in `_extract_unique_bands`
                continue

            if DEBUG and alias.lower() != 'covariates':
                # TODO: Use a top node with fewer data series for debugging
                # reduce number of API calls in debug mode
                continue

            leaf_nodes_from_branch = _traverse_api_node(alias, name)
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


def _traverse_api_node(api_path, node_name):
    """
    Recursively traverse a single node in the API hierarchy and return
    all Leaf Nodes found.

    Returns:
        list[dict]: A list of all "Leaf Nodes" found under this API path.
    """

    url = f"{METADATA_API_URL}/{api_path}"
    logger.debug(f"Crawling API node: {url}")

    try:
        response = _query_metadata_api(url)
        # Handle API failure (returns None)
        if response is None:
            logger.error(f"Failed to crawl node {api_path}. Skipping this branch.")
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
                if not alias:
                    continue

                # recurse the function (serially)
                child_nodes = _traverse_api_node(
                    api_path=f"{api_path}/{alias}",
                    node_name=next_node_name
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
    series_metadata = {
        "desc": sample_details.get("desc"),
        "source": sample_details.get("source"),
        "project": sample_details.get("project"),
        "category": sample_details.get("category").strip(),
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
        if not _validate_download_url_template(
                download_url_template, api_path, ["{year}"]
        ):
            return [] # stop processing this series

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
        if not _validate_download_url_template(
                download_url_template, api_path, required_placeholders
        ):
            return [] # stop processing this series

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
    for *one* concrete example country (e.g., ?iso3=AFG). Then return
    JSON details for the *first* entry of the resulting listing (e.g.,
    the first available year). This is what we call a "Sample Payload*,
    which we can then analyse with `_analyse_sample_payload` to check
    whether the current data series is supported or not, as well as to
    infer a download-URL template for *all* other countries covered by
    the series.

    Returns
    -------
    tuple[dict, list]:
        - `sample_details` (dict): The *first* entry from the `data`
          array of the Sample Payload (e.g., `data[0]`).
        - `sample_filenames` (list): The `files` array from that first
          entry (e.g., `data[0].get("files")`).

    Raises
    ------
    APIRequestError
        If the API call fails (returns None) or returns empty/invalid data.
    """

    url = f"{METADATA_API_URL}/{api_path}?iso3={sample_iso3}"
    logger.debug(f"Making 'Details Call' for sample country: {url}")
    response = _query_metadata_api(url)

    # --- Robustness Check ---
    # If _query_metadata_api failed (e.g., 404, 500) it will raise an error
    # after all retries. If it returns None (which it should not with backoff),
    # or empty data, we raise an error here.
    if response is None:
        raise APIRequestError(
            f"API query for sample payload failed (returned None) "
            f"for {api_path} (iso={sample_iso3})."
        )

    sample_payload_data = response.get("data", [])
    if not sample_payload_data or not isinstance(sample_payload_data, list):
        raise APIRequestError(
            f"Sample call for {api_path} (iso={sample_iso3}) "
            f"returned no valid 'data' array."
        )

    # use the *first* dataset entry (e.g., first year) as our sample
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
    Analyse the "Sample Payload" to determine the file organization scheme.

    This function distinguishes between three supported patterns:
    1.  **"flat"**: One file per country-year (e.g., population).
    2.  **"multi-year"**: Multiple files for one country, each representing
        a different year (e.g., DMSP nighttime lights 2000-2011).
    3.  **"multi-band"**: Multiple files for one country-year, each
        representing a different thematic class or band (e.g., land-cover
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
            f"(e.g., {sample_filenames[0]})."
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
        # (e.g., "2001") and the files contain different "bands" or "classes".
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


def _infer_summary_url_template(sample_details):
    """
    Convert the summary URL for a single WorldPop dataset into
    a more general template.
    ... (docstring as before) ...
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
    """
    try:
        iso_code = coverage_entry["iso3"]
        year = coverage_entry.get("popyear")  # will be None for static data
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
    permissive .format() calls (which ignore extra keys).

    Parameters:
    - template_string (str): The generated URL template (e.g., "...{iso3_lower}_{year}.tif")
    - api_path (str): The API path, for logging.
    - required_placeholders (list[str]): A list of placeholders (e.g., ["{year}", "{band}"])
                                         that *must* be in the template.

    Returns:
    - bool: True if valid, False if invalid.
    """
    missing = []
    for placeholder in required_placeholders:
        if placeholder not in template_string:
            missing.append(placeholder)

    if not missing:
        return True  # All checks passed

    # If we are here, a check failed.
    logger.error(
        f"Template validation FAILED for {api_path}. "
        f"The generated template is missing required placeholder(s): {missing}. "
        f"Template was: '{template_string}'. Skipping this entire data series."
    )
    return False
