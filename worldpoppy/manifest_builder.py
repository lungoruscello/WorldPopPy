"""
Core "engine" for building a raw data manifest for the `worldpoppy` library.

This module contains all the logic for crawling the WorldPop metadata API,
parsing the results, and saving them to a new cache file (RAW_MANIFEST_CACHE_PATH).

Its main and only public function is `build_raw_manifest_from_api()`. However,
users will rarely need to import that function directly. Instead, it is called
by the separate `manifest_loader` module whenever a cached version of the raw
manifest does not exist.

For a detailed, high-level explanation of this module's API crawl strategy,
related terminology (e.g., "Leaf Node", "Sample Payload"), and data-parsing
logic, please see the `manifest_build_strategy.md` document in the project root.

---
A note on the complexity of this module:
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


# TODO: remove legacy reference to API "crawl" except where accurate

import logging
import re
from datetime import datetime
from math import floor
from pathlib import Path

import backoff
import httpx
import pandas as pd
from tqdm.autonotebook import tqdm

from worldpoppy.config import (
    DEBUG,
    METADATA_API_URL,
    METADATA_API_TIMEOUT,
    RAW_MANIFEST_CACHE_PATH,
    SUPPORTED_ISO3_CODES,
)
from worldpoppy.tracking import api_query_log

logger = logging.getLogger(__name__)

year_extract_pattern = re.compile(r"(?<!\d{4}_)(\d{4})(?!_\d{4})")
# > lookback and lookahead used to exclude year ranges (e.g., "2000_2020")


class APIRequestError(Exception):
    """Raised when an API request fails permanently or after all retries."""

    pass


def build_raw_manifest_from_api(force_crawl=False):
    """
    Crawl WorldPop's meta-data API and analyse the results to build a new,
    raw manifest of raster datasets for `worldpoppy`. We call this manifest
    "raw" because it will be further checked and filtered (where needed)
    by the `manifest_loader` module.

    This is a SERIAL (single-threaded) implementation.

    Phase 1: Discover "Leaf Nodes" by recursively crawling the API
    (using `_discover_leaf_nodes`).

    Phase 2: Processes each discovered "Leaf Node" (using `_process_leaf_node`)
    by applying a "Sample -> Analyse -> Parse" strategy that generates our
    final list of raw manifest rows. This phase is "robust", meaning a
    failure on one Leaf Node will be logged and skipped, allowing the
    crawl to continue.

    Parameters
    ----------
    force_crawl : bool, optional
        If True, forces a full re-crawl and re-processing of WorldPop's meta-data
        API, even if cached results from a previous run exist on disk. Default
        is False.
    """

    # check if we need to run
    if RAW_MANIFEST_CACHE_PATH.is_file() and not force_crawl:
        mtime = RAW_MANIFEST_CACHE_PATH.stat().st_mtime
        age_days = (datetime.now().timestamp() - mtime) / (3600 * 24)

        if age_days > 90:
            logger.warning(
                f"Found results from cached API crawl (cached {floor(age_days)} "
                f"days ago). Use `build_raw_manifest_from_api(force_crawl=True)` to overwrite."
            )
        return

    logger.warning(
        "Crawling WorldPop's meta-data API to find supported data series & download URLs..."
    )

    try:
        # --- Phase 1 ---
        # Discover candidate data series in one call (serially)
        leaf_nodes = _discover_leaf_nodes()
        if not leaf_nodes:
            logger.error("API crawl failed: No supported data series found.")
            return

        # --- Phase 2 ---
        # Generate well-formatted listings of supported raster files for
        # all leaf nodes (serially).
        logger.info(f"Phase 2: Processing {len(leaf_nodes)} Leaf Nodes (serially)...")

        # This will be a list of lists
        raw_manifest_rows_nested = []

        # NOTE
        # The `try...except Exception` block *inside* this loop is intentional.
        # It ensures that a failure on a single Leaf Node (e.g., an APIRequestError
        # from _get_sample_payload or a parsing error) is logged and skipped.
        # This allows the crawl to continue for all other nodes.
        #
        # The *outer* `try...except` blocks are for *fatal* errors
        # (like a failure in Phase 1) that must abort the entire crawl.

        for leaf_node in tqdm(leaf_nodes, desc="Processing Leaf Nodes"):
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
        logger.error(f"API crawl failed with critical error: {e}")
        return
    except Exception as e:
        # This catches any other unexpected startup error
        logger.error(f"API crawl failed with unexpected error: {e}", exc_info=True)
        return

    # --- Post-Processing ---
    # Flatten results
    raw_manifest_rows = [row for sublist in raw_manifest_rows_nested for row in sublist]

    if not raw_manifest_rows:
        logger.error("API crawled returned no data. Check API status or logs.")
        return

    # Save crawl results to disk.
    # (This will be our raw, uncleaned data manifest).
    raw_mdf = pd.DataFrame(raw_manifest_rows)
    try:
        raw_mdf.to_feather(RAW_MANIFEST_CACHE_PATH, compression="zstd")
        logger.warning(
            f"Updated API crawl results saved to: {RAW_MANIFEST_CACHE_PATH} "
            f"({len(raw_mdf)} supported files found)"
        )

    except Exception as e:
        logger.error(
            f"Failed to save new API crawl results to {RAW_MANIFEST_CACHE_PATH}: {e}"
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
    for node in tqdm(top_nodes, desc="Discovering API leaf nodes"):
        if "alias" in node and "name" in node:
            alias = node["alias"]  # noqa
            name = node["name"]  # noqa

            if alias.lower() == 'age_structures':
                # currently not supported due to a limitation in
                # `_extract_unique_bands`
                continue

            if DEBUG and alias.lower() != 'covariates':
                # reduce number of API calls in debug mode
                continue

            leaf_nodes_from_branch = _traverse_api_node(alias, name)
            all_leaf_nodes.extend(leaf_nodes_from_branch)

    logger.info(
        f"Phase 1 crawl complete. Found {len(all_leaf_nodes)} "
        "total Leaf Nodes. Now filtering..."
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

        if iso3 not in SUPPORTED_ISO3_CODES:
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
        f"Phase 1 filtering complete: {num_supported} supported Leaf Nodes found."
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
        "category": sample_details.get("category"),
        "gtype": sample_details.get("gtype"),
    }
    # We can infer the data_series from the *first* file
    wp_data_series = _infer_data_series(sample_filenames[0])
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
                data_series=wp_data_series,
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
                    data_series=wp_data_series,
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
                    data_series=wp_data_series,
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
    if not _are_all_files_tif(sample_filenames):
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
                year = _extract_year_from_filename(fname)
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
                        f"like a 'multi-year' data series: {api_path}. Treating as "
                        "unsupported."
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
            if not _are_unique_integers_consecutive(sorted_years):
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
            bands = _extract_unique_bands(sample_filenames)

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


def _extract_year_from_filename(file_path_or_url):
    """
    Extract a year identifier from a dataset filename, ignoring YYYY_YYYY ranges.

    Uses a robust regex to find all valid 4-digit year candidates and returns the
    year only if all candidates are the same value.

    Parameters
    ----------
    file_path_or_url : str
        The full URL or file path (e.g., ".../afg_..._2020_100m.tif")

    Returns
    -------
    int | None
        The extracted year (e.g., 2020), or None if the year is ambiguous,
        implausible, or not found.
    """

    min_plausible_year, max_plausible_year = 1995, 2040

    # get just the filename (e.g., "afg_..._2020_100m.tif")
    filename = Path(file_path_or_url).name

    # get year-pattern matches
    matches = year_extract_pattern.findall(filename)

    if not matches:
        return None

    # check for ambiguity
    unique_years = set(matches)

    if len(unique_years) > 1:
        logger.warning(
            f"Ambiguous year in filename: {filename}. "
            f"Found multiple different non-range years: {sorted(unique_years)}. "
            "Skipping."
        )
        return None

    try:
        year_str = matches[0]
        year_int = int(year_str)
    except (IndexError, ValueError) as e:
        logger.warning(
            f"Error parsing unique year from matches {matches} "
            f"in filename: {filename}. Error: {e}"
        )
        return None

    # check whether the year value is plausible
    if not (min_plausible_year <= year_int <= max_plausible_year):
        logger.warning(
            f"Implausible year in filename: {filename}. "
            f"Found {year_int}, which is outside the "
            f"plausible range ({min_plausible_year}-{max_plausible_year}). "
            "Skipping."
        )
        return None

    return year_int


def _extract_unique_bands(filenames):
    """
    Extracts the unique "band" part from a list of "multi-band" filenames
    by "diffing" the filename stems.

    Assumes that multi-band filenames share the same structure (same number
    of "_" parts) and that *exactly one* part of the filename is different.
    It treats that part as the band name.

    TODO: For AgeSex_structures data, the assumption of *exactly one*
     differing does not hold. Generalise?


    """
    if not filenames or len(filenames) < 2:
        return None

    try:
        # Get stems (filename without extension)
        stems = [Path(f).stem for f in filenames]
        split_stems = [s.split('_') for s in stems]

        # Check all stems have the same number of parts
        it = iter(split_stems)
        length = len(next(it))
        if not all(len(parts) == length for parts in it):
            logger.warning(
                f"Multi-band filenames have different structures (different '_' counts): {stems}"
            )
            return None

        # Find the indices where the parts are different
        diff_indices = []
        zipped_parts = list(zip(*split_stems))
        for i, part_tuple in enumerate(zipped_parts):
            if len(set(part_tuple)) > 1:
                diff_indices.append(i)

        if not diff_indices:
            logger.warning(f"No differing parts found in multi-band filenames: {stems}")
            return None

        if len(diff_indices) > 1:
            logger.warning(
                f"Found multiple differing parts in multi-band filenames. "
                f"This is not supported. Indices: {diff_indices}"
            )
            return None

        # We now know there is exactly one differing part
        diff_index = diff_indices[0]
        bands = [parts[diff_index] for parts in split_stems]

        # Check that the *extracted* parts are also unique
        if len(set(bands)) != len(bands):
            logger.warning(
                f"Extracted band parts were not unique (this should not happen?): {bands}"
            )
            return None

        return bands

    except Exception as e:
        logger.error(f"Error extracting unique bands: {e}", exc_info=True)
        return None


def _are_all_files_tif(file_list):
    """
    Check if all file paths in a list have a valid TIFF file extension.

    Parameters
    ----------
    file_list : list
        A list of file paths or URLs (strings).

    Returns
    -------
    bool
        True if *all* files have a valid TIFF extension,
        False otherwise.
    """
    valid_suffixes = {".tif", ".tiff", ".geotiff"}

    if not file_list:
        return False

    try:
        return all(Path(f).suffix.lower() in valid_suffixes for f in file_list)
    except Exception as e:
        logger.warning(f"Error while validating file list: {e}")
        return False


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


def _infer_data_series(literal_url):
    if 'Global_2000_2020' in literal_url:
        return 'global1'
    elif 'Global_2015_2030' in literal_url:
        return 'global2'
    else:
        return 'unknown'


def _build_dataset_record(
    coverage_entry,
    download_url_template,
    summary_url_template,
    id_for_summary,
    api_path,
    series_metadata,
    node_name,
    data_series,
    band=None
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
            band=band
        )

        # --- 2. Build Summary URL ---
        url_summary = None
        if summary_url_template and id_for_summary:
            try:
                url_summary = summary_url_template.format(id=id_for_summary)
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
            "notes": coverage_entry.get("title", node_name),
            "api_path": api_path,
            "year": int(year) if year else pd.NA,
            "band": band,
            "remote_name": filename,
            "data_series": data_series,
            # --- Series-Level Metadata ---
            "desc": series_metadata.get("desc"),
            "source": series_metadata.get("source"),
            "project": series_metadata.get("project"),
            "category": series_metadata.get("category"),
            "gtype": series_metadata.get("gtype"),
            # --- Dataset-Level Metadata ---
            "url_summary": url_summary,
        }

    except (KeyError, TypeError, ValueError) as e:
        logger.warning(
            f"Skipping entry for {api_path} ({coverage_entry.get('iso3')}). "
            f"Failed to build from template. Error: {e} (Entry: {coverage_entry})"
        )
        return None


def _are_unique_integers_consecutive(unique_int_list):
    """
    Checks if a list of *unique* integers is consecutive.

    This function is efficient because it assumes the caller has
    already verified that the list contains no duplicates.

    Parameters
    ----------
    unique_int_list : list[int]
        A list of integers, assumed to be unique.

    Returns
    -------
    bool
        True if the integers are consecutive, False otherwise.
    """
    if not unique_int_list or len(unique_int_list) < 2:
        return True

    min_val = min(unique_int_list)
    max_val = max(unique_int_list)

    return (max_val - min_val + 1) == len(unique_int_list)


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
