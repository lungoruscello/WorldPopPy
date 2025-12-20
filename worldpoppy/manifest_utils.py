import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

year_extract_pattern = re.compile(r"(?<!\d{4}_)(\d{4})(?!_\d{4})")
# > lookback and lookahead used to exclude year ranges (e.g., "2000_2020")


def extract_year_from_filename(file_path_or_url):
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


def extract_unique_bands(filenames):
    """
    Extracts the unique "band" part from a list of "multi-band" filenames
    by "diffing" the filename stems.

    Assumes that multi-band filenames share the same structure (same number
    of "_" parts) and that *exactly one* part of the filename is different.
    It treats that part as the band name.

    """

    # TODO: For AgeSex_structures data, the assumption of *exactly one*
    #  differing does not hold. Generalise?

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


def are_all_files_tif(file_list):
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


def infer_data_series(literal_url):
    if 'Global_2000_2020' in literal_url:
        return 'global1'
    elif 'Global_2015_2030' in literal_url:
        return 'global2'
    else:
        return 'unknown'

def infer_resolution_from_description(desc_str):
    if desc_str is None:
        return pd.NA

    if 'Geotiff format at a resolution of 3 arc'in desc_str:
        return 3
    if 'Geotiff format at a resolution of 30 arc' in desc_str:
        return 30
    return pd.NA


def are_unique_integers_consecutive(unique_int_list):
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


def format_url_to_emoji(url):
    """Convert a URL string to a clickable link emoji."""
    if pd.isna(url) or not url or url == 'N/A':
        return 'N/A'  # show N/A if no URL
    else:
        emoji = '🔗'  # link emoji
        # use target="_blank" to open in new tab
        return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{emoji}</a>'
