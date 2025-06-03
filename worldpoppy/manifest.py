"""
This moduled provides logic to download, clean, and filter the WorldPop data manifest.

Main methods
------------------------
    - :func:`wp_manifest`
        Load the WorldPop manifest from local storage and optionally filter it
        by product, countries, or years (where applicable).

"""

import ftplib
import hashlib
import logging
import re
from datetime import datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from socket import gaierror
from tempfile import NamedTemporaryFile

import pandas as pd

from worldpoppy.config import ASSET_DIR

__all__ = [
    "wp_manifest",
    "wp_manifest_constrained",
    "get_all_isos",
    "get_annual_product_names",
    "get_static_product_names",
    "get_all_dataset_names",
    "extract_year"
]

FIRST_YEAR = 2000

_year_pattern = re.compile(r'_\d{4}')
_raw_hash_fpath = ASSET_DIR / 'raw_manifest_hash.txt'
_cleaned_manifest_fpath = ASSET_DIR / 'manifest.feather'

logger = logging.getLogger(__name__)


def wp_manifest(product_name=None, iso3_codes=None, years=None):
    """
    Load the cleaned WorldPop manifest from local storage. Optionally filter the
    manifest by product name, country codes, or years (annual products only).

    Ensures the local manifest file is up-to-date by calling `build_wp_manifest()`.

    Parameters
    ----------
    product_name : str, optional
        The name of the WorldPop product for which to retain manifest entries.
    iso3_codes : str or List[str], optional
        One or more three-letter ISO codes indicating the countries for which to
        retain manifest entries.
    years : int or List[int] or str, optional
        For annual data products, either one or more years (int or List[int]) for
        which to retain manifest entries or the 'all' keyword (str) indicating that
        all available years for annual datasets should be retained. For static data
        products, this argument must be None (default). Passing any other value
        will drop manifest entries for static datasets.

    Returns
    -------
    pandas.DataFrame

        The manifest (either full or filtered) containing metadata about the various
        raster datasets WorldPop makes available. These datasets are either specific
        to one country or to one country-year (in the case of annual data).

        For each dataset, the manifest includes the following information:

        - idx:              Numerical WorldPop dataset ID
        - country_numeric:  Three-digit country code, as defined in ISO 3166-1 ('numeric-3')
        - iso3:             Three-letter country code, as defined in ISO 3166-1 ('alpha-3')
        - country_name:     Official English country name, as used in ISO 3166
        - dataset_name:     Name of the WorldPop dataset, including year identifiers for annual datasets
        - remote_path:      Remote path of the dataset on the WorldPop server
        - notes:            Human-readable description of the WorldPop dataset
        - is_annual:        Boolean flag indicating whether the dataset is linked specific country-year
        - product_name:     Name of the WorldPop data product to which a specific dataset belongs.
                            For static WorldPop datasets, this is simply the same as the dataset name
                            (see above). For annual datasets, this is the dataset name with year identifier
                            removed. This variable is used for all user queries.
        - year:             The year of an annual WorldPop dataset. None for static datasets.
    """

    # load the full manifest
    mdf = _cached_manifest_load()

    # handle the no-filter case
    if product_name is None and iso3_codes is None and years is None:
        return mdf

    if isinstance(iso3_codes, str):
        iso3_codes = [iso3_codes]

    if isinstance(years, int):
        years = [years]

    if product_name is not None:
        is_annual = is_annual_product(product_name)  # will ensure that product exists
        if not is_annual:
            if years is not None:
                logger.info(
                    f"Ignoring the 'years' argument since '{product_name}' is a static WorldPop product"
                )
                years = None
        mdf = mdf[mdf['product_name'] == product_name].copy()

    if iso3_codes is not None:
        _validate_isos(iso3_codes)
        mdf = mdf[mdf['iso3'].isin(iso3_codes)].copy()

    if years is not None:
        _validate_years(years)
        if isinstance(years, list):
            mdf = mdf[mdf['year'].isin(years)].copy()
            mdf['year'] = mdf.year.astype(int)
        else:
            assert years == 'all'
            mdf = mdf[mdf['is_annual']].copy()  # drop static products

    return mdf


def wp_manifest_constrained(product_name, iso3_codes, years=None):
    """
    Load the cleaned WorldPop manifest from local storage and filter the manifest
    for one specific download request.

    This method is a thin wrapper for `wp_manifest`. It adds checks to ensure that
    the requested data product is available for all requested countries and years
    (where applicable). It also checks that annual data queries always explicitly
    specify the years of interest.

    Parameters
    ----------
    product_name : str
        The name of the WorldPop product of interest.
    iso3_codes : str or List[str]
        One or more three-letter ISO codes indicating the countries of interest.
    years : int or List[int] or str, optional
        For annual data products, either one or more years (int or List[int]), or
        the 'all' keyword (str) indicating that manifest entries for all annual
        datasets should be retained. To retain static data products, this argument
        must be None (default). Passing any other value will drop manifest entries
        for all static datasets.

    Returns
    -------
    pandas.DataFrame
        The filtered manifest

    Raises
    ------
    ValueError
        - If the requested data product is not available for all requested countries
          during all years (where applicable).
        - If an annual data product is requested, but the 'years' argument is None.
        - If a static data product is requested, but the 'years' argument is not None.
    """

    if isinstance(iso3_codes, str):
        iso3_codes = [iso3_codes]

    if isinstance(years, int):
        years = [years]

    # fetch the download manifest (will validate query arguments)
    filtered_mdf = wp_manifest(product_name, iso3_codes, years)

    # raise an exception if 'years' is None for an annual dataset — and vice versa
    if is_annual_product(product_name):
        if years is None:
            raise ValueError(
                f"'years' argument is required since '{product_name}' is "
                f"an annual WorldPop product. If your are interested in all "
                f"available years for this product, please indicate this "
                f"using `years='all'`."
            )
    else:
        if years is not None:
            raise ValueError(
                f"'years' argument must be `None` since '{product_name}' is "
                f"a static WorldPop product. Note that we count data products "
                f"as 'static' if they are not explicitly tied to a single year."
            )

    # raise an informative exception if the requested data product is not
    # available for any requested country and years (where applicable)
    if is_annual_product(product_name):
        if isinstance(years, str):
            assert years == 'all'
            assert filtered_mdf.product_name.nunique() == 1
            years = filtered_mdf.year.unique()

        num_expected = len(iso3_codes) * len(years)
        if len(filtered_mdf) < num_expected:
            available_grps = filtered_mdf.groupby('iso3').year.unique()

            raise ValueError(
                f"Data product '{product_name}' is not available for all combinations "
                'of countries and years. Only the following requested combinations are '
                f'available: {available_grps.to_dict()}.'
            )
    else:
        num_expected = len(iso3_codes)
        if len(filtered_mdf) < num_expected:
            missing_isos = set(iso3_codes) - set(filtered_mdf.iso3)
            raise ValueError(
                f"Data product ('{product_name}') is not available "
                f'for the following countries: {missing_isos}.'
            )

    # sanity check: duplicated records should never arise
    assert num_expected == len(filtered_mdf)
    return filtered_mdf


def build_wp_manifest(overwrite=False):
    """
    Download, clean, and store a global dataset manifest from the WorldPop FTP server.

    If a cleaned manifest already exists locally and is up-to-date (verified via an MD5 hash
    check), this function does nothing. Otherwise, it downloads the latest WorldPop manifest,
    parses and processes the data, and stores a cleaned manifest version as a pandas Dataframe
    in Feather format for future use.

    Parameters
    ----------
    overwrite : bool, optional
        If True, forces re-download and reprocessing of the manifest even if the local copy is
        up-to-date. Default is False.

    Notes
    -----
    - The cleaned manifest includes metadata to distinguish annually updated WorldPop datasets
      from static datasets. Whether a dataset is annual or static is inferred from the dataset's
      name.
    """
    if _cleaned_manifest_fpath.is_file() and not overwrite:
        # Check whether the local manifest is up-to-date.
        # Note: the hash is computed on the raw WorldPop CSV file.
        if _raw_hash_fpath.is_file():
            if _read_local_manifest_hash() == _fetch_remote_manifest_hash():
                return None

    # download the raw manifest CSV from the WorldPop website,
    # ingest the manifest using pandas, and update the local hash
    with NamedTemporaryFile() as tmp_file:
        logger.warning('Downloading fresh WorldPop data manifest via FTP...')
        tmp_csv_path = Path(tmp_file.name)
        _worldpop_ftp_download('/assets/wpgpDatasets.csv', tmp_csv_path)
        _update_local_manifest_hash(tmp_csv_path)  # noqa
        mdf = pd.read_csv(tmp_csv_path)

    # clean the manifest columns
    mdf.columns = [
        'idx',
        'country_numeric',
        'iso3',
        'country_name',
        'dataset_name',
        'remote_path',
        'notes',
    ]

    # distinguish between annually updated datasets and static datasets
    mdf['is_annual'] = mdf.dataset_name.apply(_looks_like_annual_name)

    # Make a data product name. For static WorldPop datasets, this is simply the dataset
    # name. For annual datasets, this is the dataset name with year identifier removed.
    mask = mdf.is_annual
    mdf['product_name'] = mdf.dataset_name
    mdf.loc[mask, 'product_name'] = mdf.loc[mask, 'dataset_name'].apply(_strip_year)

    # extract the year for all annual raster datasets
    mdf['year'] = None
    mdf.loc[mask, 'year'] = mdf.loc[mask, 'dataset_name'].apply(extract_year)

    # extract the raster's remote file name
    mdf['remote_fname'] = [x[-1] for x in mdf.remote_path.str.split('/').values]

    # store cleaned manifest for re-use
    mdf.to_feather(_cleaned_manifest_fpath)

    logger.warning(
        f'Cleaned WorldPop data manifest has been stored locally at: {_cleaned_manifest_fpath}'
    )

    return mdf


@lru_cache()
def get_all_isos():
    """
    Return the ISO3-codes of all countries for which at least one WorldPop dataset is available.

    Returns
    -------
    List[str]
    """
    uniq = set(wp_manifest()['iso3'])
    return sorted(uniq)


@lru_cache()
def get_static_product_names():
    """
    Return the names of all static WorldPop data products.

    Returns
    -------
    List[str]
    """
    mdf = wp_manifest()
    uniq = set(mdf[~mdf.is_annual]['product_name'])
    return sorted(uniq)


@lru_cache()
def get_annual_product_names():
    """
    Return the names of all annual WorldPop data products for which at least one year is available.

    Returns
    -------
    List[str]
    """
    mdf = wp_manifest()
    uniq = set(mdf[mdf.is_annual]['product_name'])
    return sorted(uniq)


@lru_cache()
def get_all_annual_product_years():
    """
    Return the years for which at least one annual WorldPop product is available.

    Returns
    -------
    List[str]
    """
    mdf = wp_manifest()
    uniq = set(mdf[mdf.is_annual]['year'].astype(int))
    return sorted(uniq)


@lru_cache()
def get_all_dataset_names():
    """
    Return the names of all WorldPop dataset. For annual products, each available year counts as a
    separate dataset.

    Returns
    -------
    List[str]
    """
    uniq = set(wp_manifest()['dataset_name'])
    return sorted(uniq)


def extract_year(dataset_name):
    """
    Extract the year identifier from the name of an annual WorldPop dataset.

    Parameters
    ----------
    dataset_name : str
        The dataset name or file name of a WorldPop raster.

    Returns
    -------
    int
        The extracted year.

    Raises
    ------
    ValueError
        If the dataset name contains either no valid year identifier or several
        such identifiers.
    """
    bad_format_msg = (
        f"Bad format ('{dataset_name}'). Name of an annual dataset must "
        "contain exactly one valid year identifier. Perhaps you "
    )

    matched = _year_pattern.findall(dataset_name)

    if len(matched) != 1:
        # annual datasets must contain exactly one valid year identifier
        raise ValueError(bad_format_msg)

    matched = matched[0]
    year = int(matched[1:])

    if year < FIRST_YEAR or year > datetime.now().year:
        # check plausibility of the year
        raise ValueError(bad_format_msg)

    return year


@lru_cache()
def _cached_manifest_load():
    """
    Load the cleaned WorldPop manifest from local storage.

    Ensures the local manifest file is up-to-date by calling `build_wp_manifest()`.

    Returns
    ------
    pandas.DataFrame
        The cleaned local manifest containing metadata about all WorldPop raster datasets.

    Raises
    ------
    ValueError
        - If the manifest contains duplicated entries.
        - If the manifest implies that not all country rasters use the .tif format.
    """
    build_wp_manifest()  # trigger auto-update of local manifest upon first (uncached) function call
    mdf = pd.read_feather(_cleaned_manifest_fpath)

    if mdf.duplicated(['dataset_name', 'iso3']).any():
        raise ValueError(
            'Bad manifest! There should be no duplicated WorldPop datasets '
            'at the country level.'
        )

    raster_formats = [x[-1] for x in mdf.remote_path.str.split('.').values]
    if set(raster_formats) != {'tif'}:
        raise ValueError(
            'Unexpected file formats in manifest! All raster datasets should be .tif files.'
        )

    return mdf


def is_annual_product(product_name):
    """
    Return True if the requested data product is of the annual type.
    Return False otherwise.

    Parameters
    ----------
    product_name : str
        The name of the WorldPop data product of interest.

    Raises
    ------
    ValueError
        If the requested data product does not exist at all.
    """

    # raise an informative exception if user provides a year identifier
    # as part of the product name
    try:
        year = extract_year(product_name)
    except ValueError:
        year = None

    if year is not None:
        raise ValueError(
            "'product_name' should never contain a year identifier. For annual data "
            "products, please use the separate 'years' argument to specify one or "
            "more years of interest."
        )

    if product_name in get_static_product_names():
        is_annual = False
    elif product_name in get_annual_product_names():
        is_annual = True
    else:
        raise ValueError(
            f"'{product_name}' is neither a static nor an annual data product in WorldPop. "
            'You can list available data products as follows:\n\n'
            f'>>> from worldpoppy.manifest import get_static_product_names, get_annual_product_names\n'
            f'>>> print(get_static_product_names())\n'
            f'>>> print(get_annual_product_names())\n\n'
        )
    return is_annual


def _validate_isos(iso3_codes):
    """
    Ensure that all requested country codes exist.

    Parameters
    ----------
    iso3_codes : List[str]
        The three-letter ISO codes denoting countries of interest.

    Raises
    ------
    ValueError
        If the check fails, i.e., if WorldPop has no data whatsoever for one
        or more of the requested countries.
    """
    if unknown_isos := set(iso3_codes) - set(get_all_isos()):
        raise ValueError(
            f'WorldPop has no data for the following country codes: {unknown_isos}. '
            f'You can list all available country codes as follows:\n\n'
            f'>>> from worldpoppy.manifest import get_all_isos\n'
            f'>>> print(get_all_isos())'
        )


def _validate_years(years):
    """
    Ensure that all requested years for annual data products exist.

    Parameters
    ----------
    years : List[int] or str
        The years of interest or the 'all' keyword (str).

    Raises
    ------
    ValueError
        - If the `years` argument cannot be parsed.
        - If WorldPop has no annual raster data whatsoever for one or more requested years.
    """
    if isinstance(years, str):
        if years != 'all':
            raise ValueError(
                "'years' argument invalid. Must either be one or more years of "
                "interest (int or List[int]), or the 'all' keyword (str). You "
                f"passed the type {type(years)} instead."
            )
    else:
        if unknown_years := set(years) - set(get_all_annual_product_years()):
            raise ValueError(
                f'WorldPop has no annual data whatsoever for the following years: '
                f'{unknown_years}. You can list all available years as follows:\n\n'
                f'>>> from worldpoppy.manifest import get_all_annual_product_years\n'
                f'>>> print(get_all_annual_product_years())'
            )


def _strip_year(dataset_name):
    """
    Strip the year identifier from the name of an annual WorldPop dataset.

    Parameters
    ----------
    dataset_name : str
        The dataset name

    Returns
    -------
    str
        The dataset name with year identifier stripped.
    """
    year = extract_year(dataset_name)
    stripped = dataset_name.replace(f'_{year}', '')
    return stripped


def _looks_like_annual_name(dataset_name):
    """
    Return True if the format of 'dataset_name' is consistent with an annual WorldPop product.
    Return False otherwise.

    Parameters
    ----------
    dataset_name : str
        The dataset name

    Returns
    -------
    bool
    """
    is_annual = True
    try:
        extract_year(dataset_name)
    except ValueError:
        is_annual = False

    return is_annual


def _get_file_md5_hash(fpath):
    """
    Compute the MD5 hash of a file.

    Parameters
    ----------
    fpath : str or Path
        Path to the file whose MD5 hash is to be computed.

    Returns
    -------
    str
        The hexadecimal MD5 hash of the file contents.
    """
    hasher = hashlib.md5()
    with open(fpath, 'rb') as f:
        # read file in chunks to handle large files
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def _worldpop_ftp_download(
        remote_fpath,
        local_fpath=None,
        server='ftp.worldpop.org.uk',
        login='anonymous',
        pwd=''
):
    """
    Download a file from the WorldPop FTP server.

    Parameters
    ----------
    remote_fpath : str
        The remote path to the file on the WorldPop FTP server.
    local_fpath : str or Path, optional
        The local path where the file should be saved. If None,
        the file is downloaded directly into memory (default).
    server : str, optional
        The FTP server address. The default is 'ftp.worldpop.org.uk'.
    login : str, optional
        The FTP login username. The default is 'anonymous'.
    pwd : str, optional
        The FTP login password. The default is an empty string.

    Returns
    -------
    BytesIO or None
        If `local_fpath` is None, a BytesIO object containing the downloaded file is
        returned. Otherwise, the downloaded file is saved at `local_fpath` and the
        function returns None.

    Raises
    ------
    ValueError
        If there is an issue connecting to the FTP server.
    """

    # instantiate an FTP client
    try:
        ftp_client = ftplib.FTP(server, login, pwd, timeout=20)
    except gaierror:
        raise ValueError(
            f"WorldPop FTP server '{server}' is unknown. Please check the server address."
        )
    except Exception as e:
        raise ValueError(
            f'Could not connect to the WorldPop FTP server. Error: {e}'
        )

    if local_fpath is None:
        # download the remote file directly into memory
        byte_stream = BytesIO()
        ftp_client.retrbinary(f"RETR {remote_fpath}", byte_stream.write)
        byte_stream.seek(0)
        return byte_stream

    # download remote file to the local disk
    with open(local_fpath, 'wb') as file:
        ftp_client.retrbinary(f"RETR {remote_fpath}", file.write)


def _fetch_remote_manifest_hash():
    """
    Download the latest MD5 hash of the raw WorldPop dataset manifest.

    Returns
    -------
    str
    """
    byte_stream = _worldpop_ftp_download('/assets/wpgpDatasets.md5')
    result = byte_stream.read().decode('utf-8')
    remote_csv_hash = result.strip().split(' ')[0]
    return remote_csv_hash


def _update_local_manifest_hash(raw_csv_fpath):
    """
    Compute and store the MD5 hash of WorldPop's raw manifest CSV file.

    The hash is cached on disk for future integrity checks.

    Parameters
    ----------
    raw_csv_fpath : Path
        Path to the raw manifest CSV file.
    """
    with open(_raw_hash_fpath, 'w') as f:
        local_csv_hash = _get_file_md5_hash(raw_csv_fpath)
        f.write(local_csv_hash)


def _read_local_manifest_hash():
    """
    Read the previously stored MD5 hash of WorldPop's raw manifest CSV file.

    Returns
    -------
    str
        The cached MD5 hash string.
    """
    with open(_raw_hash_fpath, 'r') as f:
        local_csv_hash = f.read().strip()
    return local_csv_hash
