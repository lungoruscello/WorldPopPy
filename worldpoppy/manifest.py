"""
Utilities to download, clean, and filter the global WorldPop data manifest.
"""

import ftplib
import hashlib
import re
from datetime import datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from socket import gaierror
from tempfile import NamedTemporaryFile

import pandas as pd

__all__ = [
    "load_global_manifest",
    "filter_global_manifest",
    "get_all_isos",
    "get_annual_product_names",
    "get_static_product_names",
    "get_all_dataset_names",
    "extract_year"
]

FIRST_YEAR = 2000
ROOT_DIR = Path(__file__).parent
ASSET_DIR = ROOT_DIR / 'assets'

_year_pattern = re.compile(r'_\d{4}')
_raw_hash_fpath = ASSET_DIR / 'raw_manifest_hash.txt'
_cleaned_manifest_fpath = ASSET_DIR / 'manifest.feather'


@lru_cache()
def load_global_manifest():
    """
    Load the cleaned global WorldPop manifest from local storage.

    Ensures the local manifest file is up-to-date by calling `build_global_manifest()`,
    updates the local copy if necessary. The cleaned manifest is subsequently loaded
    from a Feather file and returned as a pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        The cleaned manifest containing metadata about all available WorldPop datasets.
        Each dataset can be downloaded separately for individual countries, which is also
        how the manifest file is organised --- with one entry for each available combination
        of a country and dataset.


    Raises
    ------
    ValueError
        - If the manifest contains duplicated entries at the country-dataset level.
        - If the manifest implies that not all country rasters use the .tif format.

    Notes
    -----
    The cleaned manifest includes the following columns:
        - idx:              numerical WorldPop dataset ID
        - country_numeric:  three-digit country code, as defined in ISO 3166-1 ('numeric-3')
        - iso3:             three-letter country code, as defined in ISO 3166-1 ('alpha-3')
        - country_name:     official English country name, as used in ISO 3166
        - dataset_name:     name of the WorldPop dataset, including year identifiers for annual datasets
        - remote_path:      remote dataset path on the WorldPop server
        - notes:            description of the WorldPop dataset
        - is_annual:        boolean flag for annual WorldPop datasets
        - product_name:     name of the WorldPop data product to which a specific dataset belongs.
                            For static WorldPop datasets, this is the same as the dataset name (see above).
                            For annual datasets, this is the dataset name with year identifier removed.
                            Note that the resulting product name is used for all user queries.
        - year:             The year of an annual WorldPop dataset. None for static datasets.
    """
    build_global_manifest()  # trigger auto-update of manifest upon first function call
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


def build_global_manifest(overwrite=False):
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
    # ingest that manifest using pandas, and update the local hash
    with NamedTemporaryFile() as tmp_file:
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

    return mdf


def filter_global_manifest(product_name, iso3_codes, years=None):
    """
    TODO

    Parameters
    ----------
    product_name : str
        The name of the WorldPop data product of interest.
    iso3_codes : str or List[str]
        The three-letter ISO code (or ISO codes) of the country (or countries) of interest.
    years : int or List[int], optional
        For annual data products, the year (or years) of interest. For static data products,
        this argument must be None (default).

    Returns
    -------
    pd.DataFrame
        The filtered manifest

    Raises
    ------
    ValueError
        If the requested data product is not available for all combinations of
        countries and years.
    """
    if isinstance(years, (int, float)):
        years = [years]
    if isinstance(iso3_codes, str):
        iso3_codes = [iso3_codes]
    iso3_codes = [x.upper() for x in iso3_codes]

    # check user arguments
    _check_isos_exist(iso3_codes)
    _check_product_exists(product_name, years)

    # load and subset the global WorldPop manifest
    mdf = load_global_manifest()
    filtered_df = mdf[(mdf.iso3.isin(iso3_codes)) & (mdf.product_name == product_name)].copy()
    if years is not None:
        filtered_df = filtered_df[filtered_df.year.isin(years)].copy()

    # Raise an informative exception if the data product does not cover all requested countries
    # and years (if any). Note that we adjust the error message depending on whether an annual
    # product was requested.
    if years is None:
        num_expected = len(iso3_codes)
        if len(filtered_df) < num_expected:
            raise ValueError(
                f"The requested data product ('{product_name}') is not available "
                'for all requested countries. You can check data coverage using '
                'the full WorldPop manifest:\n\n'
                '>>> from worldpoppy.manifest import load_global_manifest\n'
                '>>> manifest_df = load_global_manifest()\n'
                '>>> print(manifest_df)\n\n'
            )
    else:
        num_expected = len(iso3_codes) * len(years)
        if len(filtered_df) < num_expected:
            raise ValueError(
                f"The requested data product ('{product_name}') is not available "
                'for all requested countries and years. You can check data coverage '
                'using the full WorldPop manifest:\n\n'
                '>>> from worldpoppy.manifest import load_global_manifest\n'
                '>>> manifest_df = load_global_manifest()\n'
                '>>> print(manifest_df)\n\n'
            )

    # sanity check: duplicated records should never arise
    assert num_expected == len(filtered_df)

    return filtered_df


@lru_cache()
def get_all_isos():
    """
    Return the ISO3-codes of all countries for which at least one WorldPop dataset is available.

    Returns
    -------
    List[str]
    """
    uniq = set(load_global_manifest()['iso3'])
    return sorted(uniq)


@lru_cache()
def get_static_product_names():
    """
    Return the names of all static WorldPop data products.

    Returns
    -------
    List[str]
    """
    mdf = load_global_manifest()
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
    mdf = load_global_manifest()
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
    mdf = load_global_manifest()
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
    uniq = set(load_global_manifest()['dataset_name'])
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
        f"Bad format ('{dataset_name}'). Name of a dynamic dataset must "
        "contain exactly one valid year identifier."
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


def _check_isos_exist(iso3_codes):
    """
    Ensure that all requested country codes do exist. Raise an informative error if not.

    Parameters
    ----------
    iso3_codes : List[str]
        The three-letter ISO code codes of the countries of interest.

    Raises
    ------
    ValueError
        If the check fails, i.e., if WorldPop has no data whatsoever for one
        or more of the requested countries.
    """
    if unknown_isos := set(iso3_codes) - set(get_all_isos()):
        raise ValueError(
            f'WorldPop has no data for the following country codes: {unknown_isos}. You can '
            f'list all available country codes as follows:\n\n'
            f'>>> from worldpoppy.manifest import get_all_isos\n'
            f'>>> print(get_all_isos())'
        )


def _check_product_exists(product_name, years):
    """
    Ensure that the requested product does exist. Raise an informative error if not.

    Parameters
    ----------
    product_name : str
        The name of the WorldPop data product of interest.
    years : List[int], optional
        For annual data products, the years of interest. For static data products,
        this argument must be None (default).

    Raises
    ------
    ValueError
        If the check fails.
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
            "products, please use the separate 'years' argument to specify the year "
            "(or years) of interest."
        )

    # Raise an informative exception if the requested data product does not exist
    # for any country. Note that we use the 'year' argument to infer whether a static
    # or annual product was requested, and adjust error messages accordingly.
    if years is None:
        if product_name not in get_static_product_names():
            raise ValueError(
                f"'{product_name}' is not a static data product in WorldPop. "
                'You can list all available static data products as follows:\n\n'
                f'>>> from worldpoppy.manifest import get_static_product_names\n'
                f'>>> print(get_static_product_names())\n\n'
                "For annual data products, please provide the 'years' of interest "
                "as a separate argument."
            )
    else:
        if product_name not in get_annual_product_names():
            raise ValueError(
                f"'{product_name}' is not an annual data product in WorldPop. "
                'You can list all available annual data products as follows:\n\n'
                '>>> from worldpoppy.manifest import annual_product_names\n'
                '>>> print(get_annual_product_names())\n\n'
                "For static data products, please set the 'years' argument to None."
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
