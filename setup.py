import re
from setuptools import setup, find_packages

import worldpoppy as wpp


def clean_readme_for_pypi():
    with open("README.md", 'r') as f:
        readme_lines = f.readlines()

    # strip out the project icon from the title line
    readme_lines[0] = re.sub(r'WorldPopPy <img.*?>', 'WorldPopPy', readme_lines[0])

    # remove shields
    filtered_lines = [l for l in readme_lines if not l.startswith("[![PyPI Latest Release]")]
    filtered_lines = [l for l in filtered_lines if not l.startswith("[![License]")]

    # remove example visuals
    filtered_lines = [l for l in filtered_lines if not l.startswith("<img src=")]

    # concatenate
    long_description = "".join(filtered_lines)

    return long_description


setup(
    name="WorldPopPy",
    version=wpp.__version__,
    description=wpp.__about__,
    url=wpp.__url__,
    author=wpp.__author__,
    license=wpp.__license__,
    long_description=clean_readme_for_pypi(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        'numpy',
        'matplotlib',
        'httpx',
        'nest_asyncio<2',
        'pqdm>=0.2',
        'tqdm',
        'backoff>=2,<3',
        'pandas',
        'pyarrow',
        'rioxarray',
        'xarray',
        'geopandas<2',
        'geopy',
        'pyproj',
        'shapely'
    ],
    extras_require={
        "dev": ["pytest>=7.0", "twine>=4.0.2", "bottleneck", "gdal>=3.0"],
    },
    package_data={
        'worldpoppy': [
            'assets/level0_500m_2000_2020_simplified_world.feather',  # used to convert GoeDataFrames into ISO codes
            'assets/italian_regions_simplified.feather'  # used in one worked example
        ],
    },
    classifiers=["Programming Language :: Python :: 3",
                 "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
                 ],
    python_requires='>=3.10',
)
