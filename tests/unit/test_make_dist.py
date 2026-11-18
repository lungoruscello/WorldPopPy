import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "make_dist.py"


@pytest.fixture
def make_dist():
    """Load the maintainer script as a module (scripts/ is not a package)."""
    spec = importlib.util.spec_from_file_location("make_dist", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pypi_readme_makes_relative_links_and_images_absolute(make_dist):
    """
    The README for PyPI (and the docs site) has absolute GitHub URLs for every
    relative link and image, since neither shows the repository's files.
    Absolute URLs stay as they are; the icon and the badges are removed.
    """
    blob, raw = make_dist.GITHUB_BLOB_URL, make_dist.GITHUB_RAW_URL
    readme = "\n".join([
        '# WorldPopPy <img src="worldpoppy/assets/icon.png" alt="icon"/>',
        "[![Badge](https://img.shields.io/badge.svg)](https://example.org)",
        "See the [guide](./docs/guide.md) and the [`examples/`](./examples/) folder.",
        '<a href="./examples/demo.py"><img src="./worldpoppy/assets/demo.png"/></a>',
        '<img src="worldpoppy/assets/other.png"/>',
        "![Map](./worldpoppy/assets/map.png)",
        "Docs: [site](https://worldpoppy.readthedocs.io/) and ![remote](https://example.org/a.png)",
    ])

    lines = make_dist.pypi_readme(readme).splitlines()

    assert lines == [
        "# WorldPopPy README",
        f"See the [guide]({blob}docs/guide.md) and the [`examples/`]({blob}examples/) folder.",
        f'<a href="{blob}examples/demo.py"><img src="{raw}worldpoppy/assets/demo.png"/></a>',
        f'<img src="{raw}worldpoppy/assets/other.png"/>',
        f"![Map]({raw}worldpoppy/assets/map.png)",
        "Docs: [site](https://worldpoppy.readthedocs.io/) and ![remote](https://example.org/a.png)",
    ]
