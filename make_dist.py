import re
import subprocess
import shutil
from pathlib import Path


def clean_readme_for_pypi():
    """
    Prepare a cleaned version of README.md without project icon, badges,
    or inline example images.

    Saves the result as readme_pypi.md, which is referenced in pyproject.toml.
    """
    with open("README.md", 'r') as f:
        readme_lines = f.readlines()

    # simplify header line
    readme_lines[0] = re.sub(r'WorldPopPy <img.*?>', 'WorldPopPy README', readme_lines[0])

    # remove shields
    filtered_lines = [l for l in readme_lines if not l.startswith("[![PyPI Latest Release]")]
    filtered_lines = [l for l in filtered_lines if not l.startswith("[![License]")]

    # remove example visuals
    filtered_lines = [l for l in filtered_lines if not l.startswith("<img src=")]

    # concatenate lines
    long_description = "".join(filtered_lines)

    # save to disk
    with open("readme_pypi.md", 'w', encoding='utf-8') as f:
        f.writelines(long_description)


def copy_readme_to_doc():
    """
    Copy README_PYPI.md into the docs/ directory
    """
    root = Path(__file__).parent.resolve()
    shutil.copy(
        root / "readme_pypi.md",
        root / "docs" / "readme_pypi.md"
    )


def build():
    """
    Runs the build process using `python -m build`.

    This generates both .whl and .tar.gz files in the dist/ directory.
    """
    clean_readme_for_pypi()
    copy_readme_to_doc()
    subprocess.run(["python", "-m", "build"], check=True)


if __name__ == '__main__':
    build()
