"""
Build the worldpoppy distributions (sdist and wheel) for a release.

The script first regenerates `readme_pypi.md` from `README.md` (with absolute
URLs, so that images and links work on PyPI) and copies it to `docs/`. It then
builds the distributions into `dist/` with `uv build`.

Usage (from the repo root):

    uv run scripts/make_dist.py
"""
import re
import shutil
import subprocess
from pathlib import Path

# Repo root (this script lives in `scripts/`, one level below it)
ROOT = Path(__file__).resolve().parent.parent

# Base URL for raw files on GitHub (for images)
GITHUB_RAW_URL = "https://raw.githubusercontent.com/lungoruscello/WorldPopPy/master/"

# Base URL for file blobs (for links)
GITHUB_BLOB_URL = "https://github.com/lungoruscello/WorldPopPy/blob/master/"


def clean_readme_for_pypi():
    """Prepare a cleaned version of README.md for PyPI."""
    with open(ROOT / "README.md", 'r', encoding='utf-8') as f:
        content = f.read()

    with open(ROOT / "readme_pypi.md", 'w', encoding='utf-8') as f:
        f.write(pypi_readme(content))

    print("✅ Generated readme_pypi.md with absolute URLs.")


def pypi_readme(content):
    """
    Return the README text for PyPI: without the icon and the badges, and with
    every relative link and image turned into an absolute GitHub URL (PyPI and
    the docs site show the README without the repository's files).
    """
    # Simplify header (Remove the Icon)
    content = re.sub(r'# WorldPopPy <img.*?>', '# WorldPopPy README', content)

    # Remove Badges
    # (every line that consists of a linked badge image: [![alt](image)](link))
    content = re.sub(r'^\[!\[[^\]]*\]\([^)]*\)\]\([^)]*\)[ \t]*\n', '', content, flags=re.MULTILINE)

    # Images: raw files, so that PyPI can display them. Markdown images
    # ![alt](./path) and HTML images src="./path" or src="path" (relative paths
    # only: no scheme such as https://)
    content = re.sub(r'(!\[[^\]]*\]\()\./', rf'\1{GITHUB_RAW_URL}', content)
    content = re.sub(r'src="(?:\./)?(?![a-z]+://)', f'src="{GITHUB_RAW_URL}', content)

    # Links: the files' pages on GitHub. Markdown links [text](./path) and HTML
    # links href="./path"
    content = re.sub(r'\]\(\./', f']({GITHUB_BLOB_URL}', content)
    content = content.replace('href="./', f'href="{GITHUB_BLOB_URL}')

    return content


def copy_readme_to_doc():
    docs_dir = ROOT / "docs"
    docs_dir.mkdir(exist_ok=True)
    shutil.copy(ROOT / "readme_pypi.md", docs_dir / "readme_pypi.md")
    print("✅ Copied readme to /docs.")


def build():
    # Clean previous builds
    shutil.rmtree(ROOT / "dist", ignore_errors=True)
    shutil.rmtree(ROOT / "build", ignore_errors=True)
    shutil.rmtree(ROOT / "WorldPopPy.egg-info", ignore_errors=True)

    clean_readme_for_pypi()
    copy_readme_to_doc()

    print("🚀 Starting Build...")
    subprocess.run(["uv", "build"], check=True, cwd=ROOT)

    print("\n------------------------------------------------")
    print("Build Complete. Next steps:")
    print("1. Check the distributions:  uvx twine check dist/*")
    print("2. Upload them to PyPI:      uv publish")
    print("------------------------------------------------")


if __name__ == '__main__':
    build()
