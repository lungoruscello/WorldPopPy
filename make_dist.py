import re
import shutil
import subprocess
from pathlib import Path

# Base URL for raw files on GitHub (for images)
GITHUB_RAW_URL = "https://raw.githubusercontent.com/lungoruscello/WorldPopPy/master/"

# Base URL for file blobs (for links)
GITHUB_BLOB_URL = "https://github.com/lungoruscello/WorldPopPy/blob/master/"


def clean_readme_for_pypi():
    """Prepare a cleaned version of README.md for PyPI."""
    with open("README.md", 'r', encoding='utf-8') as f:
        content = f.read()

    # Simplify header (Remove the Icon)
    #content = re.sub(r'# WorldPopPy <img.*?>', '# WorldPopPy', content)

    # Remove Badges
    content = re.sub(r'\[!\[PyPI Latest Release\].*?\)\n', '', content)
    content = re.sub(r'\[!\[License\].*?\)\n', '', content)

    # FIX HTML IMAGES: Convert relative paths to Absolute Raw GitHub URLs
    # Matches src="worldpoppy/..." or src="./worldpoppy/..."
    # We replace them with the raw URL so PyPI can display the image.
    content = content.replace('src="worldpoppy/', f'src="{GITHUB_RAW_URL}worldpoppy/')
    content = content.replace('src="./worldpoppy/', f'src="{GITHUB_RAW_URL}worldpoppy/')

    # FIX HTML LINKS: Convert relative file links to Absolute Blob URLs
    # e.g. href="./examples/..." -> href="https://github.com/.../blob/master/examples/..."
    content = content.replace('href="./examples/', f'href="{GITHUB_BLOB_URL}examples/')

    # FIX MARKDOWN LINKS
    # >> Catches [examples/](./examples/) -> (https://github.../examples/)
    content = content.replace('](./examples/)', f']({GITHUB_BLOB_URL}examples/)')

    # >> Catches [LICENSE.txt](./LICENSE.txt) -> (https://github.../LICENSE.txt)
    content = content.replace('](./LICENSE.txt)', f']({GITHUB_BLOB_URL}LICENSE.txt)')

    # Save to disk
    with open("readme_pypi.md", 'w', encoding='utf-8') as f:
        f.write(content)

    print("✅ Generated readme_pypi.md with absolute URLs.")


def copy_readme_to_doc():
    root = Path(__file__).parent.resolve()
    docs_dir = root / "docs"
    docs_dir.mkdir(exist_ok=True)
    shutil.copy(root / "readme_pypi.md", docs_dir / "readme_pypi.md")
    print("✅ Copied readme to /docs.")


def build():
    # Clean previous builds
    shutil.rmtree("dist", ignore_errors=True)
    shutil.rmtree("build", ignore_errors=True)
    shutil.rmtree("WorldPopPy.egg-info", ignore_errors=True)

    clean_readme_for_pypi()
    copy_readme_to_doc()

    print("🚀 Starting Build...")
    subprocess.run(["python", "-m", "build"], check=True)

    print("\n------------------------------------------------")
    print("Build Complete. Verification Step:")
    print("Run: twine check dist/*")
    print("------------------------------------------------")


if __name__ == '__main__':
    build()
