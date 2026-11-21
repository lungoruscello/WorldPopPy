"""
Maintainer script: rebuild the data manifest that ships with `worldpoppy`.

The manifest is built from WorldPop's meta-data API into a temporary location
first. Only if the build is complete (no failed API branches or data series)
does it replace the bundled manifest in `worldpoppy/assets/`. An incomplete
build is never shipped: the script stops and prints the build summary instead.

Usage (from the repository root; takes a few minutes):

    python scripts/update_bundled_manifest.py
"""
import shutil
import sys
import tempfile
from pathlib import Path

# Use this repository's worldpoppy, not an installed copy. (`python scripts/...`
# puts `scripts/` first on the import path, so `import worldpoppy` would otherwise
# find the installed package, and the new manifest would land in site-packages.)
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from worldpoppy.config import BUNDLED_MANIFEST_PATH, BUNDLED_MANIFEST_TIMESTAMP_PATH, RAW_MANIFEST_FNAME  # noqa: E402
from worldpoppy.manifest_builder import _build_manifest  # noqa: E402


def update_bundled_manifest(bundled_path=BUNDLED_MANIFEST_PATH, legacy_sidecar_path=BUNDLED_MANIFEST_TIMESTAMP_PATH):
    """
    Rebuild the bundled manifest. Return 0 on success, 1 if the build was
    incomplete or aborted (the bundled manifest is then left unchanged).
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        report = _build_manifest(Path(tmp_dir) / RAW_MANIFEST_FNAME)

        if not report.is_complete:
            print("\n❌ Build incomplete. The bundled manifest was NOT updated.\n")
            print(report.summary())
            return 1

        # The temporary directory may be on another filesystem: copy the new
        # manifest next to the target first, then move it into place atomically
        staged_path = bundled_path.with_name(f"{bundled_path.name}.new")
        try:
            shutil.copyfile(report.saved_to, staged_path)
            staged_path.replace(bundled_path)
        finally:
            staged_path.unlink(missing_ok=True)

    # The new manifest carries its build metadata, so the legacy sidecar is obsolete
    legacy_sidecar_path.unlink(missing_ok=True)

    print(f"\n✅ Bundled manifest updated: {report.num_files:,} raster files from {report.num_series} data series.")
    print(f"   Saved to: {bundled_path}")
    print("   Remember: commit the new manifest (and the removed timestamp sidecar),")
    print("   and drop the sidecar from `package-data` in pyproject.toml if still listed.")
    return 0


def main():
    """
    Run the update, after checking that the bundled manifest lives in this
    repository (and not, e.g. in an installed copy of worldpoppy).
    """
    if not BUNDLED_MANIFEST_PATH.resolve().is_relative_to(REPO_ROOT):
        print(
            f"❌ The bundled manifest path {BUNDLED_MANIFEST_PATH} is outside this repository "
            f"({REPO_ROOT}). Refusing to update it. Is another worldpoppy being imported?"
        )
        return 1
    return update_bundled_manifest()


if __name__ == '__main__':
    sys.exit(main())
