import importlib.util
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from worldpoppy.manifest_builder import ManifestBuildReport, _read_manifest_info, _write_manifest

SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "update_bundled_manifest.py"


# --- Fixtures ---

@pytest.fixture
def script():
    """Load the maintainer script as a module (scripts/ is not a package)."""
    spec = importlib.util.spec_from_file_location("update_bundled_manifest", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- Helpers ---

def _fake_build(complete):
    """
    Return a stand-in for `_build_manifest` that writes a tiny manifest and
    reports a complete build, or an incomplete one (with a failed series).
    """
    def fake_build_manifest(output_path):
        _write_manifest(pd.DataFrame({"iso3": ["AFG"]}), output_path)
        report = ManifestBuildReport(num_series=2, num_files=1, saved_to=output_path)
        if not complete:
            report.failed_series.append("pop/bad_series")
        return report

    return fake_build_manifest


# --- Tests ---

def test_complete_build_replaces_bundled_manifest(script, monkeypatch, tmp_path):
    """
    A complete build replaces the bundled manifest (now with embedded build
    metadata) and removes the obsolete legacy sidecar.
    """
    bundled, sidecar = tmp_path / "raw_api_manifest.feather", tmp_path / "raw_api_manifest_timestamp.txt"
    bundled.write_bytes(b"old manifest")
    sidecar.write_text("2025-12-08T19:24:34")
    monkeypatch.setattr(script, "_build_manifest", _fake_build(complete=True))

    exit_code = script.update_bundled_manifest(bundled_path=bundled, legacy_sidecar_path=sidecar)

    assert exit_code == 0
    assert _read_manifest_info(bundled, source="bundled").built_at is not None
    assert not sidecar.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["raw_api_manifest.feather"]  # no staging leftovers


def test_incomplete_build_leaves_bundled_manifest_unchanged(script, monkeypatch, tmp_path, capsys):
    """
    An incomplete build is never shipped: the bundled manifest and its sidecar
    stay as they are, and the script exits with an error and the summary.
    """
    bundled, sidecar = tmp_path / "raw_api_manifest.feather", tmp_path / "raw_api_manifest_timestamp.txt"
    bundled.write_bytes(b"old manifest")
    sidecar.write_text("2025-12-08T19:24:34")
    monkeypatch.setattr(script, "_build_manifest", _fake_build(complete=False))

    exit_code = script.update_bundled_manifest(bundled_path=bundled, legacy_sidecar_path=sidecar)

    assert exit_code == 1
    assert bundled.read_bytes() == b"old manifest"
    assert sidecar.exists()
    assert "NOT updated" in capsys.readouterr().out


def test_refuses_to_update_a_manifest_outside_the_repo(script, monkeypatch, tmp_path, capsys):
    """
    If the bundled-manifest path lies outside this repository (e.g. because an
    installed worldpoppy got imported), the script refuses to run at all.
    """
    built = []
    monkeypatch.setattr(script, "BUNDLED_MANIFEST_PATH", tmp_path / "raw_api_manifest.feather")
    monkeypatch.setattr(script, "_build_manifest", lambda output_path: built.append(output_path))

    assert script.main() == 1
    assert built == []
    assert "outside this repository" in capsys.readouterr().out


def test_script_uses_this_repos_worldpoppy(tmp_path):
    """
    Run as `python scripts/update_bundled_manifest.py`, the script must import
    this repository's worldpoppy, not an installed copy (whose manifest would
    then be updated instead). Checked in a fresh process started outside the
    repo, since the test process has already imported worldpoppy. (The script
    is only loaded, not run: no build, no requests.)
    """
    probe = (
        "import runpy, sys\n"
        f"runpy.run_path({str(SCRIPT_PATH)!r}, run_name='loaded_by_test')\n"
        "print(sys.modules['worldpoppy'].__file__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], cwd=tmp_path, capture_output=True, text=True, timeout=120
    )

    repo_root = SCRIPT_PATH.parents[1]
    assert result.returncode == 0, result.stderr[-2000:]
    assert Path(result.stdout.strip().splitlines()[-1]).is_relative_to(repo_root)

