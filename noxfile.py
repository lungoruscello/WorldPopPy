"""
Development tasks for worldpoppy, run with nox (https://nox.thea.codes).

Each task ("session") runs in its own fresh environment. List all sessions
with their descriptions:

    uv run nox -l

Setup
-----
Install uv (https://docs.astral.sh/uv/), then create the development
environment from the lockfile (`uv.lock`):

    uv sync

If a conda or mamba environment is active in your shell, deactivate it first
(e.g., `mamba deactivate`). Otherwise, it leaks variables such as `PROJ_DATA`
and `GDAL_DATA`, which make rasterio read the conda environment's (possibly
incompatible) PROJ and GDAL data files. The sessions below remove these
variables themselves, but plain `uv run pytest` does not.

Test levels
-----------
- `tests/unit`: synthetic data and fakes only. All network access is blocked.
- `tests/integration`: real data and bundled assets (marked `integration`).
  These tests use the normal worldpoppy cache and may download small files
  that are not cached yet.
- `tests/e2e`: full traversal of WorldPop's API. Opt-in only, via
  `WPY_RUN_HEAVY_TESTS=1`, which also enables the heavy URL checks in
  `tests/integration`.

Everyday use
------------
    uv run nox                     # same as `uv run nox -s tests`
    uv run nox -s tests -- -x      # arguments after `--` go to pytest

Before a release
----------------
    uv run nox -s tests_heavy      # everything, incl. e2e (contacts WorldPop)
    uv run nox -s versions         # the built wheel on all supported Pythons
    uv run nox -s docs             # the documentation, warnings as errors
    uv run scripts/make_dist.py    # build the distributions

Dependencies
------------
`tests`, `tests_heavy` and `docs` install the exact versions pinned in
`uv.lock`, so that results do not change when a dependency releases a new
version. `versions` deliberately ignores the lockfile: it installs the built
wheel with freshly resolved dependencies, as a user would.

Supported Python versions
-------------------------
`versions` tests exactly the Python versions declared by the classifiers in
`pyproject.toml` (single source of truth). To try a new version first:

    uv run nox -s versions --force-python 3.15

If it passes, add its classifier (e.g. "Programming Language :: Python :: 3.15").

Minimum versions of rasterio and GDAL
-------------------------------------
The numerical accuracy of merging and warping depends on the versions of
rasterio and the GDAL it bundles (e.g., older versions lose population when
warping). `accuracy_versions` runs the accuracy tests (marker `accuracy`) on
the hardest case only, for the minimum rasterio version and the latest series,
with all other dependencies resolved to match. Run it by hand when changing the
minimum versions:

    uv run nox -s accuracy_versions
"""
import os
import shutil
from pathlib import Path

import nox

ROOT = Path(__file__).parent
PYPROJECT = nox.project.load_toml(ROOT / "pyproject.toml")

# Python versions on which the built wheel is tested (session `versions`),
# read from the classifiers in `pyproject.toml`
SUPPORTED_PYTHONS = nox.project.python_versions(PYPROJECT)

# Python version for all other sessions (matches `.python-version`)
DEV_PYTHON = "3.12"

# rasterio versions tested by the session `accuracy_versions` (each bundles its
# own GDAL): the minimum declared in `pyproject.toml` and the latest 1.5.x.
# Measured on 2026-11-15: 1.4.2 and older fail (eager merges differ from the
# source values), 1.4.3 passes, hence the minimum `rasterio>=1.4.3`. To test
# below the minimum, lower it in `pyproject.toml` first. Also the hardest test
# case, selected with `pytest -k`
RASTERIO_SERIES = ["1.4.3", "1.5"]
HARDEST_ACCURACY_CASE = "Caucasus"

nox.needs_version = ">=2025.2.9"  # for `nox.project.dependency_groups`
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["tests"]

# An active conda/mamba environment sets these variables, which point
# rasterio's bundled PROJ and GDAL to the conda environment's data files
# (see "Setup" above). Remove them for all sessions.
for _var in ("PROJ_DATA", "PROJ_LIB", "GDAL_DATA", "GDAL_DRIVER_PATH"):
    os.environ.pop(_var, None)


# --- Sessions ---

@nox.session(python=DEV_PYTHON)
def tests(session):
    """Run the unit and integration tests (everyday use)."""
    _install_locked(session, "test")
    session.run("pytest", "tests/unit", "tests/integration", *session.posargs)


@nox.session(python=DEV_PYTHON)
def tests_heavy(session):
    """Run all tests, incl. e2e and heavy URL checks (before a release; contacts WorldPop)."""
    _install_locked(session, "test")
    session.run(
        "pytest", "tests/unit", "tests/integration", "tests/e2e", *session.posargs,
        env={"WPY_RUN_HEAVY_TESTS": "1"},
    )


@nox.session(python=SUPPORTED_PYTHONS)
def versions(session):
    """Test the built wheel, with freshly resolved dependencies, on all supported Pythons."""
    tmp_dir = Path(session.create_tmp())

    # Build the wheel and install it like a user would (no lockfile)
    dist_dir = tmp_dir / "dist"
    shutil.rmtree(dist_dir, ignore_errors=True)
    session.run("uv", "build", "--wheel", "--out-dir", str(dist_dir), str(ROOT), external=True)
    wheel = next(dist_dir.glob("*.whl"))
    session.install(str(wheel), *nox.project.dependency_groups(PYPROJECT, "test"))

    # Run the tests from a copy, away from the repo. Otherwise, pytest would
    # put the repo root on `sys.path` and test the source, not the wheel.
    run_dir = tmp_dir / "run"
    shutil.rmtree(run_dir, ignore_errors=True)
    shutil.copytree(ROOT / "tests", run_dir / "tests")
    shutil.copy(ROOT / "pyproject.toml", run_dir)  # for the pytest configuration

    with session.chdir(run_dir):
        session.run(
            "python", "-c",
            "import worldpoppy; "
            "assert 'site-packages' in worldpoppy.__file__, worldpoppy.__file__; "
            "print('Testing the installed wheel:', worldpoppy.__file__)",
        )
        session.run(
            "pytest", "tests/unit", "tests/integration",
            # These tests cover a maintainer script in `scripts/`, which is
            # not part of the wheel (the `tests` session covers them)
            "--ignore=tests/unit/test_update_bundled_manifest.py",
            *session.posargs,
        )


@nox.session(python=DEV_PYTHON)
@nox.parametrize("rasterio", RASTERIO_SERIES)
def accuracy_versions(session, rasterio):
    """Run the accuracy tests on the hardest case, for one rasterio series (by hand)."""
    # No lockfile: uv resolves all other dependencies to match this series
    session.install(".", f"rasterio=={rasterio}.*", *nox.project.dependency_groups(PYPROJECT, "test"))
    session.run(
        "python", "-c",
        "import rasterio, rioxarray; "
        "print(f'rasterio {rasterio.__version__}, GDAL {rasterio.__gdal_version__}, "
        "rioxarray {rioxarray.__version__}')",
    )
    session.run(
        "pytest", "tests/integration", "-m", "accuracy", "-k", HARDEST_ACCURACY_CASE, "-s",
        *session.posargs,
    )


@nox.session(python=DEV_PYTHON)
def docs(session):
    """Build the documentation, treating warnings as errors."""
    _install_locked(session, "docs")
    out_dir = Path(session.create_tmp()) / "html"
    session.run("sphinx-build", "-W", "--keep-going", "-b", "html", "docs", str(out_dir))
    session.log(f"Documentation built in {out_dir}")


# --- Helpers ---

def _install_locked(session, *groups):
    """
    Install worldpoppy and the given dependency groups into the session's
    environment, with the exact versions pinned in `uv.lock`.
    """
    group_args = [f"--group={group}" for group in groups]
    session.run_install(
        "uv", "sync", "--locked", "--no-default-groups", *group_args,
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
