"""Version utilities for nothing-less."""

import json
import urllib.request

from importlib.metadata import version as get_pkg_version, PackageNotFoundError


def get_version() -> str:
    """Return the installed version string, or ``'unknown'``."""
    try:
        return get_pkg_version("nothing-less")
    except PackageNotFoundError:
        return "unknown"


def is_dev_install() -> bool:
    """Return True if nothing-less is installed in editable/dev mode."""
    try:
        import nless
        import pathlib

        pkg_path = pathlib.Path(nless.__file__).resolve().parent
        # Editable installs have a pyproject.toml in the parent directory
        return (pkg_path.parent / "pyproject.toml").exists()
    except Exception:
        return False


def fetch_latest_pypi_version() -> str | None:
    """Fetch the latest version of nothing-less from PyPI.

    Returns the version string, or None on any error.
    """
    try:
        url = "https://pypi.org/pypi/nothing-less/json"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
        return data["info"]["version"]
    except Exception:
        return None
