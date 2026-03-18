"""Version utilities for nothing-less."""

import json
import urllib.request

from importlib.metadata import version as get_pkg_version, PackageNotFoundError


def get_version() -> str:
    """Get the current version of the nothing-less package.

    Returns:
        str: The current version of the nothing-less package.
    """
    try:
        return get_pkg_version("nothing-less")
    except PackageNotFoundError:
        return "unknown"


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
