"""Version utilities for nothing-less."""

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
