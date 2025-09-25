"""Version utilities for nothing-less."""

import sys
from pathlib import Path
import tomllib


def get_version() -> str:
    """Get version from pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    
    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
        return data["project"]["version"]
    except (FileNotFoundError, KeyError, Exception):
        return "unknown"
