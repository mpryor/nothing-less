#!/usr/bin/env python3
"""Run nless with WSL clipboard detection bypassed to simulate a Wayland-only environment.

Usage:
    poetry run python scripts/test-wayland-clipboard.py < example_data/people-1000.csv

Navigate to any cell and press y — you should see:
    "Clipboard not available — install wl-clipboard (wl-copy/wl-paste)."
"""

import os.path as _osp

_orig = _osp.isfile
_osp.isfile = lambda p: False if "/proc/version" in str(p) else _orig(p)

from nless.cli import main  # noqa: E402

main()
