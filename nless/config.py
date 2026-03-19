from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile

HISTORY_FILE = "~/.config/nless/history.json"
CONFIG_FILE = "~/.config/nless/config.json"

_DEFAULT_STATUS_FORMAT = (
    "[{cursor_fg}]{sort}[/{cursor_fg}] [{muted}]|[/{muted}] "
    "[{cursor_fg}]{filter}[/{cursor_fg}] [{muted}]|[/{muted}] "
    "[{cursor_fg}]{search}[/{cursor_fg}] [{muted}]|[/{muted}] "
    "[{cursor_fg}]{position}[/{cursor_fg}] [{muted}]|[/{muted}] "
    "[{cursor_fg}]{delimiter}[/{cursor_fg}] [{muted}]|[/{muted}] "
    "[{cursor_fg}]{unique}[/{cursor_fg}]"
    "[{cursor_fg}]{time_window}[/{cursor_fg}]"
    "[{cursor_fg}]{skipped}[/{cursor_fg}]"
    "[{cursor_fg}]{session}[/{cursor_fg}]"
    "[{cursor_fg}]{pipe}[/{cursor_fg}]"
    "{tailing}{loading} "
    "[{cursor_fg}]{behind}[/{cursor_fg}]"
)


def _load_config_json_file(file_name: str, defaults):
    os.makedirs(os.path.dirname(os.path.expanduser(file_name)), exist_ok=True)
    if not os.path.exists(os.path.expanduser(file_name)):
        Path(os.path.expanduser(file_name)).touch()
    with open(os.path.expanduser(file_name), "r") as f:
        try:
            config = json.load(f)
        except (json.JSONDecodeError, ValueError):
            config = defaults
    return config


@dataclass
class NlessConfig:
    show_getting_started: bool = True
    last_seen_version: str = ""
    theme: str = "default"
    keymap: str = "vim"
    latest_pypi_version: str = ""
    last_update_check: float = 0.0
    status_format: str = _DEFAULT_STATUS_FORMAT


def get_release_notes(version: str) -> str | None:
    """Extract release notes for a given version from CHANGELOG.md.

    Returns the markdown content for that version, or None if not found.
    """
    changelog_path = os.path.join(os.path.dirname(__file__), "..", "CHANGELOG.md")
    if not os.path.exists(changelog_path):
        # Try installed package location
        try:
            from importlib.resources import files

            changelog_path = str(files("nless").joinpath("..", "CHANGELOG.md"))
        except Exception:
            return None
    if not os.path.exists(changelog_path):
        return None
    try:
        with open(changelog_path) as f:
            content = f.read()
    except OSError:
        return None
    # Find the section for this version
    import re

    pattern = rf"^## {re.escape(version)}\b.*$"
    match = re.search(pattern, content, re.MULTILINE)
    if not match:
        return None
    start = match.end()
    # Find next version header or end of file
    next_match = re.search(r"^## \d+\.\d+", content[start:], re.MULTILINE)
    if next_match:
        section = content[start : start + next_match.start()]
    else:
        section = content[start:]
    return section.strip() or None


def load_input_history():
    return _load_config_json_file(HISTORY_FILE, [])


def load_config() -> NlessConfig:
    defaults = {
        "show_getting_started": True,
        "theme": "default",
        "keymap": "vim",
        "status_format": _DEFAULT_STATUS_FORMAT,
    }
    data = _load_config_json_file(CONFIG_FILE, defaults)
    # Only pass known fields to avoid errors from stale config keys
    known = set(NlessConfig.__dataclass_fields__)
    filtered = {k: v for k, v in data.items() if k in known}
    return NlessConfig(**filtered)


def save_config(config: NlessConfig):
    path = os.path.expanduser(CONFIG_FILE)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config.__dict__, f, indent=4)
        os.replace(tmp_path, path)
    except BaseException:
        os.unlink(tmp_path)
        raise
