from dataclasses import dataclass
import json
import os
from pathlib import Path
import tempfile

HISTORY_FILE = "~/.config/nless/history.json"
CONFIG_FILE = "~/.config/nless/config.json"


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
    theme: str = "default"
    keymap: str = "vim"
    status_format: str = (
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
