from dataclasses import dataclass
import os
import json

HISTORY_FILE = "~/.config/nless/history.json"
CONFIG_FILE = "~/.config/nless/config.json"


def _load_config_json_file(file_name: str, defaults):
    os.makedirs(os.path.dirname(os.path.expanduser(file_name)), exist_ok=True)
    if not os.path.exists(os.path.expanduser(file_name)):
        open(os.path.expanduser(file_name), "w").close()
    with open(os.path.expanduser(file_name), "r") as f:
        try:
            config = json.load(f)
        except:
            config = defaults
            save_config(NlessConfig(**config))
    return config


@dataclass
class NlessConfig:
    show_getting_started: bool = True


def load_input_history():
    return _load_config_json_file(HISTORY_FILE, [])


def load_config() -> NlessConfig:
    return NlessConfig(
        **_load_config_json_file(CONFIG_FILE, {"show_getting_started": True})
    )


def save_config(config: NlessConfig):
    with open(os.path.expanduser(CONFIG_FILE), "w") as f:
        json.dump(config.__dict__, f, indent=4)
