"""Tests for config loading, saving, and edge cases."""

import json
import os

from nless.config import NlessConfig, _load_config_json_file, load_config, save_config


class TestLoadConfigJsonFile:
    def test_creates_missing_file(self, tmp_path, monkeypatch):
        path = str(tmp_path / "sub" / "config.json")
        monkeypatch.setattr(
            "nless.config.os.path.expanduser", lambda p: path if "config" in p else p
        )
        result = _load_config_json_file(path, {"key": "default"})
        assert result == {"key": "default"}
        assert os.path.exists(path)

    def test_reads_valid_json(self, tmp_path):
        path = str(tmp_path / "config.json")
        with open(path, "w") as f:
            json.dump({"theme": "dracula"}, f)
        result = _load_config_json_file(path, {"theme": "default"})
        assert result == {"theme": "dracula"}

    def test_invalid_json_returns_defaults(self, tmp_path):
        path = str(tmp_path / "config.json")
        with open(path, "w") as f:
            f.write("not valid json {{{")
        result = _load_config_json_file(path, {"theme": "default"})
        assert result == {"theme": "default"}

    def test_empty_file_returns_defaults(self, tmp_path):
        path = str(tmp_path / "config.json")
        with open(path, "w") as f:
            f.write("")
        result = _load_config_json_file(path, {"key": "val"})
        assert result == {"key": "val"}


class TestLoadConfig:
    def test_returns_defaults_for_missing_file(self, tmp_path, monkeypatch):
        path = str(tmp_path / "config.json")
        monkeypatch.setattr("nless.config.CONFIG_FILE", path)
        config = load_config()
        assert isinstance(config, NlessConfig)
        assert config.theme == "default"
        assert config.keymap == "vim"

    def test_loads_saved_values(self, tmp_path, monkeypatch):
        path = str(tmp_path / "config.json")
        with open(path, "w") as f:
            json.dump({"theme": "dracula", "keymap": "less"}, f)
        monkeypatch.setattr("nless.config.CONFIG_FILE", path)
        config = load_config()
        assert config.theme == "dracula"
        assert config.keymap == "less"

    def test_ignores_unknown_keys(self, tmp_path, monkeypatch):
        path = str(tmp_path / "config.json")
        with open(path, "w") as f:
            json.dump({"theme": "nord", "unknown_future_key": True}, f)
        monkeypatch.setattr("nless.config.CONFIG_FILE", path)
        config = load_config()
        assert config.theme == "nord"
        assert not hasattr(config, "unknown_future_key")


class TestSaveConfig:
    def test_roundtrip(self, tmp_path, monkeypatch):
        path = str(tmp_path / "config.json")
        monkeypatch.setattr("nless.config.CONFIG_FILE", path)
        config = NlessConfig(theme="monokai", keymap="emacs")
        save_config(config)
        with open(path) as f:
            data = json.load(f)
        assert data["theme"] == "monokai"
        assert data["keymap"] == "emacs"

    def test_creates_parent_dirs(self, tmp_path, monkeypatch):
        path = str(tmp_path / "nested" / "dir" / "config.json")
        monkeypatch.setattr("nless.config.CONFIG_FILE", path)
        config = NlessConfig()
        save_config(config)
        assert os.path.exists(path)

    def test_atomic_write_no_partial_on_error(self, tmp_path, monkeypatch):
        path = str(tmp_path / "config.json")
        # Write valid config first
        with open(path, "w") as f:
            json.dump({"theme": "original"}, f)
        monkeypatch.setattr("nless.config.CONFIG_FILE", path)

        class BadConfig:
            @property
            def __dict__(self):
                raise RuntimeError("serialize error")

        try:
            save_config(BadConfig())
        except RuntimeError:
            pass
        # Original file should be untouched
        with open(path) as f:
            data = json.load(f)
        assert data["theme"] == "original"
