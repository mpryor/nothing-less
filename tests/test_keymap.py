"""Tests for the keymap system."""

import json

from nless.keymap import (
    BUILTIN_KEYMAPS,
    get_all_keymaps,
    load_custom_keymaps,
    resolve_keymap,
)


class TestNlessKeymap:
    def test_vim_keymap_exists(self):
        assert "vim" in BUILTIN_KEYMAPS

    def test_all_builtin_keymaps_present(self):
        expected = {"vim", "less", "emacs"}
        assert set(BUILTIN_KEYMAPS.keys()) == expected

    def test_keymap_name_matches_key(self):
        for key, keymap in BUILTIN_KEYMAPS.items():
            assert keymap.name == key

    def test_vim_has_empty_bindings(self):
        assert BUILTIN_KEYMAPS["vim"].bindings == {}

    def test_less_has_overrides(self):
        less = BUILTIN_KEYMAPS["less"]
        assert "table.page_down" in less.bindings
        assert less.bindings["table.page_down"] == "space,f,ctrl+f"

    def test_emacs_has_overrides(self):
        emacs = BUILTIN_KEYMAPS["emacs"]
        assert "table.cursor_down" in emacs.bindings
        assert emacs.bindings["table.cursor_down"] == "ctrl+n,down"


class TestCustomKeymaps:
    def test_load_from_nonexistent_dir(self):
        keymaps = load_custom_keymaps("/nonexistent/path/keymaps")
        assert keymaps == {}

    def test_load_valid_custom_keymap(self, tmp_path):
        keymap_data = {
            "name": "my-keymap",
            "bindings": {"app.search": "ctrl+slash"},
        }
        keymap_file = tmp_path / "my-keymap.json"
        keymap_file.write_text(json.dumps(keymap_data))

        keymaps = load_custom_keymaps(str(tmp_path))
        assert "my-keymap" in keymaps
        assert keymaps["my-keymap"].bindings["app.search"] == "ctrl+slash"

    def test_extends_vim_by_default(self, tmp_path):
        keymap_data = {
            "name": "custom",
            "bindings": {"app.search": "ctrl+slash"},
        }
        keymap_file = tmp_path / "custom.json"
        keymap_file.write_text(json.dumps(keymap_data))

        keymaps = load_custom_keymaps(str(tmp_path))
        custom = keymaps["custom"]
        # vim has empty bindings, so custom should only have its own
        assert custom.bindings == {"app.search": "ctrl+slash"}

    def test_extends_less_merges(self, tmp_path):
        keymap_data = {
            "name": "my-less",
            "extends": "less",
            "bindings": {"app.search": "ctrl+slash"},
        }
        keymap_file = tmp_path / "my-less.json"
        keymap_file.write_text(json.dumps(keymap_data))

        keymaps = load_custom_keymaps(str(tmp_path))
        my_less = keymaps["my-less"]
        # Should have the less override for page_down
        assert my_less.bindings["table.page_down"] == "space,f,ctrl+f"
        # Plus the user override
        assert my_less.bindings["app.search"] == "ctrl+slash"

    def test_extends_unknown_falls_back_to_vim(self, tmp_path):
        keymap_data = {
            "name": "unknown-base",
            "extends": "nonexistent",
            "bindings": {"app.search": "ctrl+slash"},
        }
        keymap_file = tmp_path / "unknown-base.json"
        keymap_file.write_text(json.dumps(keymap_data))

        keymaps = load_custom_keymaps(str(tmp_path))
        # Should fall back to vim (empty bindings) + user overrides
        assert keymaps["unknown-base"].bindings == {"app.search": "ctrl+slash"}

    def test_invalid_json_skipped(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json {{{")

        keymaps = load_custom_keymaps(str(tmp_path))
        assert keymaps == {}

    def test_missing_name_skipped(self, tmp_path):
        keymap_data = {"bindings": {"app.search": "ctrl+slash"}}
        keymap_file = tmp_path / "noname.json"
        keymap_file.write_text(json.dumps(keymap_data))

        keymaps = load_custom_keymaps(str(tmp_path))
        assert keymaps == {}

    def test_non_json_files_ignored(self, tmp_path):
        txt_file = tmp_path / "readme.txt"
        txt_file.write_text("not a keymap")

        keymaps = load_custom_keymaps(str(tmp_path))
        assert keymaps == {}

    def test_missing_bindings_uses_empty(self, tmp_path):
        keymap_data = {"name": "empty"}
        keymap_file = tmp_path / "empty.json"
        keymap_file.write_text(json.dumps(keymap_data))

        keymaps = load_custom_keymaps(str(tmp_path))
        assert "empty" in keymaps
        assert keymaps["empty"].bindings == {}


class TestResolveKeymap:
    def test_default_resolution(self):
        keymap = resolve_keymap()
        assert keymap.name == "vim"

    def test_cli_wins_over_config(self):
        keymap = resolve_keymap(cli_keymap="less", config_keymap="emacs")
        assert keymap.name == "less"

    def test_config_used_when_no_cli(self):
        keymap = resolve_keymap(cli_keymap=None, config_keymap="emacs")
        assert keymap.name == "emacs"

    def test_unknown_name_falls_back_to_vim(self):
        keymap = resolve_keymap(cli_keymap="nonexistent")
        assert keymap.name == "vim"


class TestHelpKeyResolution:
    def test_vim_default_keys_unchanged(self):
        from nless.help import _resolve_key

        # vim keymap has empty bindings, so default key should be returned
        assert _resolve_key("app.search", "/", {}) == "/"

    def test_less_overrides_shown(self):
        from nless.help import _resolve_key

        less_bindings = BUILTIN_KEYMAPS["less"].bindings
        key = _resolve_key("table.page_down", "ctrl+d", less_bindings)
        assert key == "space / f / ctrl+f"

    def test_emacs_overrides_shown(self):
        from nless.help import _resolve_key

        emacs_bindings = BUILTIN_KEYMAPS["emacs"].bindings
        key = _resolve_key("table.cursor_down", "j", emacs_bindings)
        assert key == "ctrl+n / down"

    def test_none_binding_id_returns_default(self):
        from nless.help import _resolve_key

        assert _resolve_key(None, "1-9", {"app.search": "ctrl+s"}) == "1-9"

    def test_unoverridden_binding_returns_default(self):
        from nless.help import _resolve_key

        # emacs doesn't override buffer.sort
        emacs_bindings = BUILTIN_KEYMAPS["emacs"].bindings
        key = _resolve_key("buffer.sort", "s", emacs_bindings)
        assert key == "s"


class TestGetAllKeymaps:
    def test_includes_builtins(self):
        keymaps = get_all_keymaps()
        assert "vim" in keymaps
        assert "less" in keymaps
        assert "emacs" in keymaps
