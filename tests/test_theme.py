"""Tests for the theming system."""

import json

from nless.theme import (
    BUILTIN_THEMES,
    NlessTheme,
    get_all_themes,
    load_custom_themes,
    resolve_theme,
)


class TestNlessTheme:
    def test_default_theme_exists(self):
        assert "default" in BUILTIN_THEMES

    def test_all_builtin_themes_present(self):
        expected = {
            "default",
            "dracula",
            "monokai",
            "nord",
            "solarized-dark",
            "solarized-light",
            "gruvbox",
            "tokyo-night",
            "catppuccin-mocha",
            "catppuccin-latte",
        }
        assert set(BUILTIN_THEMES.keys()) == expected

    def test_theme_name_matches_key(self):
        for key, theme in BUILTIN_THEMES.items():
            assert theme.name == key

    def test_markup_helper(self):
        theme = NlessTheme(highlight="#ff0000")
        result = theme.markup("highlight", "hello")
        assert result == "[#ff0000]hello[/#ff0000]"

    def test_markup_accent(self):
        theme = NlessTheme(accent="#abc123")
        result = theme.markup("accent", "42")
        assert result == "[#abc123]42[/#abc123]"

    def test_highlight_re_matches(self):
        theme = NlessTheme(highlight="#00ff00")
        match = theme.highlight_re.search("[#00ff00]text[/#00ff00]")
        assert match is not None
        assert match.group(1) == "text"

    def test_highlight_re_strips(self):
        theme = NlessTheme(highlight="#abcdef")
        result = theme.highlight_re.sub(r"\1", "[#abcdef]value[/#abcdef]")
        assert result == "value"

    def test_highlight_re_no_match_on_different_color(self):
        theme = NlessTheme(highlight="#00ff00")
        match = theme.highlight_re.search("[#ff0000]text[/#ff0000]")
        assert match is None


class TestCustomThemes:
    def test_load_from_nonexistent_dir(self):
        themes = load_custom_themes("/nonexistent/path/themes")
        assert themes == {}

    def test_load_valid_custom_theme(self, tmp_path):
        theme_data = {"name": "my-theme", "cursor_bg": "#ff0000", "accent": "#ff6600"}
        theme_file = tmp_path / "my-theme.json"
        theme_file.write_text(json.dumps(theme_data))

        themes = load_custom_themes(str(tmp_path))
        assert "my-theme" in themes
        assert themes["my-theme"].cursor_bg == "#ff0000"
        assert themes["my-theme"].accent == "#ff6600"
        # Inherited from default
        assert themes["my-theme"].header_bg == BUILTIN_THEMES["default"].header_bg

    def test_partial_override_inherits_defaults(self, tmp_path):
        theme_data = {"name": "partial"}
        theme_file = tmp_path / "partial.json"
        theme_file.write_text(json.dumps(theme_data))

        themes = load_custom_themes(str(tmp_path))
        default = BUILTIN_THEMES["default"]
        partial = themes["partial"]
        assert partial.cursor_bg == default.cursor_bg
        assert partial.highlight == default.highlight

    def test_invalid_json_skipped(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json {{{")

        themes = load_custom_themes(str(tmp_path))
        assert themes == {}

    def test_missing_name_skipped(self, tmp_path):
        theme_data = {"cursor_bg": "#ff0000"}
        theme_file = tmp_path / "noname.json"
        theme_file.write_text(json.dumps(theme_data))

        themes = load_custom_themes(str(tmp_path))
        assert themes == {}

    def test_non_json_files_ignored(self, tmp_path):
        txt_file = tmp_path / "readme.txt"
        txt_file.write_text("not a theme")

        themes = load_custom_themes(str(tmp_path))
        assert themes == {}

    def test_unknown_keys_ignored(self, tmp_path):
        theme_data = {"name": "extra", "unknown_field": "value", "accent": "#123456"}
        theme_file = tmp_path / "extra.json"
        theme_file.write_text(json.dumps(theme_data))

        themes = load_custom_themes(str(tmp_path))
        assert "extra" in themes
        assert themes["extra"].accent == "#123456"


class TestResolveTheme:
    def test_default_resolution(self):
        theme = resolve_theme()
        assert theme.name == "default"

    def test_cli_wins_over_config(self):
        theme = resolve_theme(cli_theme="dracula", config_theme="nord")
        assert theme.name == "dracula"

    def test_config_used_when_no_cli(self):
        theme = resolve_theme(cli_theme=None, config_theme="nord")
        assert theme.name == "nord"

    def test_unknown_name_falls_back_to_default(self):
        theme = resolve_theme(cli_theme="nonexistent")
        assert theme.name == "default"


class TestGetAllThemes:
    def test_returns_all_builtin_themes(self):
        themes = get_all_themes()
        for name in BUILTIN_THEMES:
            assert name in themes
