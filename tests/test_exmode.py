"""Tests for ex-mode command parsing and dispatching."""

import difflib
import re

import pytest

from nless.app_exmode import ExModeMixin, _COMMAND_ALIASES, _is_substitution
from nless.app_substitute import _parse_substitution
from nless.delimiter import split_line
from nless.types import Column


# ── _parse_substitution tests (moved from test_substitute.py) ────────


class TestParseSubstitution:
    """Tests for _parse_substitution()."""

    def test_basic_substitution(self):
        result = _parse_substitution("s/foo/bar/")
        assert result is not None
        pat, repl, all_cols = result
        assert pat.pattern == "foo"
        assert repl == "bar"
        assert all_cols is False

    def test_global_flag(self):
        result = _parse_substitution("s/foo/bar/g")
        assert result is not None
        _, _, all_cols = result
        assert all_cols is True

    def test_capture_groups(self):
        result = _parse_substitution(r"s/(\d+)ms/\1/")
        assert result is not None
        pat, repl, _ = result
        assert pat.pattern == r"(\d+)ms"
        assert repl == r"\1"

    def test_custom_separator(self):
        result = _parse_substitution("s|foo|bar|")
        assert result is not None
        pat, repl, _ = result
        assert pat.pattern == "foo"
        assert repl == "bar"

    def test_empty_replacement(self):
        result = _parse_substitution("s/foo//")
        assert result is not None
        pat, repl, _ = result
        assert pat.pattern == "foo"
        assert repl == ""

    def test_no_trailing_separator(self):
        result = _parse_substitution("s/foo/bar")
        assert result is not None
        pat, repl, _ = result
        assert pat.pattern == "foo"
        assert repl == "bar"

    def test_invalid_too_short(self):
        assert _parse_substitution("s/") is None

    def test_invalid_no_s_prefix(self):
        assert _parse_substitution("x/foo/bar/") is None

    def test_invalid_empty_pattern(self):
        assert _parse_substitution("s//bar/") is None

    def test_invalid_bad_regex(self):
        assert _parse_substitution("s/[invalid/bar/") is None

    def test_escaped_separator(self):
        result = _parse_substitution("s/a\\/b/c/")
        assert result is not None
        pat, repl, _ = result
        assert pat.pattern == "a/b"
        assert repl == "c"


# ── Substitution in split_line (moved from test_substitute.py) ───────


class TestSubstitutionInSplitLine:
    """Tests for substitution applied through split_line()."""

    def test_single_column_substitution(self):
        columns = [
            Column(
                name="a",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                substitution=(re.compile(r"(\d+)ms"), r"\1"),
            ),
            Column(
                name="b",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
            ),
        ]
        result = split_line("100ms,200ms", ",", columns)
        assert result == ["100", "200ms"]

    def test_all_columns_substitution(self):
        sub = (re.compile(r"(\d+)ms"), r"\1")
        columns = [
            Column(
                name="a",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                substitution=sub,
            ),
            Column(
                name="b",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                substitution=sub,
            ),
        ]
        result = split_line("100ms,200ms", ",", columns)
        assert result == ["100", "200"]

    def test_substitution_no_match(self):
        columns = [
            Column(
                name="a",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                substitution=(re.compile(r"xyz"), "replaced"),
            ),
        ]
        result = split_line("hello,world", ",", columns)
        assert result == ["hello", "world"]

    def test_substitution_greedy_star_no_double_match(self):
        """Ensure (.*) doesn't double-match (full string + empty at end)."""
        columns = [
            Column(
                name="a",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                substitution=(re.compile(r"(.*)"), r"\1lol"),
            ),
        ]
        result = split_line("hello,world", ",", columns)
        assert result == ["hellolol", "world"]

    def test_substitution_empty_replacement(self):
        columns = [
            Column(
                name="a",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                substitution=(re.compile(r"\s+"), ""),
            ),
        ]
        result = split_line("hello world,foo", ",", columns)
        assert result == ["helloworld", "foo"]


# ── _is_substitution detection ───────────────────────────────────────


class TestIsSubstitution:
    """Tests for distinguishing substitution from other commands."""

    def test_standard_substitution(self):
        assert _is_substitution("s/foo/bar/") is True

    def test_pipe_separator(self):
        assert _is_substitution("s|foo|bar|") is True

    def test_sort_is_not_substitution(self):
        assert _is_substitution("sort col") is False

    def test_set_is_not_substitution(self):
        assert _is_substitution("set theme monokai") is False

    def test_s_space_is_not_substitution(self):
        assert _is_substitution("s foo") is False

    def test_too_short(self):
        assert _is_substitution("s/") is False

    def test_not_s(self):
        assert _is_substitution("f/a/b/") is False


# ── Command alias map ────────────────────────────────────────────────


class TestCommandAliases:
    """Tests for the command alias mapping."""

    def test_filter_aliases(self):
        assert _COMMAND_ALIASES["filter"] == "filter"
        assert _COMMAND_ALIASES["f"] == "filter"

    def test_exclude_aliases(self):
        assert _COMMAND_ALIASES["exclude"] == "exclude"
        assert _COMMAND_ALIASES["e"] == "exclude"

    def test_write_aliases(self):
        assert _COMMAND_ALIASES["w"] == "write"
        assert _COMMAND_ALIASES["write"] == "write"

    def test_open_aliases(self):
        assert _COMMAND_ALIASES["o"] == "open"
        assert _COMMAND_ALIASES["open"] == "open"

    def test_quit_aliases(self):
        assert _COMMAND_ALIASES["q"] == "quit"
        assert _COMMAND_ALIASES["quit"] == "quit"
        assert _COMMAND_ALIASES["q!"] == "quit!"
        assert _COMMAND_ALIASES["quit!"] == "quit!"

    def test_delim_aliases(self):
        assert _COMMAND_ALIASES["delim"] == "delim"
        assert _COMMAND_ALIASES["delimiter"] == "delim"

    def test_unknown_command(self):
        assert _COMMAND_ALIASES.get("foobar") is None


# ── ExModeSuggestionProvider ─────────────────────────────────────────


class TestExModeSuggestionProvider:
    """Tests for ExModeSuggestionProvider."""

    @pytest.fixture()
    def provider(self):
        from nless.suggestions import ExModeSuggestionProvider

        class FakeBuffer:
            def __init__(self):
                self.current_columns = [
                    type("Col", (), {"name": "timestamp", "hidden": False})(),
                    type("Col", (), {"name": "level", "hidden": False})(),
                    type("Col", (), {"name": "message", "hidden": False})(),
                    type("Col", (), {"name": "secret", "hidden": True})(),
                ]

        class FakeApp:
            def __init__(self):
                self.input_history = []

            def _get_current_buffer(self):
                return FakeBuffer()

        return ExModeSuggestionProvider(FakeApp())

    def test_empty_input_shows_commands(self, provider):
        suggestions = provider.get_suggestions("")
        assert "sort" in suggestions
        assert "filter" in suggestions
        assert "help" in suggestions

    def test_command_prefix_match(self, provider):
        suggestions = provider.get_suggestions("so")
        assert "sort" in suggestions
        assert "filter" not in suggestions

    def test_sort_suggests_columns(self, provider):
        suggestions = provider.get_suggestions("sort ")
        assert "sort timestamp" in suggestions
        assert "sort level" in suggestions
        assert "sort message" in suggestions
        # Hidden columns excluded
        assert "sort secret" not in suggestions

    def test_sort_column_prefix(self, provider):
        suggestions = provider.get_suggestions("sort ti")
        assert "sort timestamp" in suggestions
        assert "sort level" not in suggestions

    def test_filter_suggests_columns(self, provider):
        suggestions = provider.get_suggestions("filter ")
        # Column names are prefixed with "filter "
        assert any("timestamp" in s for s in suggestions)

    def test_set_subcommands(self, provider):
        suggestions = provider.get_suggestions("set ")
        assert any("theme" in s for s in suggestions)
        assert any("keymap" in s for s in suggestions)

    def test_set_theme_suggests_names(self, provider):
        suggestions = provider.get_suggestions("set theme ")
        # Should have at least the default theme
        assert len(suggestions) > 0
        assert all(s.startswith("set theme ") for s in suggestions)

    def test_delim_suggests_values(self, provider):
        suggestions = provider.get_suggestions("delim ")
        assert "\\t" in suggestions
        assert "," in suggestions
        assert "raw" in suggestions

    def test_write_delegates_to_filepath(self, provider):
        # Just verify it returns a list (file path suggestions)
        suggestions = provider.get_suggestions("w ")
        assert isinstance(suggestions, list)

    def test_unknown_command_no_suggestions(self, provider):
        suggestions = provider.get_suggestions("foobar ")
        assert suggestions == []

    def test_empty_input_shows_new_commands(self, provider):
        suggestions = provider.get_suggestions("")
        assert "clear" in suggestions
        assert "cols" in suggestions

    def test_descriptions_for_commands(self, provider):
        assert provider.get_description("sort") == "sort by column"
        assert provider.get_description("clear") == "reset sort, search, and columns"
        assert provider.get_description("cols") == "show/hide columns"
        assert provider.get_description("nonexistent") is None

    def test_cols_suggests_all_and_columns(self, provider):
        suggestions = provider.get_suggestions("cols ")
        assert "cols all" in suggestions
        assert any("timestamp" in s for s in suggestions)

    def test_quoted_column_in_sort_suggestion(self):
        """Columns with spaces should be auto-quoted in suggestions."""
        from nless.suggestions import ExModeSuggestionProvider

        class FakeBuffer:
            def __init__(self):
                self.current_columns = [
                    type("Col", (), {"name": "multi word col", "hidden": False})(),
                    type("Col", (), {"name": "simple", "hidden": False})(),
                ]

        class FakeApp:
            def __init__(self):
                self.input_history = []

            def _get_current_buffer(self):
                return FakeBuffer()

        p = ExModeSuggestionProvider(FakeApp())
        suggestions = p.get_suggestions("sort ")
        assert 'sort "multi word col"' in suggestions
        assert "sort simple" in suggestions

    def test_type_in_commands_list(self, provider):
        suggestions = provider.get_suggestions("")
        assert "type" in suggestions

    def test_type_description(self, provider):
        assert provider.get_description("type") is not None

    def test_type_suggests_columns(self, provider):
        suggestions = provider.get_suggestions("type ")
        assert "type timestamp" in suggestions
        assert "type level" in suggestions

    def test_type_column_suggests_types(self, provider):
        suggestions = provider.get_suggestions("type timestamp ")
        assert "type timestamp numeric" in suggestions
        assert "type timestamp date" in suggestions
        assert "type timestamp string" in suggestions
        assert "type timestamp auto" in suggestions

    def test_type_column_type_prefix(self, provider):
        suggestions = provider.get_suggestions("type timestamp n")
        assert "type timestamp numeric" in suggestions
        assert "type timestamp date" not in suggestions

    def test_filter_quoted_column_suggestion(self):
        from nless.suggestions import ExModeSuggestionProvider

        class FakeBuffer:
            def __init__(self):
                self.current_columns = [
                    type("Col", (), {"name": "col name", "hidden": False})(),
                ]

        class FakeApp:
            def __init__(self):
                self.input_history = []

            def _get_current_buffer(self):
                return FakeBuffer()

        p = ExModeSuggestionProvider(FakeApp())
        suggestions = p.get_suggestions("filter ")
        assert 'filter "col name"' in suggestions


# ── Case-insensitive substitution flag ───────────────────────────────


class TestSubstitutionIgnoreCase:
    """Tests for the 'i' flag in substitution."""

    def test_case_insensitive_flag(self):
        result = _parse_substitution("s/foo/bar/i")
        assert result is not None
        pat, repl, all_cols = result
        assert pat.flags & re.IGNORECASE
        assert repl == "bar"
        assert all_cols is False

    def test_case_insensitive_with_global(self):
        result = _parse_substitution("s/foo/bar/gi")
        assert result is not None
        pat, repl, all_cols = result
        assert pat.flags & re.IGNORECASE
        assert all_cols is True

    def test_case_insensitive_ig_order(self):
        result = _parse_substitution("s/foo/bar/ig")
        assert result is not None
        pat, repl, all_cols = result
        assert pat.flags & re.IGNORECASE
        assert all_cols is True

    def test_no_flags_not_case_insensitive(self):
        result = _parse_substitution("s/foo/bar/")
        assert result is not None
        pat, _, _ = result
        assert not (pat.flags & re.IGNORECASE)


# ── _split_col_args ─────────────────────────────────────────────────


class TestSplitColArgs:
    """Tests for _split_col_args helper."""

    def test_simple_args(self):
        col, rest = ExModeMixin._split_col_args("name pattern")
        assert col == "name"
        assert rest == "pattern"

    def test_quoted_column(self):
        col, rest = ExModeMixin._split_col_args('"multi word" pattern')
        assert col == "multi word"
        assert rest == "pattern"

    def test_quoted_column_no_rest(self):
        col, rest = ExModeMixin._split_col_args('"multi word"')
        assert col == "multi word"
        assert rest == ""

    def test_empty_input(self):
        col, rest = ExModeMixin._split_col_args("")
        assert col == ""
        assert rest == ""

    def test_single_word(self):
        col, rest = ExModeMixin._split_col_args("name")
        assert col == "name"
        assert rest == ""


# ── Command alias map (new commands) ────────────────────────────────


class TestNewCommandAliases:
    """Tests for new command alias entries."""

    def test_clear_aliases(self):
        assert _COMMAND_ALIASES["clear"] == "clear"
        assert _COMMAND_ALIASES["reset"] == "clear"

    def test_cols_aliases(self):
        assert _COMMAND_ALIASES["cols"] == "cols"
        assert _COMMAND_ALIASES["columns"] == "cols"

    def test_type_alias(self):
        assert _COMMAND_ALIASES["type"] == "type"


# ── Fuzzy command suggestions ───────────────────────────────────────


class TestFuzzyCommandSuggestions:
    """Tests for fuzzy matching on unknown commands."""

    def test_close_match_for_sort(self):
        matches = difflib.get_close_matches(
            "srot", _COMMAND_ALIASES.keys(), n=2, cutoff=0.6
        )
        assert "sort" in matches

    def test_close_match_for_filter(self):
        matches = difflib.get_close_matches(
            "filtre", _COMMAND_ALIASES.keys(), n=2, cutoff=0.6
        )
        assert "filter" in matches

    def test_no_match_for_gibberish(self):
        matches = difflib.get_close_matches(
            "xyzzy", _COMMAND_ALIASES.keys(), n=2, cutoff=0.6
        )
        assert len(matches) == 0
