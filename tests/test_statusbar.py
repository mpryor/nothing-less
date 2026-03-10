"""Tests for status bar text building."""

import re
from unittest.mock import MagicMock, patch

from nless.input import StdinLineStream
from nless.statusbar import build_status_text
from nless.theme import NlessTheme
from nless.types import Filter


class TestBuildStatusText:
    def _defaults(self, **overrides):
        defaults = dict(
            sort_column=None,
            sort_reverse=False,
            filters=[],
            search_term=None,
            search_matches_count=0,
            current_match_index=-1,
            total_rows=100,
            total_cols=5,
            current_row=1,
            current_col=1,
            is_tailing=False,
            unique_column_names=set(),
            loading_reason=None,
        )
        defaults.update(overrides)
        return defaults

    def test_defaults(self):
        text = build_status_text(**self._defaults())
        # Sort/Filter/Search are hidden when inactive
        assert "Sort" not in text
        assert "Filter" not in text
        assert "Search" not in text
        # Position indicators — use regex to avoid matching substrings
        assert re.search(r"\b1/100\b", text)
        assert re.search(r"\b1/5\b", text)

    def test_sort_ascending(self):
        text = build_status_text(
            **self._defaults(sort_column="name", sort_reverse=False)
        )
        assert "name asc" in text

    def test_sort_descending(self):
        text = build_status_text(
            **self._defaults(sort_column="name", sort_reverse=True)
        )
        assert "name desc" in text

    def test_filter_any_column(self):
        f = Filter(column=None, pattern=re.compile("error", re.IGNORECASE))
        text = build_status_text(**self._defaults(filters=[f]))
        assert "any='error'" in text

    def test_filter_specific_column(self):
        f = Filter(column="status", pattern=re.compile("200"))
        text = build_status_text(**self._defaults(filters=[f]))
        assert "status='200'" in text

    def test_exclude_filter(self):
        f = Filter(column="status", pattern=re.compile("200"), exclude=True)
        text = build_status_text(**self._defaults(filters=[f]))
        assert "!status='200'" in text

    def test_search_active(self):
        text = build_status_text(
            **self._defaults(
                search_term=re.compile("foo"),
                search_matches_count=10,
                current_match_index=2,
            )
        )
        assert "'foo'" in text
        assert "3 / 10 matches" in text

    def test_tailing(self):
        text = build_status_text(**self._defaults(is_tailing=True))
        assert "Tailing" in text

    def test_unique_columns(self):
        text = build_status_text(**self._defaults(unique_column_names={"host", "path"}))
        assert "Unique" in text

    def test_loading(self):
        text = build_status_text(
            **self._defaults(loading_reason="Loading", total_rows=50000)
        )
        assert "Loading" in text
        assert "50000" in text

    def test_loading_with_buffered_rows(self):
        text = build_status_text(
            **self._defaults(loading_reason="Sorting", total_rows=1000),
            buffered_rows=42000,
        )
        assert "41,000 buffered" in text
        assert "Sorting" in text

    def test_loading_without_buffered_surplus(self):
        text = build_status_text(
            **self._defaults(loading_reason="Loading", total_rows=1000),
            buffered_rows=1000,
        )
        assert "buffered" not in text

    def test_not_loading(self):
        text = build_status_text(**self._defaults(loading_reason=None))
        assert "Loading" not in text

    def test_tailing_uses_theme_color(self):
        theme = NlessTheme(status_tailing="#aabbcc")
        text = build_status_text(**self._defaults(is_tailing=True), theme=theme)
        assert "[#aabbcc]" in text
        assert "Tailing" in text

    def test_loading_uses_theme_color(self):
        theme = NlessTheme(status_loading="#ddeeff")
        text = build_status_text(
            **self._defaults(loading_reason="Loading", total_rows=1000), theme=theme
        )
        assert "[#ddeeff]" in text
        assert "Loading" in text


class TestCustomStatusFormat:
    def _defaults(self, **overrides):
        defaults = dict(
            sort_column=None,
            sort_reverse=False,
            filters=[],
            search_term=None,
            search_matches_count=0,
            current_match_index=-1,
            total_rows=100,
            total_cols=5,
            current_row=1,
            current_col=1,
            is_tailing=False,
            unique_column_names=set(),
            loading_reason=None,
        )
        defaults.update(overrides)
        return defaults

    def test_custom_format_position_only(self):
        text = build_status_text(**self._defaults(), format_str="{position}")
        assert "1/100" in text
        assert "Sort" not in text

    def test_custom_format_with_keymap_and_theme(self):
        text = build_status_text(
            **self._defaults(),
            format_str="{keymap} | {theme}",
            keymap_name="less",
            theme_name="dracula",
        )
        assert text == "less | dracula"

    def test_custom_format_row_col_variables(self):
        text = build_status_text(
            **self._defaults(current_row=42, current_col=3),
            format_str="R{row} C{col}",
        )
        assert text == "R42 C3"

    def test_custom_format_rows_cols_variables(self):
        text = build_status_text(
            **self._defaults(total_rows=1000, total_cols=10),
            format_str="{rows} rows, {cols} cols",
        )
        assert text == "1000 rows, 10 cols"

    def test_custom_format_with_rich_markup(self):
        text = build_status_text(
            **self._defaults(sort_column="name"), format_str="[bold]{sort}[/bold]"
        )
        assert "[bold]" in text
        assert "Sort" in text
        assert "name asc" in text

    def test_unknown_variable_falls_back_to_default(self):
        text = build_status_text(
            **self._defaults(sort_column="col0"), format_str="{unknown_var}"
        )
        # Should fall back to default format
        assert "Sort" in text

    def test_none_format_uses_default(self):
        text = build_status_text(**self._defaults(), format_str=None)
        # With all defaults inactive, sort/filter/search are empty
        assert "1/100" in text
        assert "1/5" in text

    def test_empty_variables_cleaned_up(self):
        """Consecutive separators from empty variables are collapsed."""
        text = build_status_text(**self._defaults())
        # Should not have multiple consecutive pipes
        assert "| |" not in text
        # Position should still be present
        assert "1/100" in text

    def test_markup_wrapped_separators_cleaned_up(self):
        """Empty markup pairs and markup-wrapped separators are cleaned up."""
        fmt = "[#ff0000]{sort}[/#ff0000] [#888]|[/#888] [#ff0000]{filter}[/#ff0000] [#888]|[/#888] {position}"
        text = build_status_text(**self._defaults(), format_str=fmt)
        # Sort and filter are empty, so their markup and separators should be gone
        assert "#ff0000" not in text
        assert "1/100" in text


class TestBehindIndicator:
    def _defaults(self, **overrides):
        defaults = dict(
            sort_column=None,
            sort_reverse=False,
            filters=[],
            search_term=None,
            search_matches_count=0,
            current_match_index=-1,
            total_rows=100,
            total_cols=5,
            current_row=1,
            current_col=1,
            is_tailing=False,
            unique_column_names=set(),
            loading_reason=None,
        )
        defaults.update(overrides)
        return defaults

    def test_behind_shows_indicator(self):
        text = build_status_text(**self._defaults(), behind=True)
        assert "⚠" in text

    def test_not_behind_hides_indicator(self):
        text = build_status_text(**self._defaults(), behind=False)
        assert "⚠" not in text

    def test_behind_shown_alongside_loading(self):
        text = build_status_text(
            **self._defaults(loading_reason="Loading"), behind=True
        )
        assert "⚠" in text
        assert "Loading" in text

    def test_behind_template_variable(self):
        text = build_status_text(**self._defaults(), behind=True, format_str="{behind}")
        assert "⚠" in text


class TestPipeIndicator:
    def _defaults(self, **overrides):
        defaults = dict(
            sort_column=None,
            sort_reverse=False,
            filters=[],
            search_term=None,
            search_matches_count=0,
            current_match_index=-1,
            total_rows=100,
            total_cols=5,
            current_row=1,
            current_col=1,
            is_tailing=False,
            unique_column_names=set(),
            loading_reason=None,
        )
        defaults.update(overrides)
        return defaults

    def test_pipe_indicator_with_row_count(self):
        text = build_status_text(
            **self._defaults(), pipe_output=True, pipe_row_count=150
        )
        assert "⇥ Pipe (150 rows)" in text
        assert "Q to send" in text

    def test_pipe_indicator_zero_rows(self):
        text = build_status_text(**self._defaults(), pipe_output=True, pipe_row_count=0)
        assert "⇥ Pipe" in text
        assert "(0 rows)" not in text
        assert "Q to send" in text

    def test_pipe_indicator_hidden_when_not_piping(self):
        text = build_status_text(**self._defaults(), pipe_output=False)
        assert "⇥ Pipe" not in text
        assert "Q to send" not in text

    def test_pipe_indicator_large_row_count(self):
        text = build_status_text(
            **self._defaults(), pipe_output=True, pipe_row_count=1_000_000
        )
        assert "1,000,000 rows" in text


class TestPipePendingBytes:
    @patch("nless.input.fcntl.ioctl")
    def test_returns_bytes(self, mock_ioctl):
        def fill_buf(fd, req, buf):
            buf[0] = 4096

        mock_ioctl.side_effect = fill_buf

        stream = MagicMock(spec=StdinLineStream)
        stream.new_fd = 3
        result = StdinLineStream.pipe_pending_bytes(stream)
        assert result == 4096

    @patch("nless.input.fcntl.ioctl", side_effect=OSError)
    def test_returns_none_on_error(self, _mock):
        stream = MagicMock(spec=StdinLineStream)
        stream.new_fd = 3
        result = StdinLineStream.pipe_pending_bytes(stream)
        assert result is None
