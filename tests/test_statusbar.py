"""Tests for status bar text building."""

import re
from unittest.mock import MagicMock, patch

from nless.input import StdinLineStream
from nless.statusbar import _format_pipe, _format_rate, build_status_text
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


class TestFormatRate:
    def test_below_1000(self):
        assert _format_rate(999) == "~999/s"

    def test_at_1000(self):
        assert _format_rate(1000) == "~1.0K/s"

    def test_below_million(self):
        assert _format_rate(125_000) == "~125.0K/s"

    def test_at_million(self):
        assert _format_rate(1_000_000) == "~1.0M/s"

    def test_above_million(self):
        assert _format_rate(1_200_000) == "~1.2M/s"

    def test_zero(self):
        assert _format_rate(0) == "~0/s"


class TestFormatPipe:
    def test_with_capacity(self):
        result = _format_pipe((32768, 65536))
        assert result == "pipe: 32KB/64KB"

    def test_without_capacity(self):
        result = _format_pipe((32768, None))
        assert result == "pipe: 32KB"

    def test_small_bytes(self):
        result = _format_pipe((512, 65536))
        assert result == "pipe: 512B/64KB"

    def test_zero_capacity(self):
        result = _format_pipe((1024, 0))
        assert result == "pipe: 1KB"


class TestBackPressureStatus:
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
            loading_reason="Loading",
        )
        defaults.update(overrides)
        return defaults

    def test_no_lag_shows_plain_loading(self):
        text = build_status_text(**self._defaults(), lag_rows=0)
        assert "Loading" in text
        assert "rows behind" not in text

    def test_small_lag_hidden(self):
        text = build_status_text(**self._defaults(), lag_rows=999)
        assert "rows behind" not in text

    def test_lag_above_threshold_shown(self):
        text = build_status_text(**self._defaults(), lag_rows=2301, throughput=50000)
        assert "2,301 rows behind" in text
        assert "~50.0K/s" in text

    def test_pipe_pressure_shown_above_50_pct(self):
        text = build_status_text(
            **self._defaults(),
            lag_rows=5000,
            pipe_pressure=(40960, 65536),
        )
        assert "pipe: 40KB/64KB" in text

    def test_pipe_pressure_hidden_below_50_pct(self):
        text = build_status_text(
            **self._defaults(),
            lag_rows=5000,
            pipe_pressure=(10000, 65536),
        )
        assert "pipe:" not in text

    def test_pipe_no_capacity_always_shown(self):
        text = build_status_text(
            **self._defaults(),
            lag_rows=5000,
            pipe_pressure=(1024, None),
        )
        assert "pipe: 1KB" in text

    def test_template_variables_exposed(self):
        text = build_status_text(
            **self._defaults(),
            lag_rows=5000,
            throughput=10000,
            pipe_pressure=(40960, 65536),  # >50% so pipe is shown
            format_str="{lag} {throughput} {pipe}",
        )
        assert "5,000 rows behind" in text
        assert "~10.0K/s" in text
        assert "pipe:" in text

    def test_no_lag_no_loading_empty_variables(self):
        text = build_status_text(
            **self._defaults(loading_reason=None),
            format_str="{lag} {throughput} {pipe}",
        )
        # All empty when not loading
        assert "rows behind" not in text
        assert "/s" not in text
        assert "pipe:" not in text


class TestPipePressure:
    @patch("nless.input.fcntl.ioctl")
    @patch("nless.input.fcntl.fcntl")
    def test_returns_tuple(self, mock_fcntl, mock_ioctl):
        def fill_buf(fd, req, buf):
            buf[0] = 4096

        mock_ioctl.side_effect = fill_buf
        mock_fcntl.return_value = 65536

        stream = MagicMock(spec=StdinLineStream)
        stream.new_fd = 3
        result = StdinLineStream.pipe_pressure(stream)
        assert result == (4096, 65536)

    @patch("nless.input.fcntl.ioctl", side_effect=OSError)
    def test_returns_none_on_error(self, _mock):
        stream = MagicMock(spec=StdinLineStream)
        stream.new_fd = 3
        result = StdinLineStream.pipe_pressure(stream)
        assert result is None

    @patch("nless.input.fcntl.ioctl")
    @patch("nless.input.fcntl.fcntl", side_effect=OSError)
    def test_no_capacity_on_macos(self, mock_fcntl, mock_ioctl):
        def fill_buf(fd, req, buf):
            buf[0] = 2048

        mock_ioctl.side_effect = fill_buf

        stream = MagicMock(spec=StdinLineStream)
        stream.new_fd = 3
        result = StdinLineStream.pipe_pressure(stream)
        assert result == (2048, None)
