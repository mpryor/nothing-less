"""Tests for status bar text building."""

import re

from nless.statusbar import build_status_text
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
            is_loading=False,
        )
        defaults.update(overrides)
        return defaults

    def test_defaults(self):
        text = build_status_text(**self._defaults())
        assert "[bold]Sort[/bold]: None" in text
        assert "[bold]Filter[/bold]: None" in text
        assert "[bold]Search[/bold]: None" in text
        assert "1/100" in text
        assert "1/5" in text

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
        assert "unique cols:" in text

    def test_loading(self):
        text = build_status_text(**self._defaults(is_loading=True, total_rows=50000))
        assert "Loading" in text
        assert "50,000" in text

    def test_not_loading(self):
        text = build_status_text(**self._defaults(is_loading=False))
        assert "Loading" not in text
