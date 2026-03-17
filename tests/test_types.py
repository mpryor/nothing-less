"""Tests for StreamState, FilterSortState, CacheState, and ChainTimerState."""

import re

import pytest

from nless.types import (
    CacheState,
    ChainTimerState,
    Filter,
    FilterSortState,
    StreamState,
)


class TestStreamStateAppend:
    def test_append_maintains_invariant(self):
        s = StreamState()
        s.append("line1", 1.0)
        s.append("line2", 2.0)
        assert len(s.raw_rows) == 2
        assert len(s.arrival_timestamps) == 2
        s.assert_invariant()

    def test_append_with_source(self):
        s = StreamState()
        s.append("line1", 1.0, source="file1")
        s.append("line2", 2.0, source="file2")
        assert s.source_labels == ["file1", "file2"]
        s.assert_invariant()

    def test_append_pads_source_when_populated(self):
        s = StreamState()
        s.append("line1", 1.0, source="file1")
        s.append("line2", 2.0)  # no source, but source_labels already populated
        assert len(s.source_labels) == 2
        assert s.source_labels[1] == ""
        s.assert_invariant()


class TestStreamStateExtend:
    def test_extend_valid(self):
        s = StreamState()
        s.extend(["a", "b", "c"], [1.0, 2.0, 3.0])
        assert len(s) == 3
        s.assert_invariant()

    def test_extend_mismatched_lengths_raises(self):
        s = StreamState()
        with pytest.raises(
            ValueError, match="lines.*timestamps.*must have equal length"
        ):
            s.extend(["a", "b"], [1.0])

    def test_extend_mismatched_sources_raises(self):
        s = StreamState()
        with pytest.raises(ValueError, match="sources.*must match lines"):
            s.extend(["a", "b"], [1.0, 2.0], sources=["s1"])

    def test_extend_with_sources(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0], sources=["s1", "s2"])
        assert s.source_labels == ["s1", "s2"]
        s.assert_invariant()

    def test_extend_pads_sources_when_populated(self):
        s = StreamState()
        s.append("a", 1.0, source="s1")
        s.extend(["b", "c"], [2.0, 3.0])  # no sources
        assert len(s.source_labels) == 3
        s.assert_invariant()


class TestStreamStatePop:
    def test_pop_returns_correct_tuple(self):
        s = StreamState()
        s.extend(["a", "b", "c"], [1.0, 2.0, 3.0], sources=["s1", "s2", "s3"])
        line, ts, src = s.pop(1)
        assert line == "b"
        assert ts == 2.0
        assert src == "s2"
        assert len(s) == 2
        s.assert_invariant()

    def test_pop_without_sources(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0])
        line, ts, src = s.pop(0)
        assert line == "a"
        assert ts == 1.0
        assert src == ""
        s.assert_invariant()


class TestStreamStateReplaceRawRows:
    def test_replace_valid(self):
        s = StreamState()
        s.append("old", 0.0)
        s.replace_raw_rows(["new1", "new2"], [1.0, 2.0])
        assert s.raw_rows == ["new1", "new2"]
        assert len(s.arrival_timestamps) == 2
        s.assert_invariant()

    def test_replace_mismatched_raises(self):
        s = StreamState()
        with pytest.raises(ValueError):
            s.replace_raw_rows(["a"], [1.0, 2.0])

    def test_replace_mismatched_sources_raises(self):
        s = StreamState()
        with pytest.raises(ValueError):
            s.replace_raw_rows(["a", "b"], [1.0, 2.0], sources=["s1"])

    def test_replace_clears_sources_when_none(self):
        s = StreamState()
        s.extend(["a"], [1.0], sources=["s1"])
        s.replace_raw_rows(["b"], [2.0])
        assert s.source_labels == []
        s.assert_invariant()


class TestStreamStateSetSourceLabels:
    def test_set_valid_labels(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0])
        s.set_source_labels(["s1", "s2"])
        assert s.source_labels == ["s1", "s2"]

    def test_set_mismatched_length_raises(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0])
        with pytest.raises(ValueError, match="source_labels.*must match.*raw_rows"):
            s.set_source_labels(["s1"])

    def test_set_empty_labels(self):
        s = StreamState()
        s.extend(["a"], [1.0], sources=["s1"])
        s.set_source_labels([])
        assert s.source_labels == []


class TestStreamStateClear:
    def test_clear_resets_all(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0], sources=["s1", "s2"])
        s.clear()
        assert len(s) == 0
        assert s.raw_rows == []
        assert s.arrival_timestamps == []
        assert s.source_labels == []
        s.assert_invariant()


class TestStreamStateAssertInvariant:
    def test_passes_on_valid_state(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0])
        s.assert_invariant()  # should not raise

    def test_fails_on_corrupted_timestamps(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0])
        s._arrival_timestamps.append(3.0)  # corrupt
        with pytest.raises(AssertionError, match="Invariant violated"):
            s.assert_invariant()

    def test_fails_on_corrupted_sources(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0], sources=["s1", "s2"])
        s._source_labels.append("s3")  # corrupt
        with pytest.raises(AssertionError, match="Invariant violated"):
            s.assert_invariant()


class TestStreamStateContains:
    def test_contains_and_index(self):
        s = StreamState()
        s.extend(["foo", "bar"], [1.0, 2.0])
        assert "foo" in s
        assert "baz" not in s
        assert s.index("bar") == 1

    def test_getitem(self):
        s = StreamState()
        s.extend(["a", "b", "c"], [1.0, 2.0, 3.0])
        assert s[0] == "a"
        assert s[2] == "c"

    def test_iter(self):
        s = StreamState()
        s.extend(["a", "b"], [1.0, 2.0])
        assert list(s) == ["a", "b"]


class TestFilterSortState:
    def test_is_expensive_default(self):
        q = FilterSortState()
        assert q.is_expensive is False

    def test_is_expensive_with_sort(self):
        q = FilterSortState(sort_column="name")
        assert q.is_expensive is True

    def test_is_expensive_with_unique(self):
        q = FilterSortState(unique_column_names={"name"})
        assert q.is_expensive is True

    def test_has_filters(self):
        q = FilterSortState()
        assert q.has_filters is False
        q.filters = [Filter(column=None, pattern=re.compile("x"))]
        assert q.has_filters is True

    def test_has_search(self):
        q = FilterSortState()
        assert q.has_search is False
        q.search_term = re.compile("foo")
        assert q.has_search is True

    def test_clear_all_resets_everything(self):
        q = FilterSortState(
            filters=[Filter(column=None, pattern=re.compile("x"))],
            sort_column="name",
            sort_reverse=True,
            search_term=re.compile("foo"),
            unique_column_names={"col"},
        )
        q.current_match_index = 5
        q.search_matches = [(0, 1)]
        q.clear_all()
        assert q.filters == []
        assert q.sort_column is None
        assert q.sort_reverse is False
        assert q.search_term is None
        assert q.search_matches == []
        assert q.current_match_index == -1
        assert q.unique_column_names == set()

    def test_clear_sort(self):
        q = FilterSortState(sort_column="name", sort_reverse=True)
        q.clear_sort()
        assert q.sort_column is None
        assert q.sort_reverse is False

    def test_clear_search(self):
        q = FilterSortState(search_term=re.compile("foo"))
        q.search_matches = [(0, 1)]
        q.current_match_index = 2
        q.clear_search()
        assert q.search_term is None
        assert q.search_matches == []
        assert q.current_match_index == -1


class TestCacheState:
    def test_invalidate_full(self):
        c = CacheState()
        c.parsed_rows = [["a", "b"]]
        c.col_widths = [10, 20]
        c.sort_keys = [1, 2]
        c.dedup_key_to_row_idx = {"k": 0}
        c.invalidate()
        assert c.parsed_rows is None
        assert c.col_widths is None
        assert c.sort_keys == []
        assert c.dedup_key_to_row_idx == {}

    def test_invalidate_widths_only(self):
        c = CacheState()
        c.parsed_rows = [["a", "b"]]
        c.col_widths = [10, 20]
        c.sort_keys = [1, 2]
        c.invalidate_widths()
        assert c.parsed_rows is not None  # preserved
        assert c.col_widths is None  # cleared
        assert c.sort_keys == [1, 2]  # preserved

    def test_reset_sort_keys(self):
        c = CacheState()
        c.sort_keys = [1, 2, 3]
        c.dedup_key_to_row_idx = {"k": 0}
        c.parsed_rows = [["a"]]
        c.reset_sort_keys()
        assert c.sort_keys == []
        assert c.dedup_key_to_row_idx == {}
        assert c.parsed_rows is not None  # preserved


class TestChainTimerState:
    def test_initial_state(self):
        c = ChainTimerState()
        assert c.delay == 0.3
        assert c.skips == 0
        assert c.timer is None
        assert c.notified is False

    def test_advance_backoff_doubles(self):
        c = ChainTimerState()
        new_delay = c.advance_backoff()
        assert c.skips == 1
        assert new_delay == 0.6
        new_delay = c.advance_backoff()
        assert c.skips == 2
        assert new_delay == 1.2

    def test_advance_backoff_caps_at_max(self):
        c = ChainTimerState()
        for _ in range(10):
            c.advance_backoff()
        assert c.delay == ChainTimerState.MAX_DELAY

    def test_should_skip(self):
        c = ChainTimerState()
        assert c.should_skip is True  # skips=0 < MAX_SKIPS=3
        c.skips = 2
        assert c.should_skip is True  # 2 < 3
        c.skips = 3
        assert c.should_skip is False  # 3 < 3 is False

    def test_reset(self):
        c = ChainTimerState()
        c.advance_backoff()
        c.advance_backoff()
        c.reset()
        assert c.delay == ChainTimerState.INITIAL_DELAY
        assert c.skips == 0

    def test_stop(self):
        c = ChainTimerState()
        c.advance_backoff()
        c.notified = True
        c.stop()
        assert c.delay == ChainTimerState.INITIAL_DELAY
        assert c.skips == 0
        assert c.notified is False
        assert c.timer is None
