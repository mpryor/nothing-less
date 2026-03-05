"""Unit tests for pure data-processing helpers in nless.dataprocessing."""

import re

from nless.dataprocessing import (
    build_composite_key,
    coerce_sort_key,
    coerce_to_numeric,
    find_sorted_insert_index,
    highlight_search_matches,
    matches_all_filters,
    strip_markup,
    update_dedup_indices_after_insertion,
    update_dedup_indices_after_removal,
    update_sort_keys_for_line,
)
from nless.types import Filter


class TestStripMarkup:
    def test_plain_text(self):
        assert strip_markup("hello") == "hello"

    def test_bold_markup(self):
        assert strip_markup("[bold]hello[/bold]") == "hello"

    def test_nested_markup(self):
        assert strip_markup("[bold][red]hello[/red][/bold]") == "hello"

    def test_color_markup(self):
        assert strip_markup("[#00ff00]value[/#00ff00]") == "value"

    def test_reverse_markup(self):
        assert strip_markup("[reverse]match[/reverse]") == "match"


class TestCoerceToNumeric:
    def test_integer_passthrough(self):
        assert coerce_to_numeric(42) == 42

    def test_float_string(self):
        assert coerce_to_numeric("3.14") == 3.14

    def test_integer_string(self):
        assert coerce_to_numeric("100") == 100.0

    def test_non_numeric_string(self):
        assert coerce_to_numeric("hello") == "hello"


class TestCoerceSortKey:
    def test_integer_string(self):
        assert coerce_sort_key("42") == 42

    def test_float_string(self):
        assert coerce_sort_key("3.14") == 3.14

    def test_plain_string(self):
        assert coerce_sort_key("abc") == "abc"

    def test_empty_string(self):
        assert coerce_sort_key("") == ""


class TestBuildCompositeKey:
    def _identity(self, text):
        return text

    def _lookup(self, col_name, render_position):
        return {"name": 0, "city": 1}.get(col_name)

    def test_single_column(self):
        key = build_composite_key(
            ["Alice", "NYC"], {"name"}, self._lookup, self._identity
        )
        assert key == "Alice"

    def test_multiple_columns(self):
        key = build_composite_key(
            ["Alice", "NYC"], {"name", "city"}, self._lookup, self._identity
        )
        # Order depends on set iteration, but both values present
        parts = key.split(",")
        assert set(parts) == {"Alice", "NYC"}

    def test_missing_column(self):
        key = build_composite_key(
            ["Alice", "NYC"], {"missing"}, self._lookup, self._identity
        )
        assert key == ""


class TestFindSortedInsertIndex:
    def _lookup(self, col_name, render_position):
        return 0

    def _identity(self, text):
        return text

    def test_no_sort_column(self):
        idx = find_sorted_insert_index(
            ["x"], [], None, False, self._lookup, self._identity, 5
        )
        assert idx == 5

    def test_ascending_insert(self):
        idx = find_sorted_insert_index(
            ["3"], [1, 2, 4, 5], "col", False, self._lookup, self._identity, 4
        )
        assert idx == 2

    def test_reverse_insert(self):
        idx = find_sorted_insert_index(
            ["3"], [1, 2, 4, 5], "col", True, self._lookup, self._identity, 4
        )
        assert idx == 2  # len(sort_keys) - bisect_left = 4 - 2 = 2

    def test_numeric_coercion(self):
        idx = find_sorted_insert_index(
            ["10"], [2, 5, 20], "col", False, self._lookup, self._identity, 3
        )
        assert idx == 2


class TestUpdateDedupIndices:
    def test_removal_shifts_down(self):
        mapping = {"a": 0, "b": 2, "c": 4}
        update_dedup_indices_after_removal(mapping, 1)
        assert mapping == {"a": 0, "b": 1, "c": 3}

    def test_removal_no_shift_below(self):
        mapping = {"a": 0, "b": 1}
        update_dedup_indices_after_removal(mapping, 5)
        assert mapping == {"a": 0, "b": 1}

    def test_insertion_shifts_up(self):
        mapping = {"a": 0, "b": 1, "c": 3}
        update_dedup_indices_after_insertion(mapping, "new", 1)
        assert mapping["a"] == 0
        assert mapping["b"] == 2
        assert mapping["c"] == 4
        assert mapping["new"] == 1


class TestUpdateSortKeysForLine:
    def _lookup(self, col_name, render_position):
        return 0

    def _identity(self, text):
        return text

    def test_no_sort_column_is_noop(self):
        keys = [1, 2, 3]
        update_sort_keys_for_line(["x"], None, None, keys, self._lookup, self._identity)
        assert keys == [1, 2, 3]

    def test_insert_new_key(self):
        keys = [1, 3, 5]
        update_sort_keys_for_line(
            ["4"], None, "col", keys, self._lookup, self._identity
        )
        assert keys == [1, 3, 4, 5]

    def test_replace_old_key(self):
        keys = [1, 3, 5]
        update_sort_keys_for_line(
            ["4"], ["3"], "col", keys, self._lookup, self._identity
        )
        assert keys == [1, 4, 5]


class TestHighlightSearchMatches:
    def test_no_search_term(self):
        rows = [["a", "b"], ["c", "d"]]
        result, matches = highlight_search_matches(rows, None, 0)
        assert result == rows
        assert matches == []

    def test_highlights_matching_cells(self):
        rows = [["hello", "world"]]
        pattern = re.compile("hello", re.IGNORECASE)
        result, matches = highlight_search_matches(rows, pattern, 0)
        assert "[reverse]hello[/reverse]" in result[0][0]
        assert matches == [(0, 0)]

    def test_skips_fixed_columns(self):
        rows = [["hello", "world"]]
        pattern = re.compile("hello", re.IGNORECASE)
        result, matches = highlight_search_matches(rows, pattern, 1)
        assert result[0][0] == "hello"  # Not highlighted (fixed column)
        assert matches == []

    def test_row_offset(self):
        rows = [["foo", "bar"]]
        pattern = re.compile("foo")
        result, matches = highlight_search_matches(rows, pattern, 0, row_offset=5)
        assert matches == [(5, 0)]

    def test_multiple_matches(self):
        rows = [["a", "b"], ["a", "c"]]
        pattern = re.compile("a")
        result, matches = highlight_search_matches(rows, pattern, 0)
        assert len(matches) == 2
        assert matches[0] == (0, 0)
        assert matches[1] == (1, 0)


class TestMatchesAllFilters:
    def _lookup(self, col_name, render_position):
        return {"name": 0, "city": 1}.get(col_name)

    def test_no_filters(self):
        assert matches_all_filters(["a", "b"], [], self._lookup) is True

    def test_any_column_match(self):
        f = Filter(column=None, pattern=re.compile("alice", re.IGNORECASE))
        assert matches_all_filters(["Alice", "NYC"], [f], self._lookup) is True

    def test_any_column_no_match(self):
        f = Filter(column=None, pattern=re.compile("bob", re.IGNORECASE))
        assert matches_all_filters(["Alice", "NYC"], [f], self._lookup) is False

    def test_specific_column_match(self):
        f = Filter(column="name", pattern=re.compile("alice", re.IGNORECASE))
        assert matches_all_filters(["Alice", "NYC"], [f], self._lookup) is True

    def test_specific_column_no_match(self):
        f = Filter(column="name", pattern=re.compile("bob", re.IGNORECASE))
        assert matches_all_filters(["Alice", "NYC"], [f], self._lookup) is False

    def test_exclude_filter(self):
        f = Filter(
            column="name", pattern=re.compile("alice", re.IGNORECASE), exclude=True
        )
        assert matches_all_filters(["Alice", "NYC"], [f], self._lookup) is False

    def test_unknown_column(self):
        f = Filter(column="unknown", pattern=re.compile("x"))
        assert matches_all_filters(["a", "b"], [f], self._lookup) is False

    def test_strips_markup(self):
        f = Filter(column="name", pattern=re.compile("Alice"))
        assert (
            matches_all_filters(["[bold]Alice[/bold]", "NYC"], [f], self._lookup)
            is True
        )

    def test_adjust_for_count(self):
        def lookup(col_name, render_position):
            return {"name": 1}.get(col_name)

        f = Filter(column="name", pattern=re.compile("Alice"))
        # With adjust_for_count and has_unique_columns, col_idx is decremented
        assert (
            matches_all_filters(
                ["Alice", "NYC"],
                [f],
                lookup,
                adjust_for_count=True,
                has_unique_columns=True,
            )
            is True
        )
