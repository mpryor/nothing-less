"""Unit tests for NlessBuffer._parse_delimiter_input, _should_reinsert_header_as_data, and regex wizard helpers."""

import re

from nless.buffer import NlessBuffer
from nless.regex_wizard import _extract_group_fragments, _inject_group_names


class TestParseDelimiterInput:
    def test_raw_passthrough(self):
        assert NlessBuffer._parse_delimiter_input("raw") == "raw"

    def test_json_passthrough(self):
        assert NlessBuffer._parse_delimiter_input("json") == "json"

    def test_tab_escape(self):
        assert NlessBuffer._parse_delimiter_input("\\t") == "\t"

    def test_regex_with_named_groups_returns_pattern(self):
        result = NlessBuffer._parse_delimiter_input(r"(?P<key>\w+)=(?P<value>\w+)")
        assert isinstance(result, re.Pattern)
        assert set(result.groupindex.keys()) == {"key", "value"}

    def test_regex_without_groups_returns_string(self):
        result = NlessBuffer._parse_delimiter_input(r"\s+")
        assert isinstance(result, str)
        assert result == r"\s+"

    def test_invalid_regex_returns_string(self):
        result = NlessBuffer._parse_delimiter_input("[invalid")
        assert isinstance(result, str)
        assert result == "[invalid"

    def test_plain_delimiter_passthrough(self):
        assert NlessBuffer._parse_delimiter_input(",") == ","
        assert NlessBuffer._parse_delimiter_input("|") == "|"


class TestShouldReinsertHeaderAsData:
    def test_same_delimiter_returns_false(self):
        assert NlessBuffer._should_reinsert_header_as_data(",", ",", False) is False

    def test_full_json_parsed_returns_false(self):
        assert NlessBuffer._should_reinsert_header_as_data(",", "raw", True) is False

    def test_standard_to_raw_returns_true(self):
        assert NlessBuffer._should_reinsert_header_as_data(",", "raw", False) is True

    def test_standard_to_json_returns_true(self):
        assert NlessBuffer._should_reinsert_header_as_data(",", "json", False) is True

    def test_standard_to_regex_returns_true(self):
        pattern = re.compile(r"(?P<a>\w+)")
        assert NlessBuffer._should_reinsert_header_as_data(",", pattern, False) is True

    def test_raw_to_standard_returns_false(self):
        assert NlessBuffer._should_reinsert_header_as_data("raw", ",", False) is False

    def test_regex_to_standard_returns_false(self):
        pattern = re.compile(r"(?P<a>\w+)")
        assert NlessBuffer._should_reinsert_header_as_data(pattern, ",", False) is False

    def test_tab_to_raw_returns_true(self):
        assert NlessBuffer._should_reinsert_header_as_data("\t", "raw", False) is True

    def test_json_to_standard_returns_false(self):
        assert NlessBuffer._should_reinsert_header_as_data("json", ",", False) is False


class TestInjectGroupNames:
    def test_simple_case(self):
        result = _inject_group_names(r"(\d+) (\w+)", ["ts", "level"])
        assert result == r"(?P<ts>\d+) (?P<level>\w+)"

    def test_non_capturing_groups_skipped(self):
        result = _inject_group_names(r"(?:\d+) (\w+)", ["level"])
        assert result == r"(?:\d+) (?P<level>\w+)"

    def test_escaped_parens(self):
        result = _inject_group_names(r"\(\d+\) (\w+)", ["x"])
        assert result == r"\(\d+\) (?P<x>\w+)"

    def test_character_classes(self):
        result = _inject_group_names(r"[(] (\w+)", ["x"])
        assert result == r"[(] (?P<x>\w+)"

    def test_already_named_groups_preserved(self):
        result = _inject_group_names(r"(?P<a>\d+) (\w+)", ["b"])
        assert result == r"(?P<a>\d+) (?P<b>\w+)"

    def test_result_compiles(self):
        result = _inject_group_names(r"(\d+) (\w+) (.*)", ["ts", "level", "msg"])
        pattern = re.compile(result)
        assert set(pattern.groupindex.keys()) == {"ts", "level", "msg"}
        m = pattern.match("123 INFO hello world")
        assert m is not None
        assert m.group("ts") == "123"
        assert m.group("level") == "INFO"
        assert m.group("msg") == "hello world"


class TestExtractGroupFragments:
    def test_simple_case(self):
        result = _extract_group_fragments(r"(\d+) (\w+)")
        assert result == [r"\d+", r"\w+"]

    def test_non_capturing_skipped(self):
        result = _extract_group_fragments(r"(?:\d+) (\w+)")
        assert result == [r"\w+"]

    def test_escaped_parens_skipped(self):
        result = _extract_group_fragments(r"\(\d+\) (\w+)")
        assert result == [r"\w+"]

    def test_character_class_parens_skipped(self):
        result = _extract_group_fragments(r"[(] (\w+)")
        assert result == [r"\w+"]

    def test_named_groups_skipped(self):
        result = _extract_group_fragments(r"(?P<a>\d+) (\w+)")
        assert result == [r"\w+"]

    def test_nested_groups(self):
        result = _extract_group_fragments(r"((\d+)-(\w+))")
        # outer group is unnamed, inner groups are also unnamed
        assert result[0] == r"(\d+)-(\w+)"

    def test_three_groups(self):
        result = _extract_group_fragments(r"(\d+) (\w+) (.*)")
        assert result == [r"\d+", r"\w+", r".*"]
