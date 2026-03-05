"""Unit tests for NlessApp._parse_delimiter_input and _should_reinsert_header_as_data."""

import re

from nless.app import NlessApp


class TestParseDelimiterInput:
    def test_raw_passthrough(self):
        assert NlessApp._parse_delimiter_input("raw") == "raw"

    def test_json_passthrough(self):
        assert NlessApp._parse_delimiter_input("json") == "json"

    def test_tab_escape(self):
        assert NlessApp._parse_delimiter_input("\\t") == "\t"

    def test_regex_with_named_groups_returns_pattern(self):
        result = NlessApp._parse_delimiter_input(r"(?P<key>\w+)=(?P<value>\w+)")
        assert isinstance(result, re.Pattern)
        assert set(result.groupindex.keys()) == {"key", "value"}

    def test_regex_without_groups_returns_string(self):
        result = NlessApp._parse_delimiter_input(r"\s+")
        assert isinstance(result, str)
        assert result == r"\s+"

    def test_invalid_regex_returns_string(self):
        result = NlessApp._parse_delimiter_input("[invalid")
        assert isinstance(result, str)
        assert result == "[invalid"

    def test_plain_delimiter_passthrough(self):
        assert NlessApp._parse_delimiter_input(",") == ","
        assert NlessApp._parse_delimiter_input("|") == "|"


class TestShouldReinsertHeaderAsData:
    def test_same_delimiter_returns_false(self):
        assert NlessApp._should_reinsert_header_as_data(",", ",", False) is False

    def test_full_json_parsed_returns_false(self):
        assert NlessApp._should_reinsert_header_as_data(",", "raw", True) is False

    def test_standard_to_raw_returns_true(self):
        assert NlessApp._should_reinsert_header_as_data(",", "raw", False) is True

    def test_standard_to_json_returns_true(self):
        assert NlessApp._should_reinsert_header_as_data(",", "json", False) is True

    def test_standard_to_regex_returns_true(self):
        pattern = re.compile(r"(?P<a>\w+)")
        assert NlessApp._should_reinsert_header_as_data(",", pattern, False) is True

    def test_raw_to_standard_returns_false(self):
        assert NlessApp._should_reinsert_header_as_data("raw", ",", False) is False

    def test_regex_to_standard_returns_false(self):
        pattern = re.compile(r"(?P<a>\w+)")
        assert NlessApp._should_reinsert_header_as_data(pattern, ",", False) is False

    def test_tab_to_raw_returns_true(self):
        assert NlessApp._should_reinsert_header_as_data("\t", "raw", False) is True

    def test_json_to_standard_returns_false(self):
        assert NlessApp._should_reinsert_header_as_data("json", ",", False) is False
