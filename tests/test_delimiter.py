import re

from nless.delimiter import (
    infer_delimiter,
    split_aligned_row,
    split_aligned_row_preserve_single_spaces,
    split_csv_row,
    split_line,
)


class TestSplitCsvRow:
    def test_standard_csv(self):
        assert split_csv_row("a,b,c") == ["a", "b", "c"]

    def test_quoted_fields_with_commas(self):
        assert split_csv_row('"hello, world",b,c') == ["hello, world", "b", "c"]

    def test_empty_fields(self):
        assert split_csv_row("a,,c") == ["a", "", "c"]

    def test_single_field(self):
        assert split_csv_row("only") == ["only"]


class TestSplitAlignedRow:
    def test_space_aligned_columns(self):
        result = split_aligned_row("nginx      Running   0          5d")
        assert result == ["nginx", "Running", "0", "5d"]

    def test_single_word(self):
        assert split_aligned_row("hello") == ["hello"]

    def test_leading_trailing_spaces(self):
        result = split_aligned_row("  foo   bar  ")
        assert result == ["foo", "bar"]


class TestSplitAlignedRowPreserveSingleSpaces:
    def test_double_space_delimited(self):
        result = split_aligned_row_preserve_single_spaces(
            "New York  San Francisco  Los Angeles"
        )
        assert result == ["New York", "San Francisco", "Los Angeles"]

    def test_mixed_spacing(self):
        result = split_aligned_row_preserve_single_spaces("one  two three  four")
        assert result == ["one", "two three", "four"]


class TestSplitLine:
    def test_comma_delimiter(self):
        result = split_line("a,b,c", ",", [])
        assert result == ["a", "b", "c"]

    def test_tab_delimiter(self):
        result = split_line("a\tb\tc", "\t", [])
        assert result == ["a", "b", "c"]

    def test_pipe_delimiter(self):
        result = split_line("a|b|c", "|", [])
        assert result == ["a", "b", "c"]

    def test_space_delimiter(self):
        result = split_line("foo   bar   baz", " ", [])
        assert result == ["foo", "bar", "baz"]

    def test_double_space_delimiter(self):
        result = split_line("New York  San Francisco", "  ", [])
        assert result == ["New York", "San Francisco"]

    def test_raw_delimiter(self):
        result = split_line("hello world", "raw", [])
        assert result == ["hello world"]

    def test_json_delimiter(self):
        result = split_line('{"a": 1, "b": "two"}', "json", [])
        assert result == ["1", "two"]

    def test_regex_with_named_groups(self):
        pattern = re.compile(r"(?P<severity>\w+) (?P<message>.*)")
        result = split_line("ERROR something broke", pattern, [])
        assert result == ["ERROR", "something broke"]

    def test_regex_no_match_returns_empty(self):
        pattern = re.compile(r"(?P<a>zzz)")
        result = split_line("no match here", pattern, [])
        assert result == []


class TestInferDelimiter:
    def test_csv_input(self):
        lines = ["name,age,city", "Alice,30,NYC", "Bob,25,LA"]
        assert infer_delimiter(lines) == ","

    def test_tsv_input(self):
        lines = ["name\tage\tcity", "Alice\t30\tNYC", "Bob\t25\tLA"]
        assert infer_delimiter(lines) == "\t"

    def test_pipe_input(self):
        lines = ["name|age|city", "Alice|30|NYC", "Bob|25|LA"]
        assert infer_delimiter(lines) == "|"

    def test_space_aligned_input(self, space_aligned_lines):
        result = infer_delimiter(space_aligned_lines)
        assert result in (" ", "  ")

    def test_empty_input_returns_raw(self):
        assert infer_delimiter([]) == "raw"

    def test_single_column_returns_raw(self):
        assert infer_delimiter(["hello", "world", "test"]) == "raw"
