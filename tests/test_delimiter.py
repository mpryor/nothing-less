import re

from nless.delimiter import (
    flatten_json_lines,
    infer_delimiter,
    split_aligned_row,
    split_aligned_row_preserve_single_spaces,
    split_csv_row,
    split_line,
)
from nless.types import Column


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

    def test_regex_optional_group_none_becomes_empty_string(self):
        pattern = re.compile(
            r"(?P<process>[^\[:]+)(?:\[(?P<pid>\d+)\])?: (?P<message>.*)"
        )
        result = split_line("kernel: some message", pattern, [])
        assert result == ["kernel", "", "some message"]


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

    def test_json_input(self):
        lines = [
            '{"name": "Alice", "age": 30, "city": "New York"}',
            '{"name": "Bob", "age": 25, "city": "San Francisco"}',
        ]
        assert infer_delimiter(lines) == "json"

    def test_json_input_single_line(self):
        lines = ['{"status": "ok", "count": 42}']
        assert infer_delimiter(lines) == "json"

    def test_json_with_empty_lines(self):
        lines = [
            '{"a": 1}',
            "",
            '{"a": 2}',
        ]
        assert infer_delimiter(lines) == "json"

    def test_json_array_input(self):
        lines = [
            "[",
            '    {"op": "add", "path": "/a/b/c", "value": "foo"},',
            '    {"op": "subtract", "path": "/b/c/d", "value": "bar"}',
            "]",
        ]
        assert infer_delimiter(lines) == "json"

    def test_non_json_with_braces_not_detected_as_json(self):
        lines = ["name,age,city", "{invalid json,30,NYC"]
        assert infer_delimiter(lines) != "json"

    def test_source_code_infers_raw(self):
        lines = [
            "def main():",
            "    x = get_value()",
            "    if x > 0:",
            '        print("positive")',
            "    else:",
            '        print("non-positive")',
            "    return x",
        ]
        assert infer_delimiter(lines) == "raw"

    def test_config_file_infers_raw(self):
        lines = [
            "{",
            '  "name": "test",',
            '  "version": "1.0",',
            '  "description": "a thing"',
            "}",
        ]
        # infer_delimiter alone won't detect this (sampled lines),
        # but it shouldn't misidentify as CSV either
        result = infer_delimiter(lines)
        assert result != ","


class TestFlattenJsonLines:
    def test_jsonl_passthrough(self):
        lines = ['{"a": 1}', '{"a": 2}']
        assert flatten_json_lines(lines) is lines

    def test_pretty_object(self):
        lines = ["{", '  "op": "add",', '  "path": "/a"', "}"]
        result = flatten_json_lines(lines)
        assert result is not lines
        assert len(result) == 1
        assert '"op"' in result[0]

    def test_pretty_array(self):
        lines = ["[", '  {"a": 1},', '  {"a": 2}', "]"]
        result = flatten_json_lines(lines)
        assert result is not lines
        assert len(result) == 2

    def test_csv_passthrough(self):
        lines = ["a,b,c", "1,2,3"]
        assert flatten_json_lines(lines) is lines

    def test_empty_passthrough(self):
        lines = []
        assert flatten_json_lines(lines) is lines

    def test_large_pretty_array(self):
        """Pretty-printed JSON larger than the 15-line sample window."""
        import json

        data = [{"id": i, "val": i * 10} for i in range(50)]
        lines = json.dumps(data, indent=2).splitlines()
        assert len(lines) > 15
        result = flatten_json_lines(lines)
        assert result is not lines
        assert len(result) == 50


class TestSplitLineComputedColumns:
    """Tests for split_line with computed columns (col_ref, col_ref_index, json_ref)."""

    def test_regex_col_ref_index_in_bounds(self):
        pattern = re.compile(r"(?P<a>\w+)-(?P<b>\w+)")
        columns = [
            Column(
                name="src",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="part_a",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                col_ref="src",
                col_ref_index=0,
                delimiter=pattern,
            ),
        ]
        result = split_line("hello-world", ",", columns)
        assert "hello" in result

    def test_regex_col_ref_index_out_of_bounds(self):
        pattern = re.compile(r"(?P<a>\w+)")  # Only 1 group
        columns = [
            Column(
                name="src",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="part_b",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                col_ref="src",
                col_ref_index=5,  # Way out of bounds
                delimiter=pattern,
            ),
        ]
        result = split_line("hello-world", ",", columns)
        # Should not crash; out-of-bounds returns empty string
        assert "" in result

    def test_literal_col_ref_index_out_of_bounds(self):
        columns = [
            Column(
                name="src",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="part_c",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                col_ref="src",
                col_ref_index=99,
                delimiter="|",
            ),
        ]
        result = split_line("a|b|c", ",", columns)
        assert "" in result

    def test_json_ref_nested_key(self):
        columns = [
            Column(
                name="data",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="data.name",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                json_ref="data.name",
                delimiter="json",
            ),
        ]
        result = split_line('{"name": "Alice", "age": 30}', "json", columns)
        assert "Alice" in result

    def test_json_ref_missing_key(self):
        columns = [
            Column(
                name="data",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="data.missing",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                json_ref="data.missing",
                delimiter="json",
            ),
        ]
        result = split_line('{"name": "Alice"}', "json", columns)
        assert "" in result
