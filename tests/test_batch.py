"""Tests for headless batch processing (nless/batch.py)."""

import io
import json
import sys


from nless.batch import run_batch
from nless.types import CliArgs, Filter
import re


def _cli_args(**overrides) -> CliArgs:
    defaults = dict(
        delimiter=None,
        filters=[],
        unique_keys=set(),
        sort_by=None,
        filename=None,
        theme=None,
        keymap=None,
        tail=False,
        time_window=None,
        columns=None,
        raw=False,
        no_tui=True,
        pipe_output=False,
        output_format="csv",
    )
    defaults.update(overrides)
    return CliArgs(**defaults)


class TestBatchPassthrough:
    def test_csv_passthrough(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\nBob,25\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f)))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"
        assert lines[2] == "Bob,25"

    def test_empty_input(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f)))
        finally:
            sys.stdout = sys.__stdout__
        assert buf.getvalue() == ""


class TestBatchFilter:
    def test_include_filter(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,city\nAlice,NYC\nBob,LA\nCharlie,NYC\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    filters=[
                        Filter(column="city", pattern=re.compile("NYC", re.IGNORECASE))
                    ],
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert "Alice" in lines[1]
        assert "Charlie" in lines[2]

    def test_exclude_filter(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,city\nAlice,NYC\nBob,LA\nCharlie,NYC\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    filters=[
                        Filter(
                            column="city",
                            pattern=re.compile("NYC", re.IGNORECASE),
                            exclude=True,
                        )
                    ],
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert len(lines) == 2  # header + 1 row
        assert "Bob" in lines[1]


class TestBatchSort:
    def test_sort_asc(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nCharlie,35\nAlice,30\nBob,25\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), sort_by="age=asc"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert "Bob" in lines[1]
        assert "Alice" in lines[2]
        assert "Charlie" in lines[3]

    def test_sort_desc(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\nBob,25\nCharlie,35\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), sort_by="age=desc"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert "Charlie" in lines[1]
        assert "Alice" in lines[2]
        assert "Bob" in lines[3]


class TestBatchUnique:
    def test_unique_dedup(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,city\nAlice,NYC\nBob,NYC\nCharlie,LA\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), unique_keys={"city"}))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert len(lines) == 3  # header + 2 unique cities


class TestBatchColumns:
    def test_column_filter(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), columns="name|city"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name,city"
        assert "30" not in lines[1]  # age column excluded


class TestBatchCombined:
    def test_filter_sort_columns(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,NYC\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    filters=[
                        Filter(column="city", pattern=re.compile("NYC", re.IGNORECASE))
                    ],
                    sort_by="age=desc",
                    columns="name|age",
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name,age"
        assert "Charlie" in lines[1]
        assert "Alice" in lines[2]
        assert len(lines) == 3


class TestBatchOutputFormats:
    def test_tsv_output(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\nBob,25\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), output_format="tsv"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name\tage"
        assert lines[1] == "Alice\t30"

    def test_json_output(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\nBob,25\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), output_format="json"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        obj1 = json.loads(lines[0])
        assert obj1 == {"name": "Alice", "age": "30"}
        obj2 = json.loads(lines[1])
        assert obj2 == {"name": "Bob", "age": "25"}

    def test_raw_output(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("hello world\nfoo bar\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), output_format="raw"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "hello world"
        assert lines[1] == "foo bar"


class TestBatchDelimiters:
    def test_tsv_input(self, tmp_path):
        f = tmp_path / "data.tsv"
        f.write_text("name\tage\nAlice\t30\nBob\t25\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), delimiter="\t"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"

    def test_format_timestamp_to_epoch(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text(
            "timestamp,level,message\n"
            "2024-01-15 10:00:00,INFO,hello\n"
            "2024-01-15 10:05:00,WARN,world\n"
        )
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    format_timestamp="timestamp -> epoch",
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "timestamp,level,message"
        # Epoch timestamps should be numeric
        ts1 = float(lines[1].split(",")[0])
        ts2 = float(lines[2].split(",")[0])
        assert ts2 > ts1
        assert ts2 - ts1 == 300.0  # 5 minutes apart

    def test_format_timestamp_to_iso(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("ts,msg\n1705312800,hello\n1705313100,world\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    format_timestamp="ts -> iso",
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "ts,msg"
        assert "T" in lines[1].split(",")[0]  # ISO format has T separator

    def test_format_timestamp_with_strftime(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("timestamp,value\n2024-01-15 10:30:00,100\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    format_timestamp="timestamp -> %H:%M",
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[1].split(",")[0] == "10:30"

    def test_format_timestamp_nonexistent_column(self, tmp_path):
        """--format-timestamp with a non-existent column should pass through unchanged."""
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\n")
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(
                _cli_args(
                    filename=str(f),
                    format_timestamp="timestamp -> epoch",
                )
            )
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"

    def test_json_input(self, tmp_path):
        f = tmp_path / "data.jsonl"
        f.write_text('{"name":"Alice","age":30}\n{"name":"Bob","age":25}\n')
        buf = io.StringIO()
        sys.stdout = buf
        try:
            run_batch(_cli_args(filename=str(f), delimiter="json"))
        finally:
            sys.stdout = sys.__stdout__
        lines = buf.getvalue().strip().replace("\r\n", "\n").split("\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"
