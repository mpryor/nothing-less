import pytest

from nless.cli import main, parse_args


class TestParseArgs:
    def test_delimiter_flag(self):
        cli_args = parse_args(["--delimiter", ","])
        assert cli_args.delimiter == ","

    def test_delimiter_short_flag(self):
        cli_args = parse_args(["-d", "\t"])
        assert cli_args.delimiter == "\t"

    def test_filter_valid(self):
        cli_args = parse_args(["-f", "name=Alice"])
        assert len(cli_args.filters) == 1
        assert cli_args.filters[0].column == "name"
        assert cli_args.filters[0].pattern.pattern == "Alice"

    def test_filter_any_column(self):
        cli_args = parse_args(["-f", "any=error"])
        assert len(cli_args.filters) == 1
        assert cli_args.filters[0].column is None
        assert cli_args.filters[0].pattern.pattern == "error"

    def test_filter_invalid_format_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["-f", "bad-filter-no-equals"])

    def test_multiple_filters(self):
        cli_args = parse_args(["-f", "name=Alice", "-f", "city=NYC"])
        assert len(cli_args.filters) == 2

    def test_sort_by_valid(self):
        cli_args = parse_args(["--sort-by", "name=asc"])
        assert cli_args.sort_by == "name=asc"

    def test_sort_by_invalid_format_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["--sort-by", "bad_format"])

    def test_unique_keys(self):
        cli_args = parse_args(["-u", "name", "-u", "city"])
        assert cli_args.unique_keys == {"name", "city"}

    def test_filename_arg(self):
        cli_args = parse_args(["myfile.csv"])
        assert cli_args.filename == "myfile.csv"

    def test_defaults(self):
        cli_args = parse_args([])
        assert cli_args.delimiter is None
        assert cli_args.filters == []
        assert cli_args.unique_keys == set()
        assert cli_args.sort_by is None
        assert cli_args.filename is None
        assert cli_args.theme is None

    def test_theme_flag(self):
        cli_args = parse_args(["--theme", "dracula"])
        assert cli_args.theme == "dracula"

    def test_theme_short_flag(self):
        cli_args = parse_args(["-t", "nord"])
        assert cli_args.theme == "nord"

    def test_keymap_flag(self):
        cli_args = parse_args(["--keymap", "less"])
        assert cli_args.keymap == "less"

    def test_keymap_short_flag(self):
        cli_args = parse_args(["-k", "emacs"])
        assert cli_args.keymap == "emacs"

    def test_keymap_default_none(self):
        cli_args = parse_args([])
        assert cli_args.keymap is None


class TestTailFlag:
    def test_tail_flag(self):
        cli_args = parse_args(["--tail"])
        assert cli_args.tail is True

    def test_tail_default_false(self):
        cli_args = parse_args([])
        assert cli_args.tail is False


class TestTimeWindowFlag:
    def test_time_window_flag(self):
        cli_args = parse_args(["--time-window", "5m"])
        assert cli_args.time_window == "5m"

    def test_time_window_short_flag(self):
        cli_args = parse_args(["-w", "1h"])
        assert cli_args.time_window == "1h"

    def test_time_window_rolling(self):
        cli_args = parse_args(["-w", "5m+"])
        assert cli_args.time_window == "5m+"

    def test_time_window_default_none(self):
        cli_args = parse_args([])
        assert cli_args.time_window is None


class TestColumnsFlag:
    def test_columns_flag(self):
        cli_args = parse_args(["--columns", "name|status"])
        assert cli_args.columns == "name|status"

    def test_columns_short_flag(self):
        cli_args = parse_args(["-c", "name"])
        assert cli_args.columns == "name"

    def test_columns_default_none(self):
        cli_args = parse_args([])
        assert cli_args.columns is None


class TestExcludeFilterArgs:
    def test_exclude_filter_valid(self):
        cli_args = parse_args(["-x", "city=NYC"])
        assert len(cli_args.filters) == 1
        assert cli_args.filters[0].column == "city"
        assert cli_args.filters[0].pattern.pattern == "NYC"
        assert cli_args.filters[0].exclude is True

    def test_exclude_filter_any_column(self):
        cli_args = parse_args(["-x", "any=error"])
        assert len(cli_args.filters) == 1
        assert cli_args.filters[0].column is None
        assert cli_args.filters[0].exclude is True

    def test_exclude_filter_invalid_format_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["-x", "bad-filter-no-equals"])

    def test_include_and_exclude_combined(self):
        cli_args = parse_args(["-f", "name=Alice", "-x", "city=NYC"])
        assert len(cli_args.filters) == 2
        assert cli_args.filters[0].exclude is False
        assert cli_args.filters[1].exclude is True


class TestNoTuiFlag:
    def test_no_tui_flag(self):
        cli_args = parse_args(["--no-tui"])
        assert cli_args.no_tui is True

    def test_no_tui_default_false(self):
        cli_args = parse_args([])
        assert cli_args.no_tui is False


class TestOutputFormatFlag:
    def test_output_format_csv(self):
        cli_args = parse_args(["--output-format", "csv"])
        assert cli_args.output_format == "csv"

    def test_output_format_tsv(self):
        cli_args = parse_args(["-o", "tsv"])
        assert cli_args.output_format == "tsv"

    def test_output_format_json(self):
        cli_args = parse_args(["-o", "json"])
        assert cli_args.output_format == "json"

    def test_output_format_raw(self):
        cli_args = parse_args(["-o", "raw"])
        assert cli_args.output_format == "raw"

    def test_output_format_default_csv(self):
        cli_args = parse_args([])
        assert cli_args.output_format == "csv"

    def test_output_format_invalid_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["-o", "xml"])


class TestMainFileErrors:
    def _mock_stdin(self, monkeypatch):
        """Mock sys.stdin to have a valid fileno in test environment."""
        import io
        import os

        r, w = os.pipe()
        os.close(w)
        monkeypatch.setattr("sys.stdin", io.TextIOWrapper(io.FileIO(r)))

    def test_nonexistent_file(self, monkeypatch):
        self._mock_stdin(monkeypatch)
        monkeypatch.setattr("sys.argv", ["nless", "nonexistent_file_12345.txt"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_directory_as_file(self, monkeypatch, tmp_path):
        self._mock_stdin(monkeypatch)
        monkeypatch.setattr("sys.argv", ["nless", str(tmp_path)])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
