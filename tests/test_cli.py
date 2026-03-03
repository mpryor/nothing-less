import pytest

from nless.cli import parse_args


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
