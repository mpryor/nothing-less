from nless.buffer import NlessBuffer


class TestMakeColumns:
    def test_creates_columns_from_names(self):
        cols = NlessBuffer._make_columns(["name", "age", "city"])
        assert len(cols) == 3
        assert cols[0].name == "name"
        assert cols[0].render_position == 0
        assert cols[0].data_position == 0
        assert cols[1].name == "age"
        assert cols[1].render_position == 1
        assert cols[2].name == "city"
        assert cols[2].render_position == 2

    def test_empty_list(self):
        cols = NlessBuffer._make_columns([])
        assert cols == []

    def test_single_column(self):
        cols = NlessBuffer._make_columns(["log"])
        assert len(cols) == 1
        assert cols[0].name == "log"


class TestStrToInt:
    def _make_buffer(self):
        from nless.types import CliArgs

        return NlessBuffer(
            pane_id=1,
            cli_args=CliArgs(
                delimiter=None, filters=[], unique_keys=set(), sort_by=None
            ),
        )

    def test_integer_passthrough(self):
        buf = self._make_buffer()
        assert buf.str_to_int(42) == 42

    def test_float_string(self):
        buf = self._make_buffer()
        assert buf.str_to_int("3.14") == 3.14

    def test_integer_string(self):
        buf = self._make_buffer()
        assert buf.str_to_int("100") == 100.0

    def test_non_numeric_string(self):
        buf = self._make_buffer()
        assert buf.str_to_int("hello") == "hello"


class TestGetCellValueWithoutMarkup:
    def _make_buffer(self):
        from nless.types import CliArgs

        return NlessBuffer(
            pane_id=1,
            cli_args=CliArgs(
                delimiter=None, filters=[], unique_keys=set(), sort_by=None
            ),
        )

    def test_plain_text(self):
        buf = self._make_buffer()
        assert buf._get_cell_value_without_markup("hello") == "hello"

    def test_bold_markup(self):
        buf = self._make_buffer()
        assert buf._get_cell_value_without_markup("[bold]hello[/bold]") == "hello"

    def test_nested_markup(self):
        buf = self._make_buffer()
        result = buf._get_cell_value_without_markup("[bold][red]hello[/red][/bold]")
        assert result == "hello"

    def test_color_markup(self):
        buf = self._make_buffer()
        result = buf._get_cell_value_without_markup("[#00ff00]value[/#00ff00]")
        assert result == "value"

    def test_reverse_markup(self):
        buf = self._make_buffer()
        result = buf._get_cell_value_without_markup("[reverse]match[/reverse]")
        assert result == "match"
