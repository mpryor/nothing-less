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
