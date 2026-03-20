"""Tests for column type detection and datetime sort key coercion."""

from nless.dataprocessing import (
    coerce_datetime_sort_key,
    coerce_sort_key,
    infer_column_type,
    _try_parse_datetime,
)
from nless.types import ColumnType


class TestInferColumnType:
    def test_numeric_column(self):
        values = ["1", "2", "3", "4.5", "100", "-3", "0.5", "1e3", "42", "99"]
        assert infer_column_type(values) == ColumnType.NUMERIC

    def test_datetime_iso_column(self):
        values = [
            "2024-01-15",
            "2024-02-20",
            "2024-03-10",
            "2024-04-05",
            "2024-05-01",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME

    def test_datetime_iso_full(self):
        values = [
            "2024-01-15T10:30:00",
            "2024-02-20T14:00:00",
            "2024-03-10T08:15:00",
            "2024-04-05T12:00:00",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME

    def test_datetime_strptime_format(self):
        values = [
            "2024-01-15 10:30:00",
            "2024-02-20 14:00:00",
            "2024-03-10 08:15:00",
            "2024-04-05 12:00:00",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME

    def test_datetime_us_format(self):
        values = [
            "01/15/2024",
            "02/20/2024",
            "03/10/2024",
            "04/05/2024",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME

    def test_string_column(self):
        values = ["alice", "bob", "charlie", "dave", "eve"]
        assert infer_column_type(values) == ColumnType.STRING

    def test_mixed_below_threshold(self):
        # 5/10 numeric = 50%, below 80% threshold
        values = ["1", "2", "3", "4", "5", "a", "b", "c", "d", "e"]
        assert infer_column_type(values) == ColumnType.STRING

    def test_mostly_numeric_above_threshold(self):
        # 9/10 numeric = 90%, above 80% threshold
        values = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "a"]
        assert infer_column_type(values) == ColumnType.NUMERIC

    def test_too_few_values(self):
        values = ["1", "2"]
        assert infer_column_type(values) == ColumnType.STRING

    def test_empty_values_excluded(self):
        values = ["", "  ", "1", "2", "3", "", "4", "5"]
        # 5 non-empty, all numeric
        assert infer_column_type(values) == ColumnType.NUMERIC

    def test_empty_list(self):
        assert infer_column_type([]) == ColumnType.STRING

    def test_custom_threshold(self):
        # 6/10 numeric = 60%, use threshold=0.5
        values = ["1", "2", "3", "4", "5", "6", "a", "b", "c", "d"]
        assert infer_column_type(values, threshold=0.5) == ColumnType.NUMERIC

    def test_jan_date_format(self):
        values = [
            "Jan 15, 2024",
            "Feb 20, 2024",
            "Mar 10, 2024",
            "Apr 05, 2024",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME


class TestTryParseDatetime:
    def test_iso_date(self):
        assert _try_parse_datetime("2024-01-15") is not None

    def test_iso_datetime(self):
        assert _try_parse_datetime("2024-01-15T10:30:00") is not None

    def test_log_format(self):
        assert _try_parse_datetime("2024-01-15 10:30:00") is not None

    def test_us_format(self):
        assert _try_parse_datetime("01/15/2024") is not None

    def test_not_a_date(self):
        assert _try_parse_datetime("hello") is None

    def test_empty(self):
        assert _try_parse_datetime("") is None


class TestCoerceDatetimeSortKey:
    def test_iso_date(self):
        result = coerce_datetime_sort_key("2024-01-15")
        assert isinstance(result, float)

    def test_iso_datetime(self):
        result = coerce_datetime_sort_key("2024-01-15T10:30:00")
        assert isinstance(result, float)

    def test_log_format(self):
        result = coerce_datetime_sort_key("2024-01-15 10:30:00")
        assert isinstance(result, float)

    def test_ordering(self):
        earlier = coerce_datetime_sort_key("2024-01-01")
        later = coerce_datetime_sort_key("2024-12-31")
        assert earlier < later

    def test_not_a_date_returns_string(self):
        assert coerce_datetime_sort_key("hello") == "hello"

    def test_empty_returns_empty(self):
        assert coerce_datetime_sort_key("") == ""

    def test_fmt_hint(self):
        result = coerce_datetime_sort_key("01/15/2024", fmt_hint="%m/%d/%Y")
        assert isinstance(result, float)


class TestCoerceSortKeyWithType:
    def test_default_numeric(self):
        assert coerce_sort_key("42") == 42
        assert coerce_sort_key("3.14") == 3.14

    def test_string_type_skips_numeric(self):
        assert coerce_sort_key("42", ColumnType.STRING) == "42"

    def test_datetime_type(self):
        result = coerce_sort_key("2024-01-15", ColumnType.DATETIME)
        assert isinstance(result, float)

    def test_datetime_type_non_date_returns_string(self):
        result = coerce_sort_key("hello", ColumnType.DATETIME)
        assert result == "hello"

    def test_numeric_type_same_as_default(self):
        assert coerce_sort_key("42", ColumnType.NUMERIC) == 42

    def test_none_type_same_as_default(self):
        assert coerce_sort_key("42", None) == 42
        assert coerce_sort_key("hello", None) == "hello"


class TestUpdateTypeLabel:
    """Test NlessBuffer._update_type_label static method."""

    def test_numeric_label(self):
        from nless.buffer import NlessBuffer
        from nless.types import Column

        col = Column(
            name="age", labels=set(), render_position=0, data_position=0, hidden=False
        )
        col.detected_type = ColumnType.NUMERIC
        NlessBuffer._update_type_label(col)
        assert "#" in col.labels
        assert "@" not in col.labels

    def test_datetime_label(self):
        from nless.buffer import NlessBuffer
        from nless.types import Column

        col = Column(
            name="date", labels=set(), render_position=0, data_position=0, hidden=False
        )
        col.detected_type = ColumnType.DATETIME
        NlessBuffer._update_type_label(col)
        assert "@" in col.labels
        assert "#" not in col.labels

    def test_string_no_label(self):
        from nless.buffer import NlessBuffer
        from nless.types import Column

        col = Column(
            name="name", labels=set(), render_position=0, data_position=0, hidden=False
        )
        col.detected_type = ColumnType.STRING
        NlessBuffer._update_type_label(col)
        assert "#" not in col.labels
        assert "@" not in col.labels

    def test_clears_previous_label(self):
        from nless.buffer import NlessBuffer
        from nless.types import Column

        col = Column(
            name="x", labels={"#"}, render_position=0, data_position=0, hidden=False
        )
        col.detected_type = ColumnType.DATETIME
        NlessBuffer._update_type_label(col)
        assert "@" in col.labels
        assert "#" not in col.labels
