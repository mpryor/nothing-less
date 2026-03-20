"""Tests for column type detection and datetime sort key coercion."""

from nless.dataprocessing import (
    coerce_datetime_sort_key,
    coerce_sort_key,
    infer_column_type,
    natural_sort_key,
    _detect_datetime_format,
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

    def test_string_type_returns_natural_key(self):
        assert coerce_sort_key("42", ColumnType.STRING) == ("", 42, "")

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


class TestNaturalSortKey:
    def test_basic(self):
        assert natural_sort_key("file2") == ("file", 2, "")

    def test_file2_before_file10(self):
        assert natural_sort_key("file2") < natural_sort_key("file10")

    def test_pod_names(self):
        assert natural_sort_key("pod-9") < natural_sort_key("pod-10")

    def test_empty_string(self):
        assert natural_sort_key("") == ("",)

    def test_pure_alpha(self):
        assert natural_sort_key("hello") == ("hello",)

    def test_leading_digits(self):
        assert natural_sort_key("42abc") == ("", 42, "abc")

    def test_multiple_numbers(self):
        assert natural_sort_key("a1b2c3") == ("a", 1, "b", 2, "c", 3, "")

    def test_case_insensitive(self):
        assert natural_sort_key("File2") == natural_sort_key("file2")


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


class TestNewDatetimeFormats:
    """Tests for expanded datetime format support."""

    def test_epoch_seconds(self):
        assert _try_parse_datetime("1705312981") is not None

    def test_epoch_millis(self):
        assert _try_parse_datetime("1705312981000") is not None

    def test_epoch_not_valid_range(self):
        # Too small for epoch seconds
        assert _try_parse_datetime("123456") is None

    def test_python_logging_comma_millis(self):
        assert _try_parse_datetime("2024-01-15 10:30:00,123") is not None

    def test_python_logging_dot_micros(self):
        assert _try_parse_datetime("2024-01-15 10:30:00.123456") is not None

    def test_apache_clf(self):
        assert _try_parse_datetime("15/Jan/2024:10:30:00 +0000") is not None

    def test_syslog_bsd(self):
        from datetime import datetime

        result = _try_parse_datetime("Jan 15 14:23:01")
        assert result is not None
        assert result.year == datetime.now().year

    def test_go_nginx_format(self):
        assert _try_parse_datetime("2024/01/15 10:30:00") is not None

    def test_infer_epoch_column(self):
        values = [
            "1705312981",
            "1705312990",
            "1705313050",
            "1705313100",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME

    def test_infer_syslog_column(self):
        values = [
            "Jan 15 14:23:01",
            "Jan 15 14:24:15",
            "Jan 15 14:25:30",
            "Jan 15 14:26:00",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME

    def test_infer_apache_clf_column(self):
        values = [
            "15/Jan/2024:10:30:00 +0000",
            "16/Jan/2024:11:00:00 +0000",
            "17/Jan/2024:12:30:00 +0000",
            "18/Jan/2024:09:15:00 +0000",
        ]
        assert infer_column_type(values) == ColumnType.DATETIME


class TestDetectDatetimeFormat:
    """Tests for _detect_datetime_format()."""

    def test_iso_returns_none(self):
        """fromisoformat handles ISO — no hint needed."""
        values = [
            "2024-01-15",
            "2024-02-20",
            "2024-03-10",
            "2024-04-05",
        ]
        assert _detect_datetime_format(values) is None

    def test_epoch_detected(self):
        values = [
            "1705312981",
            "1705312990",
            "1705313050",
            "1705313100",
        ]
        assert _detect_datetime_format(values) == "epoch"

    def test_strptime_format_detected(self):
        values = [
            "01/15/2024",
            "02/20/2024",
            "03/10/2024",
            "04/05/2024",
        ]
        assert _detect_datetime_format(values) == "%m/%d/%Y"

    def test_syslog_format_detected(self):
        values = [
            "Jan 15 14:23:01",
            "Jan 15 14:24:15",
            "Jan 15 14:25:30",
            "Jan 15 14:26:00",
        ]
        assert _detect_datetime_format(values) == "%b %d %H:%M:%S"

    def test_too_few_values(self):
        values = ["2024-01-15"]
        assert _detect_datetime_format(values) is None

    def test_mixed_returns_none(self):
        values = ["hello", "world", "foo", "bar"]
        assert _detect_datetime_format(values) is None


class TestCoerceDatetimeSortKeyEpoch:
    """Tests for epoch handling in coerce_datetime_sort_key."""

    def test_epoch_seconds(self):
        result = coerce_datetime_sort_key("1705312981")
        assert isinstance(result, float)
        assert abs(result - 1705312981.0) < 1

    def test_epoch_millis(self):
        result = coerce_datetime_sort_key("1705312981000")
        assert isinstance(result, float)
        assert abs(result - 1705312981.0) < 1

    def test_epoch_hint(self):
        result = coerce_datetime_sort_key("1705312981", fmt_hint="epoch")
        assert isinstance(result, float)
        assert abs(result - 1705312981.0) < 1

    def test_epoch_millis_hint(self):
        result = coerce_datetime_sort_key("1705312981000", fmt_hint="epoch")
        assert isinstance(result, float)
        assert abs(result - 1705312981.0) < 1

    def test_syslog_hint(self):
        result = coerce_datetime_sort_key("Jan 15 14:23:01", fmt_hint="%b %d %H:%M:%S")
        assert isinstance(result, float)

    def test_coerce_sort_key_with_fmt_hint(self):
        result = coerce_sort_key("1705312981", ColumnType.DATETIME, fmt_hint="epoch")
        assert isinstance(result, float)


class TestFormatDatetimeValue:
    """Tests for format_datetime_value() conversion function."""

    def test_iso_to_epoch(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("2024-01-15 10:30:00", None, "epoch")
        assert result.isdigit()

    def test_epoch_to_iso(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("1705312981", "epoch", "iso")
        assert "2024-01-15" in result

    def test_iso_to_strftime(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("2024-01-15 10:30:00", None, "%H:%M:%S")
        assert result == "10:30:00"

    def test_iso_to_date_only(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("2024-01-15 10:30:00", None, "%Y-%m-%d")
        assert result == "2024-01-15"

    def test_epoch_to_strftime(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("1705312981", "epoch", "%Y-%m-%d")
        assert result == "2024-01-15"

    def test_to_relative(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("2020-01-01 00:00:00", None, "relative")
        assert "ago" in result

    def test_to_epoch_ms(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("2024-01-15 10:30:00", None, "epoch_ms")
        assert len(result) == 13  # milliseconds

    def test_invalid_value_returns_original(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("not a date", None, "epoch")
        assert result == "not a date"

    def test_empty_value_returns_original(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("", None, "epoch")
        assert result == ""

    def test_syslog_to_iso(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("Jan 15 14:23:01", "%b %d %H:%M:%S", "iso")
        assert "14:23:01" in result

    def test_tz_utc_to_eastern(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value(
            "2024-01-15 15:00:00", None, "UTC>US/Eastern %H:%M:%S"
        )
        assert result == "10:00:00"

    def test_tz_source_only(self):
        from nless.dataprocessing import format_datetime_value

        # Source=UTC, target=local — result depends on local tz but should not fail
        result = format_datetime_value("2024-01-15 12:00:00", None, "UTC> iso")
        assert "2024-01-15" in result

    def test_tz_target_only(self):
        from nless.dataprocessing import format_datetime_value

        # Source=local (naive), target=UTC
        result = format_datetime_value("2024-01-15 12:00:00", None, ">UTC iso")
        assert "+00:00" in result

    def test_tz_no_conversion(self):
        from nless.dataprocessing import format_datetime_value

        # No timezone spec — same as before
        result = format_datetime_value("2024-01-15 10:30:00", None, "%H:%M:%S")
        assert result == "10:30:00"

    def test_tz_abbreviation(self):
        from nless.dataprocessing import format_datetime_value

        result = format_datetime_value("2024-01-15 15:00:00", None, "UTC>PST %H:%M:%S")
        assert result == "07:00:00"

    def test_parse_tz_and_format(self):
        from nless.dataprocessing import parse_tz_and_format

        assert parse_tz_and_format("UTC>US/Eastern %H:%M:%S") == (
            "UTC",
            "US/Eastern",
            "%H:%M:%S",
        )
        assert parse_tz_and_format(">UTC epoch") == (None, "UTC", "epoch")
        assert parse_tz_and_format("UTC> iso") == ("UTC", None, "iso")
        assert parse_tz_and_format("%H:%M:%S") == (None, None, "%H:%M:%S")
        assert parse_tz_and_format("epoch") == (None, None, "epoch")
