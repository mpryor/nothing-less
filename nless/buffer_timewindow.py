"""Time window filtering and rolling timer management for NlessBuffer."""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

from .types import UpdateReason

if TYPE_CHECKING:
    from .buffer import NlessBuffer


class TimeWindowMixin:
    """Mixin providing time window methods for NlessBuffer."""

    @staticmethod
    def _parse_duration(text: str) -> float | None:
        """Parse a duration string like '5m', '1h30m', '30s', '2d' into seconds.

        Plain numbers are treated as minutes.  Returns None on invalid input.
        """
        text = text.strip()
        if not text:
            return None
        # Plain number → minutes
        try:
            return float(text) * 60
        except ValueError:
            pass
        units = {"d": 86400, "h": 3600, "m": 60, "s": 1}
        total = 0.0
        for match in re.finditer(r"(\d+(?:\.\d+)?)\s*([dhms])", text, re.IGNORECASE):
            total += float(match.group(1)) * units[match.group(2).lower()]
        return total if total > 0 else None

    def apply_time_window_setting(self: NlessBuffer, value: str) -> None:
        """Parse a time window value and apply it to the buffer.

        Handles clearing ("0", "off", etc.), rolling windows ("5m+"), and
        one-shot windows that permanently prune raw_rows.
        """
        value = value.strip()
        if not value or value in ("0", "off", "clear", "none"):
            self.time_window = None
            self.rolling_time_window = False
            self._time_window_ceiling = None
            self._time_window_column = None
            self._stop_rolling_timer()
            self.invalidate_caches()
            self._deferred_update_table(reason=UpdateReason.CLEARING_WINDOW)
            return

        # Check for "colname duration" syntax (e.g. "timestamp 5m+")
        col_name = None
        parts = value.rsplit(None, 1)
        if len(parts) == 2:
            from .dataprocessing import strip_markup
            from .types import ColumnType

            candidate_col, duration_part = parts
            dt_names = {
                strip_markup(c.name)
                for c in self.current_columns
                if c.effective_type == ColumnType.DATETIME
            }
            if candidate_col in dt_names:
                col_name = candidate_col
                value = duration_part

        rolling = value.endswith("+")
        if rolling:
            value = value.rstrip("+").strip()

        duration = self._parse_duration(value)
        if duration is None:
            self.notify(
                "Invalid duration. Use e.g. 5m, 1h, 30s, 2h30m (+ for rolling). "
                "Prefix with column name to filter by column: timestamp 5m",
                severity="error",
            )
            return

        self._time_window_column = col_name
        self.time_window = duration
        self.rolling_time_window = rolling
        self.invalidate_caches()
        if rolling:
            self._time_window_ceiling = None
            self._deferred_update_table(reason=UpdateReason.APPLYING_WINDOW)
            self._start_rolling_timer()
        else:
            self._stop_rolling_timer()
            # Fixed window: capture the current moment as the upper bound so
            # newly streamed rows that arrive after this point are excluded.
            self._time_window_ceiling = time.time()
            self._deferred_update_table(reason=UpdateReason.APPLYING_WINDOW)

    def _apply_initial_time_window(self: NlessBuffer, window_str: str) -> None:
        """Apply a time window from CLI args.

        Supports "colname duration" syntax (e.g. "timestamp 5m+").
        Column validation is deferred until data loads and types are inferred.
        """
        window_str = window_str.strip()
        # Check for "colname duration" syntax
        col_name = None
        parts = window_str.rsplit(None, 1)
        if len(parts) == 2:
            candidate_col, duration_part = parts
            # Can't validate column yet (no data loaded), store optimistically
            col_name = candidate_col
            window_str = duration_part

        rolling = window_str.endswith("+")
        value = window_str.rstrip("+").strip()
        duration = self._parse_duration(value)
        if duration is None:
            return
        self._time_window_column = col_name
        self.time_window = duration
        self.rolling_time_window = rolling

    def _apply_time_window(self: NlessBuffer, rows: list[list[str]]) -> list[list[str]]:
        """Filter rows by the active time window.

        When a parsed datetime column is available (_time_window_column),
        uses the parsed timestamps from that column instead of arrival
        timestamps.  For fixed windows with a parsed column, the ceiling
        is the max parsed timestamp (not wall clock), making ``@5m``
        mean "last 5 minutes of the log" rather than wall-clock time.
        Falls back to arrival timestamps otherwise.
        """
        if not self.time_window:
            return rows

        # Try using parsed datetime column
        if self._time_window_column is not None:
            from .dataprocessing import coerce_datetime_sort_key, strip_markup

            col_idx = self._get_col_idx_by_name(self._time_window_column, False)
            if col_idx is not None:
                # Find the Column object for fmt_hint
                fmt_hint = None
                for col in self.current_columns:
                    if strip_markup(col.name) == self._time_window_column:
                        fmt_hint = col.datetime_fmt_hint
                        break

                # Parse timestamps from the column
                parsed_ts = []
                for row in rows:
                    if col_idx < len(row):
                        ts = coerce_datetime_sort_key(
                            strip_markup(row[col_idx]), fmt_hint
                        )
                        parsed_ts.append(ts if isinstance(ts, float) else None)
                    else:
                        parsed_ts.append(None)

                valid_ts = [t for t in parsed_ts if t is not None]
                if valid_ts:
                    if self.rolling_time_window:
                        ceiling = time.time()
                    elif self._time_window_ceiling is not None:
                        # Fixed window with explicit ceiling — use max parsed
                        ceiling = max(valid_ts)
                    else:
                        ceiling = max(valid_ts)
                    cutoff = ceiling - self.time_window
                    return [
                        row
                        for row, ts in zip(rows, parsed_ts)
                        if ts is None or (ts >= cutoff and ts <= ceiling)
                    ]

        # Fallback: use arrival timestamps
        ceiling = self._time_window_ceiling  # None for rolling windows
        if ceiling is not None:
            cutoff = ceiling - self.time_window
        else:
            cutoff = time.time() - self.time_window
        # rows are parallel to self._arrival_timestamps after _filter_rows
        return [
            row
            for i, row in enumerate(rows)
            if i < len(self._arrival_timestamps)
            and self._arrival_timestamps[i] >= cutoff
            and (ceiling is None or self._arrival_timestamps[i] <= ceiling)
        ]

    def _start_rolling_timer(self: NlessBuffer) -> None:
        """Start a periodic timer that re-applies the time window filter."""
        self._stop_rolling_timer()
        try:
            interval = min(self.time_window / 10, 5.0) if self.time_window else 5.0
            interval = max(interval, 1.0)
            self._rolling_timer = self.set_interval(interval, self._tick_rolling)
        except (RuntimeError, Exception):
            pass  # Not mounted yet

    def _stop_rolling_timer(self: NlessBuffer) -> None:
        """Stop the rolling time window timer."""
        if self._rolling_timer is not None:
            self._safe_widget_call(self._rolling_timer.stop)
            self._rolling_timer = None

    def _tick_rolling(self: NlessBuffer) -> None:
        """Re-apply the time window filter to drop expired rows."""
        if not self.time_window or not self.rolling_time_window:
            self._stop_rolling_timer()
            return
        self.invalidate_caches()
        self._deferred_update_table(reason=UpdateReason.ROLLING_TICK)
