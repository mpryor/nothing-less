"""Time window filtering and rolling timer management for NlessBuffer."""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

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
            self._stop_rolling_timer()
            self.invalidate_caches()
            self._deferred_update_table(reason="Clearing time window")
            return

        rolling = value.endswith("+")
        if rolling:
            value = value.rstrip("+").strip()

        duration = self._parse_duration(value)
        if duration is None:
            self.notify(
                "Invalid duration. Use e.g. 5m, 1h, 30s, 2h30m (+ for rolling)",
                severity="error",
            )
            return

        self.time_window = duration
        self.rolling_time_window = rolling
        self.invalidate_caches()
        if rolling:
            self._time_window_ceiling = None
            self._deferred_update_table(reason="Applying time window")
            self._start_rolling_timer()
        else:
            self._stop_rolling_timer()
            # Fixed window: capture the current moment as the upper bound so
            # newly streamed rows that arrive after this point are excluded.
            self._time_window_ceiling = time.time()
            self._deferred_update_table(reason="Applying time window")

    def _apply_initial_time_window(self: NlessBuffer, window_str: str) -> None:
        """Apply a time window from CLI args."""
        rolling = window_str.endswith("+")
        value = window_str.rstrip("+").strip()
        duration = self._parse_duration(value)
        if duration is None:
            return
        self.time_window = duration
        self.rolling_time_window = rolling

    def _apply_time_window(self: NlessBuffer, rows: list[list[str]]) -> list[list[str]]:
        """Filter rows by the active time window using arrival timestamps."""
        if not self.time_window:
            return rows
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
            try:
                self._rolling_timer.stop()
            except (RuntimeError, Exception):
                pass
            self._rolling_timer = None

    def _tick_rolling(self: NlessBuffer) -> None:
        """Re-apply the time window filter to drop expired rows."""
        if not self.time_window or not self.rolling_time_window:
            self._stop_rolling_timer()
            return
        self.invalidate_caches()
        self._deferred_update_table(reason="")
