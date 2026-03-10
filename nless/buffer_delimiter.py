"""Delimiter inference, switching, auto-detection, and formatting for NlessBuffer."""

from __future__ import annotations

import json
import re
import threading
import time
from typing import TYPE_CHECKING

from .delimiter import infer_delimiter, split_line

if TYPE_CHECKING:
    from .buffer import NlessBuffer


def _sample_lines(lines: list[str], max_total: int = 15) -> list[str]:
    """Sample lines from beginning, middle, and end for delimiter inference."""
    n = len(lines)
    if n <= max_total:
        return lines
    chunk = max_total // 3
    start = lines[:chunk]
    mid_start = (n - chunk) // 2
    middle = lines[mid_start : mid_start + chunk]
    end = lines[n - chunk :]
    return start + middle + end


def _majority_sample(lines: list[str], max_total: int = 10) -> list[str]:
    """Pick lines from the largest group sharing the same word count.

    This avoids the space-delimiter -20 penalty in ``infer_delimiter``
    that fires when lines have inconsistent field counts.
    """

    if len(lines) <= 1:
        return lines[:max_total]
    counts: dict[int, list[str]] = {}
    for line in lines:
        wc = len(line.split())
        counts.setdefault(wc, []).append(line)
    # Pick the word-count group with the most lines
    best = max(counts.values(), key=len)
    return best[:max_total]


class DelimiterMixin:
    """Mixin providing delimiter management methods for NlessBuffer."""

    # -- Formatting --------------------------------------------------------

    @staticmethod
    def _format_delimiter_label(d) -> str:
        """Return a human-readable label for a delimiter value."""
        if isinstance(d, re.Pattern):
            return f"regex({d.pattern})"
        return {" ": "space", "  ": "space+", "\t": "tab", ",": "csv"}.get(d, d)

    def _format_delimiter(self: NlessBuffer) -> str | None:
        """Return a human-readable label for the current delimiter."""
        if self.delimiter is None:
            return None
        if self.delimiter_name:
            return self.delimiter_name
        return self._format_delimiter_label(self.delimiter)

    def _reset_delimiter_state(self: NlessBuffer) -> None:
        """Reset all delimiter-related flags and counters."""
        self.delimiter_inferred = False
        self._delimiter_suggestion_shown = False
        self._mismatch_warning_shown = False
        self._total_skipped = 0

    # -- Parsing -----------------------------------------------------------

    @staticmethod
    def _parse_delimiter_input(value: str) -> str | re.Pattern:
        """Parse user delimiter input, handling tab escape and regex compilation.

        Returns a compiled regex Pattern if the input has named capture groups,
        otherwise returns the delimiter string.
        """
        if value not in ("raw", "json"):
            try:
                pattern = re.compile(rf"{value}")
                if pattern.groups > 0:
                    return pattern
            except (re.error, ValueError):
                pass
        if value == "\\t":
            return "\t"
        if value == "space":
            return " "
        if value == "space+":
            return "  "
        return value

    # -- Switching ---------------------------------------------------------

    def switch_delimiter(self: NlessBuffer, delimiter_input: str) -> bool:
        """Switch to a new delimiter, adjusting header and raw_rows internally.

        Handles all state transitions: clears filters/sort/unique, resolves the
        new header, re-inserts or removes header rows as needed, and triggers a
        table rebuild.

        Returns True if the switch succeeded and a rebuild was triggered.
        """
        if not delimiter_input:
            return False
        should_update = False
        had_filters = bool(self.current_filters)
        with self._try_lock(
            "delimiter",
            deferred=lambda: self.switch_delimiter(delimiter_input),
        ) as acquired:
            if not acquired:
                return False
            self.current_filters = []
            self.search_term = None
            self.sort_column = None
            self.unique_column_names = set()
            prev_delimiter = self.delimiter

            self._reset_delimiter_state()
            self.delimiter_name = None
            delimiter = self._parse_delimiter_input(delimiter_input)

            # Restore original file order when preamble lines were skipped
            # by find_header_index during initial load.  This ensures the
            # original first line is available as a header candidate for the
            # new delimiter, and the old "header" (which was really a data
            # line) goes back into raw_rows.
            if self._preamble_lines:
                old_first = self.first_log_line
                self.first_log_line = self._preamble_lines[0]
                # In JSON/raw/regex modes, first_log_line is already in
                # raw_rows (every line is data).  Don't re-add it.
                already_in_raw = self.raw_rows and self.raw_rows[0] == old_first
                # For headerless new delimiters (raw/json/regex), the
                # restored first_log_line must also appear as a data row.
                new_is_headerless = (
                    delimiter == "raw"
                    or delimiter == "json"
                    or isinstance(delimiter, re.Pattern)
                )
                restore = (
                    list(self._preamble_lines)
                    if new_is_headerless
                    else self._preamble_lines[1:]
                )
                if not already_in_raw:
                    restore.append(old_first)
                ts = (
                    self._arrival_timestamps[0]
                    if self._arrival_timestamps
                    else time.time()
                )
                for i, line in enumerate(restore):
                    self.raw_rows.insert(i, line)
                    self._arrival_timestamps.insert(i, ts)
                self._preamble_lines = []

            if isinstance(delimiter, re.Pattern):
                self.delimiter = delimiter
                self.current_columns = self._make_columns(
                    list(delimiter.groupindex.keys())
                )
                self._ensure_arrival_column(self.current_columns)
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
                    self.raw_rows.insert(0, self.first_log_line)
                    ts = (
                        self._arrival_timestamps[0]
                        if self._arrival_timestamps
                        else time.time()
                    )
                    self._arrival_timestamps.insert(0, ts)
                should_update = True
            else:
                self.delimiter = delimiter
                result = self._resolve_new_header(delimiter, prev_delimiter)
                if result is not None:
                    new_header, parsed_full_json_file = result

                    if self._should_reinsert_header_as_data(
                        prev_delimiter, delimiter, parsed_full_json_file
                    ):
                        self.raw_rows.insert(0, self.first_log_line)
                        ts = (
                            self._arrival_timestamps[0]
                            if self._arrival_timestamps
                            else time.time()
                        )
                        self._arrival_timestamps.insert(0, ts)

                    self.current_columns = self._make_columns(list(new_header))
                    self._ensure_arrival_column(self.current_columns)
                    should_update = True

        if should_update:
            self.raw_mode = self.delimiter == "raw"
            n_preamble = len(self._preamble_lines)

            def callback():
                if had_filters:
                    self._flash_status("Filters cleared — delimiter changed")
                if n_preamble:
                    self.notify(
                        f"{n_preamble} line{'s' if n_preamble != 1 else ''}"
                        " not matching delimiter, skipped (press ~ to view)",
                        severity="warning",
                    )

            self._deferred_update_table(reason="Changing delimiter", callback=callback)
        return should_update

    def _resolve_new_header(self: NlessBuffer, delimiter, prev_delimiter):
        """Determine the new header columns when switching delimiters.

        Returns (header_list, parsed_full_json_file) or None on error.
        All raw_rows / first_log_line mutations happen here, keeping them
        internal to the buffer.
        """
        if delimiter == "raw":
            return ["log"], False

        if delimiter == "json":
            return self._resolve_json_header(prev_delimiter)

        if prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern):
            header = split_line(
                self.raw_rows[0],
                self.delimiter,
                self.current_columns,
            )
            self.raw_rows.pop(0)
            return header, False

        return split_line(
            self.first_log_line,
            self.delimiter,
            self.current_columns,
        ), False

    def _resolve_json_header(self: NlessBuffer, prev_delimiter):
        """Resolve header columns for JSON delimiter.

        Scans first_log_line then raw_rows for the first valid JSON dict.
        Lines before the first match are saved as preamble (recoverable
        via ~ or delimiter switch).  Falls back to parsing the entire
        buffer as a single JSON payload.

        Returns (header_list, parsed_full_json_file) or None on error.
        """
        # Build candidate list: first_log_line + raw_rows, avoiding the
        # duplicate that exists in raw/regex mode (where first_log_line
        # is also raw_rows[0]).
        first_in_raw = self.raw_rows and self.raw_rows[0] == self.first_log_line
        candidates = (
            self.raw_rows if first_in_raw else [self.first_log_line] + self.raw_rows
        )

        for i, line in enumerate(candidates):
            try:
                header = list(json.loads(line).keys())
                # Save non-matching lines before this one as preamble
                if i > 0:
                    self._preamble_lines.extend(candidates[:i])
                self.first_log_line = line
                # Trim raw_rows and timestamps to start after the match.
                # When first_in_raw, candidates[i] == raw_rows[i].
                # Otherwise candidates[0] is first_log_line, so
                # raw_rows starts at candidates[1] → raw index = i - 1.
                raw_start = i + 1 if first_in_raw else max(0, i)
                self.raw_rows = self.raw_rows[raw_start:]
                self._arrival_timestamps = self._arrival_timestamps[raw_start:]
                return header, False
            except (json.JSONDecodeError, KeyError, TypeError):
                continue

        # Fallback: try to read all logs as one JSON payload
        try:
            all_logs = ""
            if prev_delimiter != "raw" and not isinstance(prev_delimiter, re.Pattern):
                all_logs = self.first_log_line + "\n"
            all_logs += "\n".join(self.raw_rows)
            buffer_json = json.loads(all_logs)
            if (
                isinstance(buffer_json, list)
                and len(buffer_json) > 0
                and isinstance(buffer_json[0], dict)
            ):
                header = list(buffer_json[0].keys())
                self.raw_rows = [json.dumps(item) for item in buffer_json]
            elif isinstance(buffer_json, dict):
                header = list(buffer_json.keys())
                self.raw_rows = [json.dumps(buffer_json)]
            else:
                self.notify(
                    "Failed to parse JSON logs: no valid JSON found",
                    severity="error",
                )
                return None
            self.first_log_line = self.raw_rows[0]
            return header, True
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            self.notify(f"Failed to parse JSON logs: {e}", severity="error")
            return None

    @staticmethod
    def _should_reinsert_header_as_data(
        prev_delimiter, new_delimiter, parsed_full_json_file
    ) -> bool:
        """Check whether the old header line should be re-inserted as a data row.

        Needed when switching from a standard delimiter (first line = header)
        to raw/json/regex (every line is data).
        """
        if prev_delimiter == new_delimiter or parsed_full_json_file:
            return False
        prev_is_standard = (
            prev_delimiter != "raw"
            and not isinstance(prev_delimiter, re.Pattern)
            and prev_delimiter != "json"
        )
        new_is_headerless = (
            new_delimiter == "raw"
            or isinstance(new_delimiter, re.Pattern)
            or new_delimiter == "json"
        )
        return prev_is_standard and new_is_headerless

    # -- Auto-switch -------------------------------------------------------

    def _warn_mismatch_once(self: NlessBuffer, count: int) -> None:
        """Show the mismatch warning at most once per delimiter."""
        if self._mismatch_warning_shown:
            return
        self._mismatch_warning_shown = True
        msg = f"{count} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing."
        if self.app._thread_id == threading.get_ident():
            self.notify(msg, severity="warning")
        else:
            self.app.call_from_thread(self.notify, msg, severity="warning")

    def _try_auto_switch_delimiter(
        self: NlessBuffer,
        bad_lines: list[str],
        n_bad: int,
        n_total: int,
        *,
        lines_already_in_raw: bool = False,
    ) -> str | None:
        """Attempt to auto-switch delimiter based on mismatched rows.

        Returns a flash message string if switched, None otherwise.
        Also calls _warn_mismatch_once if not switching.
        """
        if not self._should_auto_switch_delimiter(n_bad, n_total):
            self._warn_mismatch_once(n_bad)
            return None
        majority = _majority_sample(bad_lines)
        candidate = infer_delimiter(majority)
        if not candidate or candidate == self.delimiter or candidate == "raw":
            self._warn_mismatch_once(n_bad)
            return None
        return self._apply_auto_switch_delimiter(
            candidate,
            bad_lines,
            majority,
            n_bad,
            lines_already_in_raw=lines_already_in_raw,
        )

    def _should_auto_switch_delimiter(
        self: NlessBuffer, n_bad: int, n_total: int
    ) -> bool:
        """Check whether auto-switch conditions are met."""
        return (
            self.delimiter_inferred
            and not self._delimiter_suggestion_shown
            and not self._initial_load_done
            and n_bad >= 3
            and n_total > 0
            and n_bad / n_total > 0.3
        )

    def _apply_auto_switch_delimiter(
        self: NlessBuffer,
        candidate,
        bad_lines: list[str],
        majority: list[str],
        n_bad: int,
        *,
        lines_already_in_raw: bool = False,
    ) -> str:
        """Switch to a new delimiter and prepare for rebuild.

        Mutates buffer state: delimiter, columns, caches, raw_rows.
        Returns the flash message string.

        Args:
            lines_already_in_raw: If True, bad_lines are already preserved in
                raw_rows (e.g. by _filter_rows) and should not be re-added.
        """
        old_label = self._format_delimiter_label(self.delimiter)
        self.delimiter = candidate
        self.delimiter_inferred = True
        self._delimiter_suggestion_shown = True
        self._mismatch_warning_shown = False
        self._total_skipped = 0
        self._parsed_rows = None
        self._cached_col_widths = None
        now = time.time()
        if not lines_already_in_raw:
            self.raw_rows.extend(bad_lines)
            self._arrival_timestamps.extend([now] * len(bad_lines))
        # Old header may not parse with new delimiter — add as data
        self.raw_rows.append(self.first_log_line)
        self._arrival_timestamps.append(now)
        # New header is drawn from majority lines — remove from raw_rows
        # to avoid it appearing as both header and data row.
        new_header = majority[0]
        try:
            idx = self.raw_rows.index(new_header)
            self.raw_rows.pop(idx)
            self._arrival_timestamps.pop(idx)
        except (ValueError, IndexError):
            pass
        self.first_log_line = new_header
        parts = self._parse_first_line_columns(self.first_log_line)
        self.current_columns = self._make_columns(parts)
        self._ensure_arrival_column(self.current_columns)
        new_label = self._format_delimiter_label(candidate)
        return (
            f"Switched delimiter to {new_label} ({n_bad} rows failed with {old_label})"
        )
