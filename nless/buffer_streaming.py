"""Streaming log ingestion and incremental row updates for NlessBuffer."""

from __future__ import annotations

import csv
import json
import logging
import re
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .delimiter import split_line
from .operations import handle_mark_unique
from .types import MetadataColumn, RowLengthMismatchError

if TYPE_CHECKING:
    from .buffer import NlessBuffer

logger = logging.getLogger(__name__)


class StreamingMixin:
    """Mixin providing log ingestion and incremental update methods for NlessBuffer."""

    @contextmanager
    def _try_lock(self: NlessBuffer, action: str, deferred: Callable | None = None):
        """Context manager for non-blocking lock acquisition.

        If *deferred* is provided and the lock is busy, the action is queued
        to run once ``add_logs`` releases the lock instead of showing a
        warning.  Only one pending action is kept (last wins).
        """
        if not self._lock.acquire(blocking=False):
            if deferred:
                self._pending_action = (action, deferred)
                self.notify(f"{action.capitalize()} queued…", severity="information")
            else:
                self.notify("Data loading, please wait…", severity="warning")
            yield False
            return
        try:
            yield True
        finally:
            self._lock.release()

    def add_logs(self: NlessBuffer, log_lines: list[str]) -> None:
        needs_deferred = False
        self._skipped_lines = []
        with self._lock:
            self.locked = True
            was_loading = self._loading_reason is not None
            try:
                self._needs_deferred_update = False
                self._add_logs_inner(log_lines)
                needs_deferred = self._needs_deferred_update
            except Exception:
                logger.debug(
                    "add_logs failed (app may be shutting down)", exc_info=True
                )
            finally:
                # Don't tear down loading state when a deferred rebuild is
                # already in flight (was_loading) or freshly requested
                # (needs_deferred).  The in-flight rebuild's _apply_to_widgets
                # will handle cleanup when it finishes.
                if not needs_deferred and not was_loading:
                    had_loading = self._loading_reason is not None
                    self._loading_reason = None
                    self._request_spinner_stop()
                    try:
                        self._update_status_bar()
                        if had_loading:
                            row_count = len(self.displayed_rows)
                            if row_count > 0:
                                self.app.call_from_thread(
                                    self._flash_status,
                                    f"Loaded {row_count:,} rows",
                                )
                    except (RuntimeError, Exception):
                        pass  # Widget not mounted or app shutting down
                self.locked = False

        try:
            if needs_deferred:
                if self.app._thread_id == threading.get_ident():
                    self._deferred_update_table()
                else:
                    self.app.call_from_thread(self._deferred_update_table)

            skipped_lines = self._skipped_lines
            skipped = len(skipped_lines)
            if skipped > 0 and not needs_deferred:
                self._skipped_lines = []
                n_total = len(self.displayed_rows) + skipped
                flash_msg = self._try_auto_switch_delimiter(
                    skipped_lines,
                    skipped,
                    n_total,
                    lines_already_in_raw=True,
                )
                if flash_msg:

                    def rebuild():
                        self._flash_status(flash_msg)
                        self._deferred_update_table(reason="Switching delimiter")

                    if self.app._thread_id == threading.get_ident():
                        rebuild()
                    else:
                        self.app.call_from_thread(rebuild)
                    return

            pending = self._pending_action
            if pending is not None:
                self._pending_action = None
                _, fn = pending
                self.app.call_from_thread(fn)
        except Exception:
            pass  # App shutting down

    def _add_logs_inner(self: NlessBuffer, log_lines: list[str]) -> None:
        from .buffer_delimiter import _sample_lines

        data_table = self.query_one(".nless-view")

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            from .delimiter import infer_delimiter

            sample = _sample_lines(log_lines, max_total=15)
            self.delimiter = infer_delimiter(sample)
            self.delimiter_inferred = True
            if self.delimiter == "raw" and not self.raw_mode:
                # Auto-detected raw mode — set up minimal state and defer
                # to _deferred_update_table which will swap the widget.
                self.raw_mode = True
                self.first_log_line = log_lines[0]
                parts = self._parse_first_line_columns(self.first_log_line)
                self.current_columns = self._make_columns(parts)
                self._ensure_arrival_column(self.current_columns)
                self._rebuild_column_caches()
                self.first_row_parsed = True
                now = time.time()
                self.raw_rows.extend(log_lines)
                self._arrival_timestamps.extend([now] * len(log_lines))
                self._needs_deferred_update = True
                return

        if not self.first_row_parsed:
            self.first_log_line = log_lines[0]
            parts = self._parse_first_line_columns(self.first_log_line)
            self.current_columns = self._make_columns(parts)
            self._ensure_arrival_column(self.current_columns)
            data_table.add_columns([str(p) for p in parts])

            # For non-special delimiters, first line is the header
            if (
                self.delimiter != "raw"
                and not isinstance(self.delimiter, re.Pattern)
                and self.delimiter != "json"
            ):
                log_lines = log_lines[1:]

            if self.unique_column_names:
                for unique_col_name in self.unique_column_names:
                    handle_mark_unique(self, unique_col_name)
                data_table.clear(columns=True)
                data_table.add_columns(self._get_visible_column_labels())

            self._rebuild_column_caches()
            self.first_row_parsed = True

            if self._cli_args and self._cli_args.columns:
                self._apply_initial_column_filter(self._cli_args.columns)

            if self._cli_args and self._cli_args.time_window:
                self._apply_initial_time_window(self._cli_args.time_window)

        now = time.time()
        batch_timestamps = [now] * len(log_lines)
        filtered, filtered_timestamps = self._filter_lines(log_lines, batch_timestamps)
        self.raw_rows.extend(filtered)
        self._arrival_timestamps.extend(filtered_timestamps)

        # Reveal pivot-hidden columns when new streaming data arrives
        pivot_revealed = False
        if filtered and self._pivot_hidden_columns:
            for col in self.current_columns:
                col_name = strip_markup(col.name)
                if col_name in self._pivot_hidden_columns:
                    col.hidden = False
            self._pivot_hidden_columns.clear()
            self._rebuild_column_caches()
            pivot_revealed = True

        is_large_batch = len(filtered) > 50000

        if self.sort_column is not None or self.unique_column_names or self.time_window:
            if self._loading_reason:
                # A deferred update is in flight — just extend raw_rows (done above).
                # The in-flight update will chain another when it finishes.
                pass
            elif self._chain_timer is not None:
                # A coalesced rebuild is already scheduled — let data accumulate.
                pass
            elif len(filtered) > 1000:
                self._loading_reason = self._loading_reason or "Loading"
                self._request_spinner_start()
                self._update_status_bar()
                self._needs_deferred_update = True
            elif pivot_revealed:
                # Track new lines so the rebuild can highlight them green
                self._green_lines = set(filtered)
                self.call_after_refresh(
                    lambda: self._deferred_update_table(
                        restore_position=False, reason=self._reload_reason()
                    )
                )
            elif (
                self.line_stream
                and not self.line_stream.done
                and len(self.displayed_rows) > 1000
            ):
                # Small batch during streaming with expensive ops — schedule
                # a coalesced rebuild instead of O(N) per-line inserts.
                self._loading_reason = "Loading"
                self._request_spinner_start()
                self._update_status_bar()
                self._needs_deferred_update = True
            else:
                for line in filtered:
                    try:
                        self._add_log_line(line, arrival_ts=now)
                    except (
                        RowLengthMismatchError,
                        json.JSONDecodeError,
                        csv.Error,
                        ValueError,
                        IndexError,
                        TypeError,
                    ):
                        self._total_skipped += 1
                        if len(self._skipped_lines) < 200:
                            self._skipped_lines.append(line)
                        continue
        else:
            # Process in chunks for progressive display on large inputs
            CHUNK = 50000
            had_rows = self._initial_load_done and bool(self.displayed_rows)
            if is_large_batch:
                self._loading_reason = self._loading_reason or "Loading"
                self._request_spinner_start()
            try:
                for i in range(0, len(filtered), CHUNK):
                    self._add_rows_incremental(
                        filtered[i : i + CHUNK],
                        highlight=had_rows,
                        arrival_ts=now,
                    )
                    if is_large_batch:
                        self._update_status_bar()
            except Exception:
                logger.debug(
                    "Incremental add failed, falling back to rebuild", exc_info=True
                )
                self._needs_deferred_update = True

    def _add_rows_incremental(
        self: NlessBuffer,
        new_lines: list[str],
        highlight: bool = True,
        arrival_ts: float | None = None,
    ) -> None:
        """Parse and add new lines to the table without a full rebuild.

        Fuses parsing, column alignment, and column width tracking into a
        single pass, then bypasses the normal add_rows width computation.
        """
        data_table = self.query_one(".nless-view")
        col_positions = [col.data_position for col in self._sorted_visible_columns]
        metadata = [mc.value for mc in MetadataColumn]
        expected = len(self.current_columns) - len(
            [c for c in self.current_columns if c.name in metadata]
        )
        formatted_arrival = self._format_arrival(arrival_ts or time.time())
        column_widths = data_table.column_widths
        delimiter = self.delimiter
        has_nested = self._has_nested_delimiters
        columns = self.current_columns

        # Choose parse strategy once outside the hot loop
        if not has_nested and delimiter == ",":
            needs_cleanup = True

            def parse(line):
                s = line.strip()
                return next(csv.reader([s])) if '"' in s else s.split(",")

        elif not has_nested and delimiter == "\t":
            needs_cleanup = True

            def parse(line):
                return line.split("\t")

        elif (
            not has_nested
            and isinstance(delimiter, str)
            and delimiter not in ("raw", "json", " ", "  ")
        ):
            needs_cleanup = True

            def parse(line):
                return line.split(delimiter)

        else:
            needs_cleanup = False  # split_line already cleans cells

            def parse(line):
                return split_line(line, delimiter, columns)

        # Column widths stabilize quickly; only track for a sample
        WIDTH_SAMPLE = 10000
        already_displayed = len(self.displayed_rows)
        track_widths = already_displayed < WIDTH_SAMPLE
        _strip = str.strip
        _len = len

        _MAX_SKIPPED_SAMPLE = 200
        new_rows = []
        cached_cells = []
        skipped_lines = []
        skipped_count = 0
        for line in new_lines:
            try:
                cells = parse(line)
            except (json.JSONDecodeError, csv.Error, ValueError, StopIteration):
                skipped_count += 1
                if _len(skipped_lines) < _MAX_SKIPPED_SAMPLE:
                    skipped_lines.append(line)
                continue
            if _len(cells) != expected:
                skipped_count += 1
                if _len(skipped_lines) < _MAX_SKIPPED_SAMPLE:
                    skipped_lines.append(line)
                continue
            # Append arrival timestamp (metadata column at end)
            cells.append(formatted_arrival)
            # Strip for fast-path parsers to match split_line output
            if needs_cleanup:
                cells = [_strip(c) for c in cells]
            cached_cells.append(cells)
            # Align to visible columns
            row = [cells[p] for p in col_positions]
            new_rows.append(row)

        self._total_skipped += skipped_count
        remaining = 200 - len(self._skipped_lines)
        if remaining > 0:
            self._skipped_lines.extend(skipped_lines[:remaining])

        # Populate _parsed_rows cache so the first sort/filter after loading
        # doesn't need to re-parse all rows via split_line.
        if skipped_count == 0 and cached_cells:
            if self._parsed_rows is None:
                self._parsed_rows = []
            self._parsed_rows.extend(cached_cells)

        # Track column widths from a sample to avoid O(n*cols) len() calls
        if track_widths and new_rows:
            sample = new_rows[:WIDTH_SAMPLE]
            for row in sample:
                for i, cell in enumerate(row):
                    cl = _len(cell)
                    if cl > column_widths[i]:
                        column_widths[i] = cl

        if not new_rows:
            self._last_flushed_idx = len(self.raw_rows)
            return

        if self.search_term:
            styled = self._highlight_search_matches(
                new_rows,
                data_table.fixed_columns,
                row_offset=len(self.displayed_rows),
            )
        else:
            styled = new_rows

        # Highlight new streaming rows green (skip initial load)
        if highlight and self.displayed_rows:
            styled = [[self._highlight_markup(c) for c in row] for row in styled]

        self.displayed_rows.extend(styled)
        data_table.add_rows_precomputed(styled)
        self._last_flushed_idx = len(self.raw_rows)

        if self.is_tailing:
            data_table.action_scroll_bottom()

    def _add_log_line(
        self: NlessBuffer, log_line: str, arrival_ts: float | None = None
    ):
        """Adds a single log line, applying filters, dedup, sort, and search highlighting."""
        ts = arrival_ts or time.time()
        # Non-rolling time window: drop rows arriving after the frozen ceiling
        if self._time_window_ceiling is not None and ts > self._time_window_ceiling:
            return
        data_table = self.query_one(".nless-view")
        cells = split_line(log_line, self.delimiter, self.current_columns)
        cells.append(self._format_arrival(ts))
        if self.unique_column_names:
            cells.insert(0, "1")

        if len(cells) != len(self.current_columns):
            raise RowLengthMismatchError()

        if not self._matches_all_filters(cells):
            return

        cells, old_index, old_row = self._handle_dedup_for_line(cells)
        is_dedup_update = old_index is not None
        data_cells = list(cells)  # snapshot before alignment (data-position order)
        new_index = self._find_sorted_insert_index(cells)

        try:
            cells = self._align_cells_to_visible_columns([cells])[0]
        except (IndexError, KeyError):
            raise RowLengthMismatchError()
        cells = self._highlight_search_matches(
            [cells], data_table.fixed_columns, row_offset=new_index
        )[0]

        # Highlight new/updated rows green (dedup updates already have green from _handle_dedup_for_line)
        if not is_dedup_update and self.displayed_rows:
            cells = [self._highlight_markup(c) for c in cells]

        if old_index is not None:
            self._update_dedup_indices_after_removal(old_index)
            self.displayed_rows.pop(old_index)
            data_table.remove_row(old_index)

        data_table.add_row_at(index=new_index, row_data=cells)
        self.displayed_rows.insert(new_index, cells)

        self._update_sort_keys_for_line(data_cells, old_row)

        if self.unique_column_names:
            dedup_key = self._build_composite_key(cells, render_position=True)
            self._update_dedup_indices_after_insertion(dedup_key, new_index)

        if self.is_tailing:
            data_table.action_scroll_bottom()
