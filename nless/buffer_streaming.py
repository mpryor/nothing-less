"""Streaming log ingestion and incremental row updates for NlessBuffer.

Threading: three threads touch this module —
  - Input thread (daemon): StdinLineStream calls add_logs()
  - Worker thread (@work): _process_deferred_data() runs under lock
  - Main thread: all widget mutation via call_from_thread()

self._lock serialises StreamState + column/filter mutations.
_last_flushed_idx tracks render progress; a gap means new data arrived
during a rebuild. _update_generation prevents stale results from
superseded rebuilds.
"""

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

from .dataprocessing import (
    choose_parse_strategy,
    highlight_regex_patterns,
    strip_markup,
)
from .delimiter import split_line
from .operations import handle_mark_unique
from .types import MetadataColumn, RowLengthMismatchError, UpdateReason


if TYPE_CHECKING:
    from .buffer import NlessBuffer

logger = logging.getLogger(__name__)

# Rows processed per incremental add_rows call. Large enough to amortize
# per-call overhead, small enough for progressive display on large initial loads.
STREAMING_CHUNK_SIZE = 50_000

# Column widths stabilize after the first N rows. Tracking beyond this
# threshold costs O(N*cols) len() calls with diminishing returns.
COLUMN_WIDTH_SAMPLE_SIZE = 10_000

# Maximum number of unparseable lines retained for the ~ (excluded lines) view.
# Caps memory usage on pathological inputs.
MAX_SKIPPED_LINES_SAMPLE = 200

# Batch size above which streaming with expensive ops (sort/dedup/time window)
# triggers a deferred rebuild instead of per-row incremental inserts.
DEFERRED_REBUILD_THRESHOLD = 1000


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

    def add_logs(
        self: NlessBuffer, log_lines: list[str], source: str | None = None
    ) -> None:
        needs_deferred = False
        self._skipped_lines = []
        with self._lock:
            self.locked = True
            self._current_source = source
            # Merged streams: skip duplicate header from subsequent sources
            if (
                source is not None
                and self.first_row_parsed
                and log_lines
                and log_lines[0].strip() == self.first_log_line.strip()
            ):
                log_lines = log_lines[1:]
            was_loading = self.loading_state.reason is not None
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
                    had_loading = self.loading_state.reason is not None
                    self.loading_state.reason = None
                    self._request_spinner_stop()
                    types_changed = self._infer_column_types_from_displayed()
                    if types_changed:
                        try:
                            new_labels = self._get_visible_column_labels()
                            dt = self.query_one(".nless-view")
                            dt.columns = new_labels
                            # Expand column widths to fit new labels
                            for i, label in enumerate(new_labels):
                                if (
                                    i < len(dt.column_widths)
                                    and len(label) > dt.column_widths[i]
                                ):
                                    dt.column_widths[i] = len(label)
                            dt.refresh()
                        except Exception:
                            logger.debug(
                                "Column header refresh after type inference failed",
                                exc_info=True,
                            )
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
                        logger.debug("Post-load status update failed", exc_info=True)
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
                        self._deferred_update_table(
                            reason=UpdateReason.SWITCHING_DELIMITER
                        )

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
            logger.debug("Post-add_logs dispatch failed", exc_info=True)

    def _infer_and_set_delimiter(self: NlessBuffer, log_lines: list[str]) -> list[str]:
        """Infer delimiter from first few lines and set it on the buffer.

        May flatten pretty-printed JSON into JSONL, returning a modified
        log_lines list.
        """
        from .buffer_delimiter import _sample_lines
        from .delimiter import (
            detect_space_splitting_strategy,
            flatten_json_lines,
            infer_delimiter,
        )

        sample = _sample_lines(log_lines, max_total=15)
        self.delim.value = infer_delimiter(sample)
        self.delim.inferred = True

        if isinstance(self.delim.value, str) and self.delim.value in (" ", "  "):
            self.delim.column_positions, self.delim.max_fields = (
                detect_space_splitting_strategy(sample, self.delim.value)
            )

        flattened = flatten_json_lines(log_lines)
        if flattened is not log_lines:
            self.delim.value = "json"
            log_lines = flattened

        if self.delim.value == "raw" and not self.raw_mode:
            self.raw_mode = True

        return log_lines

    def _try_auto_detect_log_format(self: NlessBuffer, log_lines: list[str]) -> None:
        """Attempt to auto-detect a structured log format during initial load.

        Called before first parse when delimiter is inferred as raw/space+/comma.
        If exactly one format matches strongly, overrides the delimiter so
        _parse_first_row creates the correct named columns.  If multiple
        match, shows a hint to press P.
        """
        if self._log_format_checked:
            return
        self._log_format_checked = True

        from .logformats import detect_log_formats, infer_log_pattern
        from .buffer_delimiter import _sample_lines

        sample = _sample_lines(log_lines, max_total=15)
        candidates = detect_log_formats(sample)

        # Also try inference if no known format matches
        inferred = infer_log_pattern(sample)
        if inferred is not None:
            non_empty = [line for line in sample if line.strip()]
            if non_empty:
                matches = sum(1 for line in non_empty if inferred.pattern.match(line))
                ratio = matches / len(non_empty)
                score = ratio * 100 + len(inferred.pattern.groupindex) * 2
                if ratio >= 0.8:
                    candidates.append((inferred, score))
                    candidates.sort(key=lambda x: x[1], reverse=True)

        if not candidates:
            return

        # Auto-apply the top-scoring named format.  Only show a selection
        # menu when multiple "Auto-detected" (inferred) patterns compete
        # without a clear named winner.
        best_fmt, best_score = candidates[0]
        auto_apply = best_fmt.name != "Auto-detected" or len(candidates) == 1

        if auto_apply:
            fmt = best_fmt
            # Override delimiter before first parse — _parse_first_row will
            # see the regex and create named columns directly.
            self.delim.value = fmt.pattern
            self.delim.inferred = True
            if fmt.name != "Auto-detected":
                self.delim.name = fmt.name
            self.raw_mode = False

            label = fmt.name or "Auto-detected"

            if not getattr(self.app, "demo_mode", False):

                def _notify():
                    self.notify(f"Detected log format: {label}")

                if self.app._thread_id == threading.get_ident():
                    _notify()
                else:
                    self.app.call_from_thread(_notify)
        else:

            def _hint():
                self.notify(
                    f"{len(candidates)} log formats detected — press P to choose",
                    severity="information",
                )

            if self.app._thread_id == threading.get_ident():
                _hint()
            else:
                self.app.call_from_thread(_hint)

    def _skip_preamble(self: NlessBuffer, log_lines: list[str]) -> list[str]:
        """Skip leading non-data lines for standard delimiters.

        Preserves skipped preamble lines so they're available when
        switching to raw mode.
        """
        from .delimiter import find_header_index, find_preamble_end

        # Position-based splitting gives consistent field counts for full-
        # width lines, but short preamble lines (e.g. netstat's "Active
        # Internet connections") still need to be skipped.
        if self.delim.column_positions:
            preamble_end = find_preamble_end(log_lines)
            if preamble_end > 0:
                self.delim.preamble_lines = log_lines[:preamble_end]
                log_lines = log_lines[preamble_end:]
            return log_lines

        header_idx = find_header_index(
            log_lines, self.delim.value, max_fields=self.delim.max_fields
        )
        if header_idx > 0:
            self.delim.preamble_lines = log_lines[:header_idx]
            log_lines = log_lines[header_idx:]
        return log_lines

    def _parse_first_row(
        self: NlessBuffer, log_lines: list[str], data_table
    ) -> list[str]:
        """Parse the first row to establish columns, then consume the header.

        Sets first_row_parsed, current_columns, and applies initial column
        filter/time window from CLI args. Returns log_lines with header consumed.
        """
        self.first_log_line = log_lines[0]
        parts = self._parse_first_line_columns(self.first_log_line)
        self.current_columns = self._make_columns(parts)
        self._ensure_arrival_column(self.current_columns)
        if self._current_source is not None:
            self._ensure_source_column(self.current_columns)
            # Show and pin _source column first for merge mode
            for col in self.current_columns:
                if col.name != MetadataColumn.SOURCE.value and not col.hidden:
                    col.render_position += 1
            for col in self.current_columns:
                if col.name == MetadataColumn.SOURCE.value:
                    col.hidden = False
                    col.pinned = True
                    col.render_position = 0
                    break
            self._rebuild_column_caches()
            data_table.add_columns(self._get_visible_column_labels())
        else:
            data_table.add_columns(
                [c.name for c in self.current_columns if not c.hidden]
            )

        # For non-special delimiters, first line is the header
        header_consumed = (
            self.delim.value != "raw"
            and not isinstance(self.delim.value, re.Pattern)
            and self.delim.value != "json"
        )
        if header_consumed:
            log_lines = log_lines[1:]

        if self.query.unique_column_names:
            for unique_col_name in self.query.unique_column_names:
                handle_mark_unique(self, unique_col_name)
            data_table.clear(columns=True)
            data_table.add_columns(self._get_visible_column_labels())

        self._rebuild_column_caches()
        self.first_row_parsed = True

        if self._cli_args and self._cli_args.columns:
            self._apply_initial_column_filter(self._cli_args.columns)
            data_table.clear(columns=True)
            data_table.add_columns(self._get_visible_column_labels())

        if self._cli_args and self._cli_args.time_window:
            self._apply_initial_time_window(self._cli_args.time_window)

        return log_lines

    def _add_logs_inner(self: NlessBuffer, log_lines: list[str]) -> None:
        data_table = self.query_one(".nless-view")

        if not self.delim.value and log_lines:
            log_lines = self._infer_and_set_delimiter(log_lines)

            # Auto-detect log format before first parse.  Overrides the
            # inferred delimiter so _parse_first_row sees the regex and
            # creates correct named columns from the start.
            if (
                self.delim.inferred
                and self.delim.value in ("raw", "  ", ",")
                and self._pending_session_state is None
            ):
                self._try_auto_detect_log_format(log_lines)

        if (
            self.delim.inferred
            and not self.first_row_parsed
            and self.delim.value
            and self.delim.value not in ("raw", "json")
            and not isinstance(self.delim.value, re.Pattern)
        ):
            log_lines = self._skip_preamble(log_lines)

        if not self.first_row_parsed:
            log_lines = self._parse_first_row(log_lines, data_table)

            if self._pending_session_state is not None:
                from .session import apply_buffer_state

                apply_buffer_state(self, self._pending_session_state)
                self._pending_session_state = None
                # apply_buffer_state only rebuilds caches — it does NOT call
                # _deferred_update_table (that would invoke UI ops from this
                # streaming thread and hang).  _needs_deferred_update below
                # ensures the main thread triggers the rebuild.
                now = time.time()
                batch_timestamps = [now] * len(log_lines)
                batch_src = (
                    [self._current_source or ""] * len(log_lines)
                    if self._has_source_column
                    else None
                )
                filtered, filtered_timestamps, filtered_sources = self._filter_lines(
                    log_lines, batch_timestamps, source_labels=batch_src
                )
                self.stream.extend(filtered, filtered_timestamps, filtered_sources)
                self._needs_deferred_update = True
                return

        now = time.time()
        batch_timestamps = [now] * len(log_lines)
        batch_source_labels = (
            [self._current_source or ""] * len(log_lines)
            if self._has_source_column
            else None
        )
        filtered, filtered_timestamps, filtered_sources = self._filter_lines(
            log_lines, batch_timestamps, source_labels=batch_source_labels
        )
        self.stream.extend(filtered, filtered_timestamps, filtered_sources)

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

        is_large_batch = len(filtered) > STREAMING_CHUNK_SIZE

        if self._needs_full_rebuild():
            if self.loading_state.reason:
                # A deferred update is in flight — just extend raw_rows (done above).
                # The in-flight update will chain another when it finishes.
                pass
            elif self.chain.timer is not None:
                # A coalesced rebuild is already scheduled — let data accumulate.
                pass
            elif len(filtered) > DEFERRED_REBUILD_THRESHOLD:
                self.loading_state.reason = self.loading_state.reason or "Loading"
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
                and len(self.displayed_rows) > DEFERRED_REBUILD_THRESHOLD
            ):
                # Small batch during streaming with expensive ops — schedule
                # a coalesced rebuild instead of O(N) per-line inserts.
                self.loading_state.reason = "Loading"
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
                        self.delim.total_skipped += 1
                        if len(self._skipped_lines) < MAX_SKIPPED_LINES_SAMPLE:
                            self._skipped_lines.append(line)
                        continue
        else:
            # Process in chunks for progressive display on large inputs
            CHUNK = STREAMING_CHUNK_SIZE
            had_rows = self._initial_load_done and bool(self.displayed_rows)
            if is_large_batch:
                self.loading_state.reason = self.loading_state.reason or "Loading"
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

            if self.raw_mode and not self._has_source_column:
                from .rawpager import RawPager

                try:
                    if not isinstance(self.query_one(".nless-view"), RawPager):
                        if self.app._thread_id == threading.get_ident():
                            self._deferred_raw_swap()
                        else:
                            self.app.call_from_thread(self._deferred_raw_swap)
                except Exception:
                    logger.debug("Raw pager swap check failed", exc_info=True)

    def _apply_row_highlighting(
        self: NlessBuffer,
        new_rows: list[list[str]],
        data_table,
        highlight: bool,
    ) -> list[list[str]]:
        """Apply search, regex, and green highlights, then flush to display."""
        if self.query.search_term:
            styled = self._highlight_search_matches(
                new_rows,
                data_table.fixed_columns,
                row_offset=len(self.displayed_rows),
            )
        else:
            styled = new_rows

        if self.regex_highlights:
            styled = highlight_regex_patterns(
                styled, self.regex_highlights, data_table.fixed_columns
            )

        # Highlight new streaming rows green (skip initial load)
        if highlight and self.displayed_rows:
            styled = [[self._highlight_markup(c) for c in row] for row in styled]

        self.displayed_rows.extend(styled)
        data_table.add_rows_precomputed(styled)
        self._last_flushed_idx = len(self.raw_rows)

        if self.is_tailing:
            data_table.action_scroll_bottom()

        return styled

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
        col_positions = [col.data_position for col in self.cache.sorted_visible_columns]
        metadata = [mc.value for mc in MetadataColumn]
        expected = len(self.current_columns) - len(
            [c for c in self.current_columns if c.name in metadata]
        )
        formatted_arrival = self._format_arrival(arrival_ts or time.time())
        column_widths = data_table.column_widths
        n_visible = len(col_positions)
        if len(column_widths) < n_visible:
            column_widths.extend([0] * (n_visible - len(column_widths)))
            data_table.column_widths = column_widths

        parse, needs_cleanup = choose_parse_strategy(
            self.delim.value,
            self._has_nested_delimiters,
            self.current_columns,
            column_positions=self.delim.column_positions,
        )

        WIDTH_SAMPLE = COLUMN_WIDTH_SAMPLE_SIZE
        already_displayed = len(self.displayed_rows)
        track_widths = already_displayed < WIDTH_SAMPLE
        _strip = str.strip
        _len = len

        _MAX_SKIPPED_SAMPLE = MAX_SKIPPED_LINES_SAMPLE
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
            # Append source label if source column exists
            if self._has_source_column:
                cells.append(self._current_source or "")
            # Strip for fast-path parsers to match split_line output
            if needs_cleanup:
                cells = [_strip(c) for c in cells]
            cached_cells.append(cells)
            # Align to visible columns
            row = [cells[p] for p in col_positions]
            new_rows.append(row)

        self.delim.total_skipped += skipped_count
        remaining = MAX_SKIPPED_LINES_SAMPLE - len(self._skipped_lines)
        if remaining > 0:
            self._skipped_lines.extend(skipped_lines[:remaining])

        # Populate _parsed_rows cache so the first sort/filter after loading
        # doesn't need to re-parse all rows via split_line.
        if skipped_count == 0 and cached_cells:
            if self.cache.parsed_rows is None:
                self.cache.parsed_rows = []
            self.cache.parsed_rows.extend(cached_cells)

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

        self._apply_row_highlighting(new_rows, data_table, highlight)

    def _add_log_line(
        self: NlessBuffer, log_line: str, arrival_ts: float | None = None
    ):
        """Adds a single log line, applying filters, dedup, sort, and search highlighting."""
        ts = arrival_ts or time.time()
        # Non-rolling time window: drop rows arriving after the frozen ceiling
        if self._time_window_ceiling is not None and ts > self._time_window_ceiling:
            return
        data_table = self.query_one(".nless-view")
        cells = split_line(
            log_line,
            self.delim.value,
            self.current_columns,
            column_positions=self.delim.column_positions,
        )
        cells.append(self._format_arrival(ts))
        if self._has_source_column:
            cells.append(self._current_source or "")
        if self.query.unique_column_names:
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

        if self.regex_highlights:
            cells = highlight_regex_patterns(
                [cells], self.regex_highlights, data_table.fixed_columns
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

        if self.query.unique_column_names:
            dedup_key = self._build_composite_key(cells, render_position=True)
            self._update_dedup_indices_after_insertion(dedup_key, new_index)

        if self.is_tailing:
            data_table.action_scroll_bottom()
