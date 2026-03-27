import csv
import logging
from datetime import datetime, timezone
import json
import re
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from copy import deepcopy

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Static
from textual import work

from .delimiter import split_line
from .input import LineStream
from .datatable import Datatable as NlessDataTable
from .dataprocessing import (
    build_composite_key,
    coerce_sort_key,
    find_sorted_insert_index,
    highlight_regex_patterns,
    matches_all_filters,
    strip_markup,
    update_dedup_indices_after_insertion,
    update_dedup_indices_after_removal,
    update_sort_keys_for_line,
)
from .statusbar import build_status_text
from .types import (
    CliArgs,
    CacheState,
    ChainTimerState,
    Column,
    DelimiterState,
    FilterSortState,
    LoadingState,
    MetadataColumn,
    StatusContext,
    StreamState,
    UpdateReason,
)

from .buffer_actions import ActionsMixin
from .buffer_columns import ColumnMixin
from .buffer_delimiter import DelimiterMixin
from .buffer_search import SearchMixin
from .buffer_streaming import StreamingMixin
from .buffer_timewindow import TimeWindowMixin

logger = logging.getLogger(__name__)


class NlessBuffer(
    ActionsMixin,
    ColumnMixin,
    DelimiterMixin,
    TimeWindowMixin,
    StreamingMixin,
    SearchMixin,
    Static,
):
    """A modern pager with tabular data sorting/filtering capabilities.

    INVARIANTS
    ----------
    Parallel arrays
      raw_rows, _arrival_timestamps, and _source_labels are parallel
      arrays and must always satisfy:
        len(raw_rows) == len(_arrival_timestamps)
        len(_source_labels) == 0 or len(_source_labels) == len(raw_rows)
      Any method that appends to or removes from raw_rows MUST perform
      the same operation on _arrival_timestamps in the same code path.
      _source_labels is only populated in merge mode (_has_source_column
      is True); in single-source mode it remains empty.
      Violation: silent data loss in time window filtering and source
      column rendering.

    render_position vs data_position
      Every Column has two position fields:
        data_position: index into a parsed row's cell list. Stable
          after column creation. Only modified during delimiter switches
          and computed column insertion.
        render_position: visual position on screen. May be freely
          mutated by column reordering, pinning, and hiding operations.
      _align_cells_to_visible_columns() bridges the two by mapping
      render-ordered visible columns back to their data_position
      indices. Never update data_position for display purposes.

    _last_flushed_idx
      Tracks how far into raw_rows has been rendered to displayed_rows.
      Invariant: displayed_rows reflects raw_rows[0:_last_flushed_idx]
      after applying current filters, sort, and delimiter.
      Every code path that modifies displayed_rows is responsible for
      updating _last_flushed_idx to len(raw_rows) when the render is
      complete. The streaming chain rebuild logic uses the gap between
      len(raw_rows) and _last_flushed_idx to detect unrendered data.

    _update_generation
      Monotonically increasing counter incremented at the start of
      every _deferred_update_table call. Background processing checks
      this value before applying results - if the generation has
      advanced since processing started, the result is discarded.
      This ensures rapid successive operations (sort -> sort -> sort)
      only apply the final state.
    """

    # ── Class Attributes / Init / Properties ──────────────────────
    ENABLE_COMMAND_PALETTE = False

    CSS_PATH = "nless.tcss"

    BINDINGS = [
        Binding("q", "quit", "Quit", id="buffer.quit"),
        Binding("y", "copy", "Copy cell contents", id="buffer.copy"),
        Binding(
            "c", "jump_columns", "Jump to column (by select)", id="buffer.jump_columns"
        ),
        Binding(
            ">", "move_column_right", "Move column right", id="buffer.move_column_right"
        ),
        Binding(
            "<", "move_column_left", "Move column left", id="buffer.move_column_left"
        ),
        Binding("s", "sort", "Sort selected column", id="buffer.sort"),
        Binding("n", "next_search", "Next search result", id="buffer.next_search"),
        Binding(
            "p",
            "previous_search",
            "Previous search result",
            id="buffer.previous_search",
        ),
        Binding(
            "*",
            "search_cursor_word",
            "Search (all columns) for word under cursor",
            id="buffer.search_cursor_word",
        ),
        Binding(
            "~",
            "view_unparsed_logs",
            "View logs not matching delimiter",
            id="buffer.view_unparsed_logs",
        ),
        Binding(
            "t",
            "toggle_tail",
            "Keep cursor at the bottom of the screen even as new logs arrive.",
            id="buffer.toggle_tail",
        ),
        Binding(
            "x",
            "reset_highlights",
            "Reset new-line highlights",
            id="buffer.reset_highlights",
        ),
        Binding(
            "m",
            "pin_column",
            "Pin/unpin column to the left",
            id="buffer.pin_column",
        ),
        Binding(
            "X",
            "hide_column",
            "Hide the current column",
            id="buffer.hide_column",
        ),
        Binding(
            "a",
            "aggregations",
            "Show column aggregations",
            id="buffer.aggregations",
        ),
        Binding(
            "u",
            "undo_columns",
            "Undo last column split",
            id="buffer.undo_columns",
        ),
    ]

    def __init__(
        self,
        pane_id: int,
        cli_args: CliArgs | None,
        line_stream: LineStream | None = None,
    ):
        super().__init__()

        self._cli_args = cli_args
        self.line_stream = line_stream
        self._lock = threading.RLock()
        self._pending_action: tuple[str, Callable] | None = None
        self.locked = False
        self.pane_id: int = pane_id
        self.mounted = False
        self.raw_mode: bool = cli_args.raw if cli_args else False
        if line_stream:
            line_stream.subscribe(self, self.add_logs, lambda: self.mounted)

        self.stream = StreamState()
        self.first_row_parsed = False
        self._all_source_lines: list[str] | None = None  # unfiltered history for ~
        self.displayed_rows = []
        self.first_log_line = ""
        self._last_flushed_idx = 0
        self._initial_load_done = False

        self.current_columns: list[Column] = []
        self._column_history: list[list[Column]] = []
        if cli_args and cli_args.delimiter:
            pattern = re.compile(cli_args.delimiter)
            if pattern.groups > 0 and pattern.groupindex:
                initial_delim = pattern
            else:
                initial_delim = cli_args.delimiter
        else:
            initial_delim = None
        self.delim = DelimiterState(value=initial_delim)
        self._has_nested_delimiters = False

        self.query = FilterSortState(
            filters=cli_args.filters if cli_args else [],
            unique_column_names=cli_args.unique_keys if cli_args else set(),
        )
        if cli_args and cli_args.sort_by:
            sort_column, direction = cli_args.sort_by.split("=")
            self.query.sort_column = sort_column
            self.query.sort_reverse = direction.lower() == "desc"

        self.is_tailing = cli_args.tail if cli_args else False
        self._pivot_hidden_columns: set[str] = set()
        self._current_source: str | None = None
        self._has_source_column: bool = False

        self.time_window: float | None = None
        self.rolling_time_window: bool = False
        self._time_window_ceiling: float | None = None
        self._rolling_timer = None
        self._time_window_column: str | None = None

        self.cache = CacheState()
        self.loading_state = LoadingState()
        self._status_ctx = StatusContext()
        self._update_generation = 0
        self._needs_deferred_update = False
        self._skipped_lines: list[str] = []

        self._green_lines: set[str] | None = None
        self._source_parse_filter: Callable[[str], bool] | None = None
        self._bp_timer = None
        self._bp_behind: bool = False
        self.regex_highlights: list[tuple[re.Pattern, str]] = []
        self.marks: dict[str, int] = {}  # letter → display row index
        self._previous_cursor_row: int | None = None  # for '' jump-back

        self._pre_view_state = None
        self._pre_view_raw_rows: list[str] | None = None
        self._pre_view_timestamps: list[float] | None = None
        self._log_format_checked = False
        self._pending_session_state = None
        self._pending_cursor_position: tuple[int, int] | None = None
        self.chain = ChainTimerState()

    # ── StreamState compatibility properties ─────────────────
    @property
    def raw_rows(self) -> list[str]:
        return self.stream.raw_rows

    @raw_rows.setter
    def raw_rows(self, value: list[str]) -> None:
        import warnings

        warnings.warn(
            "Direct assignment to raw_rows is deprecated; use stream.replace_raw_rows()",
            DeprecationWarning,
            stacklevel=2,
        )
        self.stream._raw_rows = value

    @property
    def _arrival_timestamps(self) -> list[float]:
        return self.stream.arrival_timestamps

    @_arrival_timestamps.setter
    def _arrival_timestamps(self, value: list[float]) -> None:
        import warnings

        warnings.warn(
            "Direct assignment to _arrival_timestamps is deprecated; use stream methods",
            DeprecationWarning,
            stacklevel=2,
        )
        self.stream._arrival_timestamps = value

    @property
    def _source_labels(self) -> list[str]:
        return self.stream.source_labels

    @_source_labels.setter
    def _source_labels(self, value: list[str]) -> None:
        import warnings

        warnings.warn(
            "Direct assignment to _source_labels is deprecated; use stream.set_source_labels()",
            DeprecationWarning,
            stacklevel=2,
        )
        self.stream._source_labels = value

    # ── Lifecycle ───────────────────────────────────────────────────
    def compose(self) -> ComposeResult:
        """Create and yield the DataTable or RawPager widget."""
        with Vertical():
            theme = self._get_theme()
            if self.raw_mode:
                from .rawpager import RawPager

                yield RawPager(theme=theme)
            else:
                yield NlessDataTable(theme=theme)

    def on_mount(self) -> None:
        self.mounted = True
        if self._pending_session_state is not None and self.first_row_parsed:
            from .session import apply_buffer_state

            apply_buffer_state(self, self._pending_session_state)
            self._pending_session_state = None
            self._deferred_update_table(reason=UpdateReason.SESSION)
        if self.loading_state.reason:
            self._start_spinner()
        if not self._initial_load_done:
            self.set_timer(1.0, self._mark_initial_load_done)
        if self.rolling_time_window and self.time_window:
            self._start_rolling_timer()
        if self.line_stream and not self.line_stream.done:
            is_pipe = True
            if hasattr(self.line_stream, "is_streaming"):
                try:
                    is_pipe = self.line_stream.is_streaming()
                except (OSError, Exception):
                    is_pipe = False
            if is_pipe:
                self._start_bp_timer()

    def _mark_initial_load_done(self) -> None:
        self._initial_load_done = True

    def on_datatable_cell_highlighted(
        self, event: NlessDataTable.CellHighlighted
    ) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def copy(self, pane_id) -> "NlessBuffer":
        """Create a duplicate buffer with shared line_stream subscription.

        Most caches (_cached_col_widths, _sort_keys, _dedup_key_to_row_idx)
        are NOT copied — they are rebuilt on the first _deferred_update_table
        call.  _parsed_rows IS copied when valid, so the new buffer's first
        rebuild can skip re-parsing all rows via split_line.
        """
        # Snapshot state under lock, then release so the UI stays responsive.
        with self._lock:
            raw_rows_snapshot = list(self.raw_rows)
            new_buffer = NlessBuffer(pane_id=pane_id, cli_args=None)
            new_buffer.first_row_parsed = self.first_row_parsed
            new_buffer.displayed_rows = []
            new_buffer.first_log_line = self.first_log_line
            new_buffer.delim.preamble_lines = list(self.delim.preamble_lines)
            new_buffer.current_columns = deepcopy(self.current_columns)
            new_buffer.query.filters = deepcopy(self.query.filters)
            new_buffer.query.search_term = self.query.search_term
            new_buffer.query.sort_column = self.query.sort_column
            new_buffer.query.sort_reverse = self.query.sort_reverse
            new_buffer.query.search_matches = deepcopy(self.query.search_matches)
            new_buffer.query.current_match_index = self.query.current_match_index
            new_buffer.delim.value = self.delim.value
            new_buffer.delim.inferred = self.delim.inferred
            new_buffer.delim.name = self.delim.name
            new_buffer.is_tailing = self.is_tailing
            new_buffer.query.unique_column_names = deepcopy(
                self.query.unique_column_names
            )
            new_buffer.query.count_by_column_key = deepcopy(
                self.query.count_by_column_key
            )
            new_buffer._pivot_hidden_columns = set(self._pivot_hidden_columns)
            new_buffer.time_window = self.time_window
            new_buffer.rolling_time_window = self.rolling_time_window
            new_buffer._time_window_ceiling = self._time_window_ceiling
            new_buffer.line_stream = self.line_stream
            timestamps_snapshot = list(self._arrival_timestamps)
            source_labels_snapshot = list(self._source_labels)
            if self.line_stream:
                self.line_stream.subscribe_future_only(
                    new_buffer,
                    new_buffer.add_logs,
                    lambda: new_buffer.mounted,
                )
            new_buffer._rebuild_column_caches()
            new_buffer._initial_load_done = True

        # Expensive filtering runs outside the lock on the snapshot.
        filtered_rows, filtered_ts, filtered_sources = new_buffer._filter_lines(
            raw_rows_snapshot,
            timestamps_snapshot,
            source_labels=source_labels_snapshot or None,
        )
        new_buffer.stream.extend(filtered_rows, filtered_ts, filtered_sources)
        # Copy parsed-row cache so the new buffer's first rebuild doesn't
        # re-parse everything via split_line.  When _filter_lines removed
        # rows, len(_parsed_rows) > len(raw_rows) and _partition_rows will
        # safely ignore the stale cache.
        if self.cache.parsed_rows is not None and len(self.cache.parsed_rows) == len(
            raw_rows_snapshot
        ):
            new_buffer.cache.parsed_rows = list(self.cache.parsed_rows)
        # Keep unfiltered history so ~ can find excluded lines even without
        # a line_stream.  The snapshot is shared (not copied) to save memory.
        new_buffer._all_source_lines = raw_rows_snapshot
        return new_buffer

    @staticmethod
    def init_as_merged(
        pane_id: int,
        buf1: "NlessBuffer",
        buf2: "NlessBuffer",
        source1: str,
        source2: str,
    ) -> "NlessBuffer":
        """Create a new buffer by merging two buffers' raw_rows, interleaved by arrival time.

        The merged buffer gets a _source column to identify the origin of each row.
        """
        new_buffer = NlessBuffer(pane_id=pane_id, cli_args=None)
        # Use first buffer's structure
        new_buffer.first_row_parsed = buf1.first_row_parsed
        new_buffer.first_log_line = buf1.first_log_line
        new_buffer.delim.value = buf1.delim.value
        new_buffer.delim.inferred = buf1.delim.inferred
        new_buffer.delim.name = buf1.delim.name
        new_buffer.current_columns = deepcopy(buf1.current_columns)

        # Ensure source column exists
        from .buffer_columns import ColumnMixin

        ColumnMixin._ensure_source_column(new_buffer.current_columns)

        # Interleave rows sorted by arrival time
        pairs1 = list(
            zip(buf1._arrival_timestamps, buf1.raw_rows, [source1] * len(buf1.raw_rows))
        )
        pairs2 = list(
            zip(buf2._arrival_timestamps, buf2.raw_rows, [source2] * len(buf2.raw_rows))
        )
        merged = sorted(pairs1 + pairs2, key=lambda x: x[0])

        merged_rows = [row for _, row, _ in merged]
        merged_timestamps = [ts for ts, _, _ in merged]
        merged_sources = [src for _, _, src in merged]
        new_buffer.stream.extend(merged_rows, merged_timestamps, merged_sources)

        new_buffer._rebuild_column_caches()
        new_buffer._initial_load_done = True
        return new_buffer

    def init_as_unparsed(
        self,
        rows: list[str],
        source_parse_filter: Callable[[str], bool],
        line_stream: LineStream | None = None,
    ) -> None:
        """Set up this buffer as a raw view of unparsed/excluded lines.

        Tries to infer a delimiter from the rejected lines (they may share
        a consistent format like JSON even though they didn't match the
        parent's delimiter).  Falls back to raw mode when inference fails.

        Configures delimiter, columns, raw_rows, and optionally subscribes
        to ongoing stream updates so newly rejected lines appear automatically.
        """
        from .buffer_delimiter import _sample_lines
        from .delimiter import infer_delimiter

        sample = _sample_lines(rows, max_total=15)
        inferred = infer_delimiter(sample)

        if inferred and inferred != "raw":
            self.delim.value = inferred
            self.delim.inferred = True
            self.first_log_line = rows[0]
            parts = self._parse_first_line_columns(self.first_log_line)
            self.current_columns = self._make_columns(parts)
            # For non-raw/json/regex delimiters, first line is the header
            header_consumed = (
                not isinstance(inferred, re.Pattern) and inferred != "json"
            )
            if header_consumed:
                rows = rows[1:]
        else:
            self.delim.value = "raw"
            self.delim.inferred = False
            self.raw_mode = True
            self.first_log_line = rows[0]
            self.current_columns = self._make_columns(["log"])

        self.first_row_parsed = True
        self._ensure_arrival_column(self.current_columns)
        self._rebuild_column_caches()
        now = time.time()
        self.stream.replace_raw_rows(rows, [now] * len(rows))
        self._initial_load_done = True
        self._source_parse_filter = source_parse_filter

        if line_stream:
            parse_filter = source_parse_filter

            def unparsed_stream_filter(lines: list[str]) -> None:
                rejected = [line for line in lines if not parse_filter(line)]
                if rejected:
                    self.add_logs(rejected)

            line_stream.subscribe_future_only(
                self,
                unparsed_stream_filter,
                lambda: self.mounted,
            )
            self.line_stream = line_stream

    # ── Data Processing (filter / sort / dedup) ────────────────────
    def _filter_lines(
        self,
        lines: list[str],
        timestamps: list[float] | None = None,
        source_labels: list[str] | None = None,
    ) -> tuple[list[str], list[float], list[str] | None]:
        """Return only lines (and their timestamps/source labels) that match all current filters."""
        now = time.time()
        if timestamps is None:
            timestamps = [now] * len(lines)
        if not self.query.filters:
            return lines, timestamps, source_labels
        metadata = [mc.value for mc in MetadataColumn]
        expected = len([c for c in self.current_columns if c.name not in metadata])
        matching = []
        kept_timestamps = []
        kept_sources = [] if source_labels is not None else None
        for i, line in enumerate(lines):
            try:
                cells = split_line(
                    line,
                    self.delim.value,
                    self.current_columns,
                    column_positions=self.delim.column_positions,
                )
            except (json.JSONDecodeError, csv.Error, ValueError):
                continue
            if len(cells) != expected:
                continue
            cells.append(self._format_arrival(timestamps[i]))
            if self._has_source_column:
                cells.append(source_labels[i] if source_labels else "")
            if self._matches_all_filters(cells, adjust_for_count=True):
                matching.append(line)
                kept_timestamps.append(timestamps[i])
                if kept_sources is not None:
                    kept_sources.append(source_labels[i])
        return matching, kept_timestamps, kept_sources

    def _filter_rows(
        self, expected_cell_count: int
    ) -> tuple[list[list[str]], list[str]]:
        """Parse raw rows, filter by current filters, return (matching, mismatched).

        Uses a parsed-row cache to avoid re-parsing on sort/search operations.
        Also compacts raw_rows to only keep matching + unparseable lines so that
        subsequent _deferred_update_table() calls (sort, search, etc.) scan fewer rows.
        """
        filtered, mismatched, compacted = self._partition_rows(expected_cell_count)
        if compacted is not None:
            self._apply_raw_rows_compaction(compacted)
        elif filtered and (
            self.cache.parsed_rows is None
            or len(filtered) != len(self.cache.parsed_rows)
        ):
            # Partial-cache fast path: update cache with newly parsed rows.
            # Copy so downstream in-place sort doesn't corrupt the cache.
            self.cache.parsed_rows = list(filtered)
        return filtered, mismatched

    def _partition_rows(self, expected_cell_count: int):
        """Partition raw_rows into (filtered, mismatched, compaction_data).

        Pure read of self.raw_rows — does NOT mutate buffer state.
        Returns:
            filtered:  list of parsed cell lists that passed all filters
            mismatched: raw line strings that didn't parse with the current delimiter
            compacted: tuple of (kept_raw, kept_parsed, kept_timestamps,
                        unparseable_raw, unparseable_timestamps)
        """
        # Use cached parsed rows if available (sort/search don't need re-parse).
        # A partial cache (shorter than raw_rows) is also valid — we'll only
        # parse the new rows appended since the last rebuild.
        if self.cache.parsed_rows is not None and len(self.cache.parsed_rows) <= len(
            self.raw_rows
        ):
            parsed = self.cache.parsed_rows
        else:
            parsed = None

        # Fast path: full cache + no filters → all rows pass, skip the loop
        if (
            parsed is not None
            and len(parsed) == len(self.raw_rows)
            and not self.query.filters
        ):
            return list(parsed), [], None

        # Pre-computed by _rebuild_column_caches() before _filter_rows().
        has_computed = self._has_nested_delimiters

        # Fast path: partial cache + no filters → only parse new rows
        if (
            parsed is not None
            and not self.query.filters
            and len(parsed) < len(self.raw_rows)
        ):
            new_parsed = []
            mismatched = []
            for i in range(len(parsed), len(self.raw_rows)):
                row_str = self.raw_rows[i]
                ts = self._arrival_timestamps[i]
                try:
                    cells = split_line(
                        row_str,
                        self.delim.value,
                        self.current_columns,
                        column_positions=self.delim.column_positions,
                        has_computed=has_computed,
                    )
                except (json.JSONDecodeError, csv.Error, ValueError):
                    continue
                if len(cells) != expected_cell_count:
                    mismatched.append(row_str)
                    continue
                cells.append(self._format_arrival(ts))
                if self._has_source_column and i < len(self._source_labels):
                    cells.append(self._source_labels[i])
                new_parsed.append(cells)
            return list(parsed) + new_parsed, mismatched, None

        filtered_rows = []
        rows_with_inconsistent_length = []
        kept_raw = []
        kept_parsed = []
        kept_timestamps = []
        unparseable_raw = []
        unparseable_timestamps = []
        needs_copy = parsed is not None and bool(self.query.unique_column_names)
        parsed_len = len(parsed) if parsed is not None else 0
        for i, row_str in enumerate(self.raw_rows):
            ts = self._arrival_timestamps[i]
            if parsed is not None and i < parsed_len:
                # Copy when dedup is active to avoid mutating the cache
                # when _dedup_rows prepends a count column.
                cells = list(parsed[i]) if needs_copy else parsed[i]
            else:
                try:
                    cells = split_line(
                        row_str,
                        self.delim.value,
                        self.current_columns,
                        column_positions=self.delim.column_positions,
                        has_computed=has_computed,
                    )
                except (json.JSONDecodeError, csv.Error, ValueError):
                    unparseable_raw.append(row_str)
                    unparseable_timestamps.append(ts)
                    continue
                if len(cells) != expected_cell_count:
                    rows_with_inconsistent_length.append(row_str)
                    unparseable_raw.append(row_str)
                    unparseable_timestamps.append(ts)
                    continue
                # Append arrival timestamp to parsed cells
                cells.append(self._format_arrival(ts))
                # Append source label if source column exists
                if self._has_source_column and i < len(self._source_labels):
                    cells.append(self._source_labels[i])
            if self._matches_all_filters(cells, adjust_for_count=True):
                filtered_rows.append(cells)
                kept_raw.append(row_str)
                kept_parsed.append(cells)
                kept_timestamps.append(self._arrival_timestamps[i])

        compacted = (
            kept_raw,
            kept_parsed,
            kept_timestamps,
            unparseable_raw,
            unparseable_timestamps,
        )
        return filtered_rows, rows_with_inconsistent_length, compacted

    def _apply_raw_rows_compaction(self, compacted) -> None:
        """Apply the compaction result from _partition_rows to buffer state.

        Replaces raw_rows, _parsed_rows, and _arrival_timestamps with the
        compacted versions (matching rows + unparseable rows).
        """
        if compacted is None:
            return
        kept_raw, kept_parsed, kept_ts, unparseable_raw, unparseable_ts = compacted
        combined_rows = kept_raw + unparseable_raw
        combined_ts = kept_ts + unparseable_ts
        combined_sources = None
        if self.stream.has_sources:
            combined_sources = self.stream.source_labels[: len(combined_rows)]
        self.stream.replace_raw_rows(combined_rows, combined_ts, combined_sources)
        self.cache.parsed_rows = kept_parsed

    def _dedup_rows(self, filtered_rows: list[list[str]]) -> list[list[str]]:
        """Deduplicate rows by composite unique column key, prepending count."""
        if not self.query.unique_column_names:
            return filtered_rows
        dedup_map = {}
        for cells in filtered_rows:
            composite_key = []
            for col_name in self.query.unique_column_names:
                col_idx = self._get_col_idx_by_name(col_name)
                if col_idx is None:
                    continue
                col_idx -= 1  # account for count column
                composite_key.append(strip_markup(cells[col_idx]))
            key = ",".join(composite_key)
            dedup_map[key] = cells
            self.query.count_by_column_key[key] += 1
        deduped_rows = []
        for idx, (k, cells) in enumerate(dedup_map.items()):
            cells = [str(self.query.count_by_column_key[k])] + cells
            self.cache.dedup_key_to_row_idx[k] = idx
            deduped_rows.append(cells)
        return deduped_rows

    def _sort_rows(self, rows: list[list[str]]) -> list | None:
        """Sort rows in-place by the current sort column.

        Returns the computed sort keys (always ascending order for bisect)
        when sorting succeeds, or ``None`` if no sort was performed.
        """
        from .dataprocessing import _detect_datetime_format, infer_column_type
        from .types import ColumnType

        if self.query.sort_column is None:
            return None
        sort_column_idx = self._get_col_idx_by_name(self.query.sort_column)
        if sort_column_idx is None:
            return None

        # Lazy type inference for the sort column
        sort_col = self._get_sort_column_obj()
        col_type = None
        fmt_hint = None
        if sort_col is not None:
            if sort_col.effective_type == ColumnType.AUTO and rows:
                sample = [
                    strip_markup(r[sort_column_idx])
                    for r in rows[:100]
                    if sort_column_idx < len(r)
                ]
                sort_col.detected_type = infer_column_type(sample)
                self._update_type_label(sort_col)
                if sort_col.detected_type == ColumnType.DATETIME:
                    sort_col.datetime_fmt_hint = _detect_datetime_format(sample)
            col_type = sort_col.effective_type
            if col_type == ColumnType.AUTO:
                col_type = None
            fmt_hint = sort_col.datetime_fmt_hint

        try:
            # Precompute sort keys in one O(N) pass to avoid calling
            # coerce_to_numeric O(N log N) times inside the sort comparator.
            keys = [
                coerce_sort_key(strip_markup(r[sort_column_idx]), col_type, fmt_hint)
                for r in rows
            ]
            indices = sorted(
                range(len(rows)), key=keys.__getitem__, reverse=self.query.sort_reverse
            )
            rows[:] = [rows[i] for i in indices]
            # Return keys in ascending order for bisect-based incremental inserts
            sorted_keys = [keys[i] for i in indices]
            if self.query.sort_reverse:
                sorted_keys.reverse()
            return sorted_keys
        except (ValueError, IndexError):
            return None
        except TypeError:
            try:
                rows.sort(
                    key=lambda r: r[sort_column_idx],
                    reverse=self.query.sort_reverse,
                )
            except (TypeError, IndexError):
                pass
            return None

    def _get_sort_column_obj(self):
        """Return the Column object for the current sort column, or None."""
        if self.query.sort_column is None:
            return None
        for col in self.current_columns:
            if strip_markup(col.name) == self.query.sort_column:
                return col
        return None

    @staticmethod
    def _update_type_label(col):
        """Add/remove type indicator labels (#, @) on a column."""
        from .types import ColumnType

        col.labels.discard("#")
        col.labels.discard("@")
        effective = col.effective_type
        if effective == ColumnType.NUMERIC:
            col.labels.add("#")
        elif effective == ColumnType.DATETIME:
            col.labels.add("@")

    _DATETIME_COLUMN_NAMES = frozenset(
        {
            "timestamp",
            "time",
            "date",
            "datetime",
            "ts",
            "created_at",
            "updated_at",
        }
    )

    @property
    def datetime_column_names(self) -> list[str]:
        """Return names of columns detected as DATETIME (for time window suggestions)."""
        from .types import ColumnType

        return [
            strip_markup(c.name)
            for c in self.current_columns
            if c.effective_type == ColumnType.DATETIME
        ]

    def _infer_all_column_types(self, rows: list[list[str]]) -> None:
        """Infer detected_type for all columns that are still AUTO.

        Called during deferred rebuild with filtered rows (data-position order).
        Skips metadata columns and columns with type_override set.
        """
        from .dataprocessing import _detect_datetime_format, infer_column_type
        from .types import ColumnType

        if not rows:
            return
        metadata_names = {mc.value for mc in MetadataColumn}

        # Pre-set columns with well-known datetime names
        for col in self.current_columns:
            if col.name in metadata_names:
                continue
            if col.effective_type == ColumnType.AUTO:
                if strip_markup(col.name).lower() in self._DATETIME_COLUMN_NAMES:
                    col.detected_type = ColumnType.DATETIME
                    self._update_type_label(col)

        for col in self.current_columns:
            if col.name in metadata_names:
                continue
            if col.effective_type != ColumnType.AUTO:
                # Detect format hint for DATETIME columns (including pre-set ones)
                if (
                    col.effective_type == ColumnType.DATETIME
                    and col.datetime_fmt_hint is None
                ):
                    idx = col.data_position
                    sample = [strip_markup(r[idx]) for r in rows[:100] if idx < len(r)]
                    col.datetime_fmt_hint = _detect_datetime_format(sample)
                continue
            idx = col.data_position
            sample = [strip_markup(r[idx]) for r in rows[:100] if idx < len(r)]
            col.detected_type = infer_column_type(sample)
            self._update_type_label(col)
            if col.detected_type == ColumnType.DATETIME:
                col.datetime_fmt_hint = _detect_datetime_format(sample)

        self._detect_splittable_columns(rows)

    def _detect_splittable_columns(self, rows: list[list[str]]) -> None:
        """Add ⑃ label to STRING columns whose values consistently contain a delimiter."""
        from .types import ColumnType

        metadata_names = {mc.value for mc in MetadataColumn}
        buf_delim = self.delim.value if hasattr(self, "delim") else self.delimiter
        # Delimiters to probe (skip the buffer's own delimiter)
        probe_delims = ["|", ";", "="]
        if isinstance(buf_delim, str):
            probe_delims = [d for d in probe_delims if d != buf_delim]

        for col in self.current_columns:
            if col.name in metadata_names or col.computed:
                continue
            if col.effective_type not in (ColumnType.STRING, ColumnType.AUTO):
                continue
            idx = col.data_position
            sample = [strip_markup(r[idx]) for r in rows[:100] if idx < len(r)]
            non_empty = [s for s in sample if s.strip()]
            if len(non_empty) < 3:
                continue
            for delim in probe_delims:
                splits = sum(1 for v in non_empty if len(v.split(delim)) >= 2)
                if splits / len(non_empty) >= 0.8:
                    col.labels.add("⑃")
                    break

    def _detect_splittable_columns_from_displayed(self, rows: list[list[str]]) -> None:
        """Add ⑃ label using displayed_rows (render-position indexed)."""
        from .types import ColumnType

        metadata_names = {mc.value for mc in MetadataColumn}
        buf_delim = self.delim.value if hasattr(self, "delim") else self.delimiter
        probe_delims = ["|", ";", "="]
        if isinstance(buf_delim, str):
            probe_delims = [d for d in probe_delims if d != buf_delim]

        for render_idx, col in enumerate(self.cache.sorted_visible_columns):
            if col.name in metadata_names or col.computed:
                continue
            if col.effective_type not in (ColumnType.STRING, ColumnType.AUTO):
                continue
            sample = [
                strip_markup(r[render_idx]) for r in rows[:100] if render_idx < len(r)
            ]
            non_empty = [s for s in sample if s.strip()]
            if len(non_empty) < 3:
                continue
            for delim in probe_delims:
                splits = sum(1 for v in non_empty if len(v.split(delim)) >= 2)
                if splits / len(non_empty) >= 0.8:
                    col.labels.add("⑃")
                    break

    def _infer_column_types_from_displayed(self) -> bool:
        """Infer types using displayed_rows (render-position order).

        Used on the incremental-add path where _process_deferred_data
        is not called. Returns True if any types were inferred.
        """
        from .dataprocessing import _detect_datetime_format, infer_column_type
        from .types import ColumnType

        if not self.displayed_rows:
            return False
        changed = False
        metadata_names = {mc.value for mc in MetadataColumn}

        # Pre-set well-known datetime column names
        for col in self.cache.sorted_visible_columns:
            if col.name in metadata_names:
                continue
            if col.effective_type == ColumnType.AUTO:
                if strip_markup(col.name).lower() in self._DATETIME_COLUMN_NAMES:
                    col.detected_type = ColumnType.DATETIME
                    self._update_type_label(col)
                    changed = True

        for render_idx, col in enumerate(self.cache.sorted_visible_columns):
            if col.name in metadata_names:
                continue
            if col.effective_type != ColumnType.AUTO:
                # Detect format hint for DATETIME columns
                if (
                    col.effective_type == ColumnType.DATETIME
                    and col.datetime_fmt_hint is None
                ):
                    sample = [
                        strip_markup(r[render_idx])
                        for r in self.displayed_rows[:100]
                        if render_idx < len(r)
                    ]
                    col.datetime_fmt_hint = _detect_datetime_format(sample)
                continue
            sample = [
                strip_markup(r[render_idx])
                for r in self.displayed_rows[:100]
                if render_idx < len(r)
            ]
            col.detected_type = infer_column_type(sample)
            self._update_type_label(col)
            if col.detected_type == ColumnType.DATETIME:
                col.datetime_fmt_hint = _detect_datetime_format(sample)
            changed = True

        # Detect splittable columns on the incremental path too
        if self.displayed_rows:
            self._detect_splittable_columns_from_displayed(self.displayed_rows)

        return changed

    def _apply_cli_format_timestamp(self) -> None:
        """Apply --format-timestamp CLI arg to the matching column (once)."""
        if self._cli_args is None or self._cli_args.format_timestamp is None:
            return
        spec = self._cli_args.format_timestamp
        if " -> " not in spec:
            return
        col_part, fmt_part = spec.split(" -> ", 1)
        col_name = col_part.strip()
        target_fmt = fmt_part.strip()
        if not target_fmt:
            return
        for c in self.current_columns:
            if strip_markup(c.name) == col_name and c.datetime_display_fmt is None:
                c.datetime_display_fmt = target_fmt
                self._rebuild_column_caches()
                break

    def _matches_all_filters(
        self, cells: list[str], adjust_for_count: bool = False
    ) -> bool:
        """Check if a row matches all current filters."""
        return matches_all_filters(
            cells,
            self.query.filters,
            self._get_col_idx_by_name,
            adjust_for_count=adjust_for_count,
            has_unique_columns=bool(self.query.unique_column_names),
        )

    # ── Deferred Rebuild Pipeline ──────────────────────────────────
    _CACHE_SAFE_REASONS = frozenset(
        {
            UpdateReason.SORT,
            UpdateReason.SEARCH,
            UpdateReason.THEME,
            UpdateReason.DEDUP,
            UpdateReason.FILTER,
            UpdateReason.PIVOT,
            UpdateReason.HIGHLIGHT,
        }
    )

    def _process_deferred_data(self, gen: int) -> dict | None:
        """Pure-data work for deferred updates — no widget access."""
        if gen != self._update_generation:
            return None
        with self._lock:
            metadata_names = {m.value for m in MetadataColumn}
            curr_metadata_columns = {
                c.name for c in self.current_columns if c.name in metadata_names
            }
            expected_cell_count = len(self.current_columns) - len(curr_metadata_columns)
            self._rebuild_column_caches()
            fixed_columns = len(
                [c for c in self.current_columns if c.pinned and not c.hidden]
            )

            self.query.search_matches = []
            self.query.current_match_index = -1
            self.query.count_by_column_key = defaultdict(lambda: 0)
            self.cache.dedup_key_to_row_idx = {}
            self.cache.sort_keys = []

            filtered_rows, rows_with_inconsistent_length = self._filter_rows(
                expected_cell_count
            )
            filtered_rows = self._apply_time_window(filtered_rows)
            deduped_rows = self._dedup_rows(filtered_rows)
            self._infer_all_column_types(deduped_rows)
            self._apply_cli_format_timestamp()
            sort_keys = self._sort_rows(deduped_rows)

            # Build column labels after type inference so #/@ labels are included
            column_labels = self._get_visible_column_labels()

            # Rebuild dedup indices after sorting (sort reorders rows)
            if self.query.unique_column_names:
                self.cache.dedup_key_to_row_idx = {}
                for idx, row in enumerate(deduped_rows):
                    key = self._build_composite_key(row)
                    self.cache.dedup_key_to_row_idx[key] = idx

            # Reuse sort keys from _sort_rows for incremental inserts.
            if sort_keys is not None:
                self.cache.sort_keys = sort_keys

            aligned_rows = self._align_cells_to_visible_columns(deduped_rows)
            styled_rows = self._highlight_search_matches(aligned_rows, fixed_columns)

            # Apply user-defined regex highlights
            if self.regex_highlights:
                styled_rows = highlight_regex_patterns(
                    styled_rows, self.regex_highlights, fixed_columns
                )

            # Highlight rows affected by newly streamed lines
            green_lines = self._green_lines
            if green_lines and self.query.unique_column_names:
                green_keys = set()
                for line in green_lines:
                    cells = split_line(
                        line,
                        self.delim.value,
                        self.current_columns,
                        column_positions=self.delim.column_positions,
                    )
                    cells.append("")  # placeholder for arrival
                    cells.insert(0, "")  # placeholder for count
                    key = self._build_composite_key(cells, render_position=False)
                    green_keys.add(key)
                for i, row in enumerate(styled_rows):
                    key = self._build_composite_key(row, render_position=True)
                    if key in green_keys:
                        styled_rows[i] = [self._highlight_markup(c) for c in row]
                self._green_lines = None

            self._last_flushed_idx = len(self.raw_rows)
            # Snapshot shared state for width computation outside the lock
            cached_widths = self.cache.col_widths
            has_markup_changes = bool(self.query.search_term) or bool(
                self.regex_highlights
            )

        # Precompute column widths on the bg thread so the main thread
        # can use add_rows_precomputed (just extends + refresh).
        # Reuse cached widths on sort-only rebuilds (no search/highlights
        # → no markup changes → widths are stable).
        if (
            cached_widths is not None
            and not has_markup_changes
            and len(cached_widths) == len(column_labels)
        ):
            col_widths = cached_widths
        else:
            col_widths = [len(c) for c in column_labels]
            for row in styled_rows:
                for i, cell_str in enumerate(row):
                    if "[" in cell_str:
                        try:
                            str_len = Text.from_markup(cell_str).cell_len
                        except Exception:
                            str_len = len(cell_str)
                    else:
                        str_len = len(cell_str)
                    if str_len > col_widths[i]:
                        col_widths[i] = str_len
            with self._lock:
                self.cache.col_widths = col_widths

        return {
            "styled_rows": styled_rows,
            "column_labels": column_labels,
            "column_widths": col_widths,
            "fixed_columns": fixed_columns,
            "inconsistent_rows": rows_with_inconsistent_length,
            "n_filtered": len(filtered_rows),
        }

    def _apply_deferred_to_widgets(
        self, result, gen, restore_position, callback, cx, cy, sx, sy
    ):
        """Main-thread: push processed data into widgets."""
        if result is None or gen != self._update_generation:
            return
        self._ensure_correct_view_widget()
        dt = self.query_one(".nless-view")
        dt.clear(columns=True)
        dt.fixed_columns = result["fixed_columns"]
        dt.add_columns(result["column_labels"])
        dt.column_widths = result["column_widths"]

        self.displayed_rows = result["styled_rows"]
        dt.add_rows_precomputed(result["styled_rows"])

        # Sync marks to datatable
        dt.marked_rows = {
            row: letter
            for letter, row in self.marks.items()
            if row < len(self.displayed_rows)
        }

        bad_lines = result["inconsistent_rows"]
        if bad_lines:
            n_total = result["n_filtered"] + len(bad_lines)
            msg = self._try_auto_switch_delimiter(
                bad_lines,
                len(bad_lines),
                n_total,
                lines_already_in_raw=True,
            )
            if msg:
                self._flash_status(msg)
                self._deferred_update_table(reason=UpdateReason.SWITCHING_DELIMITER)
                return

        # Check if more data arrived while we were processing
        if len(self.raw_rows) > self._last_flushed_idx:
            stream_active = self.line_stream and not self.line_stream.done
            if stream_active and self._needs_full_rebuild():
                self._schedule_chain_rebuild(restore_position, callback)
            else:
                self._stop_chain_timer()
                self._deferred_update_table(
                    restore_position=restore_position,
                    callback=callback,
                    reason=self._reload_reason(),
                )
        else:
            self._stop_chain_timer()
            loaded_reason = self.loading_state.reason
            self.loading_state.reason = None
            self._stop_spinner()
            self._update_status_bar()
            if restore_position:
                self._restore_position(dt, cx, cy, sx, sy)
                self._pending_cursor_position = None
            if loaded_reason:
                row_count = len(self.displayed_rows)
                if row_count > 0:
                    _past = {
                        UpdateReason.SORT: "Sorted",
                        UpdateReason.SEARCH: "Searched",
                        UpdateReason.DEDUP: "Loaded",
                        UpdateReason.FILTER: "Loaded",
                    }
                    done = _past.get(loaded_reason, "Loaded")
                    self._flash_status(f"{done} {row_count:,} rows")
            if callback:
                callback()

    def _deferred_update_table(
        self,
        restore_position=True,
        callback=None,
        reason: UpdateReason = UpdateReason.LOADING,
    ):
        """Run data processing on a bg thread, then apply to widgets on main thread."""
        with self._lock:
            self._update_generation += 1
            gen = self._update_generation
        self.loading_state.reason = reason
        if reason not in self._CACHE_SAFE_REASONS:
            self.cache.parsed_rows = None
            self.cache.col_widths = None
        elif reason == UpdateReason.HIGHLIGHT:
            self.cache.invalidate_widths()
        self._start_spinner()
        self._update_status_bar()

        # Snapshot cursor/scroll from widgets before going off-thread.
        data_table = self.query_one(".nless-view")
        cx = data_table.cursor_column
        cy = data_table.cursor_row
        sx = data_table.scroll_x
        sy = data_table.scroll_y

        # Override with session-saved cursor position if pending.
        if self._pending_cursor_position is not None:
            cy, cx = self._pending_cursor_position

        def _process_data():
            return self._process_deferred_data(gen)

        def _apply_to_widgets(result):
            self._apply_deferred_to_widgets(
                result, gen, restore_position, callback, cx, cy, sx, sy
            )

        self._run_deferred_update(_process_data, _apply_to_widgets)

    @work(thread=True, exclusive=True, group="data-processing")
    def _run_deferred_update(self, _process_data, _apply_to_widgets):
        result = _process_data()
        self.app.call_from_thread(lambda: _apply_to_widgets(result))

    def _needs_full_rebuild(self) -> bool:
        """True when new streaming rows require a full deferred rebuild
        rather than incremental row insertion."""
        return self.query.is_expensive or bool(self.time_window)

    def _reload_reason(self) -> UpdateReason:
        """Build a descriptive reason for chained streaming rebuilds."""
        if self.query.sort_column is not None:
            return UpdateReason.SORT
        if self.query.unique_column_names:
            return UpdateReason.DEDUP
        if self.time_window:
            return UpdateReason.FILTER
        return UpdateReason.LOADING

    # ── Chain Timer / Back Pressure ────────────────────────────────
    def _cancel_chain_timer(self) -> None:
        """Cancel the pending chain timer without resetting delay/skip state."""
        self.chain.cancel_timer()

    def _stop_chain_timer(self) -> None:
        """Cancel any pending chain rebuild timer and reset state."""
        self.chain.stop()

    _BACKPRESSURE_NOTIFY_THRESHOLD = 100

    def _schedule_chain_rebuild(self, restore_position, callback):
        """Delay the next rebuild to let more streaming data accumulate."""
        pending = len(self.raw_rows) - self._last_flushed_idx
        if not self.chain.notified and pending >= self._BACKPRESSURE_NOTIFY_THRESHOLD:
            self.chain.notified = True
            self.notify(
                "[yellow]⚠[/yellow]  Data arriving faster than it can be processed — buffering",
                severity="warning",
            )
        self.chain.cancel_timer()

        delay = self.chain.delay

        def _do_chain():
            self.chain.timer = None
            try:
                stream_active = self.line_stream and not self.line_stream.done
                pending = len(self.raw_rows) - self._last_flushed_idx
                displayed = max(len(self.displayed_rows), 1)

                # Stream is outpacing us — skip this rebuild and wait longer.
                # Cap at 3 skips (~2s) so infinite streams still show progress.
                if stream_active and pending > displayed and self.chain.should_skip:
                    self.chain.advance_backoff()
                    self._schedule_chain_rebuild(restore_position, callback)
                    return

                self.chain.reset()
                self._deferred_update_table(
                    restore_position=restore_position,
                    callback=callback,
                    reason=self._reload_reason(),
                )
            except Exception:
                logger.debug("Chain rebuild failed", exc_info=True)

        try:
            self.chain.timer = self.set_timer(delay, _do_chain)
        except Exception:
            logger.debug("Failed to schedule chain timer", exc_info=True)

    def _start_bp_timer(self) -> None:
        """Start the back pressure sampling timer (main thread)."""
        if self._bp_timer is not None:
            return
        try:
            self._bp_timer = self.set_interval(0.5, self._tick_bp)
        except Exception:
            logger.debug("BP timer start failed", exc_info=True)

    def _stop_bp_timer(self) -> None:
        """Stop the back pressure sampling timer and any pending chain rebuild."""
        self._stop_chain_timer()
        if self._bp_timer is not None:
            self._safe_widget_call(self._bp_timer.stop)
            self._bp_timer = None

    def _tick_bp(self) -> None:
        """Check if the OS pipe buffer has data waiting (i.e. we're behind)."""
        try:
            stream_done = self.line_stream and self.line_stream.done

            behind = False
            if (
                not stream_done
                and self.line_stream
                and hasattr(self.line_stream, "pipe_pending_bytes")
            ):
                pending = self.line_stream.pipe_pending_bytes()
                if pending is not None and pending > 0:
                    behind = True

            self._bp_behind = behind

            # Fire pending chain rebuild immediately when stream finishes
            if stream_done and self.chain.timer is not None:
                self._stop_chain_timer()
                self._deferred_update_table()

            # Stop timer when stream is done and we're caught up
            if stream_done and not behind and not self.loading_state.reason:
                self._stop_bp_timer()
                self._bp_behind = False

            if self.loading_state.spinner_timer is None and not self.locked:
                self._update_status_bar()
        except Exception:
            pass

    # ── Widget / View Management ───────────────────────────────────
    def _ensure_correct_view_widget(self) -> None:
        """Swap between RawPager and DataTable if raw_mode changed."""
        from .rawpager import RawPager

        try:
            current = self.query_one(".nless-view")
        except Exception:
            return

        want_raw = self.raw_mode and not self._has_source_column
        is_raw = isinstance(current, RawPager)
        if want_raw == is_raw:
            return

        theme = self._get_theme()
        container = self.query_one(Vertical)
        current.remove_class("nless-view")
        current.remove()
        if want_raw:
            new_widget = RawPager(theme=theme)
        else:
            new_widget = NlessDataTable(theme=theme)
        container.mount(new_widget)
        new_widget.focus()

    def _deferred_raw_swap(self) -> None:
        """Swap Datatable→RawPager after incremental load, transferring rows."""
        from .rawpager import RawPager

        try:
            current = self.query_one(".nless-view")
        except Exception:
            return
        if isinstance(current, RawPager):
            return

        rows = list(current.rows)
        cursor_y = current.cursor_row
        self._ensure_correct_view_widget()

        try:
            new_widget = self.query_one(".nless-view")
            if rows:
                new_widget.add_rows_precomputed(rows)
            if cursor_y and rows:
                new_widget.move_cursor(row=min(cursor_y, len(rows) - 1))
        except Exception:
            logger.debug("Raw swap row transfer failed", exc_info=True)

    def _restore_position(
        self, data_table: NlessDataTable, cursor_x, cursor_y, scroll_x, scroll_y
    ):
        data_table.move_cursor(
            row=cursor_y, column=cursor_x, animate=False, scroll=False
        )
        self.call_after_refresh(
            lambda: data_table.scroll_to(
                scroll_x, scroll_y, animate=False, immediate=False
            )
        )

    # ── Status Bar / Spinner / Loading ─────────────────────────────
    def _update_status_bar(self) -> None:
        if self.pane_id != self.app.buffers[self.app.curr_buffer_idx].pane_id:
            return
        data_table = self.query_one(".nless-view")
        ctx = self._status_ctx
        text = build_status_text(
            sort_column=self.query.sort_column,
            sort_reverse=self.query.sort_reverse,
            filters=self.query.filters,
            search_term=self.query.search_term,
            search_matches_count=len(self.query.search_matches),
            current_match_index=self.query.current_match_index,
            total_rows=data_table.row_count,
            total_cols=len(data_table.columns),
            current_row=data_table.cursor_row + 1,
            current_col=data_table.cursor_column + 1,
            is_tailing=self.is_tailing,
            unique_column_names=self.query.unique_column_names,
            loading_reason=self.loading_state.reason,
            flash_message=self.loading_state.flash_message,
            theme=ctx.theme or self._get_theme(),
            spinner_frame=self.loading_state.spinner_frame,
            format_str=ctx.status_format,
            keymap_name=ctx.keymap_name,
            theme_name=ctx.theme_name,
            time_window=ctx.format_window(self.time_window, self.rolling_time_window)
            if self.time_window and ctx.format_window
            else None,
            time_window_column=self._time_window_column if self.time_window else None,
            delimiter=self._format_delimiter(),
            skipped_rows=self.delim.total_skipped,
            behind=self._bp_behind,
            buffered_rows=len(self.raw_rows),
            pipe_output=ctx.pipe_output,
            pipe_row_count=len(self.displayed_rows),
            session_name=ctx.session_name,
        )
        self.app.query_one("#status_bar", Static).update(text)

    def _flash_status(self, message: str, duration: float = 3.0) -> None:
        """Show a temporary message in the status bar, auto-clearing after *duration* seconds."""
        if self.loading_state.flash_timer is not None:
            self.loading_state.flash_timer.stop()
        self.loading_state.flash_message = message
        self._update_status_bar()
        self.loading_state.flash_timer = self.set_timer(duration, self._clear_flash)

    def _clear_flash(self) -> None:
        self.loading_state.flash_message = None
        self.loading_state.flash_timer = None
        self._update_status_bar()

    def start_loading(self, reason: str) -> None:
        """Show a loading spinner with the given reason text."""
        self.loading_state.reason = reason
        self._start_spinner()
        self._update_status_bar()

    def stop_loading(self) -> None:
        """Clear the loading state and stop the spinner."""
        self.loading_state.reason = None
        self._stop_spinner()
        self._update_status_bar()

    def _safe_widget_call(self, fn, *args) -> None:
        """Call fn(*args) ignoring errors from unmounted widgets or shutdown."""
        try:
            fn(*args)
        except Exception:
            pass

    def _start_spinner(self) -> None:
        """Start the status bar spinner animation (must run on main thread)."""
        if self.loading_state.spinner_timer is not None:
            return
        self.loading_state.spinner_frame = 0
        try:
            self.loading_state.spinner_timer = self.set_interval(
                0.1, self._tick_spinner
            )
        except Exception:
            logger.debug("Spinner start failed", exc_info=True)

    def _stop_spinner(self) -> None:
        """Stop the status bar spinner animation (must run on main thread)."""
        if self.loading_state.spinner_timer is not None:
            self._safe_widget_call(self.loading_state.spinner_timer.stop)
            self.loading_state.spinner_timer = None

    def _request_spinner_start(self) -> None:
        """Thread-safe: schedule spinner start on the main thread (non-blocking)."""
        self._safe_widget_call(self.app.call_later, self._start_spinner)

    def _request_spinner_stop(self) -> None:
        """Thread-safe: schedule spinner stop on the main thread (non-blocking)."""
        self._safe_widget_call(self.app.call_later, self._stop_spinner)

    def _tick_spinner(self) -> None:
        """Advance the spinner frame and refresh the status bar."""
        self.loading_state.spinner_frame += 1
        if self.loading_state.reason:
            self._update_status_bar()
        else:
            self._stop_spinner()

    # ── Theme Helpers ──────────────────────────────────────────────
    def _get_theme(self):
        """Return the current theme from the app, or the default."""
        try:
            return self.app.nless_theme
        except AttributeError:
            from .theme import BUILTIN_THEMES

            return BUILTIN_THEMES["default"]

    def _highlight_markup(self, text: str) -> str:
        """Wrap text in the current theme's highlight color markup."""
        try:
            open_tag, close_tag = self._highlight_tags
        except AttributeError:
            color = self._get_theme().highlight
            self._highlight_tags = (f"[{color}]", f"[/{color}]")
            open_tag, close_tag = self._highlight_tags
        return f"{open_tag}{text}{close_tag}"

    # ── Utilities (formatting, caches, incremental ops) ────────────
    _arrival_format_cache: dict[int, str] = {}

    @staticmethod
    def _format_arrival(ts: float) -> str:
        """Format an epoch timestamp as ISO 8601 for display."""
        sec = int(ts)
        cache = NlessBuffer._arrival_format_cache
        base = cache.get(sec)
        if base is None:
            base = datetime.fromtimestamp(sec, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            cache[sec] = base
        return f"{base}.{int(ts % 1 * 1000):03d}"

    def invalidate_caches(self) -> None:
        """Invalidate all data caches, forcing a full reparse on the next update.

        Call after changes to columns, filters, delimiter, or raw_rows.
        """
        self.cache.invalidate()

    def _build_composite_key(
        self, cells: list[str], render_position: bool = False
    ) -> str:
        """Build a composite key from the unique column values in a row."""
        return build_composite_key(
            cells,
            self.query.unique_column_names,
            self._get_col_idx_by_name,
            render_position=render_position,
        )

    def _handle_dedup_for_line(
        self, cells: list[str]
    ) -> tuple[list[str], int | None, list[str] | None]:
        """Handle deduplication for a single incoming line.

        Returns (possibly-updated cells, old_index if replacing, old_row if replacing).
        """
        if not self.query.unique_column_names:
            return cells, None, None

        new_key = self._build_composite_key(cells)

        if new_key in self.cache.dedup_key_to_row_idx:
            row_idx = self.cache.dedup_key_to_row_idx[new_key]
            if row_idx >= len(self.displayed_rows):
                # Stale index — table is being rebuilt concurrently
                return cells, None, None
            new_cells = []
            for col_idx, cell in enumerate(cells):
                if col_idx == 0:
                    self.query.count_by_column_key[new_key] += 1
                    cell = self.query.count_by_column_key[new_key]
                else:
                    cell = strip_markup(cell)
                new_cells.append(self._highlight_markup(str(cell)))
            return new_cells, row_idx, self.displayed_rows[row_idx]

        self.query.count_by_column_key[new_key] = 1
        return cells, None, None

    def _find_sorted_insert_index(self, cells: list[str]) -> int:
        """Find the insertion index for a row based on the current sort."""
        sort_col = self._get_sort_column_obj()
        col_type = sort_col.effective_type if sort_col else None
        fmt_hint = sort_col.datetime_fmt_hint if sort_col else None
        from .types import ColumnType

        if col_type == ColumnType.AUTO:
            col_type = None
        return find_sorted_insert_index(
            cells,
            self.cache.sort_keys,
            self.query.sort_column,
            self.query.sort_reverse,
            self._get_col_idx_by_name,
            num_displayed_rows=len(self.displayed_rows),
            column_type=col_type,
            fmt_hint=fmt_hint,
        )

    def _update_dedup_indices_after_removal(self, old_index: int) -> None:
        """Shift dedup index entries down after a row removal."""
        update_dedup_indices_after_removal(self.cache.dedup_key_to_row_idx, old_index)

    def _update_dedup_indices_after_insertion(
        self, dedup_key: str, new_index: int
    ) -> None:
        """Shift dedup index entries up after a row insertion, then record the new key."""
        update_dedup_indices_after_insertion(
            self.cache.dedup_key_to_row_idx, dedup_key, new_index
        )

    def _update_sort_keys_for_line(
        self,
        data_cells: list[str],
        old_row: list[str] | None,
    ) -> None:
        """Update the incremental sort keys list after insertion/removal."""
        sort_col = self._get_sort_column_obj()
        col_type = sort_col.effective_type if sort_col else None
        fmt_hint = sort_col.datetime_fmt_hint if sort_col else None
        from .types import ColumnType

        if col_type == ColumnType.AUTO:
            col_type = None
        update_sort_keys_for_line(
            data_cells,
            old_row,
            self.query.sort_column,
            self.cache.sort_keys,
            self._get_col_idx_by_name,
            column_type=col_type,
            fmt_hint=fmt_hint,
        )
