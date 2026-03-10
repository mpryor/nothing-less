import csv
from datetime import datetime, timezone
import json
import re
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from copy import deepcopy

import pyperclip
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.coordinate import Coordinate
from textual.widgets import (
    Select,
    Static,
)
from textual import work

from .delimiter import split_line
from .input import LineStream
from .nlessselect import NlessSelect
from .datatable import Datatable as NlessDataTable
from .dataprocessing import (
    build_composite_key,
    coerce_sort_key,
    coerce_to_numeric,
    find_sorted_insert_index,
    matches_all_filters,
    strip_markup,
    update_dedup_indices_after_insertion,
    update_dedup_indices_after_removal,
    update_sort_keys_for_line,
)
from .statusbar import build_status_text
from .types import CliArgs, Column, Filter, MetadataColumn

from .buffer_columns import ColumnMixin
from .buffer_delimiter import DelimiterMixin
from .buffer_search import SearchMixin
from .buffer_streaming import StreamingMixin
from .buffer_timewindow import TimeWindowMixin


class NlessBuffer(
    ColumnMixin, DelimiterMixin, TimeWindowMixin, StreamingMixin, SearchMixin, Static
):
    """A modern pager with tabular data sorting/filtering capabilities."""

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
        self.first_row_parsed = False
        self.raw_rows = []
        self._all_source_lines: list[str] | None = None  # unfiltered history for ~
        self.displayed_rows = []
        self.first_log_line = ""  # used to determine columns when delimiter is set
        self._preamble_lines: list[str] = []  # lines skipped by find_header_index
        self.current_columns: list[Column] = []
        self.current_filters: list[Filter] = cli_args.filters if cli_args else []
        self.search_term = None
        if cli_args and cli_args.sort_by:
            sort_column, direction = cli_args.sort_by.split("=")
            self.sort_column = sort_column
            self.sort_reverse = direction.lower() == "desc"
        else:
            self.sort_column = None
            self.sort_reverse = False
        self.search_matches: list[Coordinate] = []
        self.current_match_index: int = -1

        if cli_args and cli_args.delimiter:
            pattern = re.compile(cli_args.delimiter)  # validate regex
            # check if delimiter parses to regex, and has named capture groups
            if pattern.groups > 0 and pattern.groupindex:
                self.delimiter = pattern
            else:
                self.delimiter = cli_args.delimiter
        else:
            self.delimiter = None

        self.delimiter_inferred = False
        self.delimiter_name: str | None = None
        self.is_tailing = cli_args.tail if cli_args else False
        self.unique_column_names = cli_args.unique_keys if cli_args else set()
        self.count_by_column_key = defaultdict(lambda: 0)
        # Columns hidden by pivot (to reveal when new lines arrive)
        self._pivot_hidden_columns: set[str] = set()

        # Arrival timestamps parallel to raw_rows (epoch floats)
        self._arrival_timestamps: list[float] = []
        # Time window filter: only show rows within this many seconds of now
        self.time_window: float | None = None
        # When True, the time window re-evaluates periodically
        self.rolling_time_window: bool = False
        # Fixed upper bound for non-rolling windows (blocks new streaming data)
        self._time_window_ceiling: float | None = None
        self._rolling_timer = None

        # Caches rebuilt when columns change
        self._col_data_idx: dict[str, int] = {}  # plain_name → data_position
        self._col_render_idx: dict[str, int] = {}  # plain_name → render_position
        self._sorted_visible_columns: list[Column] = []
        # Dedup: composite_key → row index in displayed_rows
        self._dedup_key_to_row_idx: dict[str, int] = {}
        # Incremental sort keys for _find_sorted_insert_index
        self._sort_keys: list = []
        # Tracks how far into raw_rows has been rendered
        self._last_flushed_idx = 0
        # Cache of parsed cells to avoid re-parsing on sort/search
        self._parsed_rows: list[list[str]] | None = None
        # Cache of column widths to skip O(N×V) recomputation on sort
        self._cached_col_widths: list[int] | None = None
        self._initial_load_done = False
        self._loading_reason: str | None = None
        self._flash_message: str | None = None
        self._flash_timer = None
        self._spinner_frame: int = 0
        self._spinner_timer = None
        self._has_nested_delimiters = False
        self._update_generation = 0
        self._needs_deferred_update = False
        self._skipped_lines: list[str] = []
        self._delimiter_suggestion_shown = False
        self._mismatch_warning_shown = False
        self._total_skipped = 0
        # Lines that triggered green highlighting from streaming updates
        self._green_lines: set[str] | None = None
        # When set, rejects lines that parse with an ancestor buffer's delimiter.
        # Used by chained ~ (view unparsed) buffers.
        self._source_parse_filter: Callable[[str], bool] | None = None

        # Back pressure tracking
        self._bp_timer = None
        self._bp_behind: bool = False

        # Chain-delay for coalescing deferred rebuilds during streaming
        self._chain_delay: float = self._CHAIN_DELAY_INITIAL
        self._chain_timer = None
        self._chain_skips: int = 0
        self._chain_notified: bool = False

    def action_copy(self) -> None:
        """Copy the contents of the currently highlighted cell to the clipboard."""
        data_table = self.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")
            return
        try:
            pyperclip.copy(cell_value)
            self.notify("Cell contents copied to clipboard.", severity="information")
        except pyperclip.PyperclipException:
            self.notify(
                "Clipboard not available — is xclip/xsel installed?",
                severity="error",
            )

    def _filter_lines(
        self,
        lines: list[str],
        timestamps: list[float] | None = None,
    ) -> tuple[list[str], list[float]]:
        """Return only lines (and their timestamps) that match all current filters."""
        now = time.time()
        if timestamps is None:
            timestamps = [now] * len(lines)
        if not self.current_filters:
            return lines, timestamps
        metadata = [mc.value for mc in MetadataColumn]
        expected = len([c for c in self.current_columns if c.name not in metadata])
        matching = []
        kept_timestamps = []
        for i, line in enumerate(lines):
            try:
                cells = split_line(line, self.delimiter, self.current_columns)
            except (json.JSONDecodeError, csv.Error, ValueError):
                continue
            if len(cells) != expected:
                continue
            cells.append(self._format_arrival(timestamps[i]))
            if self._matches_all_filters(cells, adjust_for_count=True):
                matching.append(line)
                kept_timestamps.append(timestamps[i])
        return matching, kept_timestamps

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
            new_buffer._preamble_lines = list(self._preamble_lines)
            new_buffer.current_columns = deepcopy(self.current_columns)
            new_buffer.current_filters = deepcopy(self.current_filters)
            new_buffer.search_term = self.search_term
            new_buffer.sort_column = self.sort_column
            new_buffer.sort_reverse = self.sort_reverse
            new_buffer.search_matches = deepcopy(self.search_matches)
            new_buffer.current_match_index = self.current_match_index
            new_buffer.delimiter = self.delimiter
            new_buffer.delimiter_inferred = self.delimiter_inferred
            new_buffer.delimiter_name = self.delimiter_name
            new_buffer.is_tailing = self.is_tailing
            new_buffer.unique_column_names = deepcopy(self.unique_column_names)
            new_buffer.count_by_column_key = deepcopy(self.count_by_column_key)
            new_buffer._pivot_hidden_columns = set(self._pivot_hidden_columns)
            new_buffer.time_window = self.time_window
            new_buffer.rolling_time_window = self.rolling_time_window
            new_buffer._time_window_ceiling = self._time_window_ceiling
            new_buffer.line_stream = self.line_stream
            timestamps_snapshot = list(self._arrival_timestamps)
            if self.line_stream:
                self.line_stream.subscribe_future_only(
                    new_buffer,
                    new_buffer.add_logs,
                    lambda: new_buffer.mounted,
                )
            new_buffer._rebuild_column_caches()
            new_buffer._initial_load_done = True

        # Expensive filtering runs outside the lock on the snapshot.
        new_buffer.raw_rows, new_buffer._arrival_timestamps = new_buffer._filter_lines(
            raw_rows_snapshot, timestamps_snapshot
        )
        # Copy parsed-row cache so the new buffer's first rebuild doesn't
        # re-parse everything via split_line.  When _filter_lines removed
        # rows, len(_parsed_rows) > len(raw_rows) and _partition_rows will
        # safely ignore the stale cache.
        if self._parsed_rows is not None and len(self._parsed_rows) == len(
            raw_rows_snapshot
        ):
            new_buffer._parsed_rows = list(self._parsed_rows)
        # Keep unfiltered history so ~ can find excluded lines even without
        # a line_stream.  The snapshot is shared (not copied) to save memory.
        new_buffer._all_source_lines = raw_rows_snapshot
        return new_buffer

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

    def init_as_unparsed(
        self,
        rows: list[str],
        source_parse_filter: Callable[[str], bool],
        line_stream: LineStream | None = None,
    ) -> None:
        """Set up this buffer as a raw view of unparsed/excluded lines.

        Configures delimiter, columns, raw_rows, and optionally subscribes
        to ongoing stream updates so newly rejected lines appear automatically.
        """
        self.delimiter = "raw"
        self.delimiter_inferred = False
        self.first_log_line = rows[0]
        self.first_row_parsed = True
        self.current_columns = self._make_columns(["log"])
        self._ensure_arrival_column(self.current_columns)
        self._rebuild_column_caches()
        self.raw_rows = rows
        now = time.time()
        self._arrival_timestamps = [now] * len(rows)
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

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable or RawPager widget."""
        with Vertical():
            theme = self._get_theme()
            if self.raw_mode:
                from .rawpager import RawPager

                yield RawPager(theme=theme)
            else:
                yield NlessDataTable(theme=theme)

    def _ensure_correct_view_widget(self) -> None:
        """Swap between RawPager and DataTable if raw_mode changed."""
        from .rawpager import RawPager

        try:
            current = self.query_one(".nless-view")
        except Exception:
            return

        want_raw = self.raw_mode
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
            pass

    def on_mount(self) -> None:
        self.mounted = True
        if self._loading_reason:
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

    def _make_shown_filter(
        self, *, include_ancestors: bool = True
    ) -> Callable[[str], bool]:
        """Build a filter that returns True if a line would be shown in this buffer.

        A line is "shown" if it parses with the delimiter, has the right column
        count, and passes all content filters.  When *include_ancestors* is True
        (the default), a line is also considered shown if any ancestor buffer
        would show it (via _source_parse_filter).  Pass False when building the
        filter set for ``action_view_unparsed_logs`` so each buffer is evaluated
        independently.
        """
        delimiter = self.delimiter
        columns = list(self.current_columns)
        metadata = {mc.value for mc in MetadataColumn}
        expected = len([c for c in columns if c.name not in metadata])
        filters = list(self.current_filters)
        col_lookup = dict(self._col_data_idx)
        parent = self._source_parse_filter if include_ancestors else None

        def shown(line: str) -> bool:
            if parent and parent(line):
                return True
            try:
                cells = split_line(line, delimiter, columns)
            except (json.JSONDecodeError, csv.Error, ValueError, StopIteration):
                return False
            if len(cells) != expected:
                return False
            if not filters:
                return True
            # Append a dummy arrival timestamp for filter column alignment
            cells.append("")
            return matches_all_filters(
                cells, filters, lambda name, _rp=False: col_lookup.get(name)
            )

        return shown

    def action_view_unparsed_logs(self) -> None:
        """Create a new buffer containing logs not shown in any open buffer."""
        # Build shown filters for all open buffers.  Each filter answers
        # "would this buffer display this line?" without considering ancestor
        # chains — we combine them with any() ourselves.
        buffer_filters = []
        for buf in self.app.buffers:
            shown = buf._make_shown_filter(include_ancestors=False)
            # A raw buffer with no filters would claim to show every line,
            # but it may only *contain* a subset.  Fall back to a membership
            # check against its actual rows so ~ can find genuinely missing lines.
            if buf.delimiter == "raw" and not buf.current_filters:
                raw_set = set(buf.raw_rows)
                buffer_filters.append(lambda line, _s=raw_set: line in _s)
            else:
                buffer_filters.append(shown)

        def shown_in_any(line: str) -> bool:
            return any(f(line) for f in buffer_filters)

        # Use the line stream's full history if available, then unfiltered
        # snapshot from copy(), then fall back to raw_rows.
        if self.line_stream:
            all_lines = self.line_stream.lines
        elif self._all_source_lines is not None:
            all_lines = self._all_source_lines
        else:
            all_lines = self.raw_rows

        excluded_rows = [line for line in all_lines if not shown_in_any(line)]

        if not excluded_rows:
            self.notify("All logs are being shown.", severity="information")
            return

        self.app._create_unparsed_buffer(
            excluded_rows,
            source_parse_filter=shown_in_any,
            line_stream=self.line_stream,
        )

    def action_jump_columns(self) -> None:
        """Show columns by user input."""
        column_options = [
            (strip_markup(c.name), c.render_position)
            for c in sorted(self.current_columns, key=lambda c: c.render_position)
            if not c.hidden
        ]
        select = NlessSelect(
            options=column_options,
            classes="dock-bottom",
            prompt="Type a column to jump to",
            id="column_jump_select",
        )
        self.mount(select)

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.control.id and event.control.id != "column_jump_select":
            return  # Not ours — let it bubble to the app
        col_index = event.value
        event.control.remove()
        data_table = self.query_one(".nless-view")
        data_table.move_cursor(column=col_index)

    def action_move_column(self, direction: int) -> None:
        with self._try_lock(
            "move column", deferred=lambda: self.action_move_column(direction)
        ) as acquired:
            if not acquired:
                return
            data_table = self.query_one(".nless-view")
            current_cursor_column = data_table.cursor_column
            selected_column = self._get_column_at_position(current_cursor_column)
            if not selected_column:
                self.notify("No column selected to move", severity="error")
                return
            if selected_column.name in [m.value for m in MetadataColumn]:
                return  # can't move metadata columns
            if (
                direction == 1
                and selected_column.render_position == len(self.current_columns) - 1
            ) or (direction == -1 and selected_column.render_position == 0):
                return  # can't move further in that direction

            adjacent_column = self._get_column_at_position(
                selected_column.render_position + direction
            )
            if not adjacent_column or adjacent_column.name in [
                m.value for m in MetadataColumn
            ]:  # can't move past metadata columns
                return

            if (
                adjacent_column.pinned
                and not selected_column.pinned
                or (selected_column.pinned and not adjacent_column.pinned)
            ):
                return  # can't move a pinned column past a non-pinned column or vice versa

            selected_column.render_position, adjacent_column.render_position = (
                adjacent_column.render_position,
                selected_column.render_position,
            )
            new_position = selected_column.render_position

        self._deferred_update_table(
            callback=lambda: data_table.move_cursor(column=new_position),
            reason="Moving column",
        )

    def action_move_column_left(self) -> None:
        self.action_move_column(-1)

    def action_move_column_right(self) -> None:
        self.action_move_column(1)

    def action_toggle_tail(self) -> None:
        self.is_tailing = not self.is_tailing
        self._update_status_bar()

    def action_reset_highlights(self) -> None:
        """Remove new-line highlights from all displayed rows."""
        data_table = self.query_one(".nless-view")
        highlight_re = self._get_theme().highlight_re
        for row_idx, row in enumerate(self.displayed_rows):
            new_row = [highlight_re.sub(r"\1", cell) for cell in row]
            self.displayed_rows[row_idx] = new_row
            data_table.rows[row_idx] = new_row
        data_table.refresh()

    def action_next_search(self) -> None:
        """Move cursor to the next search result."""
        self._navigate_search(1)

    def action_previous_search(self) -> None:
        """Move cursor to the previous search result."""
        self._navigate_search(-1)

    def action_sort(self) -> None:
        with self._try_lock("sort", deferred=self.action_sort) as acquired:
            if not acquired:
                return
            data_table = self.query_one(".nless-view")
            current_cursor_column = data_table.cursor_column
            selected_column = self._get_column_at_position(current_cursor_column)
            if not selected_column:
                self.notify("No column selected for sorting", severity="error")
                return

            new_sort_column_name = strip_markup(selected_column.name)

            if self.sort_column == new_sort_column_name and self.sort_reverse:
                self.sort_column = None
            elif self.sort_column == new_sort_column_name and not self.sort_reverse:
                self.sort_reverse = True
            else:
                self.sort_column = new_sort_column_name
                self.sort_reverse = False

            # Update sort indicators
            if self.sort_column is None:
                selected_column.labels.discard("▲")
                selected_column.labels.discard("▼")
            elif self.sort_reverse:
                selected_column.labels.discard("▲")
                selected_column.labels.add("▼")
            else:
                selected_column.labels.discard("▼")
                selected_column.labels.add("▲")

            # Remove sort indicators from other columns
            for col in self.current_columns:
                if col.name != selected_column.name:
                    col.labels.discard("▲")
                    col.labels.discard("▼")

        self._deferred_update_table(reason="Sorting")

    def _filter_rows(
        self, expected_cell_count: int
    ) -> tuple[list[list[str]], list[str]]:
        """Parse raw rows, filter by current filters, return (matching, mismatched).

        Uses a parsed-row cache to avoid re-parsing on sort/search operations.
        Also compacts raw_rows to only keep matching + unparseable lines so that
        subsequent _update_table() calls (sort, search, etc.) scan fewer rows.
        """
        filtered, mismatched, compacted = self._partition_rows(expected_cell_count)
        if compacted is not None:
            self._apply_raw_rows_compaction(compacted)
        elif filtered and (
            self._parsed_rows is None or len(filtered) != len(self._parsed_rows)
        ):
            # Partial-cache fast path: update cache with newly parsed rows.
            # Copy so downstream in-place sort doesn't corrupt the cache.
            self._parsed_rows = list(filtered)
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
        if self._parsed_rows is not None and len(self._parsed_rows) <= len(
            self.raw_rows
        ):
            parsed = self._parsed_rows
        else:
            parsed = None

        # Fast path: full cache + no filters → all rows pass, skip the loop
        if (
            parsed is not None
            and len(parsed) == len(self.raw_rows)
            and not self.current_filters
        ):
            return list(parsed), [], None

        # Fast path: partial cache + no filters → only parse new rows
        if (
            parsed is not None
            and not self.current_filters
            and len(parsed) < len(self.raw_rows)
        ):
            new_parsed = []
            mismatched = []
            for i in range(len(parsed), len(self.raw_rows)):
                row_str = self.raw_rows[i]
                ts = (
                    self._arrival_timestamps[i]
                    if i < len(self._arrival_timestamps)
                    else time.time()
                )
                try:
                    cells = split_line(row_str, self.delimiter, self.current_columns)
                except (json.JSONDecodeError, csv.Error, ValueError):
                    continue
                if len(cells) != expected_cell_count:
                    mismatched.append(row_str)
                    continue
                cells.append(self._format_arrival(ts))
                new_parsed.append(cells)
            return list(parsed) + new_parsed, mismatched, None

        filtered_rows = []
        rows_with_inconsistent_length = []
        kept_raw = []
        kept_parsed = []
        kept_timestamps = []
        unparseable_raw = []
        unparseable_timestamps = []
        needs_copy = parsed is not None and bool(self.unique_column_names)
        parsed_len = len(parsed) if parsed is not None else 0
        for i, row_str in enumerate(self.raw_rows):
            ts = (
                self._arrival_timestamps[i]
                if i < len(self._arrival_timestamps)
                else time.time()
            )
            if parsed is not None and i < parsed_len:
                # Copy when dedup is active to avoid mutating the cache
                # when _dedup_rows prepends a count column.
                cells = list(parsed[i]) if needs_copy else parsed[i]
            else:
                try:
                    cells = split_line(row_str, self.delimiter, self.current_columns)
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
            if self._matches_all_filters(cells, adjust_for_count=True):
                filtered_rows.append(cells)
                kept_raw.append(row_str)
                kept_parsed.append(cells)
                if i < len(self._arrival_timestamps):
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
        self.raw_rows = kept_raw + unparseable_raw
        self._parsed_rows = kept_parsed
        self._arrival_timestamps = kept_ts + unparseable_ts

    def _dedup_rows(self, filtered_rows: list[list[str]]) -> list[list[str]]:
        """Deduplicate rows by composite unique column key, prepending count."""
        if not self.unique_column_names:
            return filtered_rows
        dedup_map = {}
        for cells in filtered_rows:
            composite_key = []
            for col_name in self.unique_column_names:
                col_idx = self._get_col_idx_by_name(col_name)
                if col_idx is None:
                    continue
                col_idx -= 1  # account for count column
                composite_key.append(strip_markup(cells[col_idx]))
            key = ",".join(composite_key)
            dedup_map[key] = cells
            self.count_by_column_key[key] += 1
        deduped_rows = []
        for idx, (k, cells) in enumerate(dedup_map.items()):
            cells = [str(self.count_by_column_key[k])] + cells
            self._dedup_key_to_row_idx[k] = idx
            deduped_rows.append(cells)
        return deduped_rows

    def _sort_rows(self, rows: list[list[str]]) -> None:
        """Sort rows in-place by the current sort column."""
        if self.sort_column is None:
            return
        sort_column_idx = self._get_col_idx_by_name(self.sort_column)
        if sort_column_idx is None:
            return
        try:
            # Precompute sort keys in one O(N) pass to avoid calling
            # coerce_to_numeric O(N log N) times inside the sort comparator.
            keys = [coerce_to_numeric(r[sort_column_idx]) for r in rows]
            indices = sorted(
                range(len(rows)), key=keys.__getitem__, reverse=self.sort_reverse
            )
            rows[:] = [rows[i] for i in indices]
        except (ValueError, IndexError):
            pass
        except TypeError:
            try:
                rows.sort(
                    key=lambda r: r[sort_column_idx],
                    reverse=self.sort_reverse,
                )
            except (TypeError, IndexError):
                pass

    def _deferred_update_table(
        self, restore_position=True, callback=None, reason="Loading"
    ):
        """Run data processing on a bg thread, then apply to widgets on main thread."""
        self._update_generation += 1
        gen = self._update_generation
        self._loading_reason = reason
        # Invalidate caches for structural changes (not sort/search).
        # Reasons may have a suffix like " 50,000 rows", so check prefixes.
        _CACHE_SAFE = (
            "Sorting",
            "Searching",
            "Applying theme",
            "Deduplicating",
            "Filtering",
            "Pivoting",
        )
        if not any(reason.startswith(p) for p in _CACHE_SAFE):
            self._parsed_rows = None
            self._cached_col_widths = None
        self._start_spinner()
        self._update_status_bar()

        # Snapshot cursor/scroll from widgets before going off-thread.
        data_table = self.query_one(".nless-view")
        cursor_x = data_table.cursor_column
        cursor_y = data_table.cursor_row
        scroll_x = data_table.scroll_x
        scroll_y = data_table.scroll_y

        def _process_data():
            """Pure-data work — no widget access."""
            if gen != self._update_generation:
                return None
            with self._lock:
                metadata_names = {m.value for m in MetadataColumn}
                curr_metadata_columns = {
                    c.name for c in self.current_columns if c.name in metadata_names
                }
                expected_cell_count = len(self.current_columns) - len(
                    curr_metadata_columns
                )
                self._rebuild_column_caches()
                column_labels = self._get_visible_column_labels()
                fixed_columns = len(
                    [c for c in self.current_columns if c.pinned and not c.hidden]
                )

                self.search_matches = []
                self.current_match_index = -1
                self.count_by_column_key = defaultdict(lambda: 0)
                self._dedup_key_to_row_idx = {}
                self._sort_keys = []

                filtered_rows, rows_with_inconsistent_length = self._filter_rows(
                    expected_cell_count
                )
                filtered_rows = self._apply_time_window(filtered_rows)
                deduped_rows = self._dedup_rows(filtered_rows)
                self._sort_rows(deduped_rows)

                # Rebuild dedup indices after sorting (sort reorders rows)
                if self.unique_column_names:
                    self._dedup_key_to_row_idx = {}
                    for idx, row in enumerate(deduped_rows):
                        key = self._build_composite_key(row)
                        self._dedup_key_to_row_idx[key] = idx

                # Populate _sort_keys for incremental inserts.
                # _sort_keys is always ascending for bisect. Rows are already
                # sorted by _sort_rows, so extract in order (O(N)) instead of
                # re-sorting (O(N log N)). If sort_reverse, reverse the list.
                if self.sort_column is not None:
                    sort_col_idx = self._get_col_idx_by_name(
                        self.sort_column, render_position=False
                    )
                    if sort_col_idx is not None:
                        keys = [
                            coerce_sort_key(strip_markup(str(r[sort_col_idx])))
                            for r in deduped_rows
                        ]
                        if self.sort_reverse:
                            keys.reverse()
                        self._sort_keys = keys

                aligned_rows = self._align_cells_to_visible_columns(deduped_rows)
                styled_rows = self._highlight_search_matches(
                    aligned_rows, fixed_columns
                )

                # Highlight rows affected by newly streamed lines
                green_lines = self._green_lines
                if green_lines and self.unique_column_names:
                    green_keys = set()
                    for line in green_lines:
                        cells = split_line(line, self.delimiter, self.current_columns)
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
                cached_widths = self._cached_col_widths
                has_search = bool(self.search_term)

            # Precompute column widths on the bg thread so the main thread
            # can use add_rows_precomputed (just extends + refresh).
            # Reuse cached widths on sort-only rebuilds (no search → no
            # markup changes → widths are stable).
            if (
                cached_widths is not None
                and not has_search
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
                    self._cached_col_widths = col_widths

            return {
                "styled_rows": styled_rows,
                "column_labels": column_labels,
                "column_widths": col_widths,
                "fixed_columns": fixed_columns,
                "inconsistent_rows": rows_with_inconsistent_length,
                "n_filtered": len(filtered_rows),
            }

        def _apply_to_widgets(result):
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
                    self._deferred_update_table(reason="Switching delimiter")
                    return

            # Check if more data arrived while we were processing
            if len(self.raw_rows) > self._last_flushed_idx:
                stream_active = self.line_stream and not self.line_stream.done
                has_expensive_op = (
                    self.sort_column is not None
                    or self.unique_column_names
                    or self.time_window
                )
                if stream_active and has_expensive_op:
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
                loaded_reason = self._loading_reason
                self._loading_reason = None
                self._stop_spinner()
                self._update_status_bar()
                if restore_position:
                    self._restore_position(dt, cursor_x, cursor_y, scroll_x, scroll_y)
                if loaded_reason:
                    row_count = len(self.displayed_rows)
                    if row_count > 0:
                        _past = {
                            "Sorting": "Sorted",
                            "Searching": "Searched",
                            "Deduplicating": "Loaded",
                            "Filtering": "Loaded",
                        }
                        done = _past.get(loaded_reason, "Loaded")
                        self._flash_status(f"{done} {row_count:,} rows")
                if callback:
                    callback()

        self._run_deferred_update(_process_data, _apply_to_widgets)

    @work(thread=True, exclusive=True, group="data-processing")
    def _run_deferred_update(self, _process_data, _apply_to_widgets):
        result = _process_data()
        self.app.call_from_thread(lambda: _apply_to_widgets(result))

    def _reload_reason(self) -> str:
        """Build a descriptive reason for chained streaming rebuilds."""
        if self.sort_column is not None:
            return "Sorting"
        if self.unique_column_names:
            return "Deduplicating"
        if self.time_window:
            return "Filtering"
        return "Loading"

    _CHAIN_DELAY_INITIAL: float = 0.3

    def _cancel_chain_timer(self) -> None:
        """Cancel the pending chain timer without resetting delay/skip state."""
        if self._chain_timer is not None:
            try:
                self._chain_timer.stop()
            except Exception:
                pass
            self._chain_timer = None

    def _stop_chain_timer(self) -> None:
        """Cancel any pending chain rebuild timer and reset state."""
        self._cancel_chain_timer()
        self._chain_delay = self._CHAIN_DELAY_INITIAL
        self._chain_skips = 0

    def _schedule_chain_rebuild(self, restore_position, callback):
        """Delay the next rebuild to let more streaming data accumulate."""
        if not self._chain_notified:
            self._chain_notified = True
            self.notify(
                "[yellow]⚠[/yellow]  Data arriving faster than it can be processed — buffering",
                severity="warning",
            )
        self._cancel_chain_timer()

        delay = self._chain_delay

        def _do_chain():
            self._chain_timer = None
            try:
                stream_active = self.line_stream and not self.line_stream.done
                pending = len(self.raw_rows) - self._last_flushed_idx
                displayed = max(len(self.displayed_rows), 1)

                # Stream is outpacing us — skip this rebuild and wait longer.
                # Cap at 3 skips (~2s) so infinite streams still show progress.
                if stream_active and pending > displayed and self._chain_skips < 3:
                    self._chain_skips += 1
                    self._chain_delay = min(self._chain_delay * 2, 1.5)
                    self._schedule_chain_rebuild(restore_position, callback)
                    return

                self._chain_skips = 0
                self._chain_delay = self._CHAIN_DELAY_INITIAL
                self._deferred_update_table(
                    restore_position=restore_position,
                    callback=callback,
                    reason=self._reload_reason(),
                )
            except Exception:
                pass  # Widget unmounted or app shutting down

        try:
            self._chain_timer = self.set_timer(delay, _do_chain)
        except Exception:
            pass

    def _update_table(self, restore_position: bool = True) -> None:
        """Completely refreshes the table, repopulating it with the raw backing data, applying all sorts, filters, delimiters, etc."""
        self._ensure_correct_view_widget()
        data_table = self.query_one(".nless-view")
        cursor_x = data_table.cursor_column
        cursor_y = data_table.cursor_row
        scroll_x = data_table.scroll_x
        scroll_y = data_table.scroll_y

        metadata_names = {m.value for m in MetadataColumn}
        curr_metadata_columns = {
            c.name for c in self.current_columns if c.name in metadata_names
        }
        expected_cell_count = len(self.current_columns) - len(curr_metadata_columns)
        data_table.clear(columns=True)

        data_table.fixed_columns = len(
            [c for c in self.current_columns if c.pinned and not c.hidden]
        )
        self._rebuild_column_caches()
        data_table.add_columns(self._get_visible_column_labels())

        self.search_matches = []
        self.current_match_index = -1
        self.count_by_column_key = defaultdict(lambda: 0)
        self._dedup_key_to_row_idx = {}
        self._sort_keys = []
        self._cached_col_widths = None

        filtered_rows, rows_with_inconsistent_length = self._filter_rows(
            expected_cell_count
        )
        filtered_rows = self._apply_time_window(filtered_rows)
        deduped_rows = self._dedup_rows(filtered_rows)
        self._sort_rows(deduped_rows)

        aligned_rows = self._align_cells_to_visible_columns(deduped_rows)
        styled_rows = self._highlight_search_matches(
            aligned_rows, data_table.fixed_columns
        )

        if rows_with_inconsistent_length:
            n_inconsistent = len(rows_with_inconsistent_length)
            n_total = len(filtered_rows) + n_inconsistent
            msg = self._try_auto_switch_delimiter(
                rows_with_inconsistent_length,
                n_inconsistent,
                n_total,
                lines_already_in_raw=True,
            )
            if msg:
                self._flash_status(msg)
                self._deferred_update_table(reason="Switching delimiter")
                return

        self.displayed_rows = styled_rows
        data_table.add_rows(styled_rows)
        self._last_flushed_idx = len(self.raw_rows)

        if restore_position:
            self._restore_position(data_table, cursor_x, cursor_y, scroll_x, scroll_y)

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

    def _update_status_bar(self) -> None:
        if self.pane_id != self.app.buffers[self.app.curr_buffer_idx].pane_id:
            return
        data_table = self.query_one(".nless-view")
        text = build_status_text(
            sort_column=self.sort_column,
            sort_reverse=self.sort_reverse,
            filters=self.current_filters,
            search_term=self.search_term,
            search_matches_count=len(self.search_matches),
            current_match_index=self.current_match_index,
            total_rows=data_table.row_count,
            total_cols=len(data_table.columns),
            current_row=data_table.cursor_row + 1,
            current_col=data_table.cursor_column + 1,
            is_tailing=self.is_tailing,
            unique_column_names=self.unique_column_names,
            loading_reason=self._loading_reason,
            flash_message=self._flash_message,
            theme=self._get_theme(),
            spinner_frame=self._spinner_frame,
            format_str=self.app.config.status_format,
            keymap_name=self.app.nless_keymap.name,
            theme_name=self.app.nless_theme.name,
            time_window=self.app._format_window(
                self.time_window, self.rolling_time_window
            )
            if self.time_window
            else None,
            delimiter=self._format_delimiter(),
            skipped_rows=self._total_skipped,
            behind=self._bp_behind,
            buffered_rows=len(self.raw_rows),
        )
        self.app.query_one("#status_bar", Static).update(text)

    def _flash_status(self, message: str, duration: float = 3.0) -> None:
        """Show a temporary message in the status bar, auto-clearing after *duration* seconds."""
        if self._flash_timer is not None:
            self._flash_timer.stop()
        self._flash_message = message
        self._update_status_bar()
        self._flash_timer = self.set_timer(duration, self._clear_flash)

    def _clear_flash(self) -> None:
        self._flash_message = None
        self._flash_timer = None
        self._update_status_bar()

    def start_loading(self, reason: str) -> None:
        """Show a loading spinner with the given reason text."""
        self._loading_reason = reason
        self._start_spinner()
        self._update_status_bar()

    def stop_loading(self) -> None:
        """Clear the loading state and stop the spinner."""
        self._loading_reason = None
        self._stop_spinner()
        self._update_status_bar()

    def _start_spinner(self) -> None:
        """Start the status bar spinner animation (must run on main thread)."""
        if self._spinner_timer is not None:
            return
        self._spinner_frame = 0
        try:
            self._spinner_timer = self.set_interval(0.1, self._tick_spinner)
        except Exception:
            pass  # Not mounted yet

    def _stop_spinner(self) -> None:
        """Stop the status bar spinner animation (must run on main thread)."""
        if self._spinner_timer is not None:
            try:
                self._spinner_timer.stop()
            except (RuntimeError, Exception):
                pass
            self._spinner_timer = None

    def _request_spinner_start(self) -> None:
        """Thread-safe: schedule spinner start on the main thread (non-blocking)."""
        try:
            self.app.call_later(self._start_spinner)
        except Exception:
            pass  # App shutting down or not running

    def _request_spinner_stop(self) -> None:
        """Thread-safe: schedule spinner stop on the main thread (non-blocking)."""
        try:
            self.app.call_later(self._stop_spinner)
        except Exception:
            pass  # App shutting down or not running

    def _tick_spinner(self) -> None:
        """Advance the spinner frame and refresh the status bar."""
        self._spinner_frame += 1
        if self._loading_reason:
            self._update_status_bar()
        else:
            self._stop_spinner()

    # -- Back pressure monitoring ------------------------------------------

    def _start_bp_timer(self) -> None:
        """Start the back pressure sampling timer (main thread)."""
        if self._bp_timer is not None:
            return
        try:
            self._bp_timer = self.set_interval(0.5, self._tick_bp)
        except Exception:
            pass

    def _stop_bp_timer(self) -> None:
        """Stop the back pressure sampling timer and any pending chain rebuild."""
        self._stop_chain_timer()
        if self._bp_timer is not None:
            try:
                self._bp_timer.stop()
            except (RuntimeError, Exception):
                pass
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
            if stream_done and self._chain_timer is not None:
                self._stop_chain_timer()
                self._deferred_update_table()

            # Stop timer when stream is done and we're caught up
            if stream_done and not behind and not self._loading_reason:
                self._stop_bp_timer()
                self._bp_behind = False

            if self._spinner_timer is None and not self.locked:
                self._update_status_bar()
        except Exception:
            pass  # Widget unmounted or app shutting down

    def _matches_all_filters(
        self, cells: list[str], adjust_for_count: bool = False
    ) -> bool:
        """Check if a row matches all current filters."""
        return matches_all_filters(
            cells,
            self.current_filters,
            self._get_col_idx_by_name,
            adjust_for_count=adjust_for_count,
            has_unique_columns=bool(self.unique_column_names),
        )

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
        self._parsed_rows = None
        self._cached_col_widths = None
        self._sort_keys = []

    def _build_composite_key(
        self, cells: list[str], render_position: bool = False
    ) -> str:
        """Build a composite key from the unique column values in a row."""
        return build_composite_key(
            cells,
            self.unique_column_names,
            self._get_col_idx_by_name,
            render_position=render_position,
        )

    def _handle_dedup_for_line(
        self, cells: list[str]
    ) -> tuple[list[str], int | None, list[str] | None]:
        """Handle deduplication for a single incoming line.

        Returns (possibly-updated cells, old_index if replacing, old_row if replacing).
        """
        if not self.unique_column_names:
            return cells, None, None

        new_key = self._build_composite_key(cells)

        if new_key in self._dedup_key_to_row_idx:
            row_idx = self._dedup_key_to_row_idx[new_key]
            if row_idx >= len(self.displayed_rows):
                # Stale index — table is being rebuilt concurrently
                return cells, None, None
            new_cells = []
            for col_idx, cell in enumerate(cells):
                if col_idx == 0:
                    self.count_by_column_key[new_key] += 1
                    cell = self.count_by_column_key[new_key]
                else:
                    cell = strip_markup(cell)
                new_cells.append(self._highlight_markup(str(cell)))
            return new_cells, row_idx, self.displayed_rows[row_idx]

        self.count_by_column_key[new_key] = 1
        return cells, None, None

    def _find_sorted_insert_index(self, cells: list[str]) -> int:
        """Find the insertion index for a row based on the current sort."""
        return find_sorted_insert_index(
            cells,
            self._sort_keys,
            self.sort_column,
            self.sort_reverse,
            self._get_col_idx_by_name,
            num_displayed_rows=len(self.displayed_rows),
        )

    def _update_dedup_indices_after_removal(self, old_index: int) -> None:
        """Shift dedup index entries down after a row removal."""
        update_dedup_indices_after_removal(self._dedup_key_to_row_idx, old_index)

    def _update_dedup_indices_after_insertion(
        self, dedup_key: str, new_index: int
    ) -> None:
        """Shift dedup index entries up after a row insertion, then record the new key."""
        update_dedup_indices_after_insertion(
            self._dedup_key_to_row_idx, dedup_key, new_index
        )

    def _update_sort_keys_for_line(
        self,
        data_cells: list[str],
        old_row: list[str] | None,
    ) -> None:
        """Update the incremental sort keys list after insertion/removal."""
        update_sort_keys_for_line(
            data_cells,
            old_row,
            self.sort_column,
            self._sort_keys,
            self._get_col_idx_by_name,
        )
