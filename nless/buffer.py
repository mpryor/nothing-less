import csv
from datetime import datetime, timezone
import json
import logging
import re
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import contextmanager
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

from .delimiter import infer_delimiter, split_line
from .input import LineStream
from .nlessselect import NlessSelect
from .datatable import Datatable as NlessDataTable
from .dataprocessing import (
    build_composite_key,
    coerce_sort_key,
    coerce_to_numeric,
    find_sorted_insert_index,
    highlight_search_matches,
    matches_all_filters,
    strip_markup,
    update_dedup_indices_after_insertion,
    update_dedup_indices_after_removal,
    update_sort_keys_for_line,
)
from .operations import handle_mark_unique
from .statusbar import build_status_text
from .types import CliArgs, Column, Filter, MetadataColumn, RowLengthMismatchError

logger = logging.getLogger(__name__)


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


class NlessBuffer(Static):
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
        if line_stream:
            line_stream.subscribe(self, self.add_logs, lambda: self.mounted)
        self.first_row_parsed = False
        self.raw_rows = []
        self.displayed_rows = []
        self.first_log_line = ""  # used to determine columns when delimiter is set
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

    def action_copy(self) -> None:
        """Copy the contents of the currently highlighted cell to the clipboard."""
        data_table = self.query_one(NlessDataTable)
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

        Caches (_parsed_rows, _cached_col_widths, _sort_keys, _dedup_key_to_row_idx)
        are intentionally NOT copied — they are rebuilt on the first
        _deferred_update_table call after the new buffer is mounted.
        """
        # Snapshot state under lock, then release so the UI stays responsive.
        with self._lock:
            raw_rows_snapshot = list(self.raw_rows)
            new_buffer = NlessBuffer(pane_id=pane_id, cli_args=None)
            new_buffer.first_row_parsed = self.first_row_parsed
            new_buffer.displayed_rows = []
            new_buffer.first_log_line = self.first_log_line
            new_buffer.current_columns = deepcopy(self.current_columns)
            new_buffer.current_filters = deepcopy(self.current_filters)
            new_buffer.search_term = self.search_term
            new_buffer.sort_column = self.sort_column
            new_buffer.sort_reverse = self.sort_reverse
            new_buffer.search_matches = deepcopy(self.search_matches)
            new_buffer.current_match_index = self.current_match_index
            new_buffer.delimiter = self.delimiter
            new_buffer.delimiter_inferred = self.delimiter_inferred
            new_buffer.is_tailing = self.is_tailing
            new_buffer.unique_column_names = deepcopy(self.unique_column_names)
            new_buffer.count_by_column_key = deepcopy(self.count_by_column_key)
            new_buffer._pivot_hidden_columns = set(self._pivot_hidden_columns)
            new_buffer.time_window = self.time_window
            new_buffer.rolling_time_window = self.rolling_time_window
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
        """Create and yield the DataTable widget."""
        with Vertical():
            theme = self._get_theme()
            table = NlessDataTable(theme=theme)
            yield table

    def on_mount(self) -> None:
        self.mounted = True
        if self._loading_reason:
            self._start_spinner()
        if not self._initial_load_done:
            self.set_timer(1.0, self._mark_initial_load_done)
        if self.rolling_time_window and self.time_window:
            self._start_rolling_timer()

    def _mark_initial_load_done(self) -> None:
        self._initial_load_done = True

    def on_datatable_cell_highlighted(
        self, event: NlessDataTable.CellHighlighted
    ) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def _make_shown_filter(self) -> Callable[[str], bool]:
        """Build a filter that returns True if a line would be shown in this buffer.

        A line is "shown" if it parses with the delimiter, has the right column
        count, and passes all content filters.  When chained via _source_parse_filter,
        a line is also considered shown if any ancestor buffer would show it.
        """
        delimiter = self.delimiter
        columns = list(self.current_columns)
        metadata = {mc.value for mc in MetadataColumn}
        expected = len([c for c in columns if c.name not in metadata])
        filters = list(self.current_filters)
        col_lookup = dict(self._col_data_idx)
        parent = self._source_parse_filter

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
        # Build shown filters for all open buffers
        buffer_filters = []
        for buf in self.app.buffers:
            if buf.delimiter == "raw" and not buf.current_filters:
                # Raw with no filters shows everything — nothing to exclude from
                self.notify("All logs are being shown.", severity="information")
                return
            buffer_filters.append(buf._make_shown_filter())

        def shown_in_any(line: str) -> bool:
            return any(f(line) for f in buffer_filters)

        # Use the line stream's full history if available
        if self.line_stream:
            all_lines = self.line_stream.lines
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
        data_table = self.query_one(NlessDataTable)
        data_table.move_cursor(column=col_index)

    def action_move_column(self, direction: int) -> None:
        with self._try_lock(
            "move column", deferred=lambda: self.action_move_column(direction)
        ) as acquired:
            if not acquired:
                return
            data_table = self.query_one(NlessDataTable)
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
        data_table = self.query_one(NlessDataTable)
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

    def action_search_cursor_word(self) -> None:
        """Search for the word under the cursor."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            self._perform_search(cell_value)
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def _perform_search(self, search_term: str | None) -> None:
        """Performs a search on the data and updates the table."""
        with self._try_lock(
            "search", deferred=lambda: self._perform_search(search_term)
        ) as acquired:
            if not acquired:
                return
            try:
                if search_term:
                    self.search_term = re.compile(search_term, re.IGNORECASE)
                else:
                    self.search_term = None
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        def _after_search():
            if self.search_matches:
                self._navigate_search(1)  # Jump to first match

        self._deferred_update_table(
            restore_position=False, callback=_after_search, reason="Searching"
        )

    def action_sort(self) -> None:
        with self._try_lock("sort", deferred=self.action_sort) as acquired:
            if not acquired:
                return
            data_table = self.query_one(NlessDataTable)
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

    def _get_visible_column_labels(self) -> list[str]:
        labels = []
        for col in sorted(self.current_columns, key=lambda c: c.render_position):
            if not col.hidden:
                labels.append(f"{col.name} {' '.join(col.labels)}".strip())
        return labels

    def _filter_rows(
        self, expected_cell_count: int
    ) -> tuple[list[list[str]], list[str]]:
        """Parse raw rows, filter by current filters, return (matching, mismatched).

        Uses a parsed-row cache to avoid re-parsing on sort/search operations.
        Also compacts raw_rows to only keep matching + unparseable lines so that
        subsequent _update_table() calls (sort, search, etc.) scan fewer rows.
        """
        filtered, mismatched, compacted = self._partition_rows(expected_cell_count)
        self._apply_raw_rows_compaction(compacted)
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
        # Use cached parsed rows if available (sort/search don't need re-parse)
        if self._parsed_rows is not None and len(self._parsed_rows) == len(
            self.raw_rows
        ):
            parsed = self._parsed_rows
        else:
            parsed = None

        # Fast path: cache valid + no filters → all rows pass, skip the loop
        if parsed is not None and not self.current_filters:
            return list(parsed), [], None

        filtered_rows = []
        rows_with_inconsistent_length = []
        kept_raw = []
        kept_parsed = []
        kept_timestamps = []
        unparseable_raw = []
        unparseable_timestamps = []
        for i, row_str in enumerate(self.raw_rows):
            ts = (
                self._arrival_timestamps[i]
                if i < len(self._arrival_timestamps)
                else time.time()
            )
            if parsed is not None:
                cells = parsed[i]
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
            cells.insert(0, str(self.count_by_column_key[k]))
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
            rows.sort(
                key=lambda r: coerce_to_numeric(r[sort_column_idx]),
                reverse=self.sort_reverse,
            )
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

    def _search_match_style(self) -> str:
        """Return the Rich style string for search match highlighting."""
        theme = self._get_theme()
        return f"{theme.search_match_fg} on {theme.search_match_bg}"

    def _highlight_search_matches(
        self, rows: list[list[str]], fixed_columns: int, row_offset: int = 0
    ) -> list[list[str]]:
        """Apply search highlighting to rows and populate search_matches."""
        result, new_matches = highlight_search_matches(
            rows,
            self.search_term,
            fixed_columns,
            row_offset,
            search_match_style=self._search_match_style(),
        )
        self.search_matches.extend(Coordinate(r, c) for r, c in new_matches)
        return result

    def _deferred_update_table(
        self, restore_position=True, callback=None, reason="Loading"
    ):
        """Run data processing on a bg thread, then apply to widgets on main thread."""
        self._update_generation += 1
        gen = self._update_generation
        self._loading_reason = reason
        # Invalidate caches for structural changes (not sort/search)
        if reason not in ("Sorting", "Searching", "Applying theme"):
            self._parsed_rows = None
            self._cached_col_widths = None
        self._start_spinner()
        self._update_status_bar()

        # Snapshot cursor/scroll from widgets before going off-thread.
        data_table = self.query_one(NlessDataTable)
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
                            str_len = Text.from_markup(cell_str).cell_len
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
            data_table.clear(columns=True)
            data_table.fixed_columns = result["fixed_columns"]
            data_table.add_columns(result["column_labels"])
            data_table.column_widths = result["column_widths"]

            self.displayed_rows = result["styled_rows"]
            data_table.add_rows_precomputed(result["styled_rows"])

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
                self._deferred_update_table(
                    restore_position=restore_position, callback=callback
                )
            else:
                loaded_reason = self._loading_reason
                self._loading_reason = None
                self._stop_spinner()
                self._update_status_bar()
                if restore_position:
                    self._restore_position(
                        data_table, cursor_x, cursor_y, scroll_x, scroll_y
                    )
                if loaded_reason:
                    row_count = len(self.displayed_rows)
                    if row_count > 0:
                        _past = {
                            "Sorting": "Sorted",
                            "Searching": "Searched",
                            "Rebuilding": "Rebuilt",
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

    def _update_table(self, restore_position: bool = True) -> None:
        """Completely refreshes the table, repopulating it with the raw backing data, applying all sorts, filters, delimiters, etc."""
        data_table = self.query_one(NlessDataTable)
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

    def _align_cells_to_visible_columns(self, rows: list[list[str]]) -> list[list[str]]:
        visible_cols = self._sorted_visible_columns
        new_rows = []
        for row in rows:
            new_rows.append([row[col.data_position] for col in visible_cols])
        return new_rows

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
        data_table = self.query_one(NlessDataTable)
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
        except (RuntimeError, Exception):
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
        """Thread-safe: schedule spinner start on the main thread."""
        try:
            self.app.call_from_thread(self._start_spinner)
        except (RuntimeError, Exception):
            pass  # App shutting down or not running

    def _request_spinner_stop(self) -> None:
        """Thread-safe: schedule spinner stop on the main thread."""
        try:
            self.app.call_from_thread(self._stop_spinner)
        except (RuntimeError, Exception):
            pass  # App shutting down or not running

    def _tick_spinner(self) -> None:
        """Advance the spinner frame and refresh the status bar."""
        self._spinner_frame += 1
        if self._loading_reason:
            self._update_status_bar()
        else:
            self._stop_spinner()

    def _start_rolling_timer(self) -> None:
        """Start a periodic timer that re-applies the time window filter."""
        self._stop_rolling_timer()
        try:
            interval = min(self.time_window / 10, 5.0) if self.time_window else 5.0
            interval = max(interval, 1.0)
            self._rolling_timer = self.set_interval(interval, self._tick_rolling)
        except (RuntimeError, Exception):
            pass  # Not mounted yet

    def _stop_rolling_timer(self) -> None:
        """Stop the rolling time window timer."""
        if self._rolling_timer is not None:
            try:
                self._rolling_timer.stop()
            except (RuntimeError, Exception):
                pass
            self._rolling_timer = None

    def _tick_rolling(self) -> None:
        """Re-apply the time window filter to drop expired rows."""
        if not self.time_window or not self.rolling_time_window:
            self._stop_rolling_timer()
            return
        self.invalidate_caches()
        self._deferred_update_table(reason="")

    def apply_time_window_setting(self, value: str) -> None:
        """Parse a time window value and apply it to the buffer.

        Handles clearing ("0", "off", etc.), rolling windows ("5m+"), and
        one-shot windows that permanently prune raw_rows.
        """
        value = value.strip()
        if not value or value in ("0", "off", "clear", "none"):
            self.time_window = None
            self.rolling_time_window = False
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
            self._deferred_update_table(reason="Applying time window")
            self._start_rolling_timer()
        else:
            self._stop_rolling_timer()

            # One-shot: prune raw_rows permanently then clear time_window
            # so subsequent rebuilds (sort, filter) don't re-evaluate
            def _finalize_one_shot():
                cutoff = time.time() - duration
                kept = [
                    (row, ts)
                    for row, ts in zip(self.raw_rows, self._arrival_timestamps)
                    if ts >= cutoff
                ]
                if kept:
                    self.raw_rows, self._arrival_timestamps = [
                        list(x) for x in zip(*kept)
                    ]
                else:
                    self.raw_rows = []
                    self._arrival_timestamps = []
                self.time_window = None

            self._deferred_update_table(
                reason="Applying time window", callback=_finalize_one_shot
            )

    def _navigate_search(self, direction: int) -> None:
        """Navigate through search matches."""
        if not self.search_matches:
            self.notify("No search results.", severity="warning")
            return

        num_matches = len(self.search_matches)
        self.current_match_index = (
            self.current_match_index + direction + num_matches
        ) % num_matches  # Wrap around
        target_coord = self.search_matches[self.current_match_index]
        data_table = self.query_one(NlessDataTable)
        data_table.cursor_coordinate = target_coord
        self._update_status_bar()

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

    @staticmethod
    def _format_arrival(ts: float) -> str:
        """Format an epoch timestamp as ISO 8601 for display."""
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3]  # trim to milliseconds

    @staticmethod
    def _format_delimiter_label(d) -> str:
        """Return a human-readable label for a delimiter value."""
        if isinstance(d, re.Pattern):
            return f"regex({d.pattern})"
        return {" ": "space", "  ": "space+", "\t": "tab", ",": "csv"}.get(d, d)

    def _format_delimiter(self) -> str | None:
        """Return a human-readable label for the current delimiter."""
        if self.delimiter is None:
            return None
        return self._format_delimiter_label(self.delimiter)

    def _reset_delimiter_state(self) -> None:
        """Reset all delimiter-related flags and counters."""
        self.delimiter_inferred = False
        self._delimiter_suggestion_shown = False
        self._mismatch_warning_shown = False
        self._total_skipped = 0

    def invalidate_caches(self) -> None:
        """Invalidate all data caches, forcing a full reparse on the next update.

        Call after changes to columns, filters, delimiter, or raw_rows.
        """
        self._parsed_rows = None
        self._cached_col_widths = None
        self._sort_keys = []

    def switch_delimiter(self, delimiter_input: str) -> bool:
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
            delimiter = self._parse_delimiter_input(delimiter_input)

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

                    self.current_columns = self._make_columns(list(new_header))
                    self._ensure_arrival_column(self.current_columns)
                    should_update = True

        if should_update:

            def callback():
                if had_filters:
                    self._flash_status("Filters cleared — delimiter changed")

            self._deferred_update_table(reason="Changing delimiter", callback=callback)
        return should_update

    def _resolve_new_header(self, delimiter, prev_delimiter):
        """Determine the new header columns when switching delimiters.

        Returns (header_list, parsed_full_json_file) or None on error.
        All raw_rows / first_log_line mutations happen here, keeping them
        internal to the buffer.
        """
        if delimiter == "raw":
            return ["log"], False

        if delimiter == "json":
            # Try first_log_line as JSON header
            try:
                return list(json.loads(self.first_log_line).keys()), False
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

            # When coming from raw/regex, first_log_line isn't a data row —
            # try the first raw_row instead
            if (
                prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern)
            ) and self.raw_rows:
                try:
                    header = list(json.loads(self.raw_rows[0]).keys())
                    self.first_log_line = self.raw_rows.pop(0)
                    return header, False
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

            # Fallback: try to read all logs as one JSON payload
            try:
                all_logs = ""
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
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

    def _warn_mismatch_once(self, count: int) -> None:
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
        self,
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

    def _should_auto_switch_delimiter(self, n_bad: int, n_total: int) -> bool:
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
        self,
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
            if idx < len(self._arrival_timestamps):
                self._arrival_timestamps.pop(idx)
        except ValueError:
            pass
        self.first_log_line = new_header
        parts = self._parse_first_line_columns(self.first_log_line)
        self.current_columns = self._make_columns(parts)
        self._ensure_arrival_column(self.current_columns)
        new_label = self._format_delimiter_label(candidate)
        return (
            f"Switched delimiter to {new_label} ({n_bad} rows failed with {old_label})"
        )

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

    def _apply_initial_column_filter(self, column_regex: str) -> None:
        """Apply a column visibility filter from CLI args."""
        filters = [name.strip() for name in column_regex.split("|")]
        regexes = [re.compile(rf"{name}", re.IGNORECASE) for name in filters]
        metadata_names = {mc.value for mc in MetadataColumn}
        for col in self.current_columns:
            if col.name in metadata_names or col.pinned:
                continue
            plain_name = strip_markup(col.name)
            matched = False
            for i, regex in enumerate(regexes):
                if regex.search(plain_name):
                    col.hidden = False
                    col.render_position = i
                    matched = True
                    break
            if not matched:
                col.hidden = True
                col.render_position = 99999
        self._rebuild_column_caches()

    def _apply_initial_time_window(self, window_str: str) -> None:
        """Apply a time window from CLI args."""
        rolling = window_str.endswith("+")
        value = window_str.rstrip("+").strip()
        duration = self._parse_duration(value)
        if duration is None:
            return
        self.time_window = duration
        self.rolling_time_window = rolling

    def _apply_time_window(self, rows: list[list[str]]) -> list[list[str]]:
        """Filter rows by the active time window using arrival timestamps."""
        if not self.time_window:
            return rows
        cutoff = time.time() - self.time_window
        # rows are parallel to self._arrival_timestamps after _filter_rows
        return [
            row
            for i, row in enumerate(rows)
            if i < len(self._arrival_timestamps)
            and self._arrival_timestamps[i] >= cutoff
        ]

    @staticmethod
    def _ensure_arrival_column(columns: list[Column]) -> None:
        """Ensure the hidden _arrival metadata column is present at the end."""
        if any(c.name == MetadataColumn.ARRIVAL.value for c in columns):
            return
        arrival_pos = len(columns)
        columns.append(
            Column(
                name=MetadataColumn.ARRIVAL.value,
                labels=set(),
                render_position=arrival_pos,
                data_position=arrival_pos,
                hidden=True,
                computed=True,
            )
        )

    @staticmethod
    def _make_columns(names: list) -> list[Column]:
        """Create a list of Column objects from a list of names."""
        return [
            Column(
                name=str(n),
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, n in enumerate(names)
        ]

    def _parse_first_line_columns(self, first_log_line: str) -> list:
        """Determine column names from the first line based on the delimiter."""
        if self.delimiter == "raw":
            return ["log"]
        elif isinstance(self.delimiter, re.Pattern):
            return list(self.delimiter.groupindex.keys())
        elif self.delimiter == "json":
            try:
                json_data = json.loads(first_log_line)
                if isinstance(json_data, dict):
                    return list(json_data.keys())
                elif isinstance(json_data, list) and len(json_data) > 0:
                    return list(range(len(json_data)))
            except json.JSONDecodeError:
                pass
            return ["value"]
        else:
            return split_line(first_log_line, self.delimiter, self.current_columns)

    @contextmanager
    def _try_lock(self, action: str, deferred: Callable | None = None):
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

    def add_logs(self, log_lines: list[str]) -> None:
        needs_deferred = False
        self._skipped_lines = []
        with self._lock:
            self.locked = True
            try:
                self._needs_deferred_update = False
                self._add_logs_inner(log_lines)
                needs_deferred = self._needs_deferred_update
            finally:
                if not needs_deferred:
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

        if needs_deferred:
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

    def _add_logs_inner(self, log_lines: list[str]) -> None:
        data_table = self.query_one(NlessDataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            sample = _sample_lines(log_lines, max_total=15)
            self.delimiter = infer_delimiter(sample)
            self.delimiter_inferred = True

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
                        restore_position=False, reason="Rebuilding"
                    )
                )
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
        self,
        new_lines: list[str],
        highlight: bool = True,
        arrival_ts: float | None = None,
    ) -> None:
        """Parse and add new lines to the table without a full rebuild.

        Fuses parsing, column alignment, and column width tracking into a
        single pass, then bypasses the normal add_rows width computation.
        """
        data_table = self.query_one(NlessDataTable)
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
            # Align to visible columns; strip only for fast-path delimiters
            if needs_cleanup:
                row = [_strip(cells[p]) for p in col_positions]
            else:
                row = [cells[p] for p in col_positions]
            new_rows.append(row)

        self._total_skipped += skipped_count
        remaining = 200 - len(self._skipped_lines)
        if remaining > 0:
            self._skipped_lines.extend(skipped_lines[:remaining])

        # Track column widths from a sample to avoid O(n*cols) len() calls
        if track_widths and new_rows:
            sample = new_rows[:WIDTH_SAMPLE]
            for row in sample:
                for i, cell in enumerate(row):
                    cl = _len(cell)
                    if cl > column_widths[i]:
                        column_widths[i] = cl

        self._last_flushed_idx = len(self.raw_rows)
        if not new_rows:
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

        if self.is_tailing:
            data_table.action_scroll_bottom()

    def _rebuild_column_caches(self) -> None:
        """Rebuild all column-derived caches. Call when columns change."""
        self._col_data_idx = {}
        self._col_render_idx = {}
        for col in self.current_columns:
            plain = strip_markup(col.name)
            self._col_data_idx[plain] = col.data_position
            self._col_render_idx[plain] = col.render_position
        self._sorted_visible_columns = sorted(
            [c for c in self.current_columns if not c.hidden],
            key=lambda c: c.render_position,
        )
        self._has_nested_delimiters = any(
            c.delimiter or c.json_ref or c.col_ref for c in self.current_columns
        )

    def _get_col_idx_by_name(
        self, col_name: str, render_position: bool = False
    ) -> int | None:
        cache = self._col_render_idx if render_position else self._col_data_idx
        return cache.get(col_name)

    def _get_column_at_position(self, position: int) -> Column | None:
        """Get the column at a given render position, or None."""
        for col in self.current_columns:
            if col.render_position == position:
                return col
        return None

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

    def _add_log_line(self, log_line: str, arrival_ts: float | None = None):
        """Adds a single log line, applying filters, dedup, sort, and search highlighting."""
        data_table = self.query_one(NlessDataTable)
        cells = split_line(log_line, self.delimiter, self.current_columns)
        cells.append(self._format_arrival(arrival_ts or time.time()))
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
            self.displayed_rows.remove(old_row)
            data_table.remove_row(old_index)

        data_table.add_row_at(index=new_index, row_data=cells)
        self.displayed_rows.insert(new_index, cells)

        self._update_sort_keys_for_line(data_cells, old_row)

        if self.unique_column_names:
            dedup_key = self._build_composite_key(cells, render_position=True)
            self._update_dedup_indices_after_insertion(dedup_key, new_index)

        if self.is_tailing:
            data_table.action_scroll_bottom()
