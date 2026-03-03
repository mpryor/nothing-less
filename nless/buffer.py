import bisect
import csv
import json
import re
import time
from collections import defaultdict
from copy import deepcopy
from typing import Any

import pyperclip
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.coordinate import Coordinate
from textual.screen import Screen
from textual.widgets import (
    RichLog,
    Select,
    Static,
)

from .delimiter import infer_delimiter, split_line
from .input import LineStream
from .nlessselect import NlessSelect
from .datatable import Datatable as NlessDataTable
from .types import CliArgs, Column, Filter, MetadataColumn


class UnparsedLogsScreen(Screen):
    BINDINGS = [("q", "app.pop_screen", "Close")]

    def __init__(self, unparsed_rows: list[str], delimiter: str):
        super().__init__()
        self.unparsed_rows = unparsed_rows
        self.delimiter = delimiter

    def compose(self) -> ComposeResult:
        yield Static(
            f"{len(self.unparsed_rows)} logs not matching columns (delimiter '{self.delimiter}'), press 'q' to close.",
        )
        rl = RichLog()
        for row in self.unparsed_rows:
            rl.write(row.strip())
        yield rl


class RowLengthMismatchError(Exception):
    pass


def handle_mark_unique(new_buffer: "NlessBuffer", new_unique_column_name: str) -> None:
    if new_unique_column_name in [mc.value for mc in MetadataColumn]:
        # can't toggle count column
        return

    col_idx = new_buffer._get_col_idx_by_name(new_unique_column_name)
    new_unique_column = (
        new_buffer.current_columns[col_idx] if col_idx is not None else None
    )

    if new_unique_column is None:
        return

    new_buffer.count_by_column_key = defaultdict(lambda: 0)

    if (
        new_unique_column_name in new_buffer.unique_column_names
        and new_buffer.first_row_parsed
    ):
        new_buffer.unique_column_names.remove(new_unique_column_name)
        if new_buffer.sort_column in [metadata.value for metadata in MetadataColumn]:
            new_buffer.sort_column = None
        new_unique_column.labels.discard("U")
    else:
        new_buffer.unique_column_names.add(new_unique_column_name)
        new_unique_column.labels.add("U")

    if len(new_buffer.unique_column_names) == 0:
        # remove count column
        new_buffer.current_columns = [
            Column(
                name=c.name,
                labels=c.labels,
                render_position=c.render_position - 1,
                data_position=c.data_position - 1,
                hidden=c.hidden,
                json_ref=c.json_ref,
                computed=c.computed,
                col_ref=c.col_ref,
                col_ref_index=c.col_ref_index,
                delimiter=c.delimiter,
            )
            for c in new_buffer.current_columns
            if c.name != MetadataColumn.COUNT.value
        ]
    elif MetadataColumn.COUNT.value not in [c.name for c in new_buffer.current_columns]:
        # add count column at the start
        new_buffer.current_columns = [
            Column(
                name=c.name,
                labels=c.labels,
                render_position=c.render_position + 1,
                data_position=c.data_position + 1,
                hidden=c.hidden,
                json_ref=c.json_ref,
                computed=c.computed,
                col_ref=c.col_ref,
                col_ref_index=c.col_ref_index,
                delimiter=c.delimiter,
            )
            for c in new_buffer.current_columns
        ]
        new_buffer.current_columns.insert(
            0,
            Column(
                name=MetadataColumn.COUNT.value,
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                pinned=True,
            ),
        )

    pinned_columns_visible = len(
        [c for c in new_buffer.current_columns if c.pinned and not c.hidden]
    )
    if new_unique_column_name in new_buffer.unique_column_names:
        old_position = new_unique_column.render_position
        for col in new_buffer.current_columns:
            col_name = new_buffer._get_cell_value_without_markup(col.name)
            if col_name == new_unique_column_name:
                col.render_position = (
                    pinned_columns_visible  # bubble to just after last pinned column
                )
                col.pinned = True
            elif (
                col_name != new_unique_column_name
                and col.render_position <= old_position
                and col.name not in [mc.value for mc in MetadataColumn]
                and not col.pinned
            ):
                col.render_position += 1  # shift right to make space
    else:
        old_position = new_unique_column.render_position
        pinned_columns_visible -= 1
        for col in new_buffer.current_columns:
            col_name = new_buffer._get_cell_value_without_markup(col.name)
            if col_name == new_unique_column_name:
                col.pinned = False
                col.render_position = (
                    pinned_columns_visible if pinned_columns_visible > 0 else 0
                )
            elif col.pinned and col.render_position >= old_position:
                col.render_position -= 1


def write_buffer(current_buffer: "NlessBuffer", output_path: str) -> None:
    if output_path == "-":
        output_path = "/dev/stdout"
        while current_buffer.app.is_running:
            time.sleep(0.1)
        time.sleep(0.1)

    with open(output_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(current_buffer._get_visible_column_labels())
        for row in current_buffer.displayed_rows:
            plain_row = [
                current_buffer._get_cell_value_without_markup(str(cell)) for cell in row
            ]
            writer.writerow(plain_row)


class NlessBuffer(Static):
    """A modern pager with tabular data sorting/filtering capabilities."""

    ENABLE_COMMAND_PALETTE = False
    CSS_PATH = "nless.tcss"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("y", "copy", "Copy cell contents"),
        ("c", "jump_columns", "Jump to column (by select)"),
        (">", "move_column_right", "Move column right"),
        ("<", "move_column_left", "Move column left"),
        ("s", "sort", "Sort selected column"),
        ("n", "next_search", "Next search result"),
        ("p", "previous_search", "Previous search result"),
        ("*", "search_cursor_word", "Search (all columns) for word under cursor"),
        ("~", "view_unparsed_logs", "View logs not matching delimiter"),
        (
            "t",
            "toggle_tail",
            "Keep cursor at the bottom of the screen even as new logs arrive.",
        ),
    ]

    def __init__(
        self,
        pane_id: int,
        cli_args: CliArgs | None,
        line_stream: LineStream | None = None,
    ):
        super().__init__()
        self.line_stream = line_stream
        self.locked = False
        self.pane_id: int = pane_id
        self.mounted = False
        if line_stream:
            line_stream.subscribe(
                self, self.add_logs, lambda: not self.locked and self.mounted
            )
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
        self.is_tailing = False
        self.unique_column_names = cli_args.unique_keys if cli_args else set()
        self.count_by_column_key = defaultdict(lambda: 0)

        # Caches rebuilt when columns change
        self._col_data_idx: dict[str, int] = {}  # plain_name → data_position
        self._col_render_idx: dict[str, int] = {}  # plain_name → render_position
        self._sorted_visible_columns: list[Column] = []
        # Dedup: composite_key → row index in displayed_rows
        self._dedup_key_to_row_idx: dict[str, int] = {}
        # Incremental sort keys for _find_sorted_insert_index
        self._sort_keys: list = []

    def action_copy(self) -> None:
        """Copy the contents of the currently highlighted cell to the clipboard."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = self._get_cell_value_without_markup(cell_value)
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")
            return
        try:
            pyperclip.copy(cell_value)
            self.notify("Cell contents copied to clipboard.", severity="info")
        except pyperclip.PyperclipException:
            self.notify(
                "Clipboard not available — is xclip/xsel installed?",
                severity="error",
            )

    def _filter_lines(self, lines: list[str]) -> list[str]:
        """Return only lines that match all current filters."""
        if not self.current_filters:
            return lines
        metadata = [mc.value for mc in MetadataColumn]
        expected = len([c for c in self.current_columns if c.name not in metadata])
        matching = []
        for line in lines:
            try:
                cells = split_line(line, self.delimiter, self.current_columns)
            except (json.JSONDecodeError, csv.Error, ValueError):
                continue
            if len(cells) != expected:
                continue
            if self._matches_all_filters(cells, adjust_for_count=True):
                matching.append(line)
        return matching

    def copy(self, pane_id) -> "NlessBuffer":
        new_buffer = NlessBuffer(pane_id=pane_id, cli_args=None)
        new_buffer.mounted = self.mounted
        new_buffer.first_row_parsed = self.first_row_parsed
        new_buffer.raw_rows = self._filter_lines(self.raw_rows)
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
        new_buffer.line_stream = self.line_stream
        if self.line_stream:
            self.line_stream.subscribe_future_only(
                new_buffer,
                new_buffer.add_logs,
                lambda: not new_buffer.locked and new_buffer.mounted,
            )
        new_buffer._rebuild_column_caches()
        return new_buffer

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        with Vertical():
            table = NlessDataTable()
            yield table

    def on_mount(self) -> None:
        self.mounted = True

    def on_datatable_cell_highlighted(
        self, event: NlessDataTable.CellHighlighted
    ) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def action_view_unparsed_logs(self) -> None:
        """View logs that do not match the current delimiter."""
        if self.delimiter == "raw":
            self.notify(
                "Delimiter is 'raw', all logs are being shown.", severity="info"
            )
            return

        unparsed_rows = []
        expected_cell_count = len([c for c in self.current_columns if not c.hidden])
        for row in self.raw_rows:
            cells = split_line(row, self.delimiter, self.current_columns)
            if len(cells) != expected_cell_count:
                unparsed_rows.append(row)

        if len(unparsed_rows) == 0:
            self.notify("All logs match the current delimiter.", severity="info")
            return

        delimiter = self.delimiter

        self.app.push_screen(UnparsedLogsScreen(unparsed_rows, delimiter))

    def action_jump_columns(self) -> None:
        """Show columns by user input."""
        column_options = [
            (self._get_cell_value_without_markup(c.name), c.render_position)
            for c in sorted(self.current_columns, key=lambda c: c.render_position)
            if not c.hidden
        ]
        select = NlessSelect(
            options=column_options,
            classes="dock-bottom",
            prompt="Type a column to jump to",
        )
        self.mount(select)

    def on_select_changed(self, event: Select.Changed) -> None:
        col_index = event.value
        event.control.remove()
        data_table = self.query_one(NlessDataTable)
        data_table.move_cursor(column=col_index)

    def action_move_column(self, direction: int) -> None:
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
        self._update_table()
        self.call_after_refresh(
            lambda: data_table.move_cursor(column=selected_column.render_position)
        )

    def action_move_column_left(self) -> None:
        self.action_move_column(-1)

    def action_move_column_right(self) -> None:
        self.action_move_column(1)

    def action_toggle_tail(self) -> None:
        self.is_tailing = not self.is_tailing
        self._update_status_bar()

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
            cell_value = self._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            self._perform_search(cell_value)
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def _perform_search(self, search_term: str | None) -> None:
        """Performs a search on the data and updates the table."""
        try:
            if search_term:
                self.search_term = re.compile(search_term, re.IGNORECASE)
            else:
                self.search_term = None
        except re.error:
            self.notify("Invalid regex pattern", severity="error")
            return
        self._update_table(restore_position=False)
        if self.search_matches:
            self._navigate_search(1)  # Jump to first match

    def action_sort(self) -> None:
        data_table = self.query_one(NlessDataTable)
        current_cursor_column = data_table.cursor_column
        selected_column = self._get_column_at_position(current_cursor_column)
        if not selected_column:
            self.notify("No column selected for sorting", severity="error")
            return

        new_sort_column_name = self._get_cell_value_without_markup(selected_column.name)

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

        self._update_table()

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

        Also shrinks raw_rows to only keep matching lines so that subsequent
        _update_table() calls (sort, search, etc.) scan fewer rows.
        """
        filtered_rows = []
        rows_with_inconsistent_length = []
        kept_raw = []
        for row_str in self.raw_rows:
            try:
                cells = split_line(row_str, self.delimiter, self.current_columns)
            except (json.JSONDecodeError, csv.Error, ValueError):
                continue
            if len(cells) != expected_cell_count:
                rows_with_inconsistent_length.append(row_str)
                continue
            if self._matches_all_filters(cells, adjust_for_count=True):
                filtered_rows.append(cells)
                kept_raw.append(row_str)
        self.raw_rows = kept_raw
        return filtered_rows, rows_with_inconsistent_length

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
                composite_key.append(
                    self._get_cell_value_without_markup(cells[col_idx])
                )
            key = ",".join(composite_key)
            dedup_map[key] = cells
            self.count_by_column_key[key] += 1
        deduped_rows = []
        for k, cells in dedup_map.items():
            cells.insert(0, str(self.count_by_column_key[k]))
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
                key=lambda r: self.str_to_int(r[sort_column_idx]),
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

    def _highlight_search_matches(
        self, rows: list[list[str]], fixed_columns: int, row_offset: int = 0
    ) -> list[list[str]]:
        """Apply search highlighting to rows and populate search_matches."""
        if not self.search_term:
            return [list(cells) for cells in rows]
        result = []
        for i, cells in enumerate(rows):
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if (
                    isinstance(self.search_term, re.Pattern)
                    and self.search_term.search(str(cell))
                    and col_idx > fixed_columns - 1
                ):
                    cell = re.sub(
                        self.search_term,
                        lambda m: f"[reverse]{m.group(0)}[/reverse]",
                        cell,
                    )
                    highlighted_cells.append(cell)
                    self.search_matches.append(Coordinate(row_offset + i, col_idx))
                else:
                    highlighted_cells.append(cell)
            result.append(highlighted_cells)
        return result

    def _update_table(self, restore_position: bool = True) -> None:
        """Completely refreshes the table, repopulating it with the raw backing data, applying all sorts, filters, delimiters, etc."""
        self.locked = True
        data_table = self.query_one(NlessDataTable)
        cursor_x = data_table.cursor_column
        cursor_y = data_table.cursor_row
        scroll_x = data_table.scroll_x
        scroll_y = data_table.scroll_y

        curr_metadata_columns = {
            c.name
            for c in self.current_columns
            if c.name in [m.value for m in MetadataColumn]
        }
        expected_cell_count = len(self.current_columns) - len(curr_metadata_columns)
        data_table.clear(columns=True)

        data_table.fixed_columns = len(curr_metadata_columns)
        self._rebuild_column_caches()
        data_table.add_columns(self._get_visible_column_labels())

        self.search_matches = []
        self.current_match_index = -1
        self.count_by_column_key = defaultdict(lambda: 0)
        self._dedup_key_to_row_idx = {}
        self._sort_keys = []

        filtered_rows, rows_with_inconsistent_length = self._filter_rows(
            expected_cell_count
        )
        deduped_rows = self._dedup_rows(filtered_rows)
        self._sort_rows(deduped_rows)

        aligned_rows = self._align_cells_to_visible_columns(deduped_rows)
        styled_rows = self._highlight_search_matches(
            aligned_rows, data_table.fixed_columns
        )

        if len(rows_with_inconsistent_length) > 0:
            self.notify(
                f"{len(rows_with_inconsistent_length)} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        self.displayed_rows = styled_rows
        data_table.add_rows(styled_rows)
        self.locked = False

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

    def _rich_bold(self, text):
        return f"[bold]{text}[/bold]"

    def _update_status_bar(self) -> None:
        if self.pane_id != self.app.buffers[self.app.curr_buffer_idx].pane_id:
            return
        data_table = self.query_one(NlessDataTable)

        sort_prefix = self._rich_bold("Sort")
        filter_prefix = self._rich_bold("Filter")
        search_prefix = self._rich_bold("Search")

        if self.sort_column is None:
            sort_text = f"{sort_prefix}: None"
        else:
            sort_text = f"{sort_prefix}: {self.sort_column} {'desc' if self.sort_reverse else 'asc'}"

        if len(self.current_filters) == 0:
            filter_text = f"{filter_prefix}: None"
        else:
            filter_descriptions = []
            for f in self.current_filters:
                if f.column is None:
                    filter_descriptions.append(f"any='{f.pattern.pattern}'")
                else:
                    filter_descriptions.append(f"{f.column}='{f.pattern.pattern}'")
            filter_text = f"{filter_prefix}: " + ", ".join(filter_descriptions)

        if self.search_term is not None:
            search_text = f"{search_prefix}: '{self.search_term.pattern}' ({self.current_match_index + 1} / {len(self.search_matches)} matches)"
        else:
            search_text = f"{search_prefix}: None"

        total_rows = data_table.row_count
        total_cols = len(data_table.columns)
        current_row = data_table.cursor_row + 1  # Add 1 for 1-based indexing
        current_col = data_table.cursor_column + 1  # Add 1 for 1-based indexing

        row_prefix = self._rich_bold("Row")
        col_prefix = self._rich_bold("Col")
        position_text = f"{row_prefix}: {current_row}/{total_rows} {col_prefix}: {current_col}/{total_cols}"

        if self.is_tailing:
            tailing_text = "| " + self._rich_bold(
                "[#00bb00]Tailing (`t` to stop)[/#00bb00]"
            )
        else:
            tailing_text = ""

        column_text = ""
        if len(self.unique_column_names):
            column_names = ",".join(self.unique_column_names)
            column_text = f"| unique cols: ({column_names}) "

        status_bar = self.app.query_one("#status_bar", Static)
        status_bar.update(
            f"{sort_text} | {filter_text} | {search_text} | {position_text} {column_text}{tailing_text}"
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
        if not self.current_filters:
            return True
        filter_matches = 0
        for f in self.current_filters:
            if f.column is None:
                if any(
                    f.pattern.search(self._get_cell_value_without_markup(cell))
                    for cell in cells
                ):
                    filter_matches += 1
            else:
                col_idx = self._get_col_idx_by_name(f.column)
                if col_idx is None:
                    break
                if adjust_for_count and len(self.unique_column_names) > 0:
                    col_idx -= 1
                if f.pattern.search(
                    self._get_cell_value_without_markup(cells[col_idx])
                ):
                    filter_matches += 1
        return filter_matches == len(self.current_filters)

    _MARKUP_TAG_RE = re.compile(r"\[/?[^\]]*\]")

    def _get_cell_value_without_markup(self, cell_value) -> str:
        """Extract plain text from a cell value, removing any markup."""
        if "[" not in cell_value:
            return cell_value
        return self._MARKUP_TAG_RE.sub("", cell_value)

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

    def add_logs(self, log_lines: list[str]) -> None:
        self.locked = True
        data_table = self.query_one(NlessDataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            self.delimiter = infer_delimiter(log_lines[: min(5, len(log_lines))])
            self.delimiter_inferred = True

        if not self.first_row_parsed:
            self.first_log_line = log_lines[0]
            parts = self._parse_first_line_columns(self.first_log_line)
            self.current_columns = self._make_columns(parts)
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

        filtered = self._filter_lines(log_lines)
        self.raw_rows.extend(filtered)

        mismatch_count = 0

        if len(filtered) > 1000:
            self._update_table()
        else:
            for line in filtered:
                try:
                    self._add_log_line(line)
                except RowLengthMismatchError:
                    mismatch_count += 1
                    continue
                except (json.JSONDecodeError, csv.Error, ValueError, IndexError):
                    pass

        if mismatch_count > 0:
            self.notify(
                f"{mismatch_count} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        self._update_status_bar()
        self.locked = False

    def str_to_int(self, value: Any) -> int | float | str:
        if isinstance(value, int):
            return value
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
        return value

    @staticmethod
    def _coerce_sort_key(value: str) -> int | float | str:
        """Coerce a string to numeric if possible, for sort comparison."""
        try:
            return int(value)
        except (ValueError, TypeError):
            pass
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
        return value

    def _rebuild_column_caches(self) -> None:
        """Rebuild all column-derived caches. Call when columns change."""
        self._col_data_idx = {}
        self._col_render_idx = {}
        for col in self.current_columns:
            plain = self._get_cell_value_without_markup(col.name)
            self._col_data_idx[plain] = col.data_position
            self._col_render_idx[plain] = col.render_position
        self._sorted_visible_columns = sorted(
            [c for c in self.current_columns if not c.hidden],
            key=lambda c: c.render_position,
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
        parts = []
        for col_name in self.unique_column_names:
            col_idx = self._get_col_idx_by_name(
                col_name, render_position=render_position
            )
            if col_idx is None:
                continue
            parts.append(self._get_cell_value_without_markup(cells[col_idx]))
        return ",".join(parts)

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
            new_cells = []
            for col_idx, cell in enumerate(cells):
                if col_idx == 0:
                    self.count_by_column_key[new_key] += 1
                    cell = self.count_by_column_key[new_key]
                else:
                    cell = self._get_cell_value_without_markup(cell)
                new_cells.append(f"[#00ff00]{cell}[/#00ff00]")
            return new_cells, row_idx, self.displayed_rows[row_idx]

        self.count_by_column_key[new_key] = 1
        return cells, None, None

    def _find_sorted_insert_index(self, cells: list[str]) -> int:
        """Find the insertion index for a row based on the current sort."""
        if self.sort_column is None:
            return len(self.displayed_rows)

        data_sort_col_idx = self._get_col_idx_by_name(
            self.sort_column, render_position=False
        )

        raw_key = self._get_cell_value_without_markup(str(cells[data_sort_col_idx]))
        sort_key = self._coerce_sort_key(raw_key)

        # _sort_keys is maintained in ascending order; for reverse sort,
        # we need to find the insertion point from the end
        idx = bisect.bisect_left(self._sort_keys, sort_key)
        if self.sort_reverse:
            return len(self._sort_keys) - idx
        return idx

    def _update_dedup_indices_after_removal(self, old_index: int) -> None:
        """Shift dedup index entries down after a row removal."""
        for k, idx in self._dedup_key_to_row_idx.items():
            if idx > old_index:
                self._dedup_key_to_row_idx[k] = idx - 1

    def _update_dedup_indices_after_insertion(
        self, dedup_key: str, new_index: int
    ) -> None:
        """Shift dedup index entries up after a row insertion, then record the new key."""
        for k, idx in self._dedup_key_to_row_idx.items():
            if idx >= new_index:
                self._dedup_key_to_row_idx[k] = idx + 1
        self._dedup_key_to_row_idx[dedup_key] = new_index

    def _update_sort_keys_for_line(
        self, log_line: str, new_index: int, old_index: int | None
    ) -> None:
        """Update the incremental sort keys list after insertion/removal."""
        if self.sort_column is None:
            return
        if old_index is not None and old_index < len(self._sort_keys):
            self._sort_keys.pop(old_index)
        data_sort_col_idx = self._get_col_idx_by_name(
            self.sort_column, render_position=False
        )
        if data_sort_col_idx is not None:
            raw_key = self._get_cell_value_without_markup(
                str(
                    split_line(log_line, self.delimiter, self.current_columns)[
                        data_sort_col_idx
                    ]
                )
            )
            bisect.insort_left(self._sort_keys, self._coerce_sort_key(raw_key))

    def _add_log_line(self, log_line: str):
        """Adds a single log line, applying filters, dedup, sort, and search highlighting."""
        data_table = self.query_one(NlessDataTable)
        cells = split_line(log_line, self.delimiter, self.current_columns)
        if self.unique_column_names:
            cells.insert(0, "1")

        if len(cells) != len(self.current_columns):
            raise RowLengthMismatchError()

        try:
            self._align_cells_to_visible_columns([cells])[0]
        except (IndexError, KeyError):
            raise RowLengthMismatchError()

        if not self._matches_all_filters(cells):
            return

        cells, old_index, old_row = self._handle_dedup_for_line(cells)
        new_index = self._find_sorted_insert_index(cells)

        cells = self._align_cells_to_visible_columns([cells])[0]
        cells = self._highlight_search_matches(
            [cells], data_table.fixed_columns, row_offset=new_index
        )[0]

        if old_index is not None:
            self._update_dedup_indices_after_removal(old_index)
            self.displayed_rows.remove(old_row)
            data_table.remove_row(old_index)

        data_table.add_row_at(index=new_index, row_data=cells)
        self.displayed_rows.insert(new_index, cells)

        self._update_sort_keys_for_line(log_line, new_index, old_index)

        if self.unique_column_names:
            dedup_key = self._build_composite_key(cells, render_position=True)
            self._update_dedup_indices_after_insertion(dedup_key, new_index)

        if self.is_tailing:
            data_table.action_scroll_bottom()
