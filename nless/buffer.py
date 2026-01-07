import bisect
import csv
import json
import re
import time
from collections import defaultdict
from copy import deepcopy
from typing import Any, List, Optional

import pyperclip
from rich.markup import _parse
from rich.text import Text
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

    def __init__(self, unparsed_rows: List[str], delimiter: str):
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
        self.current_filters: List[Filter] = cli_args.filters if cli_args else []
        self.search_term = None
        if cli_args and cli_args.sort_by:
            sort_column, direction = cli_args.sort_by.split("=")
            self.sort_column = sort_column
            self.sort_reverse = direction.lower() == "desc"
        else:
            self.sort_column = None
            self.sort_reverse = False
        self.search_matches: List[Coordinate] = []
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

    def action_copy(self) -> None:
        """Copy the contents of the currently highlighted cell to the clipboard."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = self._get_cell_value_without_markup(cell_value)
            pyperclip.copy(cell_value)
            self.notify("Cell contents copied to clipboard.", severity="info")
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def copy(self, pane_id) -> "NlessBuffer":
        new_buffer = NlessBuffer(pane_id=pane_id, cli_args=None)
        new_buffer.mounted = self.mounted
        new_buffer.first_row_parsed = self.first_row_parsed
        new_buffer.raw_rows = deepcopy(self.raw_rows)
        new_buffer.displayed_rows = deepcopy(self.displayed_rows)
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
            self.line_stream.subscribe(
                new_buffer,
                new_buffer.add_logs,
                lambda: not new_buffer.locked and new_buffer.mounted,
            )
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
        selected_column = [
            c
            for c in self.current_columns
            if c.render_position == current_cursor_column
        ]
        if not selected_column:
            self.notify("No column selected to move", severity="error")
            return
        selected_column = selected_column[0]
        if selected_column.name in [m.value for m in MetadataColumn]:
            return  # can't move metadata columns
        if (
            direction == 1
            and selected_column.render_position == len(self.current_columns) - 1
        ) or (direction == -1 and selected_column.render_position == 0):
            return  # can't move further in that direction

        adjacent_column = [
            c
            for c in self.current_columns
            if c.render_position == selected_column.render_position + direction
        ]
        if not adjacent_column or adjacent_column[0].name in [
            m.value for m in MetadataColumn
        ]:  # can't move past metadata columns
            return
        adjacent_column = adjacent_column[0]

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
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def _perform_search(self, search_term: Optional[str]) -> None:
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
        selected_column = [
            c
            for c in self.current_columns
            if c.render_position == current_cursor_column
        ]
        if not selected_column:
            self.notify("No column selected for sorting", severity="error")
            return
        else:
            selected_column = selected_column[0]

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

    def _get_label(self, label: Text | str) -> str:
        if isinstance(label, Text):
            return label.plain
        else:
            return label

    def _get_visible_column_labels(self) -> List[str]:
        labels = []
        for col in sorted(self.current_columns, key=lambda c: c.render_position):
            if not col.hidden:
                labels.append(f"{col.name} {' '.join(col.labels)}".strip())
        return labels

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
        data_table.clear(
            columns=True
        )  # might be needed to trigger column resizing with longer cell content

        data_table.fixed_columns = len(curr_metadata_columns)
        data_table.add_columns(self._get_visible_column_labels())

        self.search_matches = []
        self.current_match_index = -1
        self.count_by_column_key = defaultdict(lambda: 0)

        # 1. Filter rows
        filtered_rows = []
        rows_with_inconsistent_length = []
        if len(self.current_filters) > 0:
            for row_str in self.raw_rows:
                cells = split_line(row_str, self.delimiter, self.current_columns)
                if len(cells) != expected_cell_count:
                    rows_with_inconsistent_length.append(cells)
                    continue

                filter_matches = []

                for filter in self.current_filters:
                    if (
                        filter.column is None
                    ):  # If we have a current_filter, but filter_column is None, we are searching all columns
                        if any(
                            filter.pattern.search(
                                self._get_cell_value_without_markup(cell)
                            )
                            for cell in cells
                        ):
                            filter_matches.append(True)
                    else:
                        col_idx = self._get_col_idx_by_name(filter.column)
                        if col_idx is None:
                            break
                        if (
                            len(self.unique_column_names) > 0
                        ):  # account for count column
                            col_idx -= 1
                        if filter.pattern.search(
                            self._get_cell_value_without_markup(cells[col_idx])
                        ):
                            filter_matches.append(True)
                if len(filter_matches) == len(self.current_filters):
                    filtered_rows.append(cells)
        else:
            for i, row in enumerate(self.raw_rows):
                try:
                    cells = split_line(row, self.delimiter, self.current_columns)
                    if len(cells) == expected_cell_count:
                        filtered_rows.append(cells)
                    else:
                        rows_with_inconsistent_length.append(row)
                except Exception as e:
                    print(f"Error parsing row: {e}")

        # 2. Dedup by composite column key
        if len(self.unique_column_names) > 0:
            dedup_map = {}
            deduped_rows = []
            for cells in filtered_rows:
                composite_key = []
                for col_name in self.unique_column_names:
                    col_idx = self._get_col_idx_by_name(col_name)
                    if col_idx is None:
                        continue
                    if len(self.unique_column_names) > 0:  # account for count column
                        col_idx -= 1
                    composite_key.append(
                        self._get_cell_value_without_markup(cells[col_idx])
                    )
                composite_key = ",".join(composite_key)
                dedup_map[composite_key] = cells  # always overwrite to keep latest
                self.count_by_column_key[composite_key] += 1
            for k, cells in dedup_map.items():
                count = self.count_by_column_key[k]
                cells.insert(0, str(count))
                deduped_rows.append(cells)
        else:
            deduped_rows = filtered_rows

        # 3. Sort rows
        if self.sort_column is not None:
            sort_column_idx = self._get_col_idx_by_name(self.sort_column)
            if sort_column_idx is not None:
                try:
                    deduped_rows.sort(
                        key=lambda r: self.str_to_int(r[sort_column_idx]),
                        reverse=self.sort_reverse,
                    )
                except (ValueError, IndexError):
                    # Fallback if column not found or row is malformed
                    pass
                except:
                    try:
                        deduped_rows.sort(
                            key=lambda r: r[sort_column_idx],
                            reverse=self.sort_reverse,
                        )
                    except:
                        pass

        aligned_rows = self._align_cells_to_visible_columns(deduped_rows)
        unstyled_rows = []

        # 4. Add to table and find search matches
        if self.search_term:
            for displayed_row_idx, cells in enumerate(aligned_rows):
                highlighted_cells = []
                for col_idx, cell in enumerate(cells):
                    if (
                        isinstance(self.search_term, re.Pattern)
                        and self.search_term.search(str(cell))
                        and col_idx > data_table.fixed_columns - 1
                    ):
                        cell = re.sub(
                            self.search_term,
                            lambda m: f"[reverse]{m.group(0)}[/reverse]",
                            cell,
                        )
                        highlighted_cells.append(cell)
                        self.search_matches.append(
                            Coordinate(displayed_row_idx, col_idx)
                        )
                    else:
                        highlighted_cells.append(cell)

                unstyled_rows.append(highlighted_cells)
        else:
            for cells in aligned_rows:
                unstyled_rows.append(cells)

        if len(rows_with_inconsistent_length) > 0:
            self.notify(
                f"{len(rows_with_inconsistent_length)} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        self.displayed_rows = unstyled_rows
        data_table.add_rows(unstyled_rows)
        self.locked = False

        if restore_position:
            self._restore_position(data_table, cursor_x, cursor_y, scroll_x, scroll_y)

    def _align_cells_to_visible_columns(self, rows: list[list[str]]) -> list[list[str]]:
        new_rows = []
        for row in rows:
            cells = []
            for col in sorted(self.current_columns, key=lambda c: c.render_position):
                if not col.hidden:
                    cells.append(row[col.data_position])
            new_rows.append(cells)
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

    def _get_cell_value_without_markup(self, cell_value) -> str:
        """Extract plain text from a cell value, removing any markup."""
        parsed_value = [*_parse(cell_value)]
        if len(parsed_value) > 1:
            return "".join([res[1] for res in parsed_value if res[1]])
        return cell_value

    def add_logs(self, log_lines: list[str]) -> None:
        # print stack trace for debugging
        self.locked = True
        data_table = self.query_one(NlessDataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            self.delimiter = infer_delimiter(log_lines[: min(5, len(log_lines))])
            self.delimiter_inferred = True

        if not self.first_row_parsed:
            first_log_line = log_lines[0]
            self.first_log_line = first_log_line
            if self.delimiter == "raw":
                # Delimiter is raw, treat entire line as single column
                data_table.add_columns(["log"])
                self.current_columns = [
                    Column(
                        name="log",
                        labels=set(),
                        render_position=0,
                        data_position=0,
                        hidden=False,
                    )
                ]
            elif isinstance(self.delimiter, re.Pattern):
                pattern = self.delimiter
                parts = list(pattern.groupindex.keys())
                data_table.add_columns(parts)
                self.current_columns = [
                    Column(
                        name=p,
                        labels=set(),
                        render_position=i,
                        data_position=i,
                        hidden=False,
                    )
                    for i, p in enumerate(parts)
                ]
            elif self.delimiter == "json":
                try:
                    json_data = json.loads(first_log_line)
                    if isinstance(json_data, dict):
                        parts = list(json_data.keys())
                    elif isinstance(json_data, list) and len(json_data) > 0:
                        parts = [i for i in range(len(json_data))]
                    else:
                        parts = ["value"]
                except json.JSONDecodeError:
                    parts = ["value"]
                data_table.add_columns(parts)
                self.current_columns = [
                    Column(
                        name=p,
                        labels=set(),
                        render_position=i,
                        data_position=i,
                        hidden=False,
                    )
                    for i, p in enumerate(parts)
                ]
            else:
                parts = split_line(first_log_line, self.delimiter, self.current_columns)
                data_table.add_columns(parts)
                self.current_columns = [
                    Column(
                        name=p,
                        labels=set(),
                        render_position=i,
                        data_position=i,
                        hidden=False,
                    )
                    for i, p in enumerate(parts)
                ]
                log_lines = log_lines[1:]  # Exclude header line

            if len(self.unique_column_names) > 0:
                for unique_col_name in self.unique_column_names:
                    handle_mark_unique(self, unique_col_name)
                data_table.clear(columns=True)
                data_table.add_columns(self._get_visible_column_labels())

            self.first_row_parsed = True

        self.raw_rows.extend(log_lines)

        mismatch_count = 0

        if len(log_lines) > 1000:
            self._update_table()
        else:
            for line in log_lines:
                try:
                    self._add_log_line(line)
                except RowLengthMismatchError:
                    mismatch_count += 1
                    continue
                except Exception:
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
        except:
            pass
        return value

    def _bisect_left(self, r_list: list[str], value: str, reverse: bool):
        tmp_list = list(r_list)
        if value.isnumeric():
            value = int(value)
            tmp_list = [int(v) for v in tmp_list]
        tmp_list.sort()
        if reverse:
            idx_in_temp = bisect.bisect_left(tmp_list, value)
            return len(tmp_list) - idx_in_temp
        else:
            return bisect.bisect_left(tmp_list, value)

    def _strip_column_indicators(self, col_name: str) -> str:
        return col_name.replace(" U", "").replace(" ▲", "").replace(" ▼", "")

    def _get_col_idx_by_name(
        self, col_name: str, render_position: bool = False
    ) -> Optional[int]:
        for col in self.current_columns:
            if self._get_cell_value_without_markup(col.name) == col_name:
                if render_position:
                    return col.render_position
                else:
                    return col.data_position
        return None

    def _add_log_line(self, log_line: str):
        """
        Adds a single log line by determining:
        1. if it should be displayed (based on filters)
        2. if it should be highlighted (based on current search term)
        3. where it should go, based off current sort
        """
        data_table = self.query_one(NlessDataTable)
        cells = split_line(log_line, self.delimiter, self.current_columns)
        if len(self.unique_column_names) > 0:
            cells.insert(0, "1")

        expected_cell_count = len([c for c in self.current_columns])
        if len(cells) != expected_cell_count:
            raise RowLengthMismatchError()

        try:
            aligned_cells = self._align_cells_to_visible_columns([cells])[0]
        except:
            raise RowLengthMismatchError()

        if len(self.current_filters) > 0:
            filter_matches = []
            for filter in self.current_filters:
                if filter.column is None:
                    # We're filtering any column
                    if any(
                        filter.pattern.search(self._get_cell_value_without_markup(cell))
                        for cell in cells
                    ):
                        filter_matches.append(True)
                else:
                    col_idx = self._get_col_idx_by_name(filter.column)
                    if col_idx is None:
                        return
                    if filter.pattern.search(
                        self._get_cell_value_without_markup(cells[col_idx])
                    ):
                        filter_matches.append(True)

            if len(filter_matches) != len(self.current_filters):
                return

        old_index = None
        old_row = None
        if len(self.unique_column_names) > 0:
            new_row_composite_key = []
            for col_name in self.unique_column_names:
                col_idx = self._get_col_idx_by_name(col_name)
                if col_idx is None:
                    continue
                new_row_composite_key.append(
                    self._get_cell_value_without_markup(cells[col_idx])
                )
            new_row_composite_key = ",".join(new_row_composite_key)

            for row_idx, row in enumerate(self.displayed_rows):
                composite_key = []
                for col_name in self.unique_column_names:
                    col_idx = self._get_col_idx_by_name(col_name, render_position=True)
                    if col_idx is None:
                        continue
                    composite_key.append(
                        self._get_cell_value_without_markup(row[col_idx])
                    )
                composite_key = ",".join(composite_key)

                if composite_key == new_row_composite_key:
                    new_cells = []
                    for col_idx, cell in enumerate(cells):
                        if col_idx == 0:
                            self.count_by_column_key[composite_key] += 1
                            cell = self.count_by_column_key[composite_key]
                        else:
                            cell = self._get_cell_value_without_markup(cell)
                        new_cells.append(f"[#00ff00]{cell}[/#00ff00]")
                    old_index = row_idx
                    cells = new_cells
                    old_row = self.displayed_rows[old_index]
                    break

            if old_index is None:
                self.count_by_column_key[new_row_composite_key] = 1

        if self.sort_column is not None:
            displayed_sort_column_idx = self._get_col_idx_by_name(
                self.sort_column, render_position=True
            )
            data_sort_column_idx = self._get_col_idx_by_name(
                self.sort_column, render_position=False
            )

            sort_key = self._get_cell_value_without_markup(
                str(cells[data_sort_column_idx])
            )
            displayed_row_keys = [
                self._get_cell_value_without_markup(str(r[displayed_sort_column_idx]))
                for r in self.displayed_rows
            ]
            if self.sort_reverse:
                new_index = self._bisect_left(
                    displayed_row_keys, sort_key, reverse=True
                )
            else:
                new_index = self._bisect_left(
                    displayed_row_keys, sort_key, reverse=False
                )
        else:
            new_index = len(self.displayed_rows)

        cells = self._align_cells_to_visible_columns([cells])[0]

        if self.search_term:
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if (
                    isinstance(self.search_term, re.Pattern)
                    and self.search_term.search(cell)
                    and col_idx > data_table.fixed_columns - 1
                ):
                    cell = re.sub(
                        self.search_term,
                        lambda m: f"[reverse]{m.group(0)}[/reverse]",
                        cell,
                    )
                    highlighted_cells.append(cell)
                    self.search_matches.append(Coordinate(new_index, col_idx))
                else:
                    highlighted_cells.append(cell)
            cells = highlighted_cells

        if old_index is not None:
            self.displayed_rows.remove(old_row)
            data_table.remove_row(old_index)

        data_table.add_row_at(index=new_index, row_data=cells)
        self.displayed_rows.insert(new_index, cells)

        if self.is_tailing:
            data_table.action_scroll_bottom()
