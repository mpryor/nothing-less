import argparse
import bisect
from dataclasses import dataclass
import re
import sys
from collections import defaultdict
from threading import Thread
from typing import List, Optional

from rich.markup import _parse
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Grid, Vertical
from textual.coordinate import Coordinate
from textual.events import Key
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Input, Select, Static

from .delimiter import infer_delimiter, split_line
from .help import HelpScreen
from .input import InputConsumer
from .nlesstable import NlessDataTable


from enum import Enum


@dataclass
class Column:
    name: str
    labels: set[str]
    render_position: int
    data_position: int
    hidden: bool
    pinned: bool = False


class MetadataColumn(Enum):
    COUNT = "count"


class NlessApp(App):
    """A modern pager with tabular data sorting/filtering capabilities."""

    ENABLE_COMMAND_PALETTE = False
    CSS_PATH = "nless.tcss"
    SCREENS = {"HelpScreen": HelpScreen}

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("C", "filter_columns", "Filter Columns (by prompt)"),
        ("c", "show_columns", "Select Columns (by prompt)"),
        (">", "move_column_right", "Move column right"),
        ("<", "move_column_left", "Move column left"),
        ("D", "delimiter", "Change Delimiter"),
        ("s", "sort", "Sort selected column"),
        ("f", "filter", "Filter selected column (by prompt)"),
        ("|", "filter_any", "Filter any column (by prompt)"),
        ("F", "filter_cursor_word", "Filter selected column by word under cursor"),
        ("/", "search", "Search (all columns, by prompt)"),
        ("&", "search_to_filter", "Apply current search as filter"),
        ("n", "next_search", "Next search result"),
        ("p,N", "previous_search", "Previous search result"),
        ("*", "search_cursor_word", "Search (all columns) for word under cursor"),
        ("?", "push_screen('HelpScreen')", "Show Help"),
        ("v", "change_cursor", "Change cursor type - row, column, cell"),
        (
            "U",
            "mark_unique",
            "Mark a column unique to create a composite key for distinct/analysis",
        ),
        (
            "t",
            "toggle_tail",
            "Keep cursor at the bottom of the screen even as new logs arrive.",
        ),
    ]

    def __init__(self):
        super().__init__()
        self.mounted = False
        self.first_row_parsed = False
        self.raw_rows = []
        self.displayed_rows = []
        self.first_log_line = ""  # used to determine columns when delimiter is set
        self.current_columns: list[Column] = []
        self.current_filter = None
        self.filter_column_name: str | None = None
        self.search_term = None
        self.sort_column = None
        self.sort_reverse = False
        self.search_matches: List[Coordinate] = []
        self.current_match_index: int = -1
        self.delimiter = None
        self.delimiter_inferred = False
        self.is_tailing = False
        self.unique_column_names = set()
        self.count_by_column_key = defaultdict(lambda: 0)

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        table = NlessDataTable(
            zebra_stripes=True, id="data_table", show_row_labels=True
        )
        yield table
        with Vertical(id="bottom-container"):
            yield Static(
                "Sort: None | Filter: None | Search: None",
                classes="bd",
                id="status_bar",
            )

    def on_mount(self) -> None:
        self.mounted = True

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_input":
            self.handle_search_submitted(event)
        elif event.input.id == "filter_input" or event.input.id == "filter_input_any":
            self.handle_filter_submitted(event)
        elif event.input.id == "delimiter_input":
            self.handle_delimiter_submitted(event)
        elif event.input.id == "column_filter_input":
            self.handle_column_filter_submitted(event)

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if event.key == "escape" and isinstance(self.focused, Input):
            self.focused.remove()

    def on_data_table_cell_highlighted(
        self, event: NlessDataTable.CellHighlighted
    ) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def _open_select(self, select: Select) -> None:
        select.focus()
        select.expanded = True

    def action_show_columns(self) -> None:
        """Show columns by user input."""
        column_options = [
            (self._get_cell_value_without_markup(c.name), c.render_position)
            for c in self.current_columns
            if not c.hidden
        ]
        select = Select(
            options=column_options,
            classes="bottom-input",
            prompt="Type a column to jump to",
        )
        self.mount(select)
        self.call_after_refresh(lambda: self._open_select(select))

    def on_select_changed(self, event: Select.Changed) -> None:
        col_index = event.value
        event.control.remove()
        data_table = self.query_one(NlessDataTable)
        data_table.move_cursor(column=col_index)

    def action_filter_columns(self) -> None:
        """Filter columns by user input."""
        self._create_prompt(
            "Type pipe delimited column names to show (e.g. col1|col2) or 'all' to reset",
            "column_filter_input",
        )

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

    def action_mark_unique(self) -> None:
        data_table = self.query_one(NlessDataTable)
        current_cursor_column = data_table.cursor_column
        new_unique_column = [
            c
            for c in self.current_columns
            if c.render_position == current_cursor_column
        ]
        if not new_unique_column:
            self.notify("No column selected to mark as unique")
            return

        new_unique_column = new_unique_column[0]
        new_unique_column_name = self._get_cell_value_without_markup(
            new_unique_column.name
        )

        if new_unique_column_name in [mc.value for mc in MetadataColumn]:
            # can't toggle count column
            return

        self.count_by_column_key = defaultdict(lambda: 0)

        if new_unique_column_name in self.unique_column_names:
            self.unique_column_names.remove(new_unique_column_name)
            new_unique_column.labels.discard("U")
        else:
            self.unique_column_names.add(new_unique_column_name)
            new_unique_column.labels.add("U")

        if len(self.unique_column_names) == 0:
            # remove count column
            self.current_columns = [
                Column(
                    name=c.name,
                    labels=c.labels,
                    render_position=c.render_position - 1,
                    data_position=c.data_position - 1,
                    hidden=c.hidden,
                )
                for c in self.current_columns
                if c.name != MetadataColumn.COUNT.value
            ]
        elif MetadataColumn.COUNT.value not in [c.name for c in self.current_columns]:
            # add count column at the start
            self.current_columns = [
                Column(
                    name=c.name,
                    labels=c.labels,
                    render_position=c.render_position + 1,
                    data_position=c.data_position + 1,
                    hidden=c.hidden,
                )
                for c in self.current_columns
            ]
            self.current_columns.insert(
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
            [c for c in self.current_columns if c.pinned and not c.hidden]
        )
        if new_unique_column_name in self.unique_column_names:
            old_position = new_unique_column.render_position
            for col in self.current_columns:
                col_name = self._get_cell_value_without_markup(col.name)
                if col_name == new_unique_column_name:
                    col.render_position = pinned_columns_visible  # bubble to just after last pinned column
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
            for col in self.current_columns:
                col_name = self._get_cell_value_without_markup(col.name)
                if col_name == new_unique_column_name:
                    col.pinned = False
                    col.render_position = (
                        pinned_columns_visible if pinned_columns_visible > 0 else 0
                    )
                elif col.pinned and col.render_position >= old_position:
                    col.render_position -= 1

        self._update_table()

        # update the cursor position's index to match the new position of the column
        new_cursor_position = 0
        for i, col in enumerate(
            sorted(self.current_columns, key=lambda c: c.render_position)
        ):
            if self._get_cell_value_without_markup(col.name) == new_unique_column_name:
                new_cursor_position = i
                break
        self.call_after_refresh(
            lambda: data_table.move_cursor(column=new_cursor_position)
        )

    def action_toggle_tail(self) -> None:
        self.is_tailing = not self.is_tailing
        self._update_status_bar()

    def action_change_cursor(self) -> None:
        data_table = self.query_one(NlessDataTable)
        if data_table.cursor_type == "cell":
            data_table.cursor_type = "column"
        elif data_table.cursor_type == "column":
            data_table.cursor_type = "row"
        else:
            data_table.cursor_type = "cell"

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

    def action_delimiter(self) -> None:
        """Change the delimiter used for parsing."""
        self._create_prompt(
            "Type delimiter character (e.g. ',', '\\t', ' ', '|') or 'raw' for no parsing",
            "delimiter_input",
        )

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        self._create_prompt("Type search term and press Enter", "search_input")

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = self._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            selected_column = [
                c
                for c in self.current_columns
                if c.render_position == coordinate.column
            ]
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                self._get_cell_value_without_markup(selected_column[0].name),
            )
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

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

    def action_search_to_filter(self) -> None:
        """Convert current search into a filter across all columns."""
        if not self.search_term:
            self.notify("No active search to convert to filter", severity="warning")
            return

        self.current_filter = self.search_term  # Reuse the compiled regex
        self.filter_column_name = None  # Filter across all columns
        self._update_table()

    def action_filter_any(self) -> None:
        """Filter any column based on user input."""
        self._create_prompt(
            "Type filter text to match across all columns", "filter_input_any"
        )

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        data_table = self.query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.ordered_columns[column_index].label
        self._create_prompt(
            f"Type filter text for column: {column_label} and press enter",
            "filter_input",
        )

    def handle_search_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        self._perform_search(input_value)

    def handle_filter_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        data_table = self.query_one(NlessDataTable)

        if event.input.id == "filter_input_any":
            self._perform_filter_any(filter_value)
        else:
            column_index = data_table.cursor_column
            column_label = [
                c for c in self.current_columns if c.render_position == column_index
            ]
            if not column_label:
                self.notify("No column selected for filtering")
                return
            column_label = self._get_cell_value_without_markup(column_label[0].name)
            self._perform_filter(filter_value, column_label)

    def handle_delimiter_submitted(self, event: Input.Submitted) -> None:
        self.current_filter = None
        self.filter_column_name = None
        self.search_term = None
        self.sort_column = None
        self.unique_column_names = set()
        prev_delimiter = self.delimiter

        event.input.remove()
        data_table = self.query_one(NlessDataTable)
        self.delimiter_inferred = False
        delimiter = event.value
        if delimiter not in [
            ",",
            "\\t",
            " ",
            "  ",
            "|",
            ";",
            "raw",
        ]:  # if our delimiter is not one of the common ones, treat it as a regex
            try:
                pattern = re.compile(rf"{delimiter}")  # Validate regex
                self.delimiter = pattern
                data_table.clear(columns=True)
                data_table.add_columns(*list(pattern.groupindex.keys()))
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
                    self.raw_rows.insert(0, self.first_log_line)
                self._update_table()
                return
            except:
                self.notify("Invalid delimiter", severity="error")
                return

        if delimiter == "\\t":
            delimiter = "\t"

        self.delimiter = delimiter

        if delimiter == "raw":
            new_header = ["log"]
        elif prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern):
            new_header = split_line(self.raw_rows[0], self.delimiter)
            self.raw_rows.pop(0)
        else:
            new_header = split_line(self.first_log_line, self.delimiter)

        if (
            (prev_delimiter != delimiter)
            and (prev_delimiter != "raw" and not isinstance(prev_delimiter, re.Pattern))
            and (delimiter == "raw" or isinstance(delimiter, re.Pattern))
        ):
            self.raw_rows.insert(0, self.first_log_line)

        self.current_columns = [
            Column(
                name=h, labels=set(), render_position=i, data_position=i, hidden=False
            )
            for i, h in enumerate(new_header)
        ]
        self._update_table()

    def handle_column_filter_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        if input_value.lower() == "all":
            for col in self.current_columns:
                col.hidden = False
        else:
            column_name_filters = [name.strip() for name in input_value.split("|")]
            column_name_filter_regexes = [
                re.compile(rf"{name}", re.IGNORECASE) for name in column_name_filters
            ]
            metadata_columns = [mc.value for mc in MetadataColumn]
            visible_pinned_columns = [
                col for col in self.current_columns if col.pinned and not col.hidden
            ]
            for col in self.current_columns:
                matched = False
                plain_name = self._get_cell_value_without_markup(col.name)
                for i, column_name_filter in enumerate(column_name_filter_regexes):
                    if column_name_filter.search(plain_name) and not col.pinned:
                        col.hidden = False
                        col.render_position = i + len(
                            visible_pinned_columns
                        )  # keep metadata columns at the start
                        matched = True
                        break

                if matched:
                    continue

                if (
                    col.name not in [mc.value for mc in MetadataColumn]
                    and not col.pinned
                ):
                    col.hidden = True
                    col.render_position = 99999

            # Ensure at least one column is visible
            if all(col.hidden for col in self.current_columns):
                self.notify("At least one column must be visible.", severity="warning")
                for col in self.current_columns:
                    col.hidden = False

        sorted_columns = sorted(self.current_columns, key=lambda c: c.render_position)
        for i, col in enumerate(sorted_columns):
            col.render_position = i

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
        data_table.add_columns(*self._get_visible_column_labels())

        self.search_matches = []
        self.current_match_index = -1
        self.count_by_column_key = defaultdict(lambda: 0)

        # 1. Filter rows
        filtered_rows = []
        rows_with_inconsistent_length = []
        if self.current_filter:
            for row_str in self.raw_rows:
                cells = split_line(row_str, self.delimiter)
                if len(cells) != expected_cell_count:
                    rows_with_inconsistent_length.append(cells)
                    continue
                if (
                    self.filter_column_name is None
                ):  # If we have a current_filter, but filter_column is None, we are searching all columns
                    if any(
                        self.current_filter.search(
                            self._get_cell_value_without_markup(cell)
                        )
                        for cell in cells
                    ):
                        filtered_rows.append(cells)
                else:
                    col_idx = self._get_col_idx_by_name(self.filter_column_name)
                    if col_idx is None:
                        break
                    if len(self.unique_column_names) > 0:  # account for count column
                        col_idx -= 1
                    if self.current_filter.search(
                        self._get_cell_value_without_markup(cells[col_idx])
                    ):
                        filtered_rows.append(cells)
        else:
            for row in self.raw_rows:
                cells = split_line(row, self.delimiter)
                if len(cells) == expected_cell_count:
                    filtered_rows.append(cells)
                else:
                    rows_with_inconsistent_length.append(row)

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
                cells.insert(0, count)
                deduped_rows.append(cells)
        else:
            deduped_rows = filtered_rows

        # 3. Sort rows
        if self.sort_column is not None:
            sort_column_idx = self._get_col_idx_by_name(self.sort_column)
            if sort_column_idx is not None:
                try:
                    deduped_rows.sort(
                        key=lambda r: r[sort_column_idx],
                        reverse=self.sort_reverse,
                    )
                except (ValueError, IndexError):
                    # Fallback if column not found or row is malformed
                    pass

        final_rows = []

        # 4. Add to table and find search matches
        if self.search_term:
            for displayed_row_idx, cells in enumerate(deduped_rows):
                highlighted_cells = []
                for col_idx, cell in enumerate(cells):
                    if isinstance(
                        self.search_term, re.Pattern
                    ) and self.search_term.search(str(cell)):
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

                final_rows.append(highlighted_cells)
        else:
            for cells in deduped_rows:
                final_rows.append(cells)

        if len(rows_with_inconsistent_length) > 0:
            self.notify(
                f"{len(rows_with_inconsistent_length)} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        final_rows = self._align_cells_to_visible_columns(final_rows)
        self.displayed_rows = final_rows
        data_table.add_rows(final_rows)
        if restore_position:
            self.call_after_refresh(
                lambda: self._restore_position(
                    data_table, cursor_x, cursor_y, scroll_x, scroll_y
                )
            )

    def _align_cells_to_visible_columns(self, rows: list[list[str]]) -> list[list[str]]:
        new_rows = []
        for row in rows:
            cells = []
            for col in sorted(self.current_columns, key=lambda c: c.render_position):
                if not col.hidden:
                    cells.append(row[col.data_position])
            new_rows.append(cells)
        return new_rows

    def _restore_position(self, data_table, cursor_x, cursor_y, scroll_x, scroll_y):
        data_table.move_cursor(
            row=cursor_y, column=cursor_x, animate=False, scroll=False
        )
        self.call_after_refresh(
            lambda: data_table.scroll_to(
                scroll_x, scroll_y, animate=False, immediate=True
            )
        )

    def _rich_bold(self, text):
        return f"[bold]{text}[/bold]"

    def _update_status_bar(self) -> None:
        data_table = self.query_one(NlessDataTable)

        sort_prefix = self._rich_bold("Sort")
        filter_prefix = self._rich_bold("Filter")
        search_prefix = self._rich_bold("Search")

        if self.sort_column is None:
            sort_text = f"{sort_prefix}: None"
        else:
            sort_text = f"{sort_prefix}: {self.sort_column} {'desc' if self.sort_reverse else 'asc'}"

        if self.current_filter is None:
            filter_text = f"{filter_prefix}: None"
        elif self.filter_column_name is None:
            filter_text = f"{filter_prefix}: Any Column='{self.current_filter.pattern}'"
        else:
            filter_text = f"{filter_prefix}: {self.filter_column_name}='{self.current_filter.pattern}'"

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

        status_bar = self.query_one("#status_bar", Static)
        status_bar.update(
            f"{sort_text} | {filter_text} | {search_text} | {position_text} {column_text}{tailing_text}"
        )

    def _perform_filter_any(self, filter_value: Optional[str]) -> None:
        """Performs a filter across all columns and updates the table."""
        if not filter_value:
            self.current_filter = None
            self.filter_column_name = None
        else:
            try:
                self.current_filter = re.compile(filter_value, re.IGNORECASE)
                # Use None to indicate all-column filter
                self.filter_column_name = None
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        self._update_table()

    def _perform_filter(
        self, filter_value: Optional[str], column_name: Optional[str]
    ) -> None:
        """Performs a filter on the data and updates the table."""
        if not filter_value:
            self.current_filter = None
            self.filter_column_name = None
        else:
            try:
                # Compile the regex pattern
                self.current_filter = re.compile(filter_value, re.IGNORECASE)
                self.filter_column_name = (
                    column_name if column_name is not None else None
                )
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        self._update_table()

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
        data_table = self.query_one(NlessDataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            self.delimiter = infer_delimiter(log_lines[: min(5, len(log_lines))])
            self.delimiter_inferred = True

        if not self.first_row_parsed:
            first_log_line = log_lines[0]
            self.first_log_line = first_log_line
            if self.delimiter != "raw":
                parts = split_line(first_log_line, self.delimiter)
                data_table.add_columns(*parts)
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
            else:
                # Delimiter is raw, treat entire line as single column
                data_table.add_column("log")
                self.current_columns = [
                    Column(
                        name="log",
                        labels=set(),
                        render_position=0,
                        data_position=0,
                        hidden=False,
                    )
                ]
            self.first_row_parsed = True

        self.raw_rows.extend(log_lines)
        for line in log_lines:
            self._add_log_line(line)

        self._update_status_bar()

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

    def _get_col_idx_by_name(self, col_name: str, render_position: bool = False) -> Optional[int]:
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
        cells = split_line(log_line, self.delimiter)
        if len(self.unique_column_names) > 0:
            cells.insert(0, "1")

        expected_cell_count = len([c for c in self.current_columns if not c.hidden])
        aligned_cells = self._align_cells_to_visible_columns([cells])[0]
        if len(aligned_cells) != expected_cell_count:
            return

        if self.current_filter:
            matches = False
            if self.filter_column_name is None:
                # We're filtering any column
                matches = any(
                    self.current_filter.search(
                        self._get_cell_value_without_markup(cell)
                    )
                    for cell in cells
                )
            else:
                col_idx = self._get_col_idx_by_name(self.filter_column_name)
                if col_idx is None:
                    return
                matches = self.current_filter.search(
                    self._get_cell_value_without_markup(cells[col_idx])
                )
            if not matches:
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
            displayed_sort_column_idx = self._get_col_idx_by_name(self.sort_column, render_position=True)
            data_sort_column_idx = self._get_col_idx_by_name(self.sort_column, render_position=False)

            sort_key = self._get_cell_value_without_markup(str(cells[data_sort_column_idx]))
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

        if self.search_term:
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if isinstance(self.search_term, re.Pattern) and self.search_term.search(
                    cell
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

        old_row_key = None
        if old_index is not None:
            old_row_key = data_table.ordered_rows[old_index].key

        cells = self._align_cells_to_visible_columns([cells])[0]
        data_table.add_row_at(*cells, row_index=new_index)
        self.displayed_rows.insert(new_index, cells)

        if old_index is not None and old_row_key is not None:
            self.displayed_rows.remove(old_row)
            data_table.remove_row(old_row_key)

        if self.is_tailing:
            data_table.action_scroll_bottom()

    def _create_prompt(self, placeholder, id):
        input = Input(
            placeholder=placeholder,
            id=id,
            classes="bottom-input",
        )
        self.mount(input)
        input.focus()


def main():
    app = NlessApp()
    new_fd = sys.stdin.fileno()

    parser = argparse.ArgumentParser(description="Test InputConsumer with stdin.")
    parser.add_argument(
        "filename", nargs="?", help="File to read input from (defaults to stdin)"
    )
    args = parser.parse_args()

    if args.filename:
        with open(args.filename, "r") as f:
            ic = InputConsumer(
                args.filename,
                None,
                lambda: app.mounted,
                lambda lines: app.add_logs(lines),
            )
            t = Thread(target=ic.run, daemon=True)
            t.start()
    else:
        ic = InputConsumer(
            None, new_fd, lambda: app.mounted, lambda lines: app.add_logs(lines)
        )
        t = Thread(target=ic.run, daemon=True)
        t.start()

    sys.__stdin__ = open("/dev/tty")
    app.run()


if __name__ == "__main__":
    main()
