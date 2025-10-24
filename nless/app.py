import json
import os
import re
from threading import Thread
from typing import Optional

from textual import events
from textual.app import App, ComposeResult
from textual.coordinate import Coordinate
from textual.events import Key
from textual.geometry import Offset
from textual.scroll_view import ScrollView
from textual.widgets import (
    Input,
    Select,
    Static,
    Tab,
    TabbedContent,
    TabPane,
)

from nless.autocomplete import AutocompleteInput
from nless.buffer import NlessBuffer, handle_mark_unique, write_buffer
from nless.gettingstarted import GettingStartedScreen

from .config import NlessConfig, load_config, load_input_history
from .delimiter import split_line
from .help import HelpScreen
from .input import LineStream, ShellCommmandLineStream
from .nlessselect import NlessSelect
from .datatable import Datatable as NlessDataTable, Coordinate as NlessCoordinate
from .types import CliArgs, Column, Filter, MetadataColumn


class NlessApp(App):
    def __init__(
        self,
        cli_args: CliArgs,
        starting_stream: LineStream | None,
        show_help: bool = False,
    ) -> None:
        super().__init__()
        self.starting_stream = starting_stream
        self.cli_args = cli_args
        self.input_history = []
        self.config = NlessConfig()
        self.logs = []
        self.show_help = show_help
        self.mounted = False
        self.buffers = [
            NlessBuffer(pane_id=1, cli_args=cli_args, line_stream=starting_stream)
        ]
        self.curr_buffer_idx = 0

    SCREENS = {"HelpScreen": HelpScreen, "GettingStartedScreen": GettingStartedScreen}
    HISTORY_FILE = "~/.config/nless/history.json"

    CSS_PATH = "nless.tcss"

    BINDINGS = [
        ("N", "add_buffer", "New Buffer"),
        ("L", "show_tab_next", "Next Buffer"),
        ("H", "show_tab_previous", "Previous Buffer"),
        ("q", "close_active_buffer", "Close Active Buffer"),
        ("/", "search", "Search (all columns, by prompt)"),
        ("&", "search_to_filter", "Apply current search as filter"),
        ("|", "filter_any", "Filter any column (by prompt)"),
        ("f", "filter", "Filter selected column (by prompt)"),
        ("F", "filter_cursor_word", "Filter selected column by word under cursor"),
        ("D", "delimiter", "Change Delimiter"),
        ("d", "column_delimiter", "Change Column Delimiter"),
        ("W", "write_to_file", "Write current view to file"),
        ("J", "json_header", "Select new header from JSON in cell"),
        ("W", "write_to_file", "Write current view to file"),
        ("!", "run_command", "Run Shell Command (by prompt)"),
        (
            "U",
            "mark_unique",
            "Mark a column unique to create a composite key for distinct/analysis",
        ),
        ("C", "filter_columns", "Filter Columns (by prompt)"),
        ("?", "push_screen('HelpScreen')", "Show Help"),
    ]

    async def on_resize(self, event: events.Resize) -> None:
        self.refresh()

    def action_run_command(self) -> None:
        """Run a shell command and pipe the output into a new buffer."""
        self._create_prompt(
            "Type shell command (e.g. tail -f /var/log/syslog)", "run_command_input"
        )

    def handle_run_command_submitted(self, event: Input.Submitted) -> None:
        event.control.remove()
        command = event.value.strip()
        try:
            line_stream = ShellCommmandLineStream(command)
            new_buffer = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=self.cli_args,
                line_stream=line_stream,
            )
            self.add_buffer(new_buffer, name=command, add_prev_index=False)
        except Exception as e:
            self.notify(f"Error running command: {str(e)}", severity="error")

    def on_select_changed(self, event: Select.Changed) -> None:
        event.control.remove()
        if event.control.id == "json_header_select":
            curr_buffer = self._get_current_buffer()
            cursor_column = curr_buffer.query_one(NlessDataTable).cursor_column
            curr_column = [
                c
                for c in curr_buffer.current_columns
                if c.render_position == cursor_column
            ]
            if not curr_column:
                curr_buffer.notify(
                    "No column selected to add JSON key to", severity="error"
                )
                return
            curr_column = curr_column[0]
            curr_column_name = curr_buffer._get_cell_value_without_markup(
                curr_column.name
            )

            col_ref = str(event.value)
            if not col_ref.startswith("."):
                col_ref = f".{col_ref}"

            new_column_name = f"{curr_column_name}{col_ref}"

            new_cursor_x = len(curr_buffer.current_columns)

            new_col = Column(
                name=new_column_name,
                labels=set(),
                computed=True,
                render_position=new_cursor_x,
                data_position=new_cursor_x,
                hidden=False,
                json_ref=f"{curr_column_name}{col_ref}",
                delimiter="json",
            )

            curr_buffer.current_columns.append(new_col)
            data_table = curr_buffer.query_one(NlessDataTable)
            old_row = data_table.cursor_row
            curr_buffer._update_table(restore_position=False)
            self.call_after_refresh(
                lambda: data_table.move_cursor(column=new_cursor_x, row=old_row)
            )

    def action_json_header(self) -> None:
        """Set the column headers from JSON in the selected cell."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = curr_buffer._get_cell_value_without_markup(cell_value)
            json_data = json.loads(cell_value)
            if not isinstance(json_data, (dict, list)):
                curr_buffer.notify(
                    "Cell does not contain a JSON object.", severity="error"
                )
                return
            new_columns = []

            # iterate through the full JSON heirarchy of keys, building up a list of keys
            def extract_keys(obj, prefix=""):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_prefix = f"{prefix}.{k}" if prefix else k
                        new_columns.append((new_prefix, v))
                        extract_keys(v, new_prefix)
                elif isinstance(obj, list) and len(obj) > 0:
                    for i in range(len(obj)):
                        extract_keys(obj[i], prefix + f".{i}")

            extract_keys(json_data)

            select = NlessSelect(
                options=[
                    (f"[bold]{col}[/bold] - {json.dumps(v)}", col)
                    for (col, v) in new_columns
                ],
                classes="dock-bottom",
                id="json_header_select",
            )
            self.mount(select)
        except Exception as e:
            curr_buffer.notify(f"Error parsing JSON: {str(e)}", severity="error")

    def action_column_delimiter(self) -> None:
        """Change the column delimiter."""
        self._create_prompt(
            "Type column delimiter (e.g. , or \\t or 'space' or 'raw')",
            "column_delimiter_input",
        )

    def action_write_to_file(self) -> None:
        """Write the current view to a file."""
        self._create_prompt(
            "Type output file path (e.g. /tmp/output.csv)", "write_to_file_input"
        )

    def action_filter_columns(self) -> None:
        """Filter columns by user input."""
        self._create_prompt(
            "Type pipe delimited column names to show (e.g. col1|col2) or 'all' to reset",
            "column_filter_input",
        )

    def _get_new_pane_id(self) -> int:
        return max(b.pane_id for b in self.buffers) + 1 if self.buffers else 1

    def action_mark_unique(self) -> None:
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        new_buffer = curr_buffer.copy(pane_id=self._get_new_pane_id())
        current_cursor_column = data_table.cursor_column
        new_unique_column = [
            c
            for c in new_buffer.current_columns
            if c.render_position == current_cursor_column
        ]
        if not new_unique_column:
            self.notify("No column selected to mark as unique")
            return

        new_unique_column = new_unique_column[0]
        new_unique_column_name = new_buffer._get_cell_value_without_markup(
            new_unique_column.name
        )

        handle_mark_unique(new_buffer, new_unique_column_name)
        buffer_name = (
            f"+u:{new_unique_column_name}"
            if new_unique_column_name in new_buffer.unique_column_names
            else f"-u:{new_unique_column_name}"
        )
        self.add_buffer(new_buffer, name=buffer_name)

        # update the cursor position's index to match the new position of the column
        new_cursor_position = 0
        for i, col in enumerate(
            sorted(new_buffer.current_columns, key=lambda c: c.render_position)
        ):
            if (
                new_buffer._get_cell_value_without_markup(col.name)
                == new_unique_column_name
            ):
                new_cursor_position = i
                break
        self.set_timer(
            0.2,
            lambda: new_buffer.query_one(NlessDataTable).move_cursor(
                column=new_cursor_position
            ),
        )

    def action_delimiter(self) -> None:
        """Change the delimiter used for parsing."""
        self._create_prompt(
            "Type delimiter character (e.g. ',', '\\t', ' ', '|') or 'raw' for no parsing",
            "delimiter_input",
        )

    def action_search_to_filter(self) -> None:
        """Convert current search into a filter across all columns."""
        current_buffer = self._get_current_buffer()
        if not current_buffer.search_term:
            current_buffer.notify(
                "No active search to convert to filter", severity="warning"
            )
            return
        else:
            new_buffer = current_buffer.copy(pane_id=self._get_new_pane_id())
            new_buffer.current_filters.append(
                Filter(column=None, pattern=current_buffer.search_term)
            )
            self.add_buffer(
                new_buffer, name=f"+f:any={current_buffer.search_term.pattern}"
            )

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        self._create_prompt("Type search term and press Enter", "search_input")

    def _create_prompt(self, placeholder, id):
        input = AutocompleteInput(
            placeholder=placeholder,
            id=id,
            classes="bottom-input",
            history=[h["val"] for h in self.input_history if h["id"] == id],
            on_add=lambda val: self.input_history.append({"id": id, "val": val}),
        )
        tab_content = self.query_one(TabbedContent)
        active_tab = tab_content.active
        for tab_pane in tab_content.query(TabPane):
            if tab_pane.id == active_tab:
                tab_pane.mount(input)
                self.call_after_refresh(lambda: input.focus())
                break

    def _filter_composite_key(self, current_buffer: NlessBuffer) -> None:
        data_table = current_buffer.query_one(NlessDataTable)
        cursor_column = data_table.cursor_column
        selected_column = [
            c
            for c in current_buffer.current_columns
            if c.render_position == cursor_column
        ]
        if selected_column:
            selected_column_name = current_buffer._get_cell_value_without_markup(
                selected_column[0].name
            )
            if selected_column_name in current_buffer.unique_column_names:
                new_buffer = current_buffer.copy(pane_id=self._get_new_pane_id())
                filters = []
                for column in current_buffer.unique_column_names:
                    col_idx = current_buffer._get_col_idx_by_name(
                        column, render_position=True
                    )
                    cell_value = data_table.get_cell_at(
                        NlessCoordinate(data_table.cursor_row, col_idx)
                    )
                    cell_value = current_buffer._get_cell_value_without_markup(
                        cell_value
                    )
                    filters.append(
                        Filter(
                            column=current_buffer._get_cell_value_without_markup(
                                column
                            ),
                            pattern=re.compile(re.escape(cell_value), re.IGNORECASE),
                        )
                    )
                    handle_mark_unique(new_buffer, column)
                new_buffer.current_filters.extend(filters)
                self.add_buffer(
                    new_buffer,
                    name=f"+f:{','.join([f'{f.column}={f.pattern.pattern}' for f in filters])}",
                )

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if event.key == "escape" and (
            isinstance(self.focused, Input) or isinstance(self.focused, Select)
        ):
            self.focused.remove()

        current_buffer = self._get_current_buffer()
        if event.key == "enter" and isinstance(self.focused, NlessDataTable):
            self._filter_composite_key(current_buffer)

        if event.key in [str(i) for i in range(1, 10)] and isinstance(
            self.focused, NlessDataTable
        ):
            self.show_tab_by_index(int(event.key) - 1)

    def handle_column_delimiter_submitted(self, event: Input.Submitted) -> None:
        event.input.remove()
        new_col_delimiter = event.value

        current_buffer = self._get_current_buffer()
        data_table = current_buffer.query_one(NlessDataTable)
        cursor_coordinate = data_table.cursor_coordinate
        cell = data_table.get_cell_at(cursor_coordinate)
        selected_column = [
            c
            for c in current_buffer.current_columns
            if c.render_position == cursor_coordinate.column
        ]
        if not selected_column:
            current_buffer.notify("No column selected for delimiting", severity="error")
            return
        selected_column = selected_column[0]

        if new_col_delimiter == "json":
            try:
                cell_json = json.loads(
                    current_buffer._get_cell_value_without_markup(cell)
                )
                if not isinstance(cell_json, (dict, list)):
                    current_buffer.notify(
                        "Selected cell does not contain a JSON object or array",
                        severity="error",
                    )
                    return
                column_count = len(current_buffer.current_columns)
                cell_json_keys = (
                    list(cell_json.keys())
                    if isinstance(cell_json, dict)
                    else [i for i in range(len(cell_json))]
                )
                duplicates = 0
                for i, key in enumerate(cell_json_keys):
                    if f"{selected_column.name}.{key}" not in [
                        c.name for c in current_buffer.current_columns
                    ]:
                        current_buffer.current_columns.append(
                            Column(
                                name=f"{selected_column.name}.{key}",
                                labels=set(),
                                render_position=column_count + i - duplicates,
                                data_position=column_count + i - duplicates,
                                hidden=False,
                                computed=True,
                                json_ref=f"{selected_column.name}.{key}",
                                delimiter=new_col_delimiter,
                            )
                        )
                    else:
                        duplicates += 1
                current_buffer._update_table()
            except json.JSONDecodeError:
                current_buffer.notify(
                    "Selected cell does not contain a JSON object or array",
                    severity="error",
                )
                return
        else:
            if new_col_delimiter == "\\t":
                new_col_delimiter = "\t"

            try:
                pattern = re.compile(new_col_delimiter)
                if pattern.groups == 0:
                    raise Exception()
                group_names = pattern.groupindex.keys()
                duplicates = 0
                for i, group in enumerate(group_names):
                    if group not in [c.name for c in current_buffer.current_columns]:
                        current_buffer.current_columns.append(
                            Column(
                                name=group,
                                labels=set(),
                                render_position=len(current_buffer.current_columns)
                                - duplicates,
                                data_position=len(current_buffer.current_columns)
                                - duplicates,
                                hidden=False,
                                computed=True,
                                col_ref=f"{selected_column.name}",
                                col_ref_index=i,
                                delimiter=pattern,
                            )
                        )
                    else:
                        duplicates += 1
                current_buffer._update_table()
                return
            except Exception:
                pass

            try:
                cell_parts = split_line(
                    current_buffer._get_cell_value_without_markup(cell),
                    new_col_delimiter,
                    [],
                )
                if len(cell_parts) < 2:
                    current_buffer.notify(
                        "Delimiter did not split cell into multiple parts",
                        severity="error",
                    )
                    return
                column_count = len(current_buffer.current_columns)
                duplicates = 0
                for i, part in enumerate(cell_parts):
                    part = part.strip()
                    if part not in [c.name for c in current_buffer.current_columns]:
                        current_buffer.current_columns.append(
                            Column(
                                name=f"{selected_column.name}-{i + 1}",
                                labels=set(),
                                render_position=column_count + i - duplicates,
                                data_position=column_count + i - duplicates,
                                hidden=False,
                                computed=True,
                                col_ref_index=i,
                                col_ref=f"{selected_column.name}",
                                delimiter=new_col_delimiter,
                            )
                        )
                    else:
                        duplicates += 1
                current_buffer._update_table()
            except Exception as e:
                current_buffer.notify(
                    f"Error splitting cell: {str(e)}", severity="error"
                )
                return

    def handle_write_to_file_submitted(self, event: Input.Submitted) -> None:
        output_path = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()
        try:
            t = Thread(target=write_buffer, args=(current_buffer, output_path))
            if output_path != "-":
                t.start()
                t.join()
                current_buffer.notify(f"Wrote current view to {output_path}")
            else:
                t.start()
                self.exit()
        except Exception as e:
            current_buffer.notify(f"Failed to write to file: {e}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_input":
            self.handle_search_submitted(event)
        elif event.input.id == "filter_input" or event.input.id == "filter_input_any":
            self.handle_filter_submitted(event)
        elif event.input.id == "delimiter_input":
            self.handle_delimiter_submitted(event)
        elif event.input.id == "column_filter_input":
            self.handle_column_filter_submitted(event)
        elif event.input.id == "write_to_file_input":
            self.handle_write_to_file_submitted(event)
        elif event.input.id == "column_delimiter_input":
            self.handle_column_delimiter_submitted(event)
        elif event.input.id == "run_command_input":
            self.handle_run_command_submitted(event)

    def handle_search_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()
        current_buffer._perform_search(input_value)

    def _get_current_buffer(self) -> NlessBuffer:
        return self.buffers[self.curr_buffer_idx]

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.columns[column_index]
        self._create_prompt(
            f"Type filter text for column: {column_label} and press enter",
            "filter_input",
        )

    def action_filter_any(self) -> None:
        """Filter any column based on user input."""
        self._create_prompt(
            "Type filter text to match across all columns", "filter_input_any"
        )

    def handle_filter_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)

        if event.input.id == "filter_input_any":
            self._perform_filter_any(filter_value)
        else:
            column_index = data_table.cursor_column
            column_label = [
                c
                for c in curr_buffer.current_columns
                if c.render_position == column_index
            ]
            if not column_label:
                self.notify("No column selected for filtering")
                return
            column_label = curr_buffer._get_cell_value_without_markup(
                column_label[0].name
            )
            self._perform_filter(filter_value, column_label)

    def _perform_filter_any(self, filter_value: Optional[str]) -> None:
        new_buffer = self._get_current_buffer().copy(pane_id=self._get_new_pane_id())
        """Performs a filter across all columns and updates the table."""
        if not filter_value:
            new_buffer.current_filters = []
            new_buf_name = (
                new_buffer.current_filters
                and f"-f:{','.join([f'{f.column if f.column else "any"}={f.pattern.pattern}' for f in new_buffer.current_filters])}"
                or "-f"
            )
        else:
            try:
                new_buffer.current_filters.append(
                    Filter(column=None, pattern=re.compile(filter_value, re.IGNORECASE))
                )
            except re.error:
                new_buffer.notify("Invalid regex pattern", severity="error")
                return
            new_buf_name = f"+f:any={filter_value}"

        self.add_buffer(new_buffer, name=new_buf_name)

    def _perform_filter(
        self, filter_value: Optional[str], column_name: Optional[str]
    ) -> None:
        """Performs a filter on the data and updates the table."""
        new_buffer = self._get_current_buffer().copy(pane_id=self._get_new_pane_id())
        if not filter_value:
            new_buf_name = (
                new_buffer.current_filters
                and f"-f:{','.join([f'{f.column if f.column else "any"}={f.pattern.pattern}' for f in new_buffer.current_filters])}"
                or "-f"
            )
            new_buffer.current_filters = []
        else:
            if column_name in new_buffer.unique_column_names:
                handle_mark_unique(new_buffer, column_name)
                self.notify(
                    f"Removed unique column: {column_name}, to allow filtering.",
                    severity="info",
                )
            try:
                # Compile the regex pattern
                new_buffer.current_filters.append(
                    Filter(
                        column=column_name,
                        pattern=re.compile(filter_value, re.IGNORECASE),
                    )
                )
            except re.error:
                new_buffer.notify("Invalid regex pattern", severity="error")
                return
            new_buf_name = f"+f:{column_name}={filter_value}"

        self.add_buffer(new_buffer, name=new_buf_name)

    def handle_delimiter_submitted(self, event: Input.Submitted) -> None:
        curr_buffer = self._get_current_buffer()
        curr_buffer.current_filters = []
        curr_buffer.search_term = None
        curr_buffer.sort_column = None
        curr_buffer.unique_column_names = set()
        prev_delimiter = curr_buffer.delimiter

        event.input.remove()
        data_table = curr_buffer.query_one(NlessDataTable)
        curr_buffer.delimiter_inferred = False
        delimiter = event.value
        if delimiter not in [
            "raw",
            "json",
        ]:  # if our delimiter is not one of the common ones, treat it as a regex
            try:
                pattern = re.compile(rf"{delimiter}")  # Validate regex
                if pattern.groups == 0:
                    raise Exception()
                curr_buffer.delimiter = pattern
                curr_buffer.current_columns = [
                    Column(
                        name=h,
                        labels=set(),
                        render_position=i,
                        data_position=i,
                        hidden=False,
                    )
                    for i, h in enumerate(pattern.groupindex.keys())
                ]
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
                    curr_buffer.raw_rows.insert(0, curr_buffer.first_log_line)
                curr_buffer._update_table()
                return
            except:
                pass

        if delimiter == "\\t":
            delimiter = "\t"

        curr_buffer.delimiter = delimiter

        parsed_full_json_file = False

        if delimiter == "raw":
            new_header = ["log"]
        elif delimiter == "json":
            try:
                new_header = json.loads(curr_buffer.first_log_line).keys()
            except Exception as e:
                # attempt to read all logs as one json payload
                try:
                    all_logs = ""
                    if prev_delimiter != "raw" and not isinstance(
                        prev_delimiter, re.Pattern
                    ):
                        all_logs = curr_buffer.first_log_line + "\n"
                    all_logs += "\n".join(curr_buffer.raw_rows)
                    buffer_json = json.loads(all_logs)
                    if (
                        isinstance(buffer_json, list)
                        and len(buffer_json) > 0
                        and isinstance(buffer_json[0], dict)
                    ):
                        new_header = buffer_json[0].keys()
                        curr_buffer.raw_rows = [
                            json.dumps(item) for item in buffer_json
                        ]
                    elif isinstance(buffer_json, dict):
                        new_header = buffer_json.keys()
                        curr_buffer.raw_rows = [json.dumps(buffer_json)]
                    else:
                        curr_buffer.notify(
                            f"Failed to parse JSON logs: {e}", severity="error"
                        )
                        return
                    curr_buffer.first_log_line = curr_buffer.raw_rows[0]
                    parsed_full_json_file = True
                except Exception as e2:
                    curr_buffer.notify(
                        f"Failed to parse JSON logs: {e2}", severity="error"
                    )
                    return
        elif prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern):
            new_header = split_line(
                curr_buffer.raw_rows[0],
                curr_buffer.delimiter,
                curr_buffer.current_columns,
            )
            curr_buffer.raw_rows.pop(0)
        else:
            new_header = split_line(
                curr_buffer.first_log_line,
                curr_buffer.delimiter,
                curr_buffer.current_columns,
            )

        if (
            (prev_delimiter != delimiter)
            and (
                prev_delimiter != "raw"
                and not isinstance(prev_delimiter, re.Pattern)
                and prev_delimiter != "json"
            )
            and (
                delimiter == "raw"
                or isinstance(delimiter, re.Pattern)
                or delimiter == "json"
            )
            and not parsed_full_json_file
        ):
            curr_buffer.raw_rows.insert(0, curr_buffer.first_log_line)

        curr_buffer.current_columns = [
            Column(
                name=h, labels=set(), render_position=i, data_position=i, hidden=False
            )
            for i, h in enumerate(new_header)
        ]
        curr_buffer._update_table()

    def handle_column_filter_submitted(self, event: Input.Submitted) -> None:
        curr_buffer = self._get_current_buffer()
        input_value = event.value
        event.input.remove()
        if input_value.lower() == "all":
            for col in curr_buffer.current_columns:
                col.hidden = False
        else:
            column_name_filters = [name.strip() for name in input_value.split("|")]
            column_name_filter_regexes = [
                re.compile(rf"{name}", re.IGNORECASE) for name in column_name_filters
            ]
            metadata_columns = [mc.value for mc in MetadataColumn]
            visible_pinned_columns = [
                col
                for col in curr_buffer.current_columns
                if col.pinned and not col.hidden
            ]
            for col in curr_buffer.current_columns:
                matched = False
                plain_name = curr_buffer._get_cell_value_without_markup(col.name)
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
            if all(col.hidden for col in curr_buffer.current_columns):
                curr_buffer.notify(
                    "At least one column must be visible.", severity="warning"
                )
                for col in curr_buffer.current_columns:
                    col.hidden = False

        sorted_columns = sorted(
            curr_buffer.current_columns, key=lambda c: c.render_position
        )
        for i, col in enumerate(sorted_columns):
            col.render_position = i

        curr_buffer._update_table()

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = curr_buffer._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            selected_column = [
                c
                for c in curr_buffer.current_columns
                if c.render_position == coordinate.column
            ]
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                curr_buffer._get_cell_value_without_markup(selected_column[0].name),
            )
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def refresh_buffer_and_focus(
        self, new_buffer: NlessBuffer, cursor_coordinate: Coordinate, offset: Offset
    ) -> None:
        new_buffer._update_table()
        data_table = new_buffer.query_one(NlessDataTable)
        data_table.focus()
        new_buffer._restore_position(
            data_table,
            cursor_coordinate.column,
            cursor_coordinate.row,
            offset.x,
            offset.y,
        )

    def add_buffer(
        self, new_buffer: NlessBuffer, name: str, add_prev_index: bool = True
    ) -> None:
        curr_data_table = self._get_current_buffer().query_one(NlessDataTable)

        self.buffers.append(new_buffer)
        tabbed_content = self.query_one(TabbedContent)
        buffer_number = len(self.buffers)
        tab_pane = TabPane(
            f"[#00ff00]{buffer_number}[/#00ff00] {self.curr_buffer_idx + 1 if add_prev_index else ''}{name}",
            id=f"buffer{new_buffer.pane_id}",
        )
        tabbed_content.add_pane(tab_pane)
        scroll_view = ScrollView()
        tab_pane.mount(scroll_view)
        scroll_view.mount(new_buffer)
        self.curr_buffer_idx = len(self.buffers) - 1
        tabbed_content.active = f"buffer{new_buffer.pane_id}"
        self.call_after_refresh(
            lambda: self.refresh_buffer_and_focus(
                new_buffer,
                curr_data_table.cursor_coordinate,
                curr_data_table.scroll_offset,
            )
        )

    def on_exit_app(self) -> None:
        # check if file exists, if not create it
        os.makedirs(
            os.path.dirname(os.path.expanduser(self.HISTORY_FILE)), exist_ok=True
        )

        with open(os.path.expanduser(self.HISTORY_FILE), "w") as f:
            json.dump(self.input_history, f)

    def action_close_active_buffer(self) -> None:
        if len(self.buffers) == 1:
            self.exit()
            return

        tabbed_content = self.query_one(TabbedContent)
        current_buffer = self._get_current_buffer()
        if current_buffer.line_stream:
            current_buffer.line_stream.unsubscribe(current_buffer)

        tabbed_content.remove_pane(f"buffer{current_buffer.pane_id}")
        self.buffers.pop(self.curr_buffer_idx)

        if self.curr_buffer_idx >= len(self.buffers):
            self.curr_buffer_idx = len(self.buffers) - 1

        new_curr_buffer = self.buffers[self.curr_buffer_idx]

        tabbed_content.active = f"buffer{new_curr_buffer.pane_id}"
        tabbed_content.query_one(f"#buffer{new_curr_buffer.pane_id}").query_one(
            NlessDataTable
        ).focus()

        self.call_after_refresh(lambda: new_curr_buffer._update_status_bar())
        self.call_after_refresh(lambda: self._update_panes())

    def _update_panes(self) -> None:
        tabbed_content = self.query_one(TabbedContent)
        pattern = re.compile(r"((\[#00ff00\])?(\d+?)(\[/#00ff00\])?) .*")
        for i, pane in enumerate(tabbed_content.query(Tab).results()):
            curr_title = str(pane.content)
            pattern_matches = pattern.match(curr_title)
            if pattern_matches:
                old_index = pattern_matches.group(1)
                curr_title = curr_title.replace(
                    old_index, f"[#00ff00]{str(i + 1)}[/#00ff00]", count=1
                )
                pane.update(curr_title)

    def action_show_tab_next(self) -> None:
        tabbed_content = self.query_one(TabbedContent)
        self.curr_buffer_idx = (self.curr_buffer_idx + 1) % len(self.buffers)
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def action_show_tab_previous(self) -> None:
        tabbed_content = self.query_one(TabbedContent)
        self.curr_buffer_idx = (self.curr_buffer_idx - 1) % len(self.buffers)
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def show_tab_by_index(self, index: int) -> None:
        if index < 0 or index >= len(self.buffers):
            return
        tabbed_content = self.query_one(TabbedContent)
        self.curr_buffer_idx = index
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def on_mount(self) -> None:
        self.mounted = True

        self.config = load_config()
        self.input_history = load_input_history()

        if self.show_help and self.config.show_getting_started:
            self.push_screen(GettingStartedScreen())
        else:
            self.query_one(NlessDataTable).focus()

    def action_add_buffer(self) -> None:
        max_buffer_id = max(buffer.pane_id for buffer in self.buffers)
        new_buffer = NlessBuffer(
            pane_id=max_buffer_id + 1,
            cli_args=self.cli_args,
            line_stream=self.starting_stream,
        )
        self.add_buffer(
            new_buffer, name=f"buffer{new_buffer.pane_id}", add_prev_index=False
        )

    def compose(self) -> ComposeResult:
        init_buffer = self.buffers[0]
        with TabbedContent():
            with TabPane(
                "[#00ff00]1[/#00ff00] original", id=f"buffer{init_buffer.pane_id}"
            ):
                with ScrollView():
                    yield init_buffer

        yield Static(id="status_bar", classes="dock-bottom")
