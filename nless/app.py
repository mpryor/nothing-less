import csv
import json
import os
import re
import subprocess
from threading import Thread

from textual import events
from textual.css.query import NoMatches
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
from .input import LineStream, ShellCommandLineStream
from .nlessselect import NlessSelect
from .datatable import Datatable as NlessDataTable, Coordinate as NlessCoordinate
from .types import CliArgs, Column, Filter, MetadataColumn


_TAB_SWITCH_KEYS = frozenset(str(i) for i in range(1, 10))


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
        ("e", "exclude_filter", "Exclude from selected column (by prompt)"),
        (
            "E",
            "exclude_filter_cursor_word",
            "Exclude selected column by word under cursor",
        ),
        ("D", "delimiter", "Change Delimiter"),
        ("d", "column_delimiter", "Change Column Delimiter"),
        ("W", "write_to_file", "Write current view to file"),
        ("J", "json_header", "Select new header from JSON in cell"),
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
            line_stream = ShellCommandLineStream(command)
            new_buffer = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=self.cli_args,
                line_stream=line_stream,
            )
            self.add_buffer(new_buffer, name=command, add_prev_index=False)
        except (OSError, ValueError, subprocess.SubprocessError) as e:
            self.notify(f"Error running command: {str(e)}", severity="error")

    def on_select_changed(self, event: Select.Changed) -> None:
        event.control.remove()
        if event.control.id == "json_header_select":
            curr_buffer = self._get_current_buffer()
            if not curr_buffer._with_lock("json header"):
                return
            try:
                data_table = curr_buffer.query_one(NlessDataTable)
                cursor_column = data_table.cursor_column
                curr_column = curr_buffer._get_column_at_position(cursor_column)
                if not curr_column:
                    curr_buffer.notify(
                        "No column selected to add JSON key to", severity="error"
                    )
                    return
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
                old_row = data_table.cursor_row
            finally:
                curr_buffer._lock.release()

            curr_buffer._deferred_update_table(
                restore_position=False,
                callback=lambda: data_table.move_cursor(
                    column=new_cursor_x, row=old_row
                ),
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
        except (json.JSONDecodeError, KeyError, TypeError) as e:
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

    def _copy_buffer_async(
        self, setup_fn, buffer_name, after_add_fn=None, add_prev_index=True
    ):
        """Run copy() + setup on a background thread, then add_buffer on main thread."""
        curr_buffer = self._get_current_buffer()
        new_pane_id = self._get_new_pane_id()
        curr_buffer._is_loading = True
        curr_buffer._update_status_bar()

        def _run():
            try:
                new_buffer = curr_buffer.copy(pane_id=new_pane_id)
                setup_fn(new_buffer)
            except Exception as e:
                msg = str(e)

                def _on_error():
                    curr_buffer._is_loading = False
                    curr_buffer._update_status_bar()
                    self.notify(f"Error: {msg}", severity="error")

                self.call_from_thread(_on_error)
                return

            def _finish():
                curr_buffer._is_loading = False
                curr_buffer._update_status_bar()
                self.add_buffer(
                    new_buffer,
                    name=buffer_name,
                    add_prev_index=add_prev_index,
                )
                if after_add_fn:
                    after_add_fn(new_buffer)

            self.call_from_thread(_finish)

        Thread(target=_run, daemon=True).start()

    def action_mark_unique(self) -> None:
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        current_cursor_column = data_table.cursor_column

        selected_column = curr_buffer._get_column_at_position(current_cursor_column)
        if not selected_column:
            self.notify("No column selected to mark as unique")
            return

        if data_table.columns[current_cursor_column] == "count":
            self.notify("Cannot mark 'count' column as unique", severity="error")
            return

        unique_column_name = curr_buffer._get_cell_value_without_markup(
            selected_column.name
        )

        def setup(new_buffer):
            handle_mark_unique(new_buffer, unique_column_name)
            # When adding a unique key, sort by count descending by default
            if unique_column_name in new_buffer.unique_column_names:
                new_buffer.sort_column = MetadataColumn.COUNT.value
                new_buffer.sort_reverse = True
                for col in new_buffer.current_columns:
                    if col.name == MetadataColumn.COUNT.value:
                        col.labels.add("▼")
                        break

        # Determine buffer name after setup runs — use a mutable container
        # to capture the name from setup context
        will_be_unique = unique_column_name not in curr_buffer.unique_column_names
        buffer_name = (
            f"+u:{unique_column_name}" if will_be_unique else f"-u:{unique_column_name}"
        )

        def after_add(new_buffer):
            new_cursor_position = 0
            for i, col in enumerate(
                sorted(new_buffer.current_columns, key=lambda c: c.render_position)
            ):
                if (
                    new_buffer._get_cell_value_without_markup(col.name)
                    == unique_column_name
                ):
                    new_cursor_position = i
                    break
            pos = new_cursor_position
            self.set_timer(
                0.3,
                lambda: new_buffer.query_one(NlessDataTable).move_cursor(column=pos),
            )

        self._copy_buffer_async(setup, buffer_name, after_add_fn=after_add)

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

        search_pattern = current_buffer.search_term
        buffer_name = f"+f:any={search_pattern.pattern}"

        def setup(new_buffer):
            new_buffer.current_filters.append(
                Filter(column=None, pattern=search_pattern)
            )

        self._copy_buffer_async(setup, buffer_name)

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
            on_remove=lambda val: self.input_history.remove({"id": id, "val": val}),
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
        selected_column = current_buffer._get_column_at_position(cursor_column)
        if selected_column:
            selected_column_name = current_buffer._get_cell_value_without_markup(
                selected_column.name
            )
            if selected_column_name in current_buffer.unique_column_names:
                # Pre-read cell values on main thread (widget access)
                filters = []
                unique_columns = list(current_buffer.unique_column_names)
                for column in unique_columns:
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
                buffer_name = f"+f:{','.join([f'{f.column}={f.pattern.pattern}' for f in filters])}"

                def setup(new_buffer):
                    for column in unique_columns:
                        handle_mark_unique(new_buffer, column)
                    new_buffer.current_filters.extend(filters)

                self._copy_buffer_async(setup, buffer_name)

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if event.key == "escape" and (
            isinstance(self.focused, Input) or isinstance(self.focused, Select)
        ):
            self.focused.remove()

        current_buffer = self._get_current_buffer()
        if event.key == "enter" and isinstance(self.focused, NlessDataTable):
            self._filter_composite_key(current_buffer)

        if event.key in _TAB_SWITCH_KEYS and isinstance(self.focused, NlessDataTable):
            self.show_tab_by_index(int(event.key) - 1)

    @staticmethod
    def _add_computed_columns(buffer, column_names, make_column_fn):
        """Add computed columns to a buffer, skipping duplicates.

        Args:
            buffer: The NlessBuffer to add columns to.
            column_names: Iterable of candidate column names.
            make_column_fn: Callable(index, name, position) -> Column.
        """
        existing = {c.name for c in buffer.current_columns}
        base_pos = len(buffer.current_columns)
        added = 0
        for i, name in enumerate(column_names):
            if name not in existing:
                buffer.current_columns.append(make_column_fn(i, name, base_pos + added))
                added += 1

    def handle_column_delimiter_submitted(self, event: Input.Submitted) -> None:
        event.input.remove()
        new_col_delimiter = event.value

        current_buffer = self._get_current_buffer()
        if not current_buffer._with_lock("column delimiter"):
            return
        should_update = False
        try:
            data_table = current_buffer.query_one(NlessDataTable)
            cursor_coordinate = data_table.cursor_coordinate
            cell = data_table.get_cell_at(cursor_coordinate)
            selected_column = current_buffer._get_column_at_position(
                cursor_coordinate.column
            )
            if not selected_column:
                current_buffer.notify(
                    "No column selected for delimiting", severity="error"
                )
                return

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
                    cell_json_keys = (
                        list(cell_json.keys())
                        if isinstance(cell_json, dict)
                        else list(range(len(cell_json)))
                    )
                    self._add_computed_columns(
                        current_buffer,
                        [f"{selected_column.name}.{key}" for key in cell_json_keys],
                        lambda i, name, pos: Column(
                            name=name,
                            labels=set(),
                            render_position=pos,
                            data_position=pos,
                            hidden=False,
                            computed=True,
                            json_ref=name,
                            delimiter=new_col_delimiter,
                        ),
                    )
                    should_update = True
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
                        raise re.error("no named groups")
                    group_names = list(pattern.groupindex.keys())
                    self._add_computed_columns(
                        current_buffer,
                        group_names,
                        lambda i, name, pos, _pat=pattern, _sel=selected_column: Column(
                            name=name,
                            labels=set(),
                            render_position=pos,
                            data_position=pos,
                            hidden=False,
                            computed=True,
                            col_ref=f"{_sel.name}",
                            col_ref_index=i,
                            delimiter=_pat,
                        ),
                    )
                    should_update = True
                except (re.error, ValueError):
                    pass

                if not should_update:
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
                        self._add_computed_columns(
                            current_buffer,
                            [
                                f"{selected_column.name}-{i + 1}"
                                for i in range(len(cell_parts))
                            ],
                            lambda i,
                            name,
                            pos,
                            _delim=new_col_delimiter,
                            _sel=selected_column: Column(
                                name=name,
                                labels=set(),
                                render_position=pos,
                                data_position=pos,
                                hidden=False,
                                computed=True,
                                col_ref_index=i,
                                col_ref=f"{_sel.name}",
                                delimiter=_delim,
                            ),
                        )
                        should_update = True
                    except (
                        json.JSONDecodeError,
                        csv.Error,
                        ValueError,
                        IndexError,
                    ) as e:
                        current_buffer.notify(
                            f"Error splitting cell: {str(e)}", severity="error"
                        )
        finally:
            current_buffer._lock.release()

        if should_update:
            current_buffer._deferred_update_table()

    def handle_write_to_file_submitted(self, event: Input.Submitted) -> None:
        output_path = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()

        def _write_and_notify():
            try:
                write_buffer(current_buffer, output_path)
                if output_path != "-":
                    self.call_from_thread(
                        lambda: current_buffer.notify(
                            f"Wrote current view to {output_path}"
                        )
                    )
            except (OSError, csv.Error, ValueError) as exc:
                msg = str(exc)
                self.call_from_thread(
                    lambda: current_buffer.notify(
                        f"Failed to write to file: {msg}", severity="error"
                    )
                )

        t = Thread(target=_write_and_notify)
        t.start()
        if output_path == "-":
            self.exit()
        else:
            t.join()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_input":
            self.handle_search_submitted(event)
        elif event.input.id in (
            "filter_input",
            "filter_input_any",
            "exclude_filter_input",
            "exclude_filter_input_any",
        ):
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
        exclude = event.input.id in ("exclude_filter_input", "exclude_filter_input_any")

        if event.input.id in ("filter_input_any", "exclude_filter_input_any"):
            self._perform_filter(filter_value, exclude=exclude)
        else:
            column_index = data_table.cursor_column
            column = curr_buffer._get_column_at_position(column_index)
            if not column:
                self.notify("No column selected for filtering")
                return
            column_label = curr_buffer._get_cell_value_without_markup(column.name)
            self._perform_filter(filter_value, column_label, exclude=exclude)

    def _perform_filter(
        self,
        filter_value: str | None,
        column_name: str | None = None,
        exclude: bool = False,
    ) -> None:
        """Performs a filter on the data and updates the table.

        When column_name is None, filters across all columns.
        """
        curr_buffer = self._get_current_buffer()

        # Validate regex eagerly on main thread
        compiled_pattern = None
        if filter_value:
            try:
                compiled_pattern = re.compile(filter_value, re.IGNORECASE)
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        # Determine buffer name from current state (before copy)
        filter_prefix = "!f" if exclude else "+f"
        if not filter_value:
            new_buf_name = (
                curr_buffer.current_filters
                and f"-f:{','.join([f'{"!" if f.exclude else ""}{f.column if f.column else "any"}={f.pattern.pattern}' for f in curr_buffer.current_filters])}"
                or "-f"
            )
        elif column_name is None:
            new_buf_name = f"{filter_prefix}:any={filter_value}"
        else:
            new_buf_name = f"{filter_prefix}:{column_name}={filter_value}"

        notify_removed_unique = (
            filter_value
            and column_name
            and column_name in curr_buffer.unique_column_names
        )

        def setup(new_buffer):
            if not filter_value:
                new_buffer.current_filters = []
            else:
                if column_name and column_name in new_buffer.unique_column_names:
                    handle_mark_unique(new_buffer, column_name)
                new_buffer.current_filters.append(
                    Filter(
                        column=column_name,
                        pattern=compiled_pattern,
                        exclude=exclude,
                    )
                )

        def after_add(new_buffer):
            if notify_removed_unique:
                self.notify(
                    f"Removed unique column: {column_name}, to allow filtering.",
                    severity="info",
                )

        self._copy_buffer_async(setup, new_buf_name, after_add_fn=after_add)

    @staticmethod
    def _should_reinsert_header_as_data(
        prev_delimiter, new_delimiter, parsed_full_json_file
    ) -> bool:
        """Check whether the old header line should be re-inserted as a data row.

        This is needed when switching from a standard delimiter (where the first
        line was consumed as header) to raw/json/regex (where every line is data).
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
        otherwise returns the delimiter string (with \\t converted to tab).
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
        return value

    def _resolve_new_header(self, delimiter, prev_delimiter, curr_buffer):
        """Determine the new header columns and whether the full JSON file was parsed.

        Returns (header_list, parsed_full_json_file) or None if an error occurred
        and was reported to the user.
        """
        if delimiter == "raw":
            return ["log"], False

        if delimiter == "json":
            try:
                return list(json.loads(curr_buffer.first_log_line).keys()), False
            except (json.JSONDecodeError, KeyError, TypeError) as e:
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
                        header = list(buffer_json[0].keys())
                        curr_buffer.raw_rows = [
                            json.dumps(item) for item in buffer_json
                        ]
                    elif isinstance(buffer_json, dict):
                        header = list(buffer_json.keys())
                        curr_buffer.raw_rows = [json.dumps(buffer_json)]
                    else:
                        curr_buffer.notify(
                            f"Failed to parse JSON logs: {e}", severity="error"
                        )
                        return None
                    curr_buffer.first_log_line = curr_buffer.raw_rows[0]
                    return header, True
                except (json.JSONDecodeError, KeyError, TypeError) as e2:
                    curr_buffer.notify(
                        f"Failed to parse JSON logs: {e2}", severity="error"
                    )
                    return None

        if prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern):
            header = split_line(
                curr_buffer.raw_rows[0],
                curr_buffer.delimiter,
                curr_buffer.current_columns,
            )
            curr_buffer.raw_rows.pop(0)
            return header, False

        return split_line(
            curr_buffer.first_log_line,
            curr_buffer.delimiter,
            curr_buffer.current_columns,
        ), False

    def handle_delimiter_submitted(self, event: Input.Submitted) -> None:
        curr_buffer = self._get_current_buffer()
        event.input.remove()
        if not curr_buffer._with_lock("delimiter"):
            return
        should_update = False
        try:
            curr_buffer.current_filters = []
            curr_buffer.search_term = None
            curr_buffer.sort_column = None
            curr_buffer.unique_column_names = set()
            prev_delimiter = curr_buffer.delimiter

            curr_buffer.delimiter_inferred = False
            delimiter = self._parse_delimiter_input(event.value)

            # If it's a regex with named groups, apply directly
            if isinstance(delimiter, re.Pattern):
                curr_buffer.delimiter = delimiter
                curr_buffer.current_columns = NlessBuffer._make_columns(
                    list(delimiter.groupindex.keys())
                )
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
                    curr_buffer.raw_rows.insert(0, curr_buffer.first_log_line)
                should_update = True
            else:
                curr_buffer.delimiter = delimiter

                result = self._resolve_new_header(
                    delimiter, prev_delimiter, curr_buffer
                )
                if result is not None:
                    new_header, parsed_full_json_file = result

                    if self._should_reinsert_header_as_data(
                        prev_delimiter, delimiter, parsed_full_json_file
                    ):
                        curr_buffer.raw_rows.insert(0, curr_buffer.first_log_line)

                    curr_buffer.current_columns = NlessBuffer._make_columns(
                        list(new_header)
                    )
                    should_update = True
        finally:
            curr_buffer._lock.release()

        if should_update:
            curr_buffer._deferred_update_table()

    def handle_column_filter_submitted(self, event: Input.Submitted) -> None:
        curr_buffer = self._get_current_buffer()
        input_value = event.value
        event.input.remove()
        if not curr_buffer._with_lock("column filter"):
            return
        try:
            if input_value.lower() == "all":
                for col in curr_buffer.current_columns:
                    col.hidden = False
            else:
                column_name_filters = [name.strip() for name in input_value.split("|")]
                column_name_filter_regexes = [
                    re.compile(rf"{name}", re.IGNORECASE)
                    for name in column_name_filters
                ]
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
        finally:
            curr_buffer._lock.release()

        curr_buffer._deferred_update_table()

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = curr_buffer._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            selected_column = curr_buffer._get_column_at_position(coordinate.column)
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                curr_buffer._get_cell_value_without_markup(selected_column.name),
            )
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def action_exclude_filter(self) -> None:
        """Exclude rows from selected column based on user input."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.columns[column_index]
        self._create_prompt(
            f"Type exclude filter text for column: {column_label} and press enter",
            "exclude_filter_input",
        )

    def action_exclude_filter_cursor_word(self) -> None:
        """Exclude rows matching the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = curr_buffer._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)
            selected_column = curr_buffer._get_column_at_position(coordinate.column)
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                curr_buffer._get_cell_value_without_markup(selected_column.name),
                exclude=True,
            )
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def refresh_buffer_and_focus(
        self,
        new_buffer: NlessBuffer,
        cursor_coordinate: Coordinate,
        offset: Offset,
    ) -> None:
        tabbed_content = self.query_one(TabbedContent)
        tabbed_content.active = f"buffer{new_buffer.pane_id}"
        try:
            data_table = new_buffer.query_one(NlessDataTable)
        except NoMatches:
            # Buffer not yet composed; retry after next refresh
            self.call_after_refresh(
                lambda: self.refresh_buffer_and_focus(
                    new_buffer, cursor_coordinate, offset
                )
            )
            return
        data_table.focus()

        def _restore_position():
            new_buffer._restore_position(
                data_table,
                cursor_coordinate.column,
                cursor_coordinate.row,
                offset.x,
                offset.y,
            )

        new_buffer._deferred_update_table(
            restore_position=False, callback=_restore_position
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

    def _switch_to_buffer(self, index: int) -> None:
        if index < 0 or index >= len(self.buffers):
            return
        self.curr_buffer_idx = index
        tabbed_content = self.query_one(TabbedContent)
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def action_show_tab_next(self) -> None:
        self._switch_to_buffer((self.curr_buffer_idx + 1) % len(self.buffers))

    def action_show_tab_previous(self) -> None:
        self._switch_to_buffer((self.curr_buffer_idx - 1) % len(self.buffers))

    def show_tab_by_index(self, index: int) -> None:
        self._switch_to_buffer(index)

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
