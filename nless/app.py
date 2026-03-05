import csv
import json
import os
import re
import subprocess
import time
from threading import Thread

from textual.binding import Binding
from textual.css.query import NoMatches
from textual.app import App, ComposeResult
from textual.coordinate import Coordinate
from textual.events import Key
from textual.geometry import Offset
from textual.widgets import (
    Input,
    Select,
    Static,
    Tab,
    TabbedContent,
    TabPane,
)

from textual.containers import Vertical

from nless.autocomplete import AutocompleteInput
from nless.buffer import NlessBuffer
from nless.buffergroup import BufferGroup
from nless.operations import handle_mark_unique, write_buffer
from nless.gettingstarted import GettingStartedScreen
from nless.suggestions import (
    ColumnValueSuggestionProvider,
    FilePathSuggestionProvider,
    HistorySuggestionProvider,
    PipeSeparatedSuggestionProvider,
    ShellCommandSuggestionProvider,
    StaticSuggestionProvider,
)

from .config import NlessConfig, load_config, load_input_history, save_config
from .procutil import get_stdin_source
from .dataprocessing import strip_markup
from .delimiter import split_line
from .help import HelpScreen
from .input import LineStream, ShellCommandLineStream, StdinLineStream
from .nlessselect import NlessSelect
from .datatable import Datatable as NlessDataTable, Coordinate as NlessCoordinate
from .keymap import get_all_keymaps, resolve_keymap
from .theme import get_all_themes, resolve_theme
from .types import CliArgs, Column, Filter, MetadataColumn


_TAB_SWITCH_KEYS = frozenset(str(i) for i in range(1, 10))


class NlessApp(App):
    inherit_bindings = False
    ENABLE_COMMAND_PALETTE = False

    def __init__(
        self,
        cli_args: CliArgs,
        starting_stream: LineStream | None,
        show_help: bool = False,
    ) -> None:
        super().__init__()
        self.cli_args = cli_args
        self.input_history = []
        self.config = NlessConfig()
        self.show_help = show_help
        self.mounted = False
        # Theme is resolved lazily on_mount once config is loaded;
        # bootstrap with CLI arg or default so buffers can reference it.
        # Named nless_theme to avoid conflict with Textual's App.theme reactive.
        self.nless_theme = resolve_theme(cli_theme=cli_args.theme)
        self.nless_keymap = resolve_keymap(cli_keymap=cli_args.keymap)
        self._next_pane_id = 2
        self._next_group_id = 2
        init_buffer = NlessBuffer(
            pane_id=1, cli_args=cli_args, line_stream=starting_stream
        )
        self.groups: list[BufferGroup] = [
            BufferGroup(
                group_id=1,
                name=self._initial_group_name(cli_args),
                buffers=[init_buffer],
                starting_stream=starting_stream,
            )
        ]
        self.curr_group_idx = 0

    @staticmethod
    def _initial_group_name(cli_args: CliArgs) -> str:
        if cli_args.filename:
            return f"📄 {os.path.basename(cli_args.filename)}"
        source = get_stdin_source()
        if source is None:
            return "stdin"
        if source.startswith("/"):
            return f"📄 {source}"
        return f"⏵ {source}"

    SCREENS = {"HelpScreen": HelpScreen, "GettingStartedScreen": GettingStartedScreen}
    HISTORY_FILE = "~/.config/nless/history.json"

    CSS_PATH = "nless.tcss"

    BINDINGS = [
        Binding("N", "add_buffer", "New Buffer", id="app.add_buffer"),
        Binding("L", "show_tab_next", "Next Buffer", id="app.show_tab_next"),
        Binding(
            "H", "show_tab_previous", "Previous Buffer", id="app.show_tab_previous"
        ),
        Binding(
            "q",
            "close_active_buffer",
            "Close Active Buffer",
            id="app.close_active_buffer",
        ),
        Binding("/", "search", "Search (all columns, by prompt)", id="app.search"),
        Binding(
            "&",
            "search_to_filter",
            "Apply current search as filter",
            id="app.search_to_filter",
        ),
        Binding(
            "|", "filter_any", "Filter any column (by prompt)", id="app.filter_any"
        ),
        Binding("f", "filter", "Filter selected column (by prompt)", id="app.filter"),
        Binding(
            "F",
            "filter_cursor_word",
            "Filter selected column by word under cursor",
            id="app.filter_cursor_word",
        ),
        Binding(
            "e",
            "exclude_filter",
            "Exclude from selected column (by prompt)",
            id="app.exclude_filter",
        ),
        Binding(
            "E",
            "exclude_filter_cursor_word",
            "Exclude selected column by word under cursor",
            id="app.exclude_filter_cursor_word",
        ),
        Binding("D", "delimiter", "Change Delimiter", id="app.delimiter"),
        Binding(
            "d",
            "column_delimiter",
            "Change Column Delimiter",
            id="app.column_delimiter",
        ),
        Binding(
            "W", "write_to_file", "Write current view to file", id="app.write_to_file"
        ),
        Binding(
            "J",
            "json_header",
            "Select new header from JSON in cell",
            id="app.json_header",
        ),
        Binding(
            "!", "run_command", "Run Shell Command (by prompt)", id="app.run_command"
        ),
        Binding(
            "U",
            "mark_unique",
            "Mark a column unique to create a composite key for distinct/analysis",
            id="app.mark_unique",
        ),
        Binding(
            "C", "filter_columns", "Filter Columns (by prompt)", id="app.filter_columns"
        ),
        Binding("T", "select_theme", "Select Theme", id="app.select_theme"),
        Binding("K", "select_keymap", "Select Keymap", id="app.select_keymap"),
        Binding("?", "help", "Show Help", id="app.help"),
        Binding("}", "show_group_next", "Next Group", id="app.show_group_next"),
        Binding(
            "{", "show_group_previous", "Previous Group", id="app.show_group_previous"
        ),
        Binding("r", "rename_buffer", "Rename Buffer", id="app.rename_buffer"),
        Binding("R", "rename_group", "Rename Group", id="app.rename_group"),
        Binding("O", "open_file", "Open File", id="app.open_file"),
        Binding(
            "@",
            "time_window",
            "Time Window (e.g. 5m, 1h, 30s)",
            id="app.time_window",
        ),
        Binding(
            "A",
            "toggle_arrival",
            "Toggle arrival timestamp column",
            id="app.toggle_arrival",
        ),
    ]

    def action_open_file(self) -> None:
        """Open a file in a new group."""
        self._create_prompt("Enter file path", "open_file_input")

    def action_run_command(self) -> None:
        """Run a shell command and pipe the output into a new buffer."""
        self._create_prompt(
            "Type shell command (e.g. tail -f /var/log/syslog)", "run_command_input"
        )

    async def handle_run_command_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        command = event.value.strip()
        if not command:
            return
        try:
            line_stream = ShellCommandLineStream(command)
            new_buffer = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=self.cli_args,
                line_stream=line_stream,
            )
            await self.add_group(f"⏵ {command}", new_buffer, stream=line_stream)
            line_stream.start()
        except (OSError, ValueError, subprocess.SubprocessError) as e:
            self.notify(f"Error running command: {str(e)}", severity="error")

    async def handle_open_file_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        path = event.value.strip()
        if not path:
            return
        try:
            cli_args = CliArgs(
                delimiter=None,
                filters=[],
                unique_keys=set(),
                sort_by=None,
                filename=path,
            )
            line_stream = StdinLineStream(cli_args, file_name=path, new_fd=None)
            new_buffer = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=cli_args,
                line_stream=line_stream,
            )
            t = Thread(target=line_stream.run, daemon=True)
            t.start()
            await self.add_group(
                f"📄 {os.path.basename(path)}", new_buffer, stream=line_stream
            )
        except (OSError, FileNotFoundError) as e:
            self.notify(f"Error opening file: {e}", severity="error")

    def on_select_changed(self, event: Select.Changed) -> None:
        try:
            event.control.remove()
        except Exception:
            pass  # NlessSelect already removes itself in on_input_submitted
        if event.control.id == "theme_select":
            self.apply_theme(str(event.value))
            return
        if event.control.id == "keymap_select":
            self.apply_keymap(str(event.value))
            return
        if event.control.id == "json_header_select":
            self._apply_json_header(str(event.value))

    def _apply_json_header(self, col_ref_value: str) -> None:
        curr_buffer = self._get_current_buffer()
        with curr_buffer._try_lock(
            "json header",
            deferred=lambda: self._apply_json_header(col_ref_value),
        ) as acquired:
            if not acquired:
                return
            data_table = curr_buffer.query_one(NlessDataTable)
            cursor_column = data_table.cursor_column
            curr_column = curr_buffer._get_column_at_position(cursor_column)
            if not curr_column:
                curr_buffer.notify(
                    "No column selected to add JSON key to", severity="error"
                )
                return
            curr_column_name = strip_markup(curr_column.name)

            col_ref = col_ref_value
            if not col_ref.startswith("."):
                col_ref = f".{col_ref}"

            new_column_name = f"{curr_column_name}{col_ref}"

            # Insert before trailing ARRIVAL metadata so it stays last
            arrival_col = next(
                (
                    c
                    for c in curr_buffer.current_columns
                    if c.name == MetadataColumn.ARRIVAL.value
                ),
                None,
            )
            new_data_position = (
                arrival_col.data_position
                if arrival_col
                else len(curr_buffer.current_columns)
            )
            new_render_position = len(
                [c for c in curr_buffer.current_columns if not c.hidden]
            )

            new_col = Column(
                name=new_column_name,
                labels=set(),
                computed=True,
                render_position=new_render_position,
                data_position=new_data_position,
                hidden=False,
                json_ref=f"{curr_column_name}{col_ref}",
                delimiter="json",
            )

            curr_buffer.current_columns.append(new_col)
            # Push ARRIVAL to the end
            if arrival_col:
                arrival_col.data_position = new_data_position + 1
                arrival_col.render_position = (
                    max(c.render_position for c in curr_buffer.current_columns) + 1
                )
            old_row = data_table.cursor_row

        curr_buffer._deferred_update_table(
            restore_position=False,
            callback=lambda: data_table.move_cursor(
                column=new_render_position, row=old_row
            ),
            reason="Adding column",
        )

    def action_json_header(self) -> None:
        """Set the column headers from JSON in the selected cell."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
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

    _DELIMITER_OPTIONS = [",", "\\t", "space", "space+", "|", ";", ":", "raw"]

    def action_column_delimiter(self) -> None:
        """Change the column delimiter."""
        self._create_prompt(
            "Type column delimiter (e.g. , or \\t or 'space' or 'raw')",
            "column_delimiter_input",
            provider=StaticSuggestionProvider(self._DELIMITER_OPTIONS),
        )

    def action_write_to_file(self) -> None:
        """Write the current view to a file."""
        self._create_prompt(
            "Type output file path (e.g. /tmp/output.csv)", "write_to_file_input"
        )

    def action_filter_columns(self) -> None:
        """Filter columns by user input."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        column_names = [strip_markup(c) for c in data_table.columns]
        self._create_prompt(
            "Type pipe delimited column names to show (e.g. col1|col2) or 'all' to reset",
            "column_filter_input",
            provider=PipeSeparatedSuggestionProvider(column_names),
        )

    @property
    def _current_group(self) -> BufferGroup:
        return self.groups[self.curr_group_idx]

    @property
    def buffers(self) -> list[NlessBuffer]:
        return self._current_group.buffers

    @property
    def curr_buffer_idx(self) -> int:
        return self._current_group.curr_buffer_idx

    @curr_buffer_idx.setter
    def curr_buffer_idx(self, value: int) -> None:
        self._current_group.curr_buffer_idx = value

    @property
    def all_buffers(self) -> list[NlessBuffer]:
        return [buf for group in self.groups for buf in group.buffers]

    def _get_new_pane_id(self) -> int:
        pane_id = self._next_pane_id
        self._next_pane_id += 1
        return pane_id

    def _get_active_tabbed_content(self) -> TabbedContent:
        container = self.query_one(f"#group_{self._current_group.group_id}")
        return container.query_one(TabbedContent)

    def _copy_buffer_async(
        self,
        setup_fn,
        buffer_name,
        after_add_fn=None,
        add_prev_index=True,
        reason="Loading",
        done_reason="Loaded",
    ):
        """Run copy() + setup on a background thread, then add_buffer on main thread."""
        curr_buffer = self._get_current_buffer()
        source_group_idx = self.curr_group_idx
        new_pane_id = self._get_new_pane_id()
        source_row_count = len(curr_buffer.displayed_rows)
        reason = f"{reason} {source_row_count:,} rows"
        curr_buffer._loading_reason = reason
        curr_buffer._start_spinner()
        curr_buffer._update_status_bar()

        def _run():
            try:
                new_buffer = curr_buffer.copy(pane_id=new_pane_id)
                setup_fn(new_buffer)
            except Exception as e:
                msg = str(e)

                def _on_error():
                    curr_buffer._loading_reason = None
                    curr_buffer._stop_spinner()
                    curr_buffer._update_status_bar()
                    self.notify(f"Error: {msg}", severity="error")

                self.call_from_thread(_on_error)
                return

            def _finish():
                curr_buffer._loading_reason = None
                curr_buffer._stop_spinner()
                curr_buffer._update_status_bar()

                def _on_ready():
                    if after_add_fn:
                        after_add_fn(new_buffer)
                    result_rows = len(new_buffer.displayed_rows)
                    new_buffer._flash_status(
                        f"{done_reason} {source_row_count:,} → {result_rows:,} rows"
                    )
                    # Notify if the user isn't viewing the newly created buffer.
                    if self._get_current_buffer() is not new_buffer:
                        group_name = self.groups[source_group_idx].name
                        self.notify(
                            f"Buffer ready: {buffer_name} ({group_name})",
                            severity="information",
                        )

                self.add_buffer(
                    new_buffer,
                    name=buffer_name,
                    add_prev_index=add_prev_index,
                    on_ready=_on_ready,
                    reason=reason,
                )

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

        unique_column_name = strip_markup(selected_column.name)

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
                if strip_markup(col.name) == unique_column_name:
                    new_cursor_position = i
                    break
            pos = new_cursor_position
            self.set_timer(
                0.3,
                lambda: new_buffer.query_one(NlessDataTable).move_cursor(column=pos),
            )

        self._copy_buffer_async(
            setup,
            buffer_name,
            after_add_fn=after_add,
            reason="Pivoting",
            done_reason="Pivoted",
        )

    def action_delimiter(self) -> None:
        """Change the delimiter used for parsing."""
        self._create_prompt(
            "Type delimiter character (e.g. ',', '\\t', ' ', '|') or 'raw' for no parsing",
            "delimiter_input",
            provider=StaticSuggestionProvider(self._DELIMITER_OPTIONS),
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

        self._copy_buffer_async(
            setup, buffer_name, reason="Filtering", done_reason="Filtered"
        )

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        self._create_prompt("Type search term and press Enter", "search_input")

    def _create_prompt(self, placeholder, id, provider=None):
        history = [h["val"] for h in self.input_history if h["id"] == id]
        if provider is None:
            if id in ("write_to_file_input", "open_file_input"):
                provider = FilePathSuggestionProvider()
            elif id == "run_command_input":
                provider = ShellCommandSuggestionProvider(history)
            else:
                provider = HistorySuggestionProvider(history)
        input = AutocompleteInput(
            placeholder=placeholder,
            id=id,
            classes="bottom-input",
            history=history,
            provider=provider,
            on_add=lambda val: self.input_history.append({"id": id, "val": val}),
            on_remove=lambda val: self.input_history.remove({"id": id, "val": val}),
        )
        tabbed_content = self._get_active_tabbed_content()
        active_tab = tabbed_content.active
        for tab_pane in tabbed_content.query(TabPane):
            if tab_pane.id == active_tab:
                tab_pane.mount(input)
                self.call_after_refresh(lambda: input.focus())
                break

    def _filter_composite_key(self, current_buffer: NlessBuffer) -> None:
        data_table = current_buffer.query_one(NlessDataTable)
        cursor_column = data_table.cursor_column
        selected_column = current_buffer._get_column_at_position(cursor_column)
        if selected_column:
            selected_column_name = strip_markup(selected_column.name)
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
                    cell_value = strip_markup(cell_value)
                    filters.append(
                        Filter(
                            column=strip_markup(column),
                            pattern=re.compile(re.escape(cell_value), re.IGNORECASE),
                        )
                    )
                buffer_name = f"+f:{','.join([f'{f.column}={f.pattern.pattern}' for f in filters])}"

                def setup(new_buffer):
                    for column in unique_columns:
                        handle_mark_unique(new_buffer, column)
                    new_buffer.current_filters.extend(filters)

                self._copy_buffer_async(
                    setup, buffer_name, reason="Filtering", done_reason="Filtered"
                )

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if event.key == "escape" and isinstance(self.focused, Input):
            if isinstance(self.focused.parent, AutocompleteInput):
                self.focused.parent.remove()
            else:
                self.focused.remove()
        elif event.key == "escape" and isinstance(self.focused, Select):
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
        # Insert before trailing metadata (ARRIVAL) so it stays last
        arrival_col = next(
            (
                c
                for c in buffer.current_columns
                if c.name == MetadataColumn.ARRIVAL.value
            ),
            None,
        )
        base_pos = (
            arrival_col.data_position if arrival_col else len(buffer.current_columns)
        )
        added = 0
        for i, name in enumerate(column_names):
            if name not in existing:
                buffer.current_columns.append(make_column_fn(i, name, base_pos + added))
                added += 1
        # Push ARRIVAL to the end
        if arrival_col and added > 0:
            arrival_col.data_position = base_pos + added
            arrival_col.render_position = (
                max(c.render_position for c in buffer.current_columns) + 1
            )

    def handle_column_delimiter_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        event.input.remove()
        self._apply_column_delimiter(event.value)

    def _apply_column_delimiter(self, new_col_delimiter: str) -> None:
        if not new_col_delimiter:
            return
        current_buffer = self._get_current_buffer()
        should_update = False
        with current_buffer._try_lock(
            "column delimiter",
            deferred=lambda: self._apply_column_delimiter(new_col_delimiter),
        ) as acquired:
            if not acquired:
                return
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
                    cell_json = json.loads(strip_markup(cell))
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
                elif new_col_delimiter == "space":
                    new_col_delimiter = " "
                elif new_col_delimiter == "space+":
                    new_col_delimiter = "  "

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
                            strip_markup(cell),
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

        if should_update:
            current_buffer._deferred_update_table(reason="Splitting column")

    def handle_write_to_file_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        output_path = event.value
        event.input.remove()
        if not output_path.strip():
            return
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

        self.run_worker(_write_and_notify, thread=True)
        if output_path == "-":
            self.exit()

    async def on_autocomplete_input_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
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
            await self.handle_run_command_submitted(event)
        elif event.input.id == "open_file_input":
            await self.handle_open_file_submitted(event)
        elif event.input.id == "rename_group_input":
            self.handle_rename_group_submitted(event)
        elif event.input.id == "rename_buffer_input":
            self.handle_rename_buffer_submitted(event)
        elif event.input.id == "time_window_input":
            self.handle_time_window_submitted(event)

    def handle_search_submitted(self, event: AutocompleteInput.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()
        current_buffer._perform_search(input_value)

    def _get_current_buffer(self) -> NlessBuffer:
        return self.buffers[self.curr_buffer_idx]

    def _get_column_values(self, column_index: int) -> list[str]:
        """Get unique values for a column, ordered by frequency (most common first)."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        counts: dict[str, int] = {}
        for row in data_table.rows:
            if column_index < len(row):
                val = strip_markup(row[column_index])
                if val:
                    counts[val] = counts.get(val, 0) + 1
        return sorted(counts, key=lambda v: counts[v], reverse=True)

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.columns[column_index]
        provider = ColumnValueSuggestionProvider(self._get_column_values(column_index))
        self._create_prompt(
            f"Type filter text for column: {column_label} and press enter",
            "filter_input",
            provider=provider,
        )

    def action_filter_any(self) -> None:
        """Filter any column based on user input."""
        self._create_prompt(
            "Type filter text to match across all columns", "filter_input_any"
        )

    def action_toggle_arrival(self) -> None:
        """Toggle visibility of the arrival timestamp column, pinned to the left."""
        buf = self._get_current_buffer()
        arrival_col = next(
            (c for c in buf.current_columns if c.name == MetadataColumn.ARRIVAL.value),
            None,
        )
        if arrival_col is None:
            return

        if arrival_col.hidden:
            # Show and pin to the left
            arrival_col.hidden = False
            arrival_col.pinned = True
            # Find the first non-pinned render_position to insert before
            pinned_count = sum(
                1
                for c in buf.current_columns
                if c.pinned and not c.hidden and c.name != MetadataColumn.ARRIVAL.value
            )
            # Shift non-pinned columns right by 1
            for c in buf.current_columns:
                if (
                    c.name != MetadataColumn.ARRIVAL.value
                    and not c.pinned
                    and c.render_position >= pinned_count
                ):
                    c.render_position += 1
            arrival_col.render_position = pinned_count
        else:
            # Hide and unpin
            old_pos = arrival_col.render_position
            arrival_col.hidden = True
            arrival_col.pinned = False
            arrival_col.render_position = 99999
            # Close the gap
            for c in buf.current_columns:
                if (
                    c.render_position > old_pos
                    and c.name != MetadataColumn.ARRIVAL.value
                ):
                    c.render_position -= 1

        buf._parsed_rows = None
        buf._cached_col_widths = None
        buf._deferred_update_table(reason="Toggling arrival column")

    def action_time_window(self) -> None:
        """Set a time window to only show rows from the last N minutes/hours/seconds."""
        curr = self._get_current_buffer()
        current = self._format_window(curr.time_window, curr.rolling_time_window)
        hint = f"e.g. 5m, 1h, 30s — append + for rolling (current: {current})"
        self._create_prompt(
            f"Enter time window — {hint}",
            "time_window_input",
        )

    @staticmethod
    def _format_window(seconds: float | None, rolling: bool = False) -> str:
        """Format a time window duration for display."""
        if not seconds:
            return "off"
        parts = []
        remaining = seconds
        if remaining >= 86400:
            d = int(remaining // 86400)
            parts.append(f"{d}d")
            remaining %= 86400
        if remaining >= 3600:
            h = int(remaining // 3600)
            parts.append(f"{h}h")
            remaining %= 3600
        if remaining >= 60:
            m = int(remaining // 60)
            parts.append(f"{m}m")
            remaining %= 60
        if remaining > 0 and not parts:
            parts.append(f"{int(remaining)}s")
        result = "".join(parts)
        if rolling:
            result += " (rolling)"
        return result

    def handle_time_window_submitted(self, event: AutocompleteInput.Submitted) -> None:
        value = event.value.strip()
        event.input.remove()
        curr_buffer = self._get_current_buffer()

        if not value or value in ("0", "off", "clear", "none"):
            curr_buffer.time_window = None
            curr_buffer.rolling_time_window = False
            curr_buffer._stop_rolling_timer()
            curr_buffer._parsed_rows = None
            curr_buffer._cached_col_widths = None
            curr_buffer._deferred_update_table(reason="Clearing time window")
            return

        rolling = value.endswith("+")
        if rolling:
            value = value.rstrip("+").strip()

        duration = NlessBuffer._parse_duration(value)
        if duration is None:
            self.notify(
                "Invalid duration. Use e.g. 5m, 1h, 30s, 2h30m (+ for rolling)",
                severity="error",
            )
            return

        curr_buffer.time_window = duration
        curr_buffer.rolling_time_window = rolling
        curr_buffer._parsed_rows = None
        curr_buffer._cached_col_widths = None
        if rolling:
            curr_buffer._deferred_update_table(reason="Applying time window")
            curr_buffer._start_rolling_timer()
        else:
            curr_buffer._stop_rolling_timer()

            # One-shot: prune raw_rows permanently then clear time_window
            # so subsequent rebuilds (sort, filter) don't re-evaluate
            def _finalize_one_shot():
                cutoff = time.time() - duration
                kept = [
                    (row, ts)
                    for row, ts in zip(
                        curr_buffer.raw_rows, curr_buffer._arrival_timestamps
                    )
                    if ts >= cutoff
                ]
                if kept:
                    curr_buffer.raw_rows, curr_buffer._arrival_timestamps = [
                        list(x) for x in zip(*kept)
                    ]
                else:
                    curr_buffer.raw_rows = []
                    curr_buffer._arrival_timestamps = []
                curr_buffer.time_window = None

            curr_buffer._deferred_update_table(
                reason="Applying time window", callback=_finalize_one_shot
            )

    def handle_filter_submitted(self, event: AutocompleteInput.Submitted) -> None:
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
            column_label = strip_markup(column.name)
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
                    severity="information",
                )

        self._copy_buffer_async(
            setup,
            new_buf_name,
            after_add_fn=after_add,
            reason="Filtering",
            done_reason="Filtered",
        )

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
        if value == "space":
            return " "
        if value == "space+":
            return "  "
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

    def handle_delimiter_submitted(self, event: AutocompleteInput.Submitted) -> None:
        event.input.remove()
        self._apply_delimiter(event.value)

    def _apply_delimiter(self, delimiter_input: str) -> None:
        if not delimiter_input:
            return
        curr_buffer = self._get_current_buffer()
        should_update = False
        with curr_buffer._try_lock(
            "delimiter",
            deferred=lambda: self._apply_delimiter(delimiter_input),
        ) as acquired:
            if not acquired:
                return
            had_filters = bool(curr_buffer.current_filters)
            curr_buffer.current_filters = []
            curr_buffer.search_term = None
            curr_buffer.sort_column = None
            curr_buffer.unique_column_names = set()
            prev_delimiter = curr_buffer.delimiter

            curr_buffer._reset_delimiter_state()
            delimiter = self._parse_delimiter_input(delimiter_input)

            # If it's a regex with named groups, apply directly
            if isinstance(delimiter, re.Pattern):
                curr_buffer.delimiter = delimiter
                curr_buffer.current_columns = NlessBuffer._make_columns(
                    list(delimiter.groupindex.keys())
                )
                NlessBuffer._ensure_arrival_column(curr_buffer.current_columns)
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
                    NlessBuffer._ensure_arrival_column(curr_buffer.current_columns)
                    should_update = True

        if should_update:

            def callback():
                if had_filters:
                    curr_buffer._flash_status("Filters cleared — delimiter changed")

            curr_buffer._deferred_update_table(
                reason="Changing delimiter", callback=callback
            )

    def handle_column_filter_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        input_value = event.value
        event.input.remove()
        self._apply_column_filter(input_value)

    def _apply_column_filter(self, input_value: str) -> None:
        if not input_value.strip():
            return
        curr_buffer = self._get_current_buffer()
        with curr_buffer._try_lock(
            "column filter",
            deferred=lambda: self._apply_column_filter(input_value),
        ) as acquired:
            if not acquired:
                return
            if input_value.lower() == "all":
                metadata_names = {mc.value for mc in MetadataColumn}
                for col in curr_buffer.current_columns:
                    if col.name not in metadata_names or col.pinned:
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
                    plain_name = strip_markup(col.name)
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

        curr_buffer._deferred_update_table(reason="Filtering columns")

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            selected_column = curr_buffer._get_column_at_position(coordinate.column)
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                strip_markup(selected_column.name),
            )
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def action_exclude_filter(self) -> None:
        """Exclude rows from selected column based on user input."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.columns[column_index]
        provider = ColumnValueSuggestionProvider(self._get_column_values(column_index))
        self._create_prompt(
            f"Type exclude filter text for column: {column_label} and press enter",
            "exclude_filter_input",
            provider=provider,
        )

    def action_exclude_filter_cursor_word(self) -> None:
        """Exclude rows matching the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            cell_value = re.escape(cell_value)
            selected_column = curr_buffer._get_column_at_position(coordinate.column)
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                strip_markup(selected_column.name),
                exclude=True,
            )
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def refresh_buffer_and_focus(
        self,
        new_buffer: NlessBuffer,
        cursor_coordinate: Coordinate,
        offset: Offset,
        on_ready=None,
        reason="Loading",
    ) -> None:
        tabbed_content = self._get_active_tabbed_content()
        tabbed_content.active = f"buffer{new_buffer.pane_id}"
        try:
            data_table = new_buffer.query_one(NlessDataTable)
        except NoMatches:
            # Buffer not yet composed; retry after next refresh
            self.call_after_refresh(
                lambda: self.refresh_buffer_and_focus(
                    new_buffer,
                    cursor_coordinate,
                    offset,
                    on_ready=on_ready,
                    reason=reason,
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
            if on_ready:
                on_ready()

        new_buffer._deferred_update_table(
            restore_position=False, callback=_restore_position, reason=reason
        )

    def add_buffer(
        self,
        new_buffer: NlessBuffer,
        name: str,
        add_prev_index: bool = True,
        on_ready=None,
        reason="Loading",
    ) -> None:
        curr_data_table = self._get_current_buffer().query_one(NlessDataTable)

        self.buffers.append(new_buffer)
        tabbed_content = self._get_active_tabbed_content()
        buffer_number = len(self.buffers)
        tab_pane = TabPane(
            f"{self.nless_theme.markup('accent', str(buffer_number))} {self.curr_buffer_idx + 1 if add_prev_index else ''}{name}",
            id=f"buffer{new_buffer.pane_id}",
        )
        tabbed_content.add_pane(tab_pane)
        tab_pane.mount(new_buffer)
        self.curr_buffer_idx = len(self.buffers) - 1
        self.call_after_refresh(
            lambda: self.refresh_buffer_and_focus(
                new_buffer,
                curr_data_table.cursor_coordinate,
                curr_data_table.scroll_offset,
                on_ready=on_ready,
                reason=reason,
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
            if len(self.groups) == 1:
                self.exit()
                return
            self._close_current_group()
            return

        tabbed_content = self._get_active_tabbed_content()
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

    def _update_panes(self, group: BufferGroup | None = None) -> None:
        group = group or self._current_group
        try:
            container = self.query_one(f"#group_{group.group_id}")
            tabbed_content = container.query_one(TabbedContent)
        except NoMatches:
            return
        pattern = re.compile(r"((\[#[0-9a-fA-F]+\])?(\d+?)(\[/#[0-9a-fA-F]+\])?) .*")
        for i, pane in enumerate(tabbed_content.query(Tab).results()):
            curr_title = str(pane.content)
            pattern_matches = pattern.match(curr_title)
            if pattern_matches:
                old_index = pattern_matches.group(1)
                curr_title = curr_title.replace(
                    old_index,
                    self.nless_theme.markup("accent", str(i + 1)),
                    count=1,
                )
                pane.update(curr_title)

    def _switch_to_buffer(self, index: int) -> None:
        if index < 0 or index >= len(self.buffers):
            return
        self.curr_buffer_idx = index
        tabbed_content = self._get_active_tabbed_content()
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

    # ── Group management ──────────────────────────────────────────────

    async def add_group(
        self,
        name: str,
        first_buffer: NlessBuffer,
        stream: LineStream | None = None,
    ) -> None:
        group_id = self._next_group_id
        self._next_group_id += 1
        group = BufferGroup(
            group_id=group_id,
            name=name,
            buffers=[first_buffer],
            starting_stream=stream,
        )
        self.groups.append(group)

        # When going from 1 to 2 groups, rename the first buffer in the
        # original group to "original" so the user can distinguish them.
        if len(self.groups) == 2:
            self._rename_first_buffer(self.groups[0], "original")

        # Build DOM: Vertical > TabbedContent > TabPane > buffer
        container = Vertical(id=f"group_{group_id}", classes="buffer-group")
        tabbed_content = TabbedContent()
        tab_pane = TabPane(
            f"{self.nless_theme.markup('accent', '1')} {name}",
            id=f"buffer{first_buffer.pane_id}",
        )
        # Mount container before status_bar so it stays above it
        await self.mount(container, before=self.query_one("#status_bar"))
        container.display = False
        await container.mount(tabbed_content)
        await tabbed_content.add_pane(tab_pane)
        await tab_pane.mount(first_buffer)

        self._switch_to_group(len(self.groups) - 1)

    def _switch_to_group(self, index: int) -> None:
        if index < 0 or index >= len(self.groups):
            return
        # Hide current group container
        old_container = self.query_one(f"#group_{self._current_group.group_id}")
        old_container.display = False

        self.curr_group_idx = index

        # Show new group container
        new_container = self.query_one(f"#group_{self._current_group.group_id}")
        new_container.display = True

        # Focus active buffer in new group
        current_buffer = self._get_current_buffer()
        try:
            current_buffer.query_one(NlessDataTable).focus()
        except NoMatches:
            pass
        self.call_after_refresh(lambda: current_buffer._update_status_bar())
        self._update_group_bar()

    def _close_current_group(self) -> None:
        group = self._current_group
        # Unsubscribe all buffers
        for buf in group.buffers:
            if buf.line_stream:
                buf.line_stream.unsubscribe(buf)

        # Remove DOM container
        container = self.query_one(f"#group_{group.group_id}")
        container.remove()

        self.groups.pop(self.curr_group_idx)
        if self.curr_group_idx >= len(self.groups):
            self.curr_group_idx = len(self.groups) - 1

        # Show adjacent group
        new_container = self.query_one(f"#group_{self._current_group.group_id}")
        new_container.display = True

        current_buffer = self._get_current_buffer()
        try:
            current_buffer.query_one(NlessDataTable).focus()
        except NoMatches:
            pass
        self.call_after_refresh(lambda: current_buffer._update_status_bar())
        self._update_group_bar()

        # When back to 1 group, restore the first buffer name to the group name.
        if len(self.groups) == 1:
            self._rename_first_buffer(self.groups[0], self.groups[0].name)

    # Unicode figure space — used as a blank icon placeholder during animation
    # so regex replacement doesn't accidentally match normal spaces.
    _ICON_BLANK = "\u2007"
    _ICON_CHARS = "⏵📄✓\u2007"

    @staticmethod
    def _get_group_source_icon(group: BufferGroup) -> str | None:
        """Return the source icon character (⏵ or 📄) from a group name."""
        for char in ("⏵", "📄"):
            if char in group.name:
                return char
        return None

    def _resolve_icon(self, group: BufferGroup, animate: bool = False) -> str:
        """Return the resolved icon character for a group's current state."""
        source = self._get_group_source_icon(group)
        if source is None:
            return ""
        if (
            source == "⏵"
            and group.starting_stream is not None
            and group.starting_stream.done
        ):
            return "✓"
        if source == "⏵" and animate and self._group_bar_frame % 2 != 0:
            return self._ICON_BLANK
        return source

    def _rename_first_buffer(self, group: BufferGroup, name: str) -> None:
        """Rename the first buffer tab in a group, prepending the group icon."""
        try:
            container = self.query_one(f"#group_{group.group_id}")
            tabbed_content = container.query_one(TabbedContent)
            first_tab = next(tabbed_content.query(Tab).results())
            # Strip any existing icon prefix from the name
            for prefix in ("⏵ ", "📄 ", "✓ ", f"{self._ICON_BLANK} "):
                if name.startswith(prefix):
                    name = name[len(prefix) :]
                    break
            icon = self._resolve_icon(group)
            icon_prefix = f"{icon} " if icon else ""
            first_tab.update(
                f"{self.nless_theme.markup('accent', '1')} {icon_prefix}{name}"
            )
        except (NoMatches, StopIteration):
            pass

    def _refresh_first_buffer_icon(
        self, group: BufferGroup, animate: bool = False
    ) -> None:
        """Refresh the icon on the first buffer tab without changing the name."""
        try:
            container = self.query_one(f"#group_{group.group_id}")
            tabbed_content = container.query_one(TabbedContent)
            first_tab = next(tabbed_content.query(Tab).results())
            content = str(first_tab.content)
            # Strip Rich markup, index number, and icon to get the plain name
            name = re.sub(r"\[/?[^\]]*\]", "", content)
            name = re.sub(r"^\d+\s*", "", name)
            for prefix in ("⏵ ", "📄 ", "✓ ", f"{self._ICON_BLANK} "):
                if name.startswith(prefix):
                    name = name[len(prefix) :]
                    break
            # Rebuild with the resolved icon
            icon = self._resolve_icon(group, animate=animate)
            icon_prefix = f"{icon} " if icon else ""
            first_tab.update(
                f"{self.nless_theme.markup('accent', '1')} {icon_prefix}{name}"
            )
        except (NoMatches, StopIteration):
            pass

    _group_bar_frame: int = 0
    _group_bar_timer = None

    def _group_is_streaming(self, group: BufferGroup) -> bool:
        return group.starting_stream is not None and not group.starting_stream.done

    def _build_group_bar(self) -> str:
        t = self.nless_theme
        parts = []
        for i, group in enumerate(self.groups):
            name = group.name
            source = self._get_group_source_icon(group)
            if source is not None:
                icon = self._resolve_icon(group, animate=(i == self.curr_group_idx))
                name = name.replace(source, icon, 1)
            if i == self.curr_group_idx:
                parts.append(f"[bold {t.accent}]\\[{name}][/bold {t.accent}]")
            else:
                parts.append(f"[{t.muted}]{name}[/{t.muted}]")
        return " " + "   ".join(parts)

    def _tick_group_bar(self) -> None:
        self._group_bar_frame += 1
        has_active = any(self._group_is_streaming(g) for g in self.groups)
        if has_active and len(self.groups) > 1:
            bar = self.query_one("#group_bar", Static)
            bar.update(self._build_group_bar())
            # Also update the first buffer tab icon in the current group
            group = self._current_group
            if self._get_group_source_icon(group) is not None:
                self._refresh_first_buffer_icon(
                    group, animate=self._group_is_streaming(group)
                )
        else:
            self._stop_group_bar_timer()
            # Final update to show ✓ on completed streams
            if len(self.groups) > 1:
                bar = self.query_one("#group_bar", Static)
                bar.update(self._build_group_bar())
                for group in self.groups:
                    if self._get_group_source_icon(group) == "⏵":
                        self._refresh_first_buffer_icon(group)

    def _start_group_bar_timer(self) -> None:
        if self._group_bar_timer is None:
            self._group_bar_timer = self.set_interval(0.8, self._tick_group_bar)

    def _stop_group_bar_timer(self) -> None:
        if self._group_bar_timer is not None:
            self._group_bar_timer.stop()
            self._group_bar_timer = None

    def _update_group_bar(self) -> None:
        try:
            bar = self.query_one("#group_bar", Static)
        except NoMatches:
            return
        if len(self.groups) > 1:
            t = self.nless_theme
            bar.update(self._build_group_bar())
            bar.styles.border = ("round", t.muted)
            bar.styles.height = "auto"
            if any(self._group_is_streaming(g) for g in self.groups):
                self._start_group_bar_timer()
        else:
            bar.styles.height = 0
            bar.styles.border = None
            self._stop_group_bar_timer()

    def action_rename_buffer(self) -> None:
        self._create_prompt("Enter new buffer name", "rename_buffer_input")

    def handle_rename_buffer_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        name = event.value.strip()
        if name:
            tabbed_content = self._get_active_tabbed_content()
            idx = self.curr_buffer_idx
            tab_label = f"{self.nless_theme.markup('accent', str(idx + 1))} {name}"
            for i, tab in enumerate(tabbed_content.query(Tab).results()):
                if i == idx:
                    tab.update(tab_label)
                    break

    def action_rename_group(self) -> None:
        self._create_prompt("Enter new group name", "rename_group_input")

    def handle_rename_group_submitted(self, event: AutocompleteInput.Submitted) -> None:
        event.control.remove()
        name = event.value.strip()
        if name:
            self._current_group.name = name
            self._update_group_bar()

    def action_show_group_next(self) -> None:
        if len(self.groups) <= 1:
            return
        self._switch_to_group((self.curr_group_idx + 1) % len(self.groups))

    def action_show_group_previous(self) -> None:
        if len(self.groups) <= 1:
            return
        self._switch_to_group((self.curr_group_idx - 1) % len(self.groups))

    def on_mount(self) -> None:
        self.mounted = True

        self.config = load_config()
        self.input_history = load_input_history()

        # Re-resolve theme now that config is loaded (CLI arg still wins)
        new_theme = resolve_theme(
            cli_theme=self.cli_args.theme, config_theme=self.config.theme
        )
        if new_theme.name != self.nless_theme.name:
            self.apply_theme(new_theme.name, notify=False)

        # Re-resolve keymap now that config is loaded (CLI arg still wins)
        new_keymap = resolve_keymap(
            cli_keymap=self.cli_args.keymap, config_keymap=self.config.keymap
        )
        if new_keymap.name != self.nless_keymap.name:
            self.apply_keymap(new_keymap.name, notify=False)
        elif new_keymap.bindings:
            self.set_keymap(new_keymap.bindings)

        if self.show_help and self.config.show_getting_started:
            self.push_screen(GettingStartedScreen())
        else:
            self.query_one(NlessDataTable).focus()

    def action_help(self) -> None:
        """Show the help screen."""
        self.push_screen(
            HelpScreen(
                keymap_name=self.nless_keymap.name,
                keymap_bindings=self.nless_keymap.bindings,
                theme=self.nless_theme,
                config=self.config,
            )
        )

    def action_select_keymap(self) -> None:
        """Open a select widget to pick a keymap."""
        all_keymaps = get_all_keymaps()
        options = [(name, name) for name in sorted(all_keymaps)]
        select = NlessSelect(
            options=options,
            prompt="Select a keymap",
            classes="dock-bottom",
            id="keymap_select",
        )
        self._get_current_buffer().mount(select)

    def apply_keymap(self, name: str, *, notify: bool = True) -> None:
        """Apply a keymap by name and save to config."""
        all_keymaps = get_all_keymaps()
        new_keymap = all_keymaps.get(name)
        if new_keymap is None:
            self.notify(f"Unknown keymap: {name}", severity="error")
            return
        self.nless_keymap = new_keymap
        self.set_keymap(new_keymap.bindings)

        # Update title bar (help key may have changed) and status bar
        self._update_title_bar()
        self._get_current_buffer()._update_status_bar()

        # Save to config
        self.config.keymap = name
        save_config(self.config)

        if notify:
            self.notify(f"Keymap: {name}")

    def action_select_theme(self) -> None:
        """Open a select widget to pick a theme."""
        all_themes = get_all_themes()
        options = [(name, name) for name in sorted(all_themes)]
        select = NlessSelect(
            options=options,
            prompt="Select a theme",
            classes="dock-bottom",
            id="theme_select",
        )
        self._get_current_buffer().mount(select)

    def apply_theme(self, name: str, *, notify: bool = True) -> None:
        """Apply a theme by name, update all buffers, and save to config."""
        all_themes = get_all_themes()
        new_theme = all_themes.get(name)
        if new_theme is None:
            self.notify(f"Unknown theme: {name}", severity="error")
            return
        self.nless_theme = new_theme

        # Rebuild each buffer so search highlights, new-row highlights,
        # and datatable styles all pick up the new theme colors.
        for buf in self.all_buffers:
            try:
                # Clear cached highlight tags so they pick up the new color
                buf.__dict__.pop("_highlight_tags", None)
                data_table = buf.query_one(NlessDataTable)
                data_table.apply_theme(new_theme)
                buf._deferred_update_table(
                    restore_position=True, reason="Applying theme"
                )
            except Exception:
                pass

        # Re-render tab labels with new accent color in all groups
        for group in self.groups:
            self._update_panes(group)

        # Update group bar colors
        self._update_group_bar()

        # Update title bar (theme colors changed) and status bar
        self._update_title_bar()
        self._get_current_buffer()._update_status_bar()

        # Save to config
        self.config.theme = name
        save_config(self.config)

        if notify:
            self.notify(f"Theme: {name}")

    def action_add_buffer(self) -> None:
        new_pane_id = self._get_new_pane_id()
        new_buffer = NlessBuffer(
            pane_id=new_pane_id,
            cli_args=self.cli_args,
            line_stream=self._current_group.starting_stream,
        )
        self.add_buffer(
            new_buffer, name=f"buffer{new_buffer.pane_id}", add_prev_index=False
        )

    def _build_title_bar(self) -> str:
        """Build the title bar text with app name and help hint."""
        help_key = "?"
        bindings = self.nless_keymap.bindings
        if "app.help" in bindings:
            help_key = bindings["app.help"].split(",")[0]
        name = self.nless_theme.markup("brand", "nless")
        hint = self.nless_theme.markup("muted", f"{help_key} help")
        return f" {name}  {hint}"

    def _update_title_bar(self) -> None:
        """Refresh the title bar content."""
        try:
            title = self.query_one("#title_bar", Static)
            title.update(self._build_title_bar())
        except Exception:
            pass

    def compose(self) -> ComposeResult:
        with Vertical(id="top_bar"):
            yield Static(self._build_title_bar(), id="title_bar")
            yield Static(id="group_bar")
        init_buffer = self.groups[0].buffers[0]
        with Vertical(id="group_1", classes="buffer-group"):
            with TabbedContent():
                with TabPane(
                    f"{self.nless_theme.markup('accent', '1')} {self.groups[0].name}",
                    id=f"buffer{init_buffer.pane_id}",
                ):
                    yield init_buffer

        yield Static(id="status_bar", classes="dock-bottom")
