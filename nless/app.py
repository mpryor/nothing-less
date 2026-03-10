import csv
import json
import logging
import os
import re
import subprocess
from collections.abc import Callable
from threading import Thread

from textual.binding import Binding
from textual.css.query import NoMatches
from textual.dom import DOMError
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
    FilePathSuggestionProvider,
    HistorySuggestionProvider,
    ShellCommandSuggestionProvider,
    StaticSuggestionProvider,
)

from .config import NlessConfig, load_config, load_input_history, save_config
from .procutil import get_stdin_source
from .dataprocessing import strip_markup
from .help import HelpScreen
from .input import LineStream, ShellCommandLineStream, StdinLineStream
from .nlessselect import NlessSelect
from .datatable import Coordinate as NlessCoordinate
from .keymap import get_all_keymaps, resolve_keymap
from .theme import get_all_themes, resolve_theme
from .types import CliArgs, Filter, MetadataColumn
from .app_columns import ColumnOpsMixin
from .app_filters import FilterMixin
from .app_groups import GroupMixin
from .buffer_delimiter import _sample_lines
from .logformats import detect_log_format, save_custom_format
from .regex_wizard import RegexWizardMixin

logger = logging.getLogger(__name__)


_TAB_SWITCH_KEYS = frozenset(str(i) for i in range(1, 10))


class NlessApp(RegexWizardMixin, ColumnOpsMixin, FilterMixin, GroupMixin, App):
    inherit_bindings = False
    ENABLE_COMMAND_PALETTE = False
    _TAB_LABEL_RE = re.compile(r"((\[#[0-9a-fA-F]+\])?(\d+?)(\[/#[0-9a-fA-F]+\])?) .*")

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
        self._regex_wizard_state = None
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

    def exit(self, *args, **kwargs) -> None:
        """Unsubscribe all buffers from their streams before exiting."""
        for buf in getattr(self, "all_buffers", []):
            if buf.line_stream:
                buf.line_stream.unsubscribe(buf)
        super().exit(*args, **kwargs)

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
        Binding(
            "P",
            "detect_log_format",
            "Auto-detect log format",
            id="app.detect_log_format",
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
        except (NoMatches, DOMError):
            pass  # NlessSelect already removes itself in on_input_submitted
        if event.control.id == "theme_select":
            self.apply_theme(str(event.value))
            return
        if event.control.id == "keymap_select":
            self.apply_keymap(str(event.value))
            return
        if event.control.id == "json_header_select":
            self._apply_json_header(str(event.value))

    def action_write_to_file(self) -> None:
        """Write the current view to a file."""
        self._create_prompt(
            "Type output file path (e.g. /tmp/output.csv)", "write_to_file_input"
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
        curr_buffer.start_loading(reason)

        def _run():
            try:
                new_buffer = curr_buffer.copy(pane_id=new_pane_id)
                setup_fn(new_buffer)
            except Exception as e:
                msg = str(e)

                def _on_error():
                    curr_buffer.stop_loading()
                    self.notify(f"Error: {msg}", severity="error")

                self.call_from_thread(_on_error)
                return

            def _finish():
                curr_buffer.stop_loading()

                def _on_ready():
                    if after_add_fn:
                        after_add_fn(new_buffer)
                    result_rows = len(new_buffer.displayed_rows)
                    new_buffer._flash_status(
                        f"{done_reason} {source_row_count:,} → {result_rows:,} rows"
                    )
                    # Notify if the user isn't viewing the newly created buffer.
                    if self._get_current_buffer() is not new_buffer:
                        group_name = (
                            self.groups[source_group_idx].name
                            if source_group_idx < len(self.groups)
                            else "unknown"
                        )
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
        data_table = curr_buffer.query_one(".nless-view")
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
                lambda: new_buffer.query_one(".nless-view").move_cursor(column=pos),
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
        history = [h["val"] for h in self.input_history if h["id"] == "delimiter_input"]
        self._create_prompt(
            "Type delimiter character (e.g. ',', '\\t', ' ', '|') or 'raw' for no parsing",
            "delimiter_input",
            provider=StaticSuggestionProvider(self._DELIMITER_OPTIONS, history=history),
        )

    def action_detect_log_format(self) -> None:
        """Sample data and auto-detect a known log format."""
        buffer = self._get_current_buffer()
        all_lines: list[str] = []
        if buffer.delimiter not in ("raw",) and not isinstance(
            buffer.delimiter, re.Pattern
        ):
            all_lines.append(buffer.first_log_line)
        all_lines.extend(buffer.raw_rows)
        if not all_lines:
            self.notify("No data to analyze", severity="warning")
            return
        sample = _sample_lines(all_lines)
        result = detect_log_format(sample)
        if result is None:
            self.notify("No known log format detected", severity="warning")
            return
        buffer.switch_delimiter(result.pattern.pattern)
        buffer.delimiter_name = result.name
        self.notify(f"Detected: {result.name}")

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
            on_remove=lambda val: (
                self.input_history.remove({"id": id, "val": val})
                if {"id": id, "val": val} in self.input_history
                else None
            ),
        )
        tabbed_content = self._get_active_tabbed_content()
        active_tab = tabbed_content.active
        for tab_pane in tabbed_content.query(TabPane):
            if tab_pane.id == active_tab:
                tab_pane.mount(input)
                self.call_after_refresh(lambda: input.focus())
                break

    def _filter_composite_key(self, current_buffer: NlessBuffer) -> None:
        data_table = current_buffer.query_one(".nless-view")
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
                if self.focused.parent.id == "regex_wizard_name_input":
                    self._regex_wizard_state = None
                self.focused.parent.remove()
            else:
                self.focused.remove()
        elif event.key == "escape" and isinstance(self.focused, Select):
            self.focused.remove()

        current_buffer = self._get_current_buffer()
        if (
            event.key == "enter"
            and hasattr(self.focused, "has_class")
            and self.focused.has_class("nless-view")
        ):
            self._filter_composite_key(current_buffer)

        if (
            event.key in _TAB_SWITCH_KEYS
            and hasattr(self.focused, "has_class")
            and self.focused.has_class("nless-view")
        ):
            self.show_tab_by_index(int(event.key) - 1)

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
        elif event.input.id == "save_log_format_input":
            self.handle_save_log_format_submitted(event)
        elif event.input.id == "regex_wizard_name_input":
            self._handle_regex_wizard_name_submitted(event)

    def handle_search_submitted(self, event: AutocompleteInput.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()
        current_buffer._perform_search(input_value)

    def _get_current_buffer(self) -> NlessBuffer:
        return self.buffers[self.curr_buffer_idx]

    def _get_column_values(self, column_index: int) -> list[str]:
        """Get unique values for a column, ordered by frequency (most common first)."""
        data_table = self._get_current_buffer().query_one(".nless-view")
        counts: dict[str, int] = {}
        for row in data_table.rows:
            if column_index < len(row):
                val = strip_markup(row[column_index])
                if val:
                    counts[val] = counts.get(val, 0) + 1
        return sorted(counts, key=lambda v: counts[v], reverse=True)

    _TIME_WINDOW_OPTIONS = ["off", "30s", "1m", "5m", "5m+", "15m", "15m+", "1h", "1h+"]

    def action_time_window(self) -> None:
        """Set a time window to only show rows from the last N minutes/hours/seconds."""
        curr = self._get_current_buffer()
        current = self._format_window(curr.time_window, curr.rolling_time_window)
        hint = f"e.g. 5m, 1h, 30s — append + for rolling (current: {current})"
        history = [
            h["val"] for h in self.input_history if h["id"] == "time_window_input"
        ]
        self._create_prompt(
            f"Enter time window — {hint}",
            "time_window_input",
            provider=StaticSuggestionProvider(
                self._TIME_WINDOW_OPTIONS, history=history
            ),
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
        event.input.remove()
        self._get_current_buffer().apply_time_window_setting(event.value)

    def handle_delimiter_submitted(self, event: AutocompleteInput.Submitted) -> None:
        event.input.remove()
        value = event.value
        if value and value not in ("raw", "json", "\\t", "space", "space+"):
            try:
                pattern = re.compile(rf"{value}")
                if pattern.groups > len(pattern.groupindex):
                    self._start_regex_wizard(value, pattern, "delimiter")
                    return
            except re.error as e:
                self.notify(f"Invalid regex: {e}", severity="error")
                return
        self._get_current_buffer().switch_delimiter(value)
        # Offer to save regex with named groups as a custom log format
        if value and value not in ("raw", "json", "\\t", "space", "space+"):
            try:
                pattern = re.compile(rf"{value}")
                if pattern.groupindex:
                    self._pending_log_format_pattern = value
                    self._create_prompt(
                        "Save as log format? Enter name (Esc to skip)",
                        "save_log_format_input",
                    )
            except re.error:
                pass

    def handle_save_log_format_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        event.input.remove()
        name = event.value.strip()
        if not name:
            return
        pattern = getattr(self, "_pending_log_format_pattern", None)
        if not pattern:
            return
        self._pending_log_format_pattern = None
        try:
            save_custom_format(name, pattern)
            self._get_current_buffer().delimiter_name = name
            self.notify(f"Saved log format: {name}")
        except OSError as e:
            self.notify(f"Failed to save: {e}", severity="error")

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
            data_table = new_buffer.query_one(".nless-view")
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

    def _create_unparsed_buffer(
        self,
        unparsed_rows: list[str],
        source_parse_filter: Callable[[str], bool],
        line_stream=None,
    ) -> None:
        """Create a new raw-delimiter buffer from lines that didn't parse."""
        new_pane_id = self._get_new_pane_id()
        new_buffer = NlessBuffer(pane_id=new_pane_id, cli_args=None)
        new_buffer.init_as_unparsed(unparsed_rows, source_parse_filter, line_stream)
        self.add_buffer(new_buffer, "~unparsed", reason="Unparsed logs")

    def add_buffer(
        self,
        new_buffer: NlessBuffer,
        name: str,
        add_prev_index: bool = True,
        on_ready=None,
        reason="Loading",
    ) -> None:
        curr_data_table = self._get_current_buffer().query_one(".nless-view")

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
        self._stop_group_bar_timer()
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
            ".nless-view"
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
        for i, pane in enumerate(tabbed_content.query(Tab).results()):
            curr_title = str(pane.content)
            pattern_matches = self._TAB_LABEL_RE.match(curr_title)
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
            ".nless-view"
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
            self.query_one(".nless-view").focus()

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
                data_table = buf.query_one(".nless-view")
                data_table.apply_theme(new_theme)
                buf._deferred_update_table(
                    restore_position=True, reason="Applying theme"
                )
            except NoMatches:
                pass  # Buffer not mounted yet
            except Exception:
                logger.debug("Error applying theme to buffer", exc_info=True)

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
        except NoMatches:
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
