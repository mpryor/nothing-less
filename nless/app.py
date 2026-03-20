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
from textual.events import Click, Key
from textual.geometry import Offset
from textual.widgets import (
    Input,
    Select,
    Static,
    Tab,
    TabbedContent,
    TabPane,
)

from textual.containers import Horizontal, Vertical

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
    TimeWindowSuggestionProvider,
)

from .config import NlessConfig, load_config, load_input_history, save_config
from .session import (
    find_session_for_source,
    load_session_by_name,
)
from .procutil import get_stdin_source
from .dataprocessing import strip_markup
from .help import HelpScreen
from .input import LineStream, ShellCommandLineStream, StdinLineStream
from .nlessselect import NlessSelect
from .datatable import Coordinate as NlessCoordinate, Datatable
from .rawpager import RawPager
from .keymap import get_all_keymaps, resolve_keymap
from .theme import get_all_themes, resolve_theme
from .types import CliArgs, Filter, MetadataColumn, StatusContext, UpdateReason
from .app_columns import ColumnOpsMixin
from .app_filters import FilterMixin
from .app_groups import GroupMixin
from .app_highlights import HighlightMixin
from .app_sessions import SessionViewMixin
from .buffer_delimiter import _sample_lines
from .logformats import (
    LogFormat,
    detect_log_formats,
    infer_log_pattern,
    load_custom_formats,
    save_custom_format,
)
from .caption import CaptionOverlay
from .contextmenu import ContextMenu, MenuItem
from .regex_wizard import RegexWizardMixin
from .app_exmode import ExModeMixin

logger = logging.getLogger(__name__)


_TAB_SWITCH_KEYS = frozenset(str(i) for i in range(1, 10))


class NlessApp(
    SessionViewMixin,
    HighlightMixin,
    RegexWizardMixin,
    ExModeMixin,
    ColumnOpsMixin,
    FilterMixin,
    GroupMixin,
    App,
):
    inherit_bindings = False
    ENABLE_COMMAND_PALETTE = False
    _TAB_LABEL_RE = re.compile(r"((\[#[0-9a-fA-F]+\])?(\d+?)(\[/#[0-9a-fA-F]+\])?) .*")

    def _build_demo_key_map(self) -> dict[str, str]:
        """Build a mapping of Textual key name → description from all bindings."""
        from textual.keys import _character_to_key

        from .datatable import Datatable
        from .rawpager import RawPager

        key_map: dict[str, str] = {}
        all_bindings = (
            list(self.BINDINGS)
            + list(NlessBuffer.BINDINGS)
            + list(Datatable.BINDINGS)
            + list(RawPager.BINDINGS)
        )
        for binding in all_bindings:
            if isinstance(binding, Binding) and binding.description:
                for key in binding.key.split(","):
                    # Convert binding key (e.g. "/") to Textual key name (e.g. "slash")
                    textual_key = (
                        _character_to_key(key.strip())
                        if len(key.strip()) == 1
                        else key.strip()
                    )
                    key_map.setdefault(textual_key, binding.description)
        return key_map

    def _demo_caption(self, key: str) -> None:
        """Show a demo caption for the given key if in demo mode."""
        # Don't show caption for the caption key itself
        if key == "number_sign":
            return
        desc = self._demo_key_map.get(key)
        if desc:
            # Show the original character, not the Textual key name
            display_key = (
                key
                if len(key) == 1
                else {
                    "slash": "/",
                    "ampersand": "&",
                    "exclamation_mark": "!",
                    "question_mark": "?",
                    "dollar_sign": "$",
                    "at_sign": "@",
                    "pipe": "|",
                    "plus": "+",
                    "minus": "-",
                    "tilde": "~",
                }.get(key, key)
            )
            try:
                overlay = self.query_one("#caption_overlay", CaptionOverlay)
                overlay.show_caption(f"{display_key}  →  {desc}", duration=2.0)
            except NoMatches:
                pass

    def _key_for_action(self, action: str) -> str:
        """Return the key hint for an action name, or empty string."""
        from .buffer import NlessBuffer

        for binding in self.BINDINGS:
            if isinstance(binding, Binding) and binding.action == action:
                return binding.key.split(",")[0]
        for binding in NlessBuffer.BINDINGS:
            if isinstance(binding, Binding) and binding.action == action:
                return binding.key.split(",")[0]
        return ""

    def _menu_item(self, label: str, action: str) -> MenuItem:
        """Create a MenuItem with the key hint resolved from bindings."""
        return MenuItem(label, action, self._key_for_action(action))

    _MENU_BAR_ITEMS = [
        ("menu_file", "File"),
        ("menu_view", "View"),
        ("menu_data", "Data"),
        ("menu_search", "Search"),
    ]

    _MENU_DEFS: dict[str, list[tuple[str, str]]] = {
        "menu_file": [
            ("Open file", "open_file"),
            ("New buffer", "add_buffer"),
            ("Rename buffer", "rename_buffer"),
            ("Merge buffers", "merge_buffers"),
            ("Rename group", "rename_group"),
            ("Write to file", "write_to_file"),
            ("Run command", "run_command"),
            ("Close buffer", "close_active_buffer"),
        ],
        "menu_view": [
            ("Show/hide columns", "filter_columns"),
            ("Jump to column", "jump_columns"),
            ("Toggle arrival timestamps", "toggle_arrival"),
            ("Toggle tail mode", "toggle_tail"),
            ("Column aggregations", "aggregations"),
            ("View excluded lines", "view_unparsed_logs"),
            ("Reset highlights", "reset_highlights"),
            ("Select theme", "select_theme"),
            ("Select keymap", "select_keymap"),
            ("Sessions", "session_menu"),
            ("Views", "view_menu"),
        ],
        "menu_data": [
            ("Change delimiter", "delimiter"),
            ("Split column", "column_delimiter"),
            ("Extract JSON key", "json_header"),
            ("Auto-detect log format", "detect_log_format"),
            ("Ex mode", "exmode"),
            ("Time window", "time_window"),
        ],
        "menu_search": [
            ("Search", "search"),
            ("Next match", "next_search"),
            ("Previous match", "previous_search"),
            ("Search to filter", "search_to_filter"),
            ("Filter column", "filter"),
            ("Exclude from column", "exclude_filter"),
            ("Filter all columns", "filter_any"),
            ("Add highlight", "add_highlight"),
            ("Navigate highlights", "navigate_highlight"),
        ],
    }

    def _build_menu(self, menu_id: str) -> list[MenuItem]:
        return [
            self._menu_item(label, action) for label, action in self._MENU_DEFS[menu_id]
        ]

    def __init__(
        self,
        cli_args: CliArgs,
        starting_stream: LineStream | None,
        show_help: bool = False,
    ) -> None:
        super().__init__()
        self.cli_args = cli_args
        self.pipe_output = cli_args.pipe_output
        self.output_format = cli_args.output_format
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
        self._pending_file_groups: list[tuple[str, StdinLineStream]] = []
        self._exit_event = __import__("threading").Event()
        self.demo_mode = cli_args.demo
        if self.demo_mode:
            self._demo_key_map = self._build_demo_key_map()

    @staticmethod
    def _initial_group_name(cli_args: CliArgs) -> str:
        file_icon = "\uf0f6" if cli_args.demo else "📄"  # nf-fa-file_text_o
        cmd_icon = "\uf120" if cli_args.demo else "⏵"  # nf-fa-terminal
        if cli_args.merge:
            n = len(cli_args.filenames)
            return f"{cmd_icon} merged ({n} files)"
        if cli_args.filename:
            return f"{file_icon} {os.path.basename(cli_args.filename)}"
        source = get_stdin_source()
        if source is None:
            return "stdin"
        if source.startswith("/"):
            return f"{file_icon} {source}"
        return f"{cmd_icon} {source}"

    SCREENS = {"HelpScreen": HelpScreen, "GettingStartedScreen": GettingStartedScreen}
    HISTORY_FILE = "~/.config/nless/history.json"

    def exit(self, *args, **kwargs) -> None:
        """Unsubscribe all buffers from their streams before exiting."""
        for buf in getattr(self, "all_buffers", []):
            if buf.line_stream:
                buf.line_stream.unsubscribe(buf)
        super().exit(*args, **kwargs)

    def on_unmount(self) -> None:
        self._exit_event.set()

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
        Binding(
            "Q",
            "pipe_and_exit",
            "Quit immediately (pipe & exit)",
            id="app.pipe_and_exit",
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
        Binding("#", "caption", "Show Caption", id="app.caption"),
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
            "Time window / timestamp conversion",
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
        Binding(
            "plus",
            "add_highlight",
            "Add regex highlight",
            id="app.add_highlight",
        ),
        Binding(
            "minus",
            "navigate_highlight",
            "Navigate highlight",
            id="app.navigate_highlight",
        ),
        Binding("S", "session_menu", "Sessions", id="app.session_menu"),
        Binding("v", "view_menu", "Views", id="app.view_menu"),
        Binding(
            "M",
            "merge_buffers",
            "Merge with another buffer",
            id="app.merge_buffers",
        ),
        Binding(":", "exmode", "Ex mode", id="app.exmode"),
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
            cmd_icon = "\uf120" if self.demo_mode else "⏵"
            await self.add_group(
                f"{cmd_icon} {command}", new_buffer, stream=line_stream
            )
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
            file_icon = "\uf0f6" if self.demo_mode else "📄"
            await self.add_group(
                f"{file_icon} {os.path.basename(path)}", new_buffer, stream=line_stream
            )
        except (OSError, FileNotFoundError) as e:
            self.notify(f"Error opening file: {e}", severity="error")

    # ── Select event dispatch ──────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        try:
            event.control.remove()
        except (NoMatches, DOMError):
            pass  # NlessSelect already removes itself in on_input_submitted
        if event.control.id:
            handler = getattr(self, f"_on_{event.control.id}", None)
            if handler:
                handler(event)

    def _on_theme_select(self, event: Select.Changed) -> None:
        self.apply_theme(str(event.value))

    def _on_keymap_select(self, event: Select.Changed) -> None:
        self.apply_keymap(str(event.value))

    def _on_json_header_select(self, event: Select.Changed) -> None:
        self._apply_json_header(str(event.value))

    def _on_log_format_select(self, event: Select.Changed) -> None:
        candidates = getattr(self, "_pending_log_format_candidates", None)
        self._pending_log_format_candidates = None
        if candidates:
            fmt = candidates.get(str(event.value))
            if fmt:
                self._apply_log_format(fmt)

    def _on_merge_select(self, event: Select.Changed) -> None:
        value = str(event.value)
        try:
            group_id_str, pane_id_str = value.split(":")
            group_id, pane_id = int(group_id_str), int(pane_id_str)
        except (ValueError, TypeError):
            return
        target_buf = None
        target_group_name = ""
        for group in self.groups:
            if group.group_id == group_id:
                for b in group.buffers:
                    if b.pane_id == pane_id:
                        target_buf = b
                        target_group_name = group.name
                        break
        if target_buf is None:
            self.notify("Buffer not found", severity="error")
            return
        cur_buf = self._get_current_buffer()
        cur_group_name = self._current_group.name
        new_pane_id = self._get_new_pane_id()
        merged = NlessBuffer.init_as_merged(
            new_pane_id, cur_buf, target_buf, cur_group_name, target_group_name
        )
        for col in merged.current_columns:
            if col.name == MetadataColumn.SOURCE.value:
                col.hidden = False
                col.pinned = True
                col.render_position = 0
                break
        merged._rebuild_column_caches()
        self.add_buffer(merged, name="merged")

    def action_write_to_file(self) -> None:
        """Write the current view to a file."""
        self._create_prompt(
            "Type output file path (.csv, .tsv, .json, .txt)", "write_to_file_input"
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
        reason: UpdateReason = UpdateReason.LOADING,
        done_reason="Loaded",
    ):
        """Run copy() + setup on a background thread, then add_buffer on main thread."""
        curr_buffer = self._get_current_buffer()
        source_group_idx = self.curr_group_idx
        new_pane_id = self._get_new_pane_id()
        source_row_count = len(curr_buffer.displayed_rows)
        display_reason = f"{reason} {source_row_count:,} rows"
        curr_buffer.start_loading(display_reason)

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
            if unique_column_name in new_buffer.query.unique_column_names:
                new_buffer.query.sort_column = MetadataColumn.COUNT.value
                new_buffer.query.sort_reverse = True
                for col in new_buffer.current_columns:
                    if col.name == MetadataColumn.COUNT.value:
                        col.labels.add("▼")
                        break

        # Determine buffer name after setup runs — use a mutable container
        # to capture the name from setup context
        will_be_unique = unique_column_name not in curr_buffer.query.unique_column_names
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
            reason=UpdateReason.PIVOT,
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
        if buffer.delim.value not in ("raw",) and not isinstance(
            buffer.delim.value, re.Pattern
        ):
            all_lines.append(buffer.first_log_line)
        all_lines.extend(buffer.raw_rows)
        if not all_lines:
            self.notify("No data to analyze", severity="warning")
            return
        sample = _sample_lines(all_lines)

        # Collect all matching formats (known + inferred)
        candidates = detect_log_formats(sample)
        inferred = infer_log_pattern(sample)
        if inferred is not None:
            # Score the inferred pattern the same way
            non_empty = [line for line in sample if line.strip()]
            if non_empty:
                matches = sum(1 for line in non_empty if inferred.pattern.match(line))
                ratio = matches / len(non_empty)
                score = ratio * 100 + len(inferred.pattern.groupindex) * 2
                candidates.append((inferred, score))
                candidates.sort(key=lambda x: x[1], reverse=True)

        if not candidates:
            self.notify("No known log format detected", severity="warning")
            return

        if len(candidates) == 1:
            self._apply_log_format(candidates[0][0])
            return

        # Multiple candidates — show selection menu
        self._pending_log_format_candidates = {
            str(i): fmt for i, (fmt, _score) in enumerate(candidates)
        }
        options: list[tuple[str, str]] = []
        for i, (fmt, score) in enumerate(candidates):
            fields = sorted(fmt.pattern.groupindex.keys())
            field_str = ", ".join(fields[:5])
            if len(fields) > 5:
                field_str += f" +{len(fields) - 5}"
            label = f"{fmt.name} ({field_str})"
            options.append((label, str(i)))
        select = NlessSelect(
            options=options,
            prompt="Multiple log formats match — select one",
            classes="dock-bottom",
            id="log_format_select",
        )
        buffer.mount(select)

    def _apply_log_format(self, result: LogFormat) -> None:
        """Apply a detected log format to the current buffer."""
        buffer = self._get_current_buffer()
        buffer.switch_delimiter(result.pattern.pattern)
        if result.name == "Auto-detected":
            self._pending_log_format_pattern = result.pattern.pattern
            self._create_prompt(
                "Save as log format? Enter name (Esc to skip)",
                "save_log_format_input",
                save_history=False,
            )
        else:
            buffer.delim.name = result.name
            self.notify(f"Detected: {result.name}")

    def action_search_to_filter(self) -> None:
        """Convert current search into a filter across all columns."""
        current_buffer = self._get_current_buffer()
        if not current_buffer.query.search_term:
            current_buffer.notify(
                "No active search to convert to filter", severity="warning"
            )
            return

        search_pattern = current_buffer.query.search_term
        buffer_name = f"+f:any={search_pattern.pattern}"

        def setup(new_buffer):
            new_buffer.query.filters.append(Filter(column=None, pattern=search_pattern))

        self._copy_buffer_async(
            setup, buffer_name, reason=UpdateReason.FILTER, done_reason="Filtered"
        )

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        self._create_prompt("Type search term and press Enter", "search_input")

    def _create_prompt(
        self, placeholder, id, provider=None, save_history=True, prefix=""
    ):
        history = [h["val"] for h in self.input_history if h["id"] == id]
        if provider is None:
            if id in ("write_to_file_input", "open_file_input"):
                provider = FilePathSuggestionProvider()
            elif id == "run_command_input":
                provider = ShellCommandSuggestionProvider(history)
            else:
                provider = HistorySuggestionProvider(history)
        if not save_history:
            history = []
        input = AutocompleteInput(
            placeholder=placeholder,
            id=id,
            classes="bottom-input",
            history=history,
            provider=provider,
            prefix=prefix,
            on_add=lambda val: self.input_history.append({"id": id, "val": val})
            if save_history
            else None,
            on_remove=lambda val: (
                self.input_history.remove({"id": id, "val": val})
                if {"id": id, "val": val} in self.input_history
                else None
            )
            if save_history
            else None,
        )
        tabbed_content = self._get_active_tabbed_content()
        active_tab = tabbed_content.active
        for tab_pane in tabbed_content.query(TabPane):
            if tab_pane.id == active_tab:
                existing = tab_pane.query(f"#{id}")
                if existing:
                    return
                tab_pane.mount(input)
                self.call_after_refresh(lambda: input.focus())
                break

    def _filter_composite_key(self, current_buffer: NlessBuffer) -> None:
        if not current_buffer.query.unique_column_names:
            return
        data_table = current_buffer.query_one(".nless-view")
        # Pre-read cell values on main thread (widget access)
        filters = []
        unique_columns = list(current_buffer.query.unique_column_names)
        for column in unique_columns:
            col_idx = current_buffer._get_col_idx_by_name(column, render_position=True)
            cell_value = data_table.get_cell_at(
                NlessCoordinate(data_table.cursor_row, col_idx)
            )
            cell_value = strip_markup(cell_value) if cell_value else ""
            filters.append(
                Filter(
                    column=strip_markup(column),
                    pattern=re.compile(re.escape(cell_value), re.IGNORECASE),
                )
            )
        buffer_name = (
            f"+f:{','.join([f'{f.column}={f.pattern.pattern}' for f in filters])}"
        )

        def setup(new_buffer):
            # Toggle off pivot columns — reveals raw data
            for column in unique_columns:
                handle_mark_unique(new_buffer, column)
            new_buffer.query.filters.extend(filters)

        self._copy_buffer_async(
            setup,
            buffer_name,
            reason=UpdateReason.FILTER,
            done_reason="Filtered",
        )

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if self.demo_mode and not isinstance(self.focused, (Input, Select)):
            self._demo_caption(event.key)
        # Forward keys to context menu when open
        try:
            menu = self.query_one(ContextMenu)
            if menu.is_open and menu.process_key(event.key):
                event.stop()
                event.prevent_default()
                return
        except NoMatches:
            pass
        if event.key == "escape" and isinstance(self.focused, Input):
            if isinstance(self.focused.parent, AutocompleteInput):
                if self.focused.parent.id == "regex_wizard_name_input":
                    self._regex_wizard_state = None
                elif self.focused.parent.id == "save_log_format_input":
                    pattern = getattr(self, "_pending_log_format_pattern", None)
                    if pattern:
                        self._pending_log_format_pattern = None
                        self._get_current_buffer().switch_delimiter(pattern)
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
        elif event.input.id == "session_name_input":
            self._handle_session_save_submitted(event)
        elif event.input.id == "session_rename_input":
            self._handle_session_rename_submitted(event)
        elif event.input.id == "view_name_input":
            self._handle_view_save_submitted(event)
        elif event.input.id == "view_rename_input":
            self._handle_view_rename_submitted(event)
        elif event.input.id == "exmode_input":
            self.handle_exmode_submitted(event)
        elif event.input.id == "caption_input":
            self.handle_caption_submitted(event)

    def action_merge_buffers(self) -> None:
        """Open a select menu to pick a buffer to merge with the current one."""
        buf = self._get_current_buffer()
        options = []
        for group in self.groups:
            for b in group.buffers:
                if b is buf:
                    continue
                fi = "\uf0f6" if self.demo_mode else "📄"
                label = f"{fi} {group.name} / buffer{b.pane_id}"
                options.append((label, f"{group.group_id}:{b.pane_id}"))
        if not options:
            self.notify("No other buffers to merge with", severity="warning")
            return
        select = NlessSelect(
            options=options,
            prompt="Select buffer to merge with current",
            classes="dock-bottom",
            id="merge_select",
        )
        buf.mount(select)

    # ── Search ────────────────────────────────────────────────────────

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

        options = list(self._TIME_WINDOW_OPTIONS)
        dt_cols = curr.datetime_column_names
        durations = ["5m", "5m+", "15m", "15m+", "1h", "1h+"]
        for col in dt_cols:
            for dur in durations:
                options.append(f"{col} {dur}")

        col_hint = ""
        if dt_cols:
            col_hint = f" — use '{dt_cols[0]} 5m' to filter by column"
        hint = f"e.g. 5m, 1h, 30s — append + for rolling{col_hint} (current: {current})"

        self._create_prompt(
            f"Enter time window — {hint}",
            "time_window_input",
            provider=TimeWindowSuggestionProvider(options, dt_cols),
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
        value = event.value
        if " -> " in value:
            self._apply_datetime_format(value)
            return
        self._get_current_buffer().apply_time_window_setting(value)

    def _apply_datetime_format(self, value: str) -> None:
        """Apply a datetime format conversion: 'colname -> format'."""
        col_part, fmt_part = value.split(" -> ", 1)
        col_name = col_part.strip()
        target_fmt = fmt_part.strip()
        if not target_fmt:
            self.notify("No target format specified", severity="error")
            return

        curr = self._get_current_buffer()
        dt_cols = curr.datetime_column_names
        if col_name not in dt_cols:
            self.notify(f"'{col_name}' is not a datetime column", severity="error")
            return

        target_names = set()
        source_hint = None
        for c in curr.current_columns:
            if strip_markup(c.name) == col_name:
                target_names.add(c.name)
                source_hint = c.datetime_fmt_hint
                break

        def setup(new_buffer):
            for c in new_buffer.current_columns:
                if c.name in target_names:
                    c.datetime_display_fmt = target_fmt
                    if source_hint is not None:
                        c.datetime_fmt_hint = source_hint
            new_buffer._rebuild_column_caches()

        self._copy_buffer_async(
            setup,
            f"@{col_name}->{target_fmt}",
            reason=UpdateReason.SUBSTITUTION,
            done_reason="Formatted",
        )

    def handle_delimiter_submitted(self, event: AutocompleteInput.Submitted) -> None:
        event.input.remove()
        value = event.value
        if value and value not in ("raw", "json", "\\t", "space", "space+"):
            try:
                pattern = re.compile(rf"{value}")
                if pattern.groups > len(pattern.groupindex):
                    self._start_regex_wizard(value, pattern, "delimiter")
                    return
                # Regex with named groups — apply directly if already saved,
                # otherwise ask to save
                if pattern.groupindex:
                    existing = [
                        f for f in load_custom_formats() if f.pattern.pattern == value
                    ]
                    if existing:
                        buf = self._get_current_buffer()
                        buf.switch_delimiter(value)
                        buf.delim.name = existing[0].name
                        return
                    self._pending_log_format_pattern = value
                    self._create_prompt(
                        "Save as log format? Enter name (Esc to skip)",
                        "save_log_format_input",
                        save_history=False,
                    )
                    return
            except re.error as e:
                self.notify(f"Invalid regex: {e}", severity="error")
                return
        self._get_current_buffer().switch_delimiter(value)

    def handle_save_log_format_submitted(
        self, event: AutocompleteInput.Submitted
    ) -> None:
        event.input.remove()
        pattern = getattr(self, "_pending_log_format_pattern", None)
        if not pattern:
            return
        self._pending_log_format_pattern = None
        self._get_current_buffer().switch_delimiter(pattern)
        name = event.value.strip()
        if name:
            try:
                save_custom_format(name, pattern)
                self._get_current_buffer().delim.name = name
                self.notify(f"Saved log format: {name}")
            except OSError as e:
                self.notify(f"Failed to save: {e}", severity="error")

    def refresh_buffer_and_focus(
        self,
        new_buffer: NlessBuffer,
        cursor_coordinate: Coordinate,
        offset: Offset,
        on_ready=None,
        reason: UpdateReason = UpdateReason.LOADING,
        activate: bool = True,
    ) -> None:
        if activate:
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
                    activate=activate,
                )
            )
            return
        if activate:
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

    def _recreate_unparsed_buffer(
        self,
        parent_buf: NlessBuffer,
        stream: LineStream | None,
        buf_state,
    ) -> NlessBuffer | None:
        """Re-derive an ~unparsed buffer from the parent during session restore."""
        shown = parent_buf._make_shown_filter(include_ancestors=False)
        all_lines = stream.lines if stream else parent_buf.raw_rows
        excluded = [line for line in all_lines if not shown(line)]
        if not excluded:
            return None
        extra_buf = NlessBuffer(pane_id=self._get_new_pane_id(), cli_args=None)
        extra_buf.init_as_unparsed(excluded, shown, stream)
        extra_buf._pending_session_state = buf_state
        return extra_buf

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
        self.add_buffer(new_buffer, "~unparsed", reason=UpdateReason.LOADING)

    def add_buffer(
        self,
        new_buffer: NlessBuffer,
        name: str,
        add_prev_index: bool = True,
        on_ready=None,
        reason: UpdateReason = UpdateReason.LOADING,
        activate: bool = True,
    ) -> None:
        curr_data_table = self._get_current_buffer().query_one(".nless-view")

        self.buffers.append(new_buffer)
        self._sync_status_context()
        tabbed_content = self._get_active_tabbed_content()
        buffer_number = len(self.buffers)
        tab_pane = TabPane(
            f"{self.nless_theme.markup('accent', str(buffer_number))} {self.curr_buffer_idx + 1 if add_prev_index else ''}{name}",
            id=f"buffer{new_buffer.pane_id}",
        )
        tabbed_content.add_pane(tab_pane)
        tab_pane.mount(new_buffer)
        if activate:
            self.curr_buffer_idx = len(self.buffers) - 1
        self.call_after_refresh(
            lambda: self.refresh_buffer_and_focus(
                new_buffer,
                curr_data_table.cursor_coordinate,
                curr_data_table.scroll_offset,
                on_ready=on_ready,
                reason=reason,
                activate=activate,
            )
        )

    def on_exit_app(self) -> None:
        self._stop_group_bar_timer()
        os.makedirs(
            os.path.dirname(os.path.expanduser(self.HISTORY_FILE)), exist_ok=True
        )

        with open(os.path.expanduser(self.HISTORY_FILE), "w") as f:
            json.dump(self.input_history, f)

    def action_pipe_and_exit(self) -> None:
        """Pipe current buffer to stdout and exit immediately."""
        self.exit()

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

    def on_tabbed_content_tab_activated(
        self, event: TabbedContent.TabActivated
    ) -> None:
        """Sync curr_buffer_idx when the user clicks a tab."""
        pane_id = event.pane.id or ""
        if pane_id.startswith("buffer"):
            for i, buf in enumerate(self.buffers):
                if f"buffer{buf.pane_id}" == pane_id:
                    self.curr_buffer_idx = i
                    buf._update_status_bar()
                    break

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

    def _sync_status_context(self) -> None:
        """Push app-level state to all buffers' StatusContext."""
        ctx = StatusContext(
            status_format=self.config.status_format,
            keymap_name=self.nless_keymap.name,
            theme_name=self.nless_theme.name,
            pipe_output=getattr(self, "pipe_output", False),
            session_name=getattr(self, "_active_session_name", None),
            format_window=self._format_window,
            theme=self.nless_theme,
        )
        for buf in self.all_buffers:
            buf._status_ctx = ctx

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

        self._sync_status_context()

        # Check for session to restore (deferred so DOM is fully ready)
        if self.cli_args.session:
            session = load_session_by_name(self.cli_args.session)
            if session:
                self._pending_auto_session = session
                self.set_timer(
                    0.3, lambda: self.run_worker(self._load_session(session))
                )
            else:
                self.set_timer(
                    0.3,
                    lambda: self.notify(
                        f"Session '{self.cli_args.session}' not found", severity="error"
                    ),
                )
        else:
            source = self._get_data_source()
            if source:
                session = find_session_for_source(source)
                if session:
                    self._pending_auto_session = session
                    self.set_timer(0.3, lambda: self._show_session_load_prompt(session))

        if self._pending_file_groups:
            self.set_timer(
                0.1, lambda: self.run_worker(self._add_pending_file_groups())
            )

        if self.show_help and self.config.show_getting_started:
            self.push_screen(GettingStartedScreen())
        else:
            self.query_one(".nless-view").focus()

        # Show release notes on version upgrade
        self._check_release_notes()

        # Check for newer version on PyPI (non-blocking, skip in dev installs)
        if not self.demo_mode:
            from .version import is_dev_install

            if not is_dev_install():
                self._check_for_update()

    async def _add_pending_file_groups(self) -> None:
        for filepath, stream in self._pending_file_groups:
            new_buffer = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=self.cli_args,
                line_stream=stream,
            )
            fi = "\uf0f6" if self.demo_mode else "📄"
            name = f"{fi} {os.path.basename(filepath)}"
            await self.add_group(name, new_buffer, stream=stream)
        self._pending_file_groups = []
        self._switch_to_group(0)

    def _open_menu_bar(self, widget) -> None:
        """Open a menu bar dropdown from the given label widget."""
        items = self._build_menu(widget.id)
        menu = self.query_one(ContextMenu)
        region = widget.region
        menu.source_menu_id = widget.id
        menu.show(region.x, region.y + 1, items)

    def on_mouse_move(self, event) -> None:
        """Switch menu bar dropdown on hover; highlight group bar on hover."""
        # Menu bar hover-to-switch
        try:
            menu = self.query_one(ContextMenu)
            if menu.is_open and menu.source_menu_id:
                widget, _ = self.get_widget_at(event.screen_x, event.screen_y)
                if (
                    hasattr(widget, "id")
                    and widget.id in self._MENU_DEFS
                    and widget.id != menu.source_menu_id
                ):
                    self._open_menu_bar(widget)
                    return
        except NoMatches:
            pass
        # Menu label hover color
        widget, _ = self.get_widget_at(event.screen_x, event.screen_y)
        hovered_menu = (
            widget.id
            if hasattr(widget, "id") and widget.id in self._MENU_DEFS
            else None
        )
        old_menu = getattr(self, "_hovered_menu_id", None)
        if hovered_menu != old_menu:
            self._hovered_menu_id = hovered_menu
            t = self.nless_theme
            for menu_id, label in self._MENU_BAR_ITEMS:
                try:
                    w = self.query_one(f"#{menu_id}", Static)
                    if menu_id == hovered_menu:
                        w.update(f" {t.markup('accent', label)} ")
                    else:
                        w.update(f" {t.markup('muted', label)} ")
                except NoMatches:
                    pass
        # Group bar hover highlight
        if hasattr(widget, "id") and widget.id == "group_bar" and len(self.groups) > 1:
            idx = self._group_idx_at_x(event.x)
            old = getattr(self, "_hovered_group_idx", -1)
            if idx != old:
                self._hovered_group_idx = idx
                self._update_group_bar()
        else:
            old = getattr(self, "_hovered_group_idx", -1)
            if old != -1:
                self._hovered_group_idx = -1
                self._update_group_bar()

    def on_mouse_down(self, event) -> None:
        """Show context menu on right-click over a tab."""
        if event.button != 3:
            return
        widget, _ = self.get_widget_at(event.screen_x, event.screen_y)
        if isinstance(widget, Tab):
            items = [
                self._menu_item("Rename buffer", "rename_buffer"),
                self._menu_item("Close buffer", "close_active_buffer"),
            ]
            menu = self.query_one(ContextMenu)
            menu.source_menu_id = None
            menu.show(event.screen_x, event.screen_y, items)
        elif (
            hasattr(widget, "id") and widget.id == "group_bar" and len(self.groups) > 1
        ):
            idx = self._group_idx_at_x(event.x)
            if idx >= 0:
                # Store the target group so the action applies to it
                self._context_menu_group_idx = idx
                items = [
                    self._menu_item("Rename group", "rename_group"),
                    self._menu_item("Close group", "close_current_group"),
                ]
                menu = self.query_one(ContextMenu)
                menu.source_menu_id = None
                menu.show(event.screen_x, event.screen_y, items)

    def on_click(self, event: Click) -> None:
        """Handle clicks — open help, switch groups, dismiss context menu."""
        widget, _ = self.get_widget_at(event.screen_x, event.screen_y)
        if hasattr(widget, "id") and widget.id == "help_hint":
            self.action_help()
        # Menu bar labels
        if event.button == 1 and hasattr(widget, "id") and widget.id in self._MENU_DEFS:
            self._open_menu_bar(widget)
            return
        # Click on group bar to switch groups
        if (
            event.button == 1
            and hasattr(widget, "id")
            and widget.id == "group_bar"
            and len(self.groups) > 1
        ):
            self._handle_group_bar_click(event.x)
        # Dismiss context menu on left-click outside it
        if event.button == 1:
            try:
                menu = self.query_one(ContextMenu)
                if menu.is_open and widget is not menu:
                    menu.dismiss()
            except NoMatches:
                pass

    def on_datatable_row_double_clicked(
        self, event: Datatable.RowDoubleClicked
    ) -> None:
        """Drill into a pivot row on double-click."""
        buf = self._get_current_buffer()
        if buf.query.unique_column_names:
            self._filter_composite_key(buf)

    def on_datatable_header_clicked(self, event: Datatable.HeaderClicked) -> None:
        """Sort by the clicked column header."""
        buf = self._get_current_buffer()
        dt = buf.query_one(".nless-view")
        dt.move_cursor(column=event.column)
        buf.action_sort()

    def on_datatable_right_clicked(self, event: Datatable.RightClicked) -> None:
        """Show context menu on right-click over a datatable cell or header."""
        buf = self._get_current_buffer()
        dt = buf.query_one(".nless-view")
        dt.move_cursor(column=event.column)
        mi = self._menu_item
        if event.is_header:
            n_cols = len(dt.columns)
            items = [
                mi("Sort column", "sort"),
                mi("Pin/unpin column", "pin_column"),
                mi("Hide column", "hide_column"),
            ]
            if event.column > 0:
                items.append(mi("Move left", "move_column_left"))
            if event.column < n_cols - 1:
                items.append(mi("Move right", "move_column_right"))
            items.extend(
                [
                    mi("Split column", "column_delimiter"),
                    mi("Pivot", "mark_unique"),
                ]
            )
        else:
            items = [
                mi("Copy cell", "copy"),
                mi("Search cursor word", "search_cursor_word"),
                mi("Filter by value", "filter_cursor_word"),
                mi("Exclude value", "exclude_filter_cursor_word"),
                mi("Add highlight", "add_highlight"),
                mi("Sort column", "sort"),
            ]
        menu = self.query_one(ContextMenu)
        menu.source_menu_id = None
        menu.show(event.screen_x, event.screen_y, items)

    def on_raw_pager_right_clicked(self, event: RawPager.RightClicked) -> None:
        """Show context menu on right-click over a raw pager row."""
        mi = self._menu_item
        items = [
            mi("Copy cell", "copy"),
            mi("Search cursor word", "search_cursor_word"),
            mi("Filter by value", "filter_cursor_word"),
            mi("Exclude value", "exclude_filter_cursor_word"),
            mi("Add highlight", "add_highlight"),
        ]
        menu = self.query_one(ContextMenu)
        menu.source_menu_id = None
        menu.show(event.screen_x, event.screen_y, items)

    async def on_context_menu_selected(self, event: ContextMenu.Selected) -> None:
        """Dispatch the selected context menu action."""
        menu = self.query_one(ContextMenu)
        menu.dismiss()
        # If a group was right-clicked, switch to it before running the action
        target_group = getattr(self, "_context_menu_group_idx", None)
        if target_group is not None:
            self._context_menu_group_idx = None
            if target_group != self.curr_group_idx:
                self._switch_to_group(target_group)
        buf = self._get_current_buffer()
        # Buffer-level actions (pin, sort, mark_unique) live on the buffer;
        # app-level actions (filter, search, copy) live on the app.
        # Check which one actually has the handler method.
        if hasattr(buf, f"action_{event.action}"):
            await buf.run_action(event.action)
        else:
            await self.run_action(event.action)

    def _check_release_notes(self) -> None:
        """Show release notes if the version changed since last run."""
        from importlib.metadata import version as pkg_version

        from .config import save_config

        try:
            current = pkg_version("nothing-less")
        except Exception:
            return
        last = self.config.last_seen_version
        if current == last:
            return
        # Update config immediately so we only show once
        self.config.last_seen_version = current
        save_config(self.config)
        if not last:
            return  # first run, don't show notes
        self.notify(f"Updated to v{current} — press ? for What's New", timeout=5)

    def _check_for_update(self) -> None:
        """Show a toast if a newer version is available on PyPI."""
        import time

        from packaging.version import Version

        from .version import fetch_latest_pypi_version, get_version

        current_str = get_version()
        if current_str == "unknown":
            return

        now = time.time()
        cache_fresh = now - self.config.last_update_check < 86400

        if cache_fresh and self.config.latest_pypi_version:
            try:
                if Version(self.config.latest_pypi_version) > Version(current_str):
                    self.notify(
                        f"nless v{self.config.latest_pypi_version} available "
                        "— pip install --upgrade nothing-less",
                        timeout=8,
                    )
            except Exception:
                pass
            return

        def _do_check() -> None:
            latest = fetch_latest_pypi_version()
            if latest is None:
                return
            self.config.latest_pypi_version = latest
            self.config.last_update_check = time.time()
            save_config(self.config)
            try:
                if Version(latest) > Version(current_str):
                    self.call_from_thread(
                        self.notify,
                        f"nless v{latest} available "
                        "— pip install --upgrade nothing-less",
                        timeout=8,
                    )
            except Exception:
                pass

        self.run_worker(_do_check, thread=True)

    def action_open_link(self, url: str) -> None:
        """Open a URL in the default browser (WSL-aware)."""
        import shutil
        import subprocess

        if shutil.which("wslview"):
            subprocess.Popen(["wslview", url])
        else:
            self.open_url(url)

    def action_caption(self) -> None:
        """Prompt for a caption to display as a centered overlay."""
        self._create_prompt(
            "Type caption and press Enter", "caption_input", save_history=False
        )

    def handle_caption_submitted(self, event: AutocompleteInput.Submitted) -> None:
        event.input.remove()
        text = event.value.strip()
        if not text:
            return
        self.query_one("#caption_overlay", CaptionOverlay).show_caption(text)

    def action_help(self) -> None:
        """Show the help screen."""
        from importlib.metadata import version as pkg_version

        from .config import get_release_notes

        release_notes = None
        try:
            current = pkg_version("nothing-less")
            notes = get_release_notes(current)
            if notes:
                release_notes = (current, notes)
        except Exception:
            pass
        self.push_screen(
            HelpScreen(
                keymap_name=self.nless_keymap.name,
                keymap_bindings=self.nless_keymap.bindings,
                theme=self.nless_theme,
                config=self.config,
                release_notes=release_notes,
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
        self._sync_status_context()
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
                    restore_position=True, reason=UpdateReason.THEME
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
        self._sync_status_context()
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

    def _build_title_name(self) -> str:
        return f" {self.nless_theme.markup('brand', 'nless')}  "

    def _build_help_hint(self) -> str:
        help_key = "?"
        bindings = self.nless_keymap.bindings
        if "app.help" in bindings:
            help_key = bindings["app.help"].split(",")[0]
        return self.nless_theme.markup("muted", f"{help_key} help")

    def _update_title_bar(self) -> None:
        """Refresh the title bar content."""
        try:
            self.query_one("#title_name", Static).update(self._build_title_name())
            self.query_one("#help_hint", Static).update(self._build_help_hint())
        except NoMatches:
            pass

    def compose(self) -> ComposeResult:
        yield CaptionOverlay(id="caption_overlay")
        yield ContextMenu(id="context_menu")
        with Vertical(id="top_bar"):
            with Horizontal(id="title_bar"):
                yield Static(self._build_title_name(), id="title_name")
                for menu_id, label in self._MENU_BAR_ITEMS:
                    yield Static(
                        f" {self.nless_theme.markup('muted', label)} ",
                        id=menu_id,
                        classes="menu-label",
                    )
                yield Static("", id="menu_spacer")
                yield Static(self._build_help_hint(), id="help_hint")
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
