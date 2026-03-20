from __future__ import annotations

import os

from rich.columns import Columns
from rich.markup import escape
from rich.table import Table
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from rich.markdown import Markdown as RichMarkdown
from textual.widgets import Static, TabbedContent, TabPane, Tabs

from nless.config import CONFIG_FILE, NlessConfig
from nless.theme import NlessTheme, BUILTIN_THEMES


def _format_key(key_str: str) -> str:
    """Format a binding key string for display (e.g. 'h,b,B' → 'h / b / B')."""
    return " / ".join(key_str.split(","))


# Categorized keybindings matching the docs structure.
# Each entry is (binding_id | None, default_key_display, description).
# When binding_id is None, the key is static (not remappable).
KEYBINDING_CATEGORIES: list[tuple[str, list[tuple[str | None, str, str]]]] = [
    (
        "Buffers",
        [
            (None, "1-9", "Switch to buffer"),
            ("app.show_tab_next", "L", "Next buffer"),
            ("app.show_tab_previous", "H", "Previous buffer"),
            ("app.close_active_buffer", "q", "Close buffer or quit"),
            ("app.pipe_and_exit", "Q", "Quit (pipe output if piped)"),
            ("app.add_buffer", "N", "New buffer"),
            ("app.rename_buffer", "r", "Rename buffer"),
            ("app.merge_buffers", "M", "Merge with another buffer"),
        ],
    ),
    (
        "Groups",
        [
            ("app.show_group_next", "}", "Next group"),
            ("app.show_group_previous", "{", "Previous group"),
            ("app.rename_group", "R", "Rename group"),
            ("app.open_file", "O", "Open file"),
        ],
    ),
    (
        "Navigation",
        [
            ("table.cursor_left", "h / b / B", "Left"),
            ("table.cursor_right", "l / w", "Right"),
            ("table.cursor_down", "j", "Down"),
            ("table.cursor_up", "k", "Up"),
            ("table.scroll_to_beginning", "0", "First column"),
            ("table.scroll_to_end", "$", "Last column"),
            ("table.scroll_top", "g", "First row"),
            ("table.scroll_bottom", "G", "Last row"),
            ("table.page_up", "ctrl+u", "Page up"),
            ("table.page_down", "ctrl+d", "Page down"),
            ("buffer.jump_columns", "c", "Jump to column"),
        ],
    ),
    (
        "Column Visibility",
        [
            ("app.filter_columns", "C", "Show/hide columns"),
            ("app.toggle_arrival", "A", "Toggle arrival timestamps"),
            ("buffer.pin_column", "m", "Pin/unpin column"),
            ("buffer.hide_column", "X", "Hide column"),
            ("buffer.move_column_right", ">", "Move column right"),
            ("buffer.move_column_left", "<", "Move column left"),
        ],
    ),
    (
        "Pivoting",
        [
            ("app.mark_unique", "U", "Mark column as key"),
            (None, "enter", "Drill into pivot"),
        ],
    ),
    (
        "Filtering",
        [
            ("app.filter", "f", "Filter column"),
            ("app.filter_cursor_word", "F", "Filter by cursor word"),
            ("app.exclude_filter", "e", "Exclude from column"),
            ("app.exclude_filter_cursor_word", "E", "Exclude by cursor word"),
            ("app.filter_any", "|", "Filter all columns"),
            ("app.search_to_filter", "&", "Search to filter"),
            ("app.time_window", "@", "Time window (+ for rolling)"),
        ],
    ),
    (
        "Searching & Highlighting",
        [
            ("app.search", "/", "Search"),
            ("buffer.search_cursor_word", "*", "Search cursor word"),
            ("buffer.next_search", "n", "Next match"),
            ("buffer.previous_search", "p", "Previous match"),
            (
                "app.add_highlight",
                "+",
                "Pin search as highlight (clears all if no search)",
            ),
            (
                "app.navigate_highlight",
                "-",
                "Navigate, recolor, or remove pinned highlights",
            ),
        ],
    ),
    (
        "Sorting & Analysis",
        [
            ("buffer.sort", "s", "Sort column"),
            (
                "buffer.aggregations",
                "a",
                "Column aggregations (count, sum, avg, min, max)",
            ),
            ("app.exmode", ":", "Ex mode (s/, sort, filter, w, q, ...)"),
        ],
    ),
    (
        "Output",
        [
            ("app.write_to_file", "W", "Write to file"),
            ("buffer.copy", "y", "Copy cell"),
        ],
    ),
    (
        "Shell Commands",
        [
            ("app.run_command", "!", "Run shell command"),
        ],
    ),
    (
        "Delimiter / File Parsing",
        [
            ("app.delimiter", "D", "Change delimiter"),
            ("app.column_delimiter", "d", "Split column"),
            ("app.detect_log_format", "P", "Auto-detect log format"),
        ],
    ),
    (
        "JSON",
        [
            ("app.json_header", "J", "Extract JSON key"),
        ],
    ),
    (
        "Tail Mode",
        [
            ("buffer.toggle_tail", "t", "Toggle tail mode"),
            ("buffer.reset_highlights", "x", "Reset highlights"),
        ],
    ),
    (
        "Excluded Lines",
        [
            ("buffer.view_unparsed_logs", "~", "View excluded lines"),
        ],
    ),
    (
        "Appearance",
        [
            ("app.select_theme", "T", "Select theme"),
            ("app.select_keymap", "K", "Select keymap"),
        ],
    ),
    (
        "Sessions & Views",
        [
            ("app.session_menu", "S", "Save, load, or delete sessions"),
            ("app.view_menu", "v", "Save, load, or delete views"),
        ],
    ),
    (
        "Help",
        [
            ("app.help", "?", "Show help"),
            ("app.caption", "#", "Show caption overlay"),
        ],
    ),
]


def _resolve_key(
    binding_id: str | None,
    default_key: str,
    keymap_bindings: dict[str, str],
) -> str:
    """Return the display key for a help entry, applying keymap overrides."""
    if binding_id is None:
        return default_key
    if binding_id in keymap_bindings:
        return _format_key(keymap_bindings[binding_id])
    return default_key


class HelpScroll(VerticalScroll):
    """VerticalScroll with cursor-movement bindings for the help screen."""

    BINDINGS = [
        Binding("down,j", "scroll_down", "Down", id="table.cursor_down"),
        Binding("up,k", "scroll_up", "Up", id="table.cursor_up"),
        Binding("ctrl+d", "page_down", "Page Down", id="table.page_down"),
        Binding("ctrl+u", "page_up", "Page Up", id="table.page_up"),
        Binding("G", "scroll_end", "Bottom", id="table.scroll_bottom"),
        Binding("g", "scroll_home", "Top", id="table.scroll_top"),
    ]


class HelpScreen(ModalScreen):
    """A screen displaying keybindings and config in separate tabs."""

    DEFAULT_CSS = """
    #help-footer {
        dock: bottom;
        height: 1;
        padding: 0 1;
    }
    #help-close {
        dock: right;
        width: 3;
        height: 100%;
        content-align: center top;
        padding-top: 1;
        background: $surface;
    }
    """

    BINDINGS = [
        ("q", "app.pop_screen", "Close Help"),
        Binding("l", "next_tab", "Next tab", show=False),
        Binding("h", "previous_tab", "Previous tab", show=False),
    ]

    def __init__(
        self,
        keymap_name: str = "vim",
        keymap_bindings: dict[str, str] | None = None,
        theme: NlessTheme | None = None,
        config: NlessConfig | None = None,
        release_notes: tuple[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.keymap_name = keymap_name
        self.keymap_bindings = keymap_bindings or {}
        self.help_theme = theme or BUILTIN_THEMES["default"]
        self.config = config
        self.release_notes = release_notes

    def on_mount(self) -> None:
        if self.keymap_bindings:
            self.set_keymap(self.keymap_bindings)
        self._focus_active_scroll()

    def on_tabbed_content_tab_activated(self) -> None:
        self._focus_active_scroll()

    def _focus_active_scroll(self) -> None:
        """Focus the HelpScroll inside the active tab pane."""
        tc = self.query_one(TabbedContent)
        pane = tc.get_pane(tc.active)
        pane.query_one(HelpScroll).focus()

    def action_next_tab(self) -> None:
        self.query_one(Tabs).action_next_tab()

    def action_previous_tab(self) -> None:
        self.query_one(Tabs).action_previous_tab()

    def _build_column_table(
        self,
        categories: list[tuple[str, list[tuple[str | None, str, str]]]],
        t: NlessTheme,
    ) -> Table:
        """Build a Rich Table for one side of the two-column help layout."""
        table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
        table.add_column(justify="right", width=12, style=t.highlight, no_wrap=True)
        table.add_column()
        for i, (category, bindings) in enumerate(categories):
            table.add_row("", f"[bold {t.accent}]{category}[/bold {t.accent}]")
            for binding_id, default_key, description in bindings:
                key = _resolve_key(binding_id, default_key, self.keymap_bindings)
                table.add_row(key, description)
            if i < len(categories) - 1:
                table.add_row("", "")
        return table

    def _build_exmode_table(self, t: NlessTheme) -> Table:
        """Build a Rich Table showing ex-mode commands."""
        table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
        table.add_column(justify="right", width=22, style=t.highlight, no_wrap=True)
        table.add_column()

        commands: list[tuple[str, str, str]] = [
            ("Substitution", "", ""),
            ("s/pat/rep/", "", "Substitute in current column"),
            ("s/pat/rep/g", "", "Substitute in all columns"),
            ("s/pat/rep/i", "", "Case-insensitive substitute"),
            ("s/pat/rep/gi", "", "All columns, case-insensitive"),
            ("", "", ""),
            ("Filtering & Sorting", "", ""),
            ("sort <col> [asc|desc]", "", "Sort by column name"),
            ("filter <col> <pat>", "f", "Filter column by pattern"),
            ("exclude <col> <pat>", "e", "Exclude matches from column"),
            ("clear", "reset", "Reset sort, search, and columns"),
            ("", "", ""),
            ("Columns", "", ""),
            ("cols <c1|c2|...>", "columns", "Show only named columns"),
            ("cols all", "", "Show all columns"),
            ("", "", ""),
            ("Navigation", "", ""),
            (":<number>", "", "Jump to line number"),
            ("", "", ""),
            ("Files & Buffers", "", ""),
            ("w [path]", "write", "Write buffer to file (prompts if no path)"),
            ("o <path>", "open", "Open file in new group"),
            ("q", "quit", "Close buffer or quit"),
            ("q!", "quit!", "Pipe to stdout and exit"),
            ("", "", ""),
            ("Settings", "", ""),
            ("set theme <name>", "", "Switch theme"),
            ("set keymap <name>", "", "Switch keymap"),
            ("delim <d>", "delimiter", "Change delimiter"),
            ("", "", ""),
            ("Column Types", "", ""),
            ("type <col> <type>", "", "Set column type (numeric/date/string/auto)"),
            ("", "", ""),
            ("Other", "", ""),
            ("help", "", "Show this help screen"),
        ]

        for command, alias, description in commands:
            if not command and not alias and not description:
                table.add_row("", "")
            elif not alias and not description:
                # Section header
                table.add_row("", f"[bold {t.accent}]{command}[/bold {t.accent}]")
            else:
                desc = description
                if alias:
                    desc += f" [{t.muted}](alias: {alias})[/{t.muted}]"
                table.add_row(command, desc)

        return table

    def _build_config_table(self, t: NlessTheme) -> Table:
        """Build a Rich Table showing current config values."""
        table = Table(show_header=True, box=None, pad_edge=False, padding=(0, 2))
        table.add_column("Setting", style=t.highlight, no_wrap=True)
        table.add_column("Value")
        if self.config:
            table.add_row("theme", self.config.theme)
            table.add_row("keymap", self.config.keymap)
            table.add_row("status_format", escape(self.config.status_format))
            table.add_row(
                "show_getting_started",
                str(self.config.show_getting_started).lower(),
            )
        return table

    def compose(self) -> ComposeResult:
        t = self.help_theme

        left_categories = KEYBINDING_CATEGORIES[:5]
        right_categories = KEYBINDING_CATEGORIES[5:]

        left_table = self._build_column_table(left_categories, t)
        right_table = self._build_column_table(right_categories, t)

        columns = Columns([left_table, right_table], padding=(0, 3), equal=True)

        keybindings_title = Static(
            f"  [{t.muted}]keymap: {self.keymap_name}[/{t.muted}]"
        )

        with TabbedContent():
            with TabPane("Keybindings"):
                yield HelpScroll(keybindings_title, Static(columns))
            with TabPane("Ex Mode"):
                exmode_title = Static(
                    f"  [{t.muted}]Press : to open ex-mode prompt[/{t.muted}]"
                )
                yield HelpScroll(exmode_title, Static(self._build_exmode_table(t)))
            if self.config:
                config_path = os.path.expanduser(CONFIG_FILE)
                config_title = Static(f"  [{t.muted}]{config_path}[/{t.muted}]")
                with TabPane("Config"):
                    yield HelpScroll(config_title, Static(self._build_config_table(t)))
            if self.release_notes:
                version, notes = self.release_notes
                with TabPane("What's New"):
                    yield HelpScroll(
                        Static(f"  [bold {t.accent}]v{version}[/bold {t.accent}]"),
                        Static(RichMarkdown(notes)),
                    )

        yield Static(
            f"[{t.muted}]q to close · h/l to switch tabs[/{t.muted}]",
            id="help-footer",
        )
        yield Static(f"[bold {t.accent}] x [/bold {t.accent}]", id="help-close")

    def on_click(self, event) -> None:
        widget, _ = self.app.get_widget_at(event.screen_x, event.screen_y)
        if hasattr(widget, "id") and widget.id == "help-close":
            self.app.pop_screen()
