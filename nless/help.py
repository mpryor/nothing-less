from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.screen import Screen
from textual.widgets import Static


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
            (None, "1-9", "Select the buffer at the corresponding index"),
            ("app.show_tab_next", "L", "Next Buffer"),
            ("app.show_tab_previous", "H", "Previous Buffer"),
            ("app.close_active_buffer", "q", "Close Active Buffer (or quit if last)"),
            ("app.add_buffer", "N", "New Buffer"),
        ],
    ),
    (
        "Navigation",
        [
            ("table.cursor_left", "h / b / B", "Move cursor left"),
            ("table.cursor_right", "l / w", "Move cursor right"),
            ("table.cursor_down", "j", "Move cursor down"),
            ("table.cursor_up", "k", "Move cursor up"),
            ("table.scroll_to_beginning", "0", "Jump to first column"),
            ("table.scroll_to_end", "$", "Jump to final column"),
            ("table.scroll_top", "g", "Jump to first row"),
            ("table.scroll_bottom", "G", "Jump to final row"),
            ("table.page_up", "ctrl+u", "Page up"),
            ("table.page_down", "ctrl+d", "Page down"),
            ("buffer.jump_columns", "c", "Jump to column (by select)"),
        ],
    ),
    (
        "Column Visibility",
        [
            ("app.filter_columns", "C", "Filter Columns (by prompt)"),
            ("buffer.move_column_right", ">", "Move column right"),
            ("buffer.move_column_left", "<", "Move column left"),
        ],
    ),
    (
        "Pivoting",
        [
            ("app.mark_unique", "U", "Mark column as composite key for grouping"),
            (None, "enter", "Dive into data behind a pivot"),
        ],
    ),
    (
        "Filtering",
        [
            ("app.filter", "f", "Filter selected column (by prompt)"),
            (
                "app.filter_cursor_word",
                "F",
                "Filter selected column by word under cursor",
            ),
            ("app.exclude_filter", "e", "Exclude from selected column (by prompt)"),
            (
                "app.exclude_filter_cursor_word",
                "E",
                "Exclude selected column by word under cursor",
            ),
            ("app.filter_any", "|", "Filter any column (by prompt)"),
            ("app.search_to_filter", "&", "Apply current search as filter"),
        ],
    ),
    (
        "Searching",
        [
            ("app.search", "/", "Search (all columns, by prompt)"),
            (
                "buffer.search_cursor_word",
                "*",
                "Search all columns for word under cursor",
            ),
            ("buffer.next_search", "n", "Next search result"),
            ("buffer.previous_search", "p", "Previous search result"),
        ],
    ),
    (
        "Sorting",
        [
            ("buffer.sort", "s", "Sort selected column"),
        ],
    ),
    (
        "Output",
        [
            ("app.write_to_file", "W", "Write current view to file"),
            ("buffer.copy", "y", "Copy cell contents"),
        ],
    ),
    (
        "Shell Commands",
        [
            ("app.run_command", "!", "Run Shell Command (by prompt)"),
        ],
    ),
    (
        "Delimiter / File Parsing",
        [
            ("app.delimiter", "D", "Change Delimiter"),
            ("app.column_delimiter", "d", "Change Column Delimiter"),
        ],
    ),
    (
        "JSON",
        [
            ("app.json_header", "J", "Select new header from JSON in cell"),
        ],
    ),
    (
        "Tail Mode",
        [
            ("buffer.toggle_tail", "t", "Toggle tail mode"),
            ("buffer.reset_highlights", "r", "Reset new-line highlights"),
        ],
    ),
    (
        "Unparsed Logs",
        [
            ("buffer.view_unparsed_logs", "~", "View logs not matching delimiter"),
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
        "Help",
        [
            ("app.help", "?", "Show this help screen"),
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


class HelpScreen(Screen):
    """A widget to display keybindings help."""

    BINDINGS = [("q", "app.pop_screen", "Close Help")]

    def __init__(
        self,
        keymap_name: str = "vim",
        keymap_bindings: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.keymap_name = keymap_name
        self.keymap_bindings = keymap_bindings or {}

    def compose(self) -> ComposeResult:
        help_text = f"[bold]Keybindings[/bold]  (keymap: {self.keymap_name})\n"
        for category, bindings in KEYBINDING_CATEGORIES:
            help_text += f"\n[bold underline]{category}[/bold underline]\n"
            for binding_id, default_key, description in bindings:
                key = _resolve_key(binding_id, default_key, self.keymap_bindings)
                help_text += f"  {key:<12} {description}\n"
        yield VerticalScroll(Static(help_text))
        yield Static("[bold]Press 'q' to close this help.[/bold]", id="help-footer")
