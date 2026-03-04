from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.screen import Screen
from textual.widgets import Static


# Categorized keybindings matching the docs structure.
# Each entry is (category_name, [(key, description), ...]).
KEYBINDING_CATEGORIES = [
    (
        "Buffers",
        [
            ("1-9", "Select the buffer at the corresponding index"),
            ("L", "Next Buffer"),
            ("H", "Previous Buffer"),
            ("q", "Close Active Buffer (or quit if last)"),
            ("N", "New Buffer"),
        ],
    ),
    (
        "Navigation",
        [
            ("h", "Move cursor left"),
            ("l", "Move cursor right"),
            ("j", "Move cursor down"),
            ("k", "Move cursor up"),
            ("0", "Jump to first column"),
            ("$", "Jump to final column"),
            ("g", "Jump to first row"),
            ("G", "Jump to final row"),
            ("w", "Move cursor right"),
            ("b / B", "Move cursor left"),
            ("ctrl+u", "Page up"),
            ("ctrl+d", "Page down"),
            ("c", "Jump to column (by select)"),
        ],
    ),
    (
        "Column Visibility",
        [
            ("C", "Filter Columns (by prompt)"),
            (">", "Move column right"),
            ("<", "Move column left"),
        ],
    ),
    (
        "Pivoting",
        [
            ("U", "Mark column as composite key for grouping"),
            ("enter", "Dive into data behind a pivot"),
        ],
    ),
    (
        "Filtering",
        [
            ("f", "Filter selected column (by prompt)"),
            ("F", "Filter selected column by word under cursor"),
            ("e", "Exclude from selected column (by prompt)"),
            ("E", "Exclude selected column by word under cursor"),
            ("|", "Filter any column (by prompt)"),
            ("&", "Apply current search as filter"),
        ],
    ),
    (
        "Searching",
        [
            ("/", "Search (all columns, by prompt)"),
            ("*", "Search all columns for word under cursor"),
            ("n", "Next search result"),
            ("p", "Previous search result"),
        ],
    ),
    (
        "Sorting",
        [
            ("s", "Sort selected column"),
        ],
    ),
    (
        "Output",
        [
            ("W", "Write current view to file"),
            ("y", "Copy cell contents"),
        ],
    ),
    (
        "Shell Commands",
        [
            ("!", "Run Shell Command (by prompt)"),
        ],
    ),
    (
        "Delimiter / File Parsing",
        [
            ("D", "Change Delimiter"),
            ("d", "Change Column Delimiter"),
        ],
    ),
    (
        "JSON",
        [
            ("J", "Select new header from JSON in cell"),
        ],
    ),
    (
        "Tail Mode",
        [
            ("t", "Toggle tail mode"),
            ("r", "Reset new-line highlights"),
        ],
    ),
    (
        "Unparsed Logs",
        [
            ("~", "View logs not matching delimiter"),
        ],
    ),
    (
        "Help",
        [
            ("?", "Show this help screen"),
        ],
    ),
]


class HelpScreen(Screen):
    """A widget to display keybindings help."""

    BINDINGS = [("q", "app.pop_screen", "Close Help")]

    def compose(self) -> ComposeResult:
        help_text = "[bold]Keybindings[/bold]\n"
        for category, bindings in KEYBINDING_CATEGORIES:
            help_text += f"\n[bold underline]{category}[/bold underline]\n"
            for key, description in bindings:
                help_text += f"  {key:<12} {description}\n"
        yield VerticalScroll(Static(help_text))
        yield Static("[bold]Press 'q' to close this help.[/bold]", id="help-footer")
