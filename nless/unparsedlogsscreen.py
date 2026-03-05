"""Screen for displaying rows that don't match the current delimiter."""

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import RichLog, Static


class UnparsedLogsScreen(Screen):
    BINDINGS = [("q", "app.pop_screen", "Close")]

    def __init__(self, unparsed_rows: list[str], delimiter: str):
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
