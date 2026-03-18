"""Release notes screen shown on version upgrade."""

from __future__ import annotations

from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Markdown, Static


class ReleaseNotesScreen(ModalScreen):
    """Modal screen displaying release notes for a new version."""

    DEFAULT_CSS = """
    ReleaseNotesScreen {
        align: center middle;
    }
    #release-notes {
        width: 70;
        max-height: 80%;
        border: tall $accent;
        background: $surface;
        padding: 1 2;
    }
    #release-notes-footer {
        dock: bottom;
        height: 1;
        padding: 0 1;
    }
    """

    BINDINGS = [
        ("q", "app.pop_screen", "Close"),
        ("escape", "app.pop_screen", "Close"),
        Binding("down,j", "scroll_down", "Down", show=False),
        Binding("up,k", "scroll_up", "Up", show=False),
    ]

    def __init__(self, version: str, notes: str) -> None:
        super().__init__()
        self.version = version
        self.notes = notes

    def compose(self):
        try:
            t = self.app.nless_theme
            accent = t.accent
            muted = t.muted
        except AttributeError:
            accent = "green"
            muted = "#888888"

        with VerticalScroll(id="release-notes"):
            yield Static(
                f"[bold {accent}]What's New in v{self.version}[/bold {accent}]"
            )
            yield Static("")
            yield Markdown(self.notes)
            yield Static("")
            yield Static(
                f"[{muted}]Press q to close[/{muted}]",
                id="release-notes-footer",
            )

    def on_mount(self):
        try:
            accent = self.app.nless_theme.accent
            container = self.query_one("#release-notes")
            container.styles.border = ("tall", accent)
        except Exception:
            pass
