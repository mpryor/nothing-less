
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Static

from .nlesstable import NlessDataTable


class HelpScreen(Screen):
    """A widget to display keybindings help."""

    BINDINGS = [("q", "app.pop_screen", "Close Help")]

    def compose(self) -> ComposeResult:
        bindings = self.app.BINDINGS + NlessDataTable.BINDINGS
        help_text = "[bold]Keybindings[/bold]:\n\n"
        for binding in bindings:
            keys, _, description = binding
            help_text += f"{keys:<12} - {description}\n"
        yield Static(help_text)
        yield Static(
            "[bold]Press 'q' to close this help.[/bold]", id="help-footer"
        )

