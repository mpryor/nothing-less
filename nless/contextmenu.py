"""Right-click context menu for datatable and rawpager cells."""

from __future__ import annotations

from dataclasses import dataclass

from textual import events
from textual.binding import Binding
from textual.message import Message
from textual.reactive import var
from textual.widgets import Static


@dataclass
class MenuItem:
    label: str
    action: str  # app action name, e.g. "filter_cursor_word"


class ContextMenu(Static, can_focus=True):
    """A popup context menu rendered as an overlay at a screen position."""

    DEFAULT_CSS = """
    ContextMenu {
        display: none;
        overlay: screen;
        width: auto;
        max-width: 30;
        height: auto;
        border: tall $accent;
        background: $surface;
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Dismiss", show=False),
        Binding("up,k", "move_up", "Up", show=False),
        Binding("down,j", "move_down", "Down", show=False),
        Binding("enter", "select", "Select", show=False),
    ]

    highlight_index: int = var(0)

    class Selected(Message):
        def __init__(self, action: str) -> None:
            super().__init__()
            self.action = action

    def __init__(self, items: list[MenuItem] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.items: list[MenuItem] = items or []

    def show(self, x: int, y: int, items: list[MenuItem]) -> None:
        """Display the menu at screen position (x, y) with the given items."""
        self.items = items
        self.highlight_index = 0
        self.styles.offset = (x, y)
        self.display = True
        self._render_items()
        self.focus()

    def dismiss(self) -> None:
        self.display = False

    def on_blur(self) -> None:
        self.dismiss()

    def action_dismiss(self) -> None:
        self.dismiss()

    def action_move_up(self) -> None:
        if self.highlight_index > 0:
            self.highlight_index -= 1
            self._render_items()

    def action_move_down(self) -> None:
        if self.highlight_index < len(self.items) - 1:
            self.highlight_index += 1
            self._render_items()

    def action_select(self) -> None:
        if self.items:
            self.post_message(self.Selected(self.items[self.highlight_index].action))
            self.dismiss()

    def on_click(self, event: events.Click) -> None:
        """Select item on left-click, dismiss on click outside."""
        if event.button != 1:
            return
        if 0 <= event.y < len(self.items):
            self.highlight_index = event.y
            self.action_select()
        else:
            self.dismiss()

    def on_mouse_down(self, event: events.MouseDown) -> None:
        """Prevent right-click on menu from bubbling."""
        event.stop()

    def _render_items(self) -> None:
        lines = []
        for i, item in enumerate(self.items):
            if i == self.highlight_index:
                lines.append(f"[reverse] {item.label} [/reverse]")
            else:
                lines.append(f" {item.label} ")
        self.update("\n".join(lines))
