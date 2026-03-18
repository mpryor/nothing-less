"""Right-click context menu for datatable and rawpager cells."""

from __future__ import annotations

from dataclasses import dataclass

from textual import events
from textual.message import Message
from textual.widgets import Static


@dataclass
class MenuItem:
    label: str
    action: str  # app action name, e.g. "filter_cursor_word"
    key_hint: str = ""  # e.g. "f", "ctrl+d"


class ContextMenu(Static):
    """A popup context menu rendered as an overlay at a screen position.

    Keyboard navigation is handled by the app (since focus on overlay
    widgets is unreliable).  The app should forward key events to
    ``handle_key`` when the menu is visible.
    """

    DEFAULT_CSS = """
    ContextMenu {
        display: none;
        overlay: screen;
        width: auto;
        max-width: 50;
        height: auto;
        border: tall $accent;
        background: $surface;
        padding: 0 1;
    }
    """

    class Selected(Message):
        def __init__(self, action: str) -> None:
            super().__init__()
            self.action = action

    def __init__(self, items: list[MenuItem] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.items: list[MenuItem] = items or []
        self.highlight_index: int = 0
        self.source_menu_id: str | None = None  # set when opened from menu bar

    @property
    def is_open(self) -> bool:
        return self.display

    def show(self, x: int, y: int, items: list[MenuItem]) -> None:
        """Display the menu at screen position (x, y) with the given items."""
        self.items = items
        self.highlight_index = 0
        # Compute column width: label + gap + key hint
        max_label = max(len(item.label) for item in items)
        max_hint = max((len(item.key_hint) for item in items), default=0)
        self._col_width = max_label + (3 + max_hint if max_hint else 0)
        self._render_items()
        # Compute explicit size so overlay layout works reliably
        content_w = self._col_width + 2  # padding
        content_h = len(items)
        menu_w = content_w + 4  # border + padding
        menu_h = content_h + 2  # border
        self.styles.width = menu_w
        self.styles.max_width = menu_w
        self.styles.height = menu_h
        # Apply theme border color
        try:
            accent = self.app.nless_theme.accent
            self.styles.border = ("tall", accent)
        except AttributeError:
            pass
        # Clamp to terminal bounds, flip if overflowing
        try:
            term_h = self.app.size.height
            term_w = self.app.size.width
        except Exception:
            term_h = self.screen.size.height
            term_w = self.screen.size.width
        if y + menu_h > term_h:
            y = max(0, y - menu_h)
        if x + menu_w > term_w:
            x = max(0, term_w - menu_w)
        x = max(0, x)
        y = max(0, y)
        self.styles.offset = (x, y)
        self.display = True

    def dismiss(self) -> None:
        self.display = False

    def process_key(self, key: str) -> bool:
        """Process a key press.  Returns True if the key was consumed."""
        if key == "escape":
            self.dismiss()
            return True
        if key in ("up", "k"):
            if self.highlight_index > 0:
                self.highlight_index -= 1
                self._render_items()
            return True
        if key in ("down", "j"):
            if self.highlight_index < len(self.items) - 1:
                self.highlight_index += 1
                self._render_items()
            return True
        if key == "enter":
            if self.items:
                self.post_message(
                    self.Selected(self.items[self.highlight_index].action)
                )
                self.dismiss()
            return True
        return False

    def on_mouse_move(self, event: events.MouseMove) -> None:
        """Highlight item under cursor on hover."""
        item_y = event.y - 1
        if 0 <= item_y < len(self.items) and item_y != self.highlight_index:
            self.highlight_index = item_y
            self._render_items()

    def on_mouse_down(self, event: events.MouseDown) -> None:
        """Handle item selection on left-click, block right-click bubbling."""
        event.stop()
        if event.button == 1:
            # event.y is widget-relative; subtract border (1) to get item index
            item_y = event.y - 1
            if 0 <= item_y < len(self.items):
                self.highlight_index = item_y
                self.post_message(self.Selected(self.items[item_y].action))
                self.dismiss()
            else:
                self.dismiss()

    def _render_items(self) -> None:
        w = getattr(self, "_col_width", 0) or max(
            (len(item.label) for item in self.items), default=0
        )
        lines = []
        for i, item in enumerate(self.items):
            if item.key_hint:
                pad = w - len(item.label) - len(item.key_hint)
                text = f"{item.label}{' ' * pad}[dim]{item.key_hint}[/dim]"
            else:
                text = item.label.ljust(w)
            if i == self.highlight_index:
                lines.append(f"[reverse] {text} [/reverse]")
            else:
                lines.append(f" {text} ")
        self.update("\n".join(lines))
