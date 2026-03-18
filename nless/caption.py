"""Center-screen caption overlay for demos and presentations."""

from __future__ import annotations

from textual.widgets import Static


class CaptionOverlay(Static):
    """A temporary, centered text banner that auto-dismisses."""

    DEFAULT_CSS = """
    CaptionOverlay {
        display: none;
        overlay: screen;
        width: auto;
        max-width: 80%;
        height: auto;
        padding: 1 3;
        text-align: center;
        text-style: bold;
        background: $surface;
        border: tall $accent;
        layer: overlay;
    }
    """

    _timer = None

    def show_caption(self, text: str, duration: float = 3.0) -> None:
        """Display *text* centered on screen, auto-dismiss after *duration* seconds."""
        if self._timer is not None:
            self._timer.stop()
        self.update(text)
        # Center on screen
        try:
            term_w = self.app.size.width
            term_h = self.app.size.height
        except Exception:
            term_w = self.screen.size.width
            term_h = self.screen.size.height
        text_w = len(text) + 8  # padding + border
        x = max(0, (term_w - text_w) // 2)
        y = max(0, term_h // 3)
        self.styles.offset = (x, y)
        # Apply theme accent border
        try:
            accent = self.app.nless_theme.accent
            self.styles.border = ("tall", accent)
        except AttributeError:
            pass
        self.display = True
        self._timer = self.set_timer(duration, self._dismiss)

    def _dismiss(self) -> None:
        self.display = False
        self._timer = None
