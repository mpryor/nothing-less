from textual import events
from textual.widgets import Input, RichLog, Select, Static


class _SelectLog(RichLog):
    """RichLog with mouse hover/click support for NlessSelect."""

    can_focus = False

    def on_mouse_move(self, event: events.MouseMove) -> None:
        parent = self.parent
        if isinstance(parent, NlessSelect):
            # -2 = border (1) + prompt line (1)
            idx = event.y + int(self.scroll_y) - 2
            if (
                0 <= idx < len(parent.filtered_options)
                and idx != parent.highlight_index
            ):
                saved_scroll = self.scroll_y
                parent.highlight_index = idx
                parent._write_options(scroll=False)
                self.scroll_to(y=saved_scroll, animate=False)

    def on_click(self, event: events.Click) -> None:
        parent = self.parent
        if isinstance(parent, NlessSelect) and event.button == 1:
            # -2 = border (1) + prompt line (1)
            idx = event.y + int(self.scroll_y) - 2
            if 0 <= idx < len(parent.filtered_options):
                if parent._is_separator(idx):
                    return
                parent.highlight_index = idx
                parent.post_message(
                    Select.Changed(parent, parent.filtered_options[idx][1])
                )
                parent.remove()


class NlessSelect(Static):
    DEFAULT_CSS = """
    _SelectLog {
      height: 10;
      border: solid green;
    }
    """

    def __init__(self, options, prompt: str | None = None, *args, **kwargs):
        self.options = options
        self.filtered_options = options
        self.highlight_index = 0
        self.filter = None
        self.prompt = prompt
        super().__init__(*args, **kwargs)

    def _write_options(self, scroll: bool = True):
        rich_log = self.query_one(_SelectLog)
        rich_log.clear()
        rich_log.write(f"[{self._muted}]{self._prompt_text}")
        for i, (k, v) in enumerate(self.filtered_options):
            if i == self.highlight_index:
                rich_log.write(f"[reverse]{k}[/reverse]")
            else:
                rich_log.write(k)
        if scroll:
            rich_log.scroll_to(y=self.highlight_index + 1)

    def _is_separator(self, index: int) -> bool:
        return (
            index < len(self.filtered_options)
            and self.filtered_options[index][1] == "separator"
        )

    def _skip_separators(self, direction: int) -> None:
        """Advance highlight_index past any separator rows."""
        n = len(self.filtered_options)
        if n == 0:
            return
        attempts = 0
        while self._is_separator(self.highlight_index) and attempts < n:
            self.highlight_index = (self.highlight_index + direction) % n
            attempts += 1

    def on_key(self, event):
        if event.key == "down":
            if self.highlight_index < len(self.filtered_options) - 1:
                self.highlight_index += 1
            else:
                self.highlight_index = 0
            self._skip_separators(1)
            self._write_options()
        elif event.key == "up":
            if self.highlight_index > 0:
                self.highlight_index -= 1
            else:
                self.highlight_index = len(self.filtered_options) - 1
            self._skip_separators(-1)
            self._write_options()
        elif event.key == "escape":
            self.remove()

    def on_mount(self):
        self.query_one(Input).focus()

    def on_input_changed(self, event: Input.Changed):
        self.highlight_index = 0
        self.filter = event.input.value.lower()

        self.filtered_options = []
        for k, v in self.options:
            if v == "separator":
                if not self.filter:
                    self.filtered_options.append((k, v))
                continue
            if self.filter and self.filter not in k.lower():
                continue
            self.filtered_options.append((k, v))
        self._skip_separators(1)
        self._write_options()

    def on_input_submitted(self, event: Input.Submitted):
        if not self.filtered_options:
            return
        if self._is_separator(self.highlight_index):
            return
        self.post_message(
            Select.Changed(self, self.filtered_options[self.highlight_index][1])
        )
        self.remove()

    def compose(self):
        try:
            self._muted = self.app.nless_theme.muted
        except AttributeError:
            self._muted = "#888888"
        display_prompt = self.prompt or "Select a JSON key to add as a column"
        self._prompt_text = (
            f"{display_prompt} - Type to filter, Enter to select, Up/Down to navigate"
        )
        rich_log = _SelectLog(markup=True, auto_scroll=False)
        rich_log.write(f"[{self._muted}]{self._prompt_text}")
        for i, (k, v) in enumerate(self.options):
            if i == self.highlight_index:
                rich_log.write(f"[reverse]{k}[/reverse]")
            else:
                rich_log.write(k)
        rich_log.scroll_to(y=0)
        yield rich_log
        yield Input(placeholder=self.prompt or "Type to filter...")
