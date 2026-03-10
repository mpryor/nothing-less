from textual.widgets import Input, RichLog, Select, Static


class NlessSelect(Static):
    DEFAULT_CSS = """
    RichLog {
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

    def _write_options(self):
        rich_log = self.query_one(RichLog)
        rich_log.clear()
        for i, (k, v) in enumerate(self.filtered_options):
            if i == self.highlight_index:
                rich_log.write(f"[reverse]{k}[/reverse]")
            else:
                rich_log.write(k)
        rich_log.scroll_to(y=self.highlight_index)

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
        rich_log = self.query_one(RichLog)
        rich_log.clear()

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
        rich_log = RichLog(markup=True, auto_scroll=False)
        display_prompt = self.prompt or "Select a JSON key to add as a column"
        try:
            muted = self.app.nless_theme.muted
        except AttributeError:
            muted = "#888888"
        rich_log.write(
            f"[{muted}]{display_prompt} - Type to filter, Enter to select, Up/Down to navigate"
        )
        for i, (k, v) in enumerate(self.options):
            if i == self.highlight_index:
                rich_log.write(f"[reverse]{k}[/reverse]")
            else:
                rich_log.write(k)
        rich_log.scroll_to(y=0)
        yield rich_log
        yield Input(placeholder=self.prompt or "Type to filter...")
