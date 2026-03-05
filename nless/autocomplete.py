from typing import Callable

from textual.message import Message
from textual.widgets import Input, RichLog, Static

from nless.suggestions import HistorySuggestionProvider, SuggestionProvider


class AutocompleteInput(Static):
    """An Input widget with autocomplete dropdown functionality."""

    class Submitted(Message):
        """Posted when the user submits the input (Enter with dropdown hidden)."""

        def __init__(self, autocomplete_input: "AutocompleteInput", value: str) -> None:
            super().__init__()
            self._autocomplete_input = autocomplete_input
            self.value = value

        @property
        def input(self) -> "AutocompleteInput":
            return self._autocomplete_input

        @property
        def control(self) -> "AutocompleteInput":
            return self._autocomplete_input

    def __init__(
        self,
        *args,
        on_add: Callable[[str], None],
        on_remove: Callable[[str], None],
        history: list[str] | None = None,
        provider: SuggestionProvider | None = None,
        placeholder: str = "",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.history = history or []
        self.history_index = len(self.history)
        self.on_add = on_add
        self.on_remove = on_remove
        self.placeholder = placeholder
        self.provider = provider or HistorySuggestionProvider(self.history)
        self._suggestions: list[str] = []
        self._highlight_index = 0
        self._dropdown_visible = False

    def compose(self):
        yield RichLog(markup=True, auto_scroll=False)
        yield Input(placeholder=self.placeholder)

    def on_mount(self):
        self.query_one(Input).focus()

    @property
    def _input(self) -> Input:
        return self.query_one(Input)

    @property
    def value(self) -> str:
        return self._input.value

    @value.setter
    def value(self, val: str) -> None:
        self._input.value = val

    def _show_dropdown(self, suggestions: list[str]) -> None:
        self._suggestions = suggestions
        self._highlight_index = 0
        self._dropdown_visible = True
        self._render_dropdown()
        self.query_one(RichLog).add_class("visible")

    def _hide_dropdown(self) -> None:
        self._dropdown_visible = False
        self._suggestions = []
        self.query_one(RichLog).remove_class("visible")

    def _render_dropdown(self) -> None:
        rich_log = self.query_one(RichLog)
        rich_log.clear()
        try:
            muted = self.app.nless_theme.muted
        except AttributeError:
            muted = "#888888"
        for i, item in enumerate(self._suggestions):
            if i == self._highlight_index:
                rich_log.write(f"[reverse]{item}[/reverse]")
            else:
                rich_log.write(f"[{muted}]{item}[/{muted}]")
        if self._suggestions:
            rich_log.scroll_to(y=self._highlight_index)

    def _accept_suggestion(self) -> None:
        """Fill the input with the highlighted suggestion."""
        if self._suggestions and 0 <= self._highlight_index < len(self._suggestions):
            self._input.value = self._suggestions[self._highlight_index]
            self._input.cursor_position = len(self._input.value)
        self._hide_dropdown()

    def on_input_changed(self, event: Input.Changed) -> None:
        event.stop()
        suggestions = self.provider.get_suggestions(event.value)
        if suggestions:
            self._show_dropdown(suggestions)
        else:
            self._hide_dropdown()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.stop()
        if self._dropdown_visible:
            self._hide_dropdown()
        # History bookkeeping
        value = self._input.value
        if value != "":
            if value in self.history:
                self.on_remove(value)
            self.on_add(value)
        self.post_message(self.Submitted(self, value))

    def on_key(self, event) -> None:
        if self._dropdown_visible:
            if event.key == "down":
                event.stop()
                event.prevent_default()
                if self._highlight_index < len(self._suggestions) - 1:
                    self._highlight_index += 1
                else:
                    self._highlight_index = 0
                self._render_dropdown()
            elif event.key == "up":
                event.stop()
                event.prevent_default()
                if self._highlight_index > 0:
                    self._highlight_index -= 1
                else:
                    self._highlight_index = len(self._suggestions) - 1
                self._render_dropdown()
            elif event.key == "tab":
                event.stop()
                event.prevent_default()
                self._accept_suggestion()
            elif event.key == "escape":
                event.stop()
                event.prevent_default()
                self._hide_dropdown()
        else:
            if event.key == "down":
                event.stop()
                event.prevent_default()
                if self.history_index < len(self.history):
                    self.history_index += 1
                if self.history and self.history_index < len(self.history):
                    self._input.value = self.history[self.history_index]
                else:
                    self._input.value = ""
                self._input.cursor_position = len(self._input.value)
            elif event.key == "up":
                event.stop()
                event.prevent_default()
                if self.history_index > -1:
                    self.history_index -= 1
                if len(self.history) > 0 and self.history_index > -1:
                    self._input.value = self.history[self.history_index]
                else:
                    self._input.value = ""
                self._input.cursor_position = len(self._input.value)
