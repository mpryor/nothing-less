from typing import Callable
from textual.widgets import Input


class AutocompleteInput(Input):
    """An Input widget with autocomplete functionality."""

    BINDINGS = [
        ("up", "previous_history", "Previous in history"),
        ("down", "next_history", "Next in history"),
        ("enter", "select_history", "Select from history"),
    ]

    def action_previous_history(self):
        if self.history_index > -1:
            self.history_index = (self.history_index - 1)

        if len(self.history) > 0 and self.history_index > -1:
            self.value = self.history[self.history_index]
        else:
            self.value = ""

    def action_next_history(self):
        if self.history_index < len(self.history):
            self.history_index = (self.history_index + 1)

        if self.history and self.history_index < len(self.history):
            self.value = self.history[self.history_index]
        else:
            self.value = ""

    async def action_select_history(self):
        if self.value != "" and self.value not in self.history:
            self.on_add(self.value)
        await super().action_submit()


    def __init__(self, *args, on_add: Callable[[str], None], history=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.history_index = len(history) if history else 0
        self.on_add = on_add
        self.history = history or []

