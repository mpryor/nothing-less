"""Row filtering actions and handlers for NlessApp."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .operations import handle_mark_unique
from .suggestions import ColumnValueSuggestionProvider
from .types import Filter, UpdateReason

if TYPE_CHECKING:
    from .app import NlessApp
    from .autocomplete import AutocompleteInput


class FilterMixin:
    """Mixin providing row filter methods for NlessApp."""

    def action_filter(self: NlessApp) -> None:
        """Filter rows based on user input."""
        data_table = self._get_current_buffer().query_one(".nless-view")
        column_index = data_table.cursor_column
        column_label = data_table.columns[column_index]
        provider = ColumnValueSuggestionProvider(self._get_column_values(column_index))
        self._create_prompt(
            f"Type filter text for column: {column_label} and press enter",
            "filter_input",
            provider=provider,
        )

    def action_filter_any(self: NlessApp) -> None:
        """Filter any column based on user input."""
        self._create_prompt(
            "Type filter text to match across all columns", "filter_input_any"
        )

    def handle_filter_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        filter_value = event.value
        event.input.remove()
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(".nless-view")
        exclude = event.input.id in ("exclude_filter_input", "exclude_filter_input_any")

        if event.input.id in ("filter_input_any", "exclude_filter_input_any"):
            self._perform_filter(filter_value, exclude=exclude)
        else:
            column_index = data_table.cursor_column
            column = curr_buffer._get_column_at_position(column_index)
            if not column:
                self.notify("No column selected for filtering")
                return
            column_label = strip_markup(column.name)
            self._perform_filter(filter_value, column_label, exclude=exclude)

    def _perform_filter(
        self: NlessApp,
        filter_value: str | None,
        column_name: str | None = None,
        exclude: bool = False,
    ) -> None:
        """Performs a filter on the data and updates the table.

        When column_name is None, filters across all columns.
        """
        curr_buffer = self._get_current_buffer()

        # Validate regex eagerly on main thread
        compiled_pattern = None
        if filter_value:
            try:
                compiled_pattern = re.compile(filter_value, re.IGNORECASE)
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        # Determine buffer name from current state (before copy)
        filter_prefix = "!f" if exclude else "+f"
        if not filter_value:
            new_buf_name = (
                curr_buffer.query.filters
                and f"-f:{','.join([f'{"!" if f.exclude else ""}{f.column if f.column else "any"}={f.pattern.pattern}' for f in curr_buffer.query.filters])}"
                or "-f"
            )
        elif column_name is None:
            new_buf_name = f"{filter_prefix}:any={filter_value}"
        else:
            new_buf_name = f"{filter_prefix}:{column_name}={filter_value}"

        notify_removed_unique = (
            filter_value
            and column_name
            and column_name in curr_buffer.query.unique_column_names
        )

        def setup(new_buffer):
            if not filter_value:
                new_buffer.query.filters = []
            else:
                if column_name and column_name in new_buffer.query.unique_column_names:
                    handle_mark_unique(new_buffer, column_name)
                new_buffer.query.filters.append(
                    Filter(
                        column=column_name,
                        pattern=compiled_pattern,
                        exclude=exclude,
                    )
                )

        def after_add(new_buffer):
            if notify_removed_unique:
                self.notify(
                    f"Removed unique column: {column_name}, to allow filtering.",
                    severity="information",
                )

        self._copy_buffer_async(
            setup,
            new_buf_name,
            after_add_fn=after_add,
            reason=UpdateReason.FILTER,
            done_reason="Filtered",
        )

    def action_filter_cursor_word(self: NlessApp) -> None:
        """Filter by the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            selected_column = curr_buffer._get_column_at_position(coordinate.column)
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                strip_markup(selected_column.name),
            )
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def action_exclude_filter(self: NlessApp) -> None:
        """Exclude rows from selected column based on user input."""
        data_table = self._get_current_buffer().query_one(".nless-view")
        column_index = data_table.cursor_column
        column_label = data_table.columns[column_index]
        provider = ColumnValueSuggestionProvider(self._get_column_values(column_index))
        self._create_prompt(
            f"Type exclude filter text for column: {column_label} and press enter",
            "exclude_filter_input",
            provider=provider,
        )

    def action_exclude_filter_cursor_word(self: NlessApp) -> None:
        """Exclude rows matching the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            cell_value = re.escape(cell_value)
            selected_column = curr_buffer._get_column_at_position(coordinate.column)
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                strip_markup(selected_column.name),
                exclude=True,
            )
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")
