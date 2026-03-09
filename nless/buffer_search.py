"""Search, navigation, and match highlighting for NlessBuffer."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from textual.coordinate import Coordinate

from .dataprocessing import highlight_search_matches, strip_markup

if TYPE_CHECKING:
    from .buffer import NlessBuffer


class SearchMixin:
    """Mixin providing search methods for NlessBuffer."""

    def action_search_cursor_word(self: NlessBuffer) -> None:
        """Search for the word under the cursor."""
        data_table = self.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            self._perform_search(cell_value)
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")

    def _perform_search(self: NlessBuffer, search_term: str | None) -> None:
        """Performs a search on the data and updates the table."""
        with self._try_lock(
            "search", deferred=lambda: self._perform_search(search_term)
        ) as acquired:
            if not acquired:
                return
            try:
                if search_term:
                    self.search_term = re.compile(search_term, re.IGNORECASE)
                else:
                    self.search_term = None
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        def _after_search():
            if self.search_matches:
                self._navigate_search(1)  # Jump to first match

        self._deferred_update_table(
            restore_position=False, callback=_after_search, reason="Searching"
        )

    def _search_match_style(self: NlessBuffer) -> str:
        """Return the Rich style string for search match highlighting."""
        theme = self._get_theme()
        return f"{theme.search_match_fg} on {theme.search_match_bg}"

    def _highlight_search_matches(
        self: NlessBuffer,
        rows: list[list[str]],
        fixed_columns: int,
        row_offset: int = 0,
    ) -> list[list[str]]:
        """Apply search highlighting to rows and populate search_matches."""
        result, new_matches = highlight_search_matches(
            rows,
            self.search_term,
            fixed_columns,
            row_offset,
            search_match_style=self._search_match_style(),
        )
        self.search_matches.extend(Coordinate(r, c) for r, c in new_matches)
        return result

    def _navigate_search(self: NlessBuffer, direction: int) -> None:
        """Navigate through search matches."""
        if not self.search_matches:
            self.notify("No search results.", severity="warning")
            return

        num_matches = len(self.search_matches)
        self.current_match_index = (
            self.current_match_index + direction + num_matches
        ) % num_matches  # Wrap around
        target_coord = self.search_matches[self.current_match_index]
        data_table = self.query_one(".nless-view")
        data_table.move_cursor(row=target_coord.row, column=target_coord.column)
        self._update_status_bar()
