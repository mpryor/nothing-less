"""User-facing actions (key bindings) for NlessBuffer."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable
from typing import TYPE_CHECKING

import pyperclip

from .dataprocessing import matches_all_filters, strip_markup
from .delimiter import split_line
from .nlessselect import NlessSelect
from .types import MetadataColumn, UpdateReason

if TYPE_CHECKING:
    from .buffer import NlessBuffer

    from textual.widgets import Select


class ActionsMixin:
    """Mixin providing user-facing action methods for NlessBuffer."""

    def action_copy(self: NlessBuffer) -> None:
        """Copy the contents of the currently highlighted cell to the clipboard."""
        data_table = self.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
        except (IndexError, TypeError):
            self.notify("Cannot get cell value.", severity="error")
            return
        try:
            pyperclip.copy(cell_value)
            self.notify("Cell contents copied to clipboard.", severity="information")
        except pyperclip.PyperclipException:
            self.notify(
                "Clipboard not available — is xclip/xsel installed?",
                severity="error",
            )

    def _make_shown_filter(
        self: NlessBuffer, *, include_ancestors: bool = True
    ) -> Callable[[str], bool]:
        """Build a filter that returns True if a line would be shown in this buffer.

        A line is "shown" if it parses with the delimiter, has the right column
        count, and passes all content filters.  When *include_ancestors* is True
        (the default), a line is also considered shown if any ancestor buffer
        would show it (via _source_parse_filter).  Pass False when building the
        filter set for ``action_view_unparsed_logs`` so each buffer is evaluated
        independently.
        """
        delimiter = self.delim.value
        columns = list(self.current_columns)
        metadata = {mc.value for mc in MetadataColumn}
        expected = len([c for c in columns if c.name not in metadata])
        filters = list(self.query.filters)
        col_lookup = dict(self.cache.col_data_idx)
        parent = self._source_parse_filter if include_ancestors else None

        def shown(line: str) -> bool:
            if parent and parent(line):
                return True
            try:
                cells = split_line(line, delimiter, columns)
            except (json.JSONDecodeError, csv.Error, ValueError, StopIteration):
                return False
            if len(cells) != expected:
                return False
            if not filters:
                return True
            # Append a dummy arrival timestamp for filter column alignment
            cells.append("")
            return matches_all_filters(
                cells, filters, lambda name, _rp=False: col_lookup.get(name)
            )

        return shown

    def action_view_unparsed_logs(self: NlessBuffer) -> None:
        """Create a new buffer containing logs not shown in any open buffer."""
        buffer_filters = []
        for buf in self.app.buffers:
            shown = buf._make_shown_filter(include_ancestors=False)
            if buf.delim.value == "raw" and not buf.query.filters:
                raw_set = set(buf.raw_rows)
                buffer_filters.append(lambda line, _s=raw_set: line in _s)
            else:
                buffer_filters.append(shown)

        def shown_in_any(line: str) -> bool:
            return any(f(line) for f in buffer_filters)

        if self.line_stream:
            all_lines = self.line_stream.lines
        elif self._all_source_lines is not None:
            all_lines = self._all_source_lines
        else:
            all_lines = self.raw_rows

        excluded_rows = [line for line in all_lines if not shown_in_any(line)]

        if not excluded_rows:
            self.notify("All logs are being shown.", severity="information")
            return

        self.app._create_unparsed_buffer(
            excluded_rows,
            source_parse_filter=shown_in_any,
            line_stream=self.line_stream,
        )

    def action_jump_columns(self: NlessBuffer) -> None:
        """Show columns by user input."""
        column_options = [
            (strip_markup(c.name), c.render_position)
            for c in sorted(self.current_columns, key=lambda c: c.render_position)
            if not c.hidden
        ]
        select = NlessSelect(
            options=column_options,
            classes="dock-bottom",
            prompt="Type a column to jump to",
            id="column_jump_select",
        )
        self.mount(select)

    def on_select_changed(self: NlessBuffer, event: Select.Changed) -> None:
        if event.control.id and event.control.id != "column_jump_select":
            return  # Not ours — let it bubble to the app
        col_index = event.value
        event.control.remove()
        data_table = self.query_one(".nless-view")
        data_table.move_cursor(column=col_index)

    def action_move_column(self: NlessBuffer, direction: int) -> None:
        with self._try_lock(
            "move column", deferred=lambda: self.action_move_column(direction)
        ) as acquired:
            if not acquired:
                return
            data_table = self.query_one(".nless-view")
            current_cursor_column = data_table.cursor_column
            selected_column = self._get_column_at_position(current_cursor_column)
            if not selected_column:
                self.notify("No column selected to move", severity="error")
                return
            if selected_column.name in [m.value for m in MetadataColumn]:
                return  # can't move metadata columns
            if (
                direction == 1
                and selected_column.render_position == len(self.current_columns) - 1
            ) or (direction == -1 and selected_column.render_position == 0):
                return  # can't move further in that direction

            adjacent_column = self._get_column_at_position(
                selected_column.render_position + direction
            )
            if not adjacent_column or adjacent_column.name in [
                m.value for m in MetadataColumn
            ]:  # can't move past metadata columns
                return

            if (
                adjacent_column.pinned
                and not selected_column.pinned
                or (selected_column.pinned and not adjacent_column.pinned)
            ):
                return

            selected_column.render_position, adjacent_column.render_position = (
                adjacent_column.render_position,
                selected_column.render_position,
            )
            new_position = selected_column.render_position

        self._deferred_update_table(
            callback=lambda: data_table.move_cursor(column=new_position),
            reason=UpdateReason.MOVING_COLUMN,
        )

    def action_move_column_left(self: NlessBuffer) -> None:
        self.action_move_column(-1)

    def action_move_column_right(self: NlessBuffer) -> None:
        self.action_move_column(1)

    def action_pin_column(self: NlessBuffer) -> None:
        """Pin or unpin the currently selected column to the left."""
        if self.raw_mode:
            return
        data_table = self.query_one(".nless-view")
        selected_column = self._get_column_at_position(data_table.cursor_column)
        if not selected_column:
            return
        if selected_column.name in [m.value for m in MetadataColumn]:
            return  # can't pin/unpin metadata columns

        if selected_column.pinned:
            old_pos = selected_column.render_position
            selected_column.pinned = False
            selected_column.labels.discard("P")
            pinned_count = sum(
                1 for c in self.current_columns if c.pinned and not c.hidden
            )
            for c in self.current_columns:
                if c is selected_column:
                    continue
                if c.pinned and c.render_position > old_pos:
                    c.render_position -= 1
            selected_column.render_position = pinned_count
            for c in self.current_columns:
                if c is selected_column or c.pinned:
                    continue
                if not c.hidden and pinned_count <= c.render_position < old_pos:
                    c.render_position += 1
        else:
            old_pos = selected_column.render_position
            pinned_count = sum(
                1 for c in self.current_columns if c.pinned and not c.hidden
            )
            selected_column.pinned = True
            selected_column.labels.add("P")
            for c in self.current_columns:
                if c is selected_column or c.pinned or c.hidden:
                    continue
                if pinned_count <= c.render_position < old_pos:
                    c.render_position += 1
            selected_column.render_position = pinned_count

        new_pos = selected_column.render_position
        self.invalidate_caches()
        self._deferred_update_table(
            callback=lambda: data_table.move_cursor(column=new_pos),
            reason=UpdateReason.PINNING_COLUMN,
        )

    def action_toggle_tail(self: NlessBuffer) -> None:
        self.is_tailing = not self.is_tailing
        self._update_status_bar()

    def action_reset_highlights(self: NlessBuffer) -> None:
        """Remove new-line highlights from all displayed rows."""
        data_table = self.query_one(".nless-view")
        highlight_re = self._get_theme().highlight_re
        for row_idx, row in enumerate(self.displayed_rows):
            new_row = [highlight_re.sub(r"\1", cell) for cell in row]
            self.displayed_rows[row_idx] = new_row
            data_table.rows[row_idx] = new_row
        data_table.refresh()

    def action_next_search(self: NlessBuffer) -> None:
        self._navigate_search(1)

    def action_previous_search(self: NlessBuffer) -> None:
        self._navigate_search(-1)

    def action_sort(self: NlessBuffer) -> None:
        with self._try_lock("sort", deferred=self.action_sort) as acquired:
            if not acquired:
                return
            data_table = self.query_one(".nless-view")
            current_cursor_column = data_table.cursor_column
            selected_column = self._get_column_at_position(current_cursor_column)
            if not selected_column:
                self.notify("No column selected for sorting", severity="error")
                return

            new_sort_column_name = strip_markup(selected_column.name)

            if (
                self.query.sort_column == new_sort_column_name
                and self.query.sort_reverse
            ):
                self.query.sort_column = None
            elif (
                self.query.sort_column == new_sort_column_name
                and not self.query.sort_reverse
            ):
                self.query.sort_reverse = True
            else:
                self.query.sort_column = new_sort_column_name
                self.query.sort_reverse = False

            # Update sort indicators
            if self.query.sort_column is None:
                selected_column.labels.discard("▲")
                selected_column.labels.discard("▼")
            elif self.query.sort_reverse:
                selected_column.labels.discard("▲")
                selected_column.labels.add("▼")
            else:
                selected_column.labels.discard("▼")
                selected_column.labels.add("▲")

            # Remove sort indicators from other columns
            for col in self.current_columns:
                if col.name != selected_column.name:
                    col.labels.discard("▲")
                    col.labels.discard("▼")

        self._deferred_update_table(reason=UpdateReason.SORT)

    def action_aggregations(self: NlessBuffer) -> None:
        from .operations import compute_column_aggregations

        data_table = self.query_one(".nless-view")
        selected_column = self._get_column_at_position(data_table.cursor_column)
        if not selected_column:
            self.notify("No column selected", severity="error")
            return

        col_name = strip_markup(selected_column.name)
        render_idx = data_table.cursor_column
        result = compute_column_aggregations(self, render_idx)
        if result is None:
            self.notify(f"No data in column '{col_name}'", severity="warning")
            return

        self.notify(f"[bold]{col_name}[/bold]: {result}", timeout=10)
