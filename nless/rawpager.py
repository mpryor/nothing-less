"""Raw text pager widget — renders lines as-is without columnar formatting."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.text import Text
from textual.geometry import Region, Size
from textual.strip import Strip

from rich.segment import Segment
from rich.style import Style

from .datatable import Coordinate, Datatable

if TYPE_CHECKING:
    from .theme import NlessTheme

TAB_WIDTH = 8
SCROLL_STEP = 4


class RawPager(Datatable):
    """ScrollView that renders raw text lines with horizontal scroll.

    Subclasses Datatable for interface compatibility with NlessBuffer
    (cursor, navigation, rows, etc.) but overrides rendering to show
    lines as plain text without header or column padding.
    """

    def __init__(self, theme: NlessTheme | None = None) -> None:
        super().__init__(theme)
        self._max_line_width = 0
        # Single dummy column so parent code that indexes column_widths works.
        self.columns = [""]
        self.column_widths = [0]

    # -- Column interface -----------------------------------------------------

    def add_columns(self, columns: list[str]) -> None:
        # Keep the single dummy column; just ensure column_widths exists.
        self.columns = [""]
        self.column_widths = [0]

    # -- Row management -------------------------------------------------------

    def _track_line_widths(self, rows: list[list[str]]) -> None:
        for row in rows:
            if not row:
                continue
            line = row[0]
            if "[" in line:
                w = Text.from_markup(line).cell_len
            else:
                w = len(line.expandtabs(TAB_WIDTH))
            if w > self._max_line_width:
                self._max_line_width = w
        # Keep the dummy column_widths in sync so parent code works.
        self.column_widths = [self._max_line_width]

    def _update_virtual_size(self) -> None:
        # +1 so render_line row 0 is reserved for the (empty) header slot,
        # matching the parent Datatable's scrolling model.
        self.virtual_size = Size(self._max_line_width, len(self.rows) + 1)

    def add_rows(self, rows_data: list[list[str]]) -> None:
        self._track_line_widths(rows_data)
        self.rows.extend(rows_data)
        self._update_virtual_size()
        self.row_count = len(self.rows)
        self.refresh()

    def add_rows_precomputed(self, rows_data: list[list[str]]) -> None:
        self.add_rows(rows_data)

    def add_row(self, row_data: list[str]) -> None:
        self._track_line_widths([row_data])
        self.rows.append(row_data)
        self._update_virtual_size()
        self.row_count = len(self.rows)
        self.refresh()

    def add_row_at(self, index: int, row_data: list[str]) -> None:
        self._track_line_widths([row_data])
        self.rows.insert(index, row_data)
        self.row_count = len(self.rows)
        self._update_virtual_size()
        self.refresh()

    def remove_row(self, index: int) -> None:
        self.rows.pop(index)
        self.row_count = len(self.rows)
        self._update_virtual_size()
        self.refresh()

    def clear(self, columns: bool | None = None) -> None:
        self.rows = []
        self.row_count = 0
        self._max_line_width = 0
        self.columns = [""]
        self.column_widths = [0]
        self.virtual_size = Size(0, 0)
        self.refresh()

    # -- Cursor / navigation --------------------------------------------------

    def move_cursor(
        self,
        column: int | None = None,
        row: int | None = None,
        scroll: bool | None = None,
        animate: bool | None = None,
    ) -> None:
        self.cursor_column = 0
        self.cursor_row = row if row is not None else self.cursor_row

        if self.rows and self.cursor_row > len(self.rows) - 1:
            self.cursor_row = len(self.rows) - 1
        if not self.rows:
            self.cursor_row = 0

        self.cursor_coordinate = Coordinate(self.cursor_row, 0)

        # +1 because row 0 in the scroll model is the empty header slot.
        self.scroll_to_region(
            region=Region(
                x=self.scroll_offset.x,
                y=self.cursor_row + 1,
                width=1,
                height=2,
            ),
            animate=animate if animate else False,
        )
        self.refresh()
        self.post_message(Datatable.CellHighlighted())

    def action_page_up(self) -> None:
        page = self.size.height
        new_row = max(0, self.cursor_row - page)
        self.move_cursor(row=new_row)

    def action_page_down(self) -> None:
        page = self.size.height
        new_row = min(len(self.rows) - 1, self.cursor_row + page)
        self.move_cursor(row=new_row)

    def action_cursor_right(self) -> None:
        """Scroll right."""
        max_x = max(0, self._max_line_width - self.size.width)
        new_x = min(self.scroll_offset.x + SCROLL_STEP, max_x)
        self.scroll_to(new_x, self.scroll_offset.y, animate=False)

    def action_cursor_left(self) -> None:
        """Scroll left."""
        new_x = max(0, self.scroll_offset.x - SCROLL_STEP)
        self.scroll_to(new_x, self.scroll_offset.y, animate=False)

    def action_scroll_to_end(self) -> None:
        """Scroll to end of longest line."""
        new_x = max(0, self._max_line_width - self.size.width)
        self.scroll_to(new_x, self.scroll_offset.y, animate=False)

    def action_scroll_to_beginning(self) -> None:
        """Scroll to beginning of line."""
        self.scroll_to(0, self.scroll_offset.y, animate=False)

    # -- Rendering ------------------------------------------------------------

    def render_line(self, y: int) -> Strip:
        # Match parent Datatable's scroll model: y is viewport-relative,
        # row 0 at scroll_offset.y is the header slot (render blank).
        y_abs = y + self.scroll_offset.y
        x = self.scroll_offset.x
        width = self.size.width

        # Row 0 in the scroll model is the header — render empty for raw pager.
        if y_abs == self.scroll_offset.y:
            return Strip([Segment(" " * width, Style())])

        row_idx = y_abs - 1  # -1 to account for header slot

        if row_idx < 0 or row_idx >= len(self.rows):
            return Strip([Segment(" " * width, Style())])

        row = self.rows[row_idx]
        line = row[0] if row else ""
        is_cursor = row_idx == self.cursor_row

        if is_cursor:
            base_style = self._style_cursor
        else:
            base_style = Style()

        if "[" in line:
            return self._render_markup_line(line, x, width, base_style)
        else:
            expanded = line.expandtabs(TAB_WIDTH)
            visible = expanded[x : x + width]
            padded = visible.ljust(width)
            return Strip([Segment(padded[:width], base_style)])

    def _render_markup_line(self, line, x, width, base_style):
        """Render a line containing Rich markup with horizontal scroll."""
        text = Text.from_markup(line)
        console = self.app.console
        segments = []
        pos = 0
        rendered = 0

        for plain_text, style, _ in text.render(console):
            expanded = plain_text.expandtabs(TAB_WIDTH)
            seg_end = pos + len(expanded)

            if seg_end <= x:
                pos = seg_end
                continue
            if rendered >= width:
                break

            clip_start = max(0, x - pos)
            clip_end = min(len(expanded), x + width - pos)
            visible_text = expanded[clip_start:clip_end]

            segments.append(Segment(visible_text, base_style + style))
            rendered += len(visible_text)
            pos = seg_end

        if rendered < width:
            segments.append(Segment(" " * (width - rendered), base_style))

        return Strip(segments)
