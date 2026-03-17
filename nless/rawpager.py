"""Raw text pager widget — renders lines as-is without columnar formatting.

Uses Textual's ScrollView for virtual rendering (only visible lines are
rendered), with a cursor overlay for line-by-line navigation compatible
with the Datatable interface that NlessBuffer expects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.segment import Segment
from rich.style import Style
from rich.text import Text
from textual import events
from textual.binding import Binding
from textual.geometry import Region, Size
from textual.message import Message
from textual.scroll_view import ScrollView
from textual.strip import Strip

from .datatable import Coordinate, Datatable

if TYPE_CHECKING:
    from .theme import NlessTheme

DEFAULT_CLASSES = "nless-view"


class RawPager(ScrollView):
    """ScrollView-based raw text pager with virtual rendering and cursor navigation.

    Only renders visible lines — handles millions of lines without
    materializing Rich objects for every row.

    Exposes the same interface as Datatable so NlessBuffer can use it
    transparently: rows, columns, cursor, add_rows, clear, etc.
    """

    DEFAULT_CLASSES = "nless-view"

    BINDINGS = [
        Binding("G", "scroll_bottom", "Scroll to Bottom", id="table.scroll_bottom"),
        Binding("g", "scroll_top", "Scroll to Top", id="table.scroll_top"),
        Binding("ctrl+d", "page_down", "Page Down", id="table.page_down"),
        Binding("ctrl+u", "page_up", "Page up", id="table.page_up"),
        Binding("up,k", "cursor_up", "Up", id="table.cursor_up"),
        Binding("down,j", "cursor_down", "Down", id="table.cursor_down"),
        Binding("l,w", "cursor_right", "Right", id="table.cursor_right"),
        Binding("h,b,B", "cursor_left", "Left", id="table.cursor_left"),
        Binding("$", "scroll_to_end", "End of Line", id="table.scroll_to_end"),
        Binding(
            "0", "scroll_to_beginning", "Start of Line", id="table.scroll_to_beginning"
        ),
    ]

    def __init__(self, theme: NlessTheme | None = None) -> None:
        super().__init__()
        # Datatable-compatible state
        self.rows: list[list[str]] = []
        self.columns: list[str] = [""]
        self.column_widths: list[int] = [0]
        self.cursor_row: int = 0
        self.cursor_column: int = 0
        self.cursor_coordinate = Coordinate(0, 0)
        self.row_count: int = 0
        self.fixed_columns: int = 0
        self._max_width: int = 0
        self._hover_row: int = -1
        self._init_styles(theme)

    def _init_styles(self, theme: NlessTheme | None = None) -> None:
        if theme is None:
            from .theme import BUILTIN_THEMES

            theme = BUILTIN_THEMES["default"]
        self._style_cursor = Style(
            bold=True, color=theme.cursor_fg, bgcolor=theme.cursor_bg
        )
        self._style_default = Style(color=theme.col_even_fg, bgcolor=theme.row_even_bg)
        self._style_hover = Style(
            color=theme.col_even_fg,
            bgcolor=theme.search_match_bg,
        )
        self._style_blank = Style(bgcolor=theme.row_even_bg)
        self.styles.scrollbar_background = theme.scrollbar_bg
        self.styles.scrollbar_color = theme.scrollbar_fg

    def apply_theme(self, theme: NlessTheme) -> None:
        self._init_styles(theme)
        self.refresh()

    # -- Column interface (no-op for raw mode) --------------------------------

    def add_columns(self, columns: list[str]) -> None:
        pass

    # -- Row management -------------------------------------------------------

    def _update_virtual_size(self) -> None:
        self.virtual_size = Size(self._max_width, len(self.rows))

    def _track_width(self, line: str) -> None:
        """Track max line width for horizontal scrolling."""
        # Use plain len — close enough for virtual_size and avoids markup parsing
        w = len(line)
        if w > self._max_width:
            self._max_width = w

    def add_rows(self, rows_data: list[list[str]]) -> None:
        for row in rows_data:
            self.rows.append(row)
            self._track_width(row[0] if row else "")
        self.row_count = len(self.rows)
        self._update_virtual_size()
        self.refresh()

    def add_rows_precomputed(self, rows_data: list[list[str]]) -> None:
        self.rows.extend(rows_data)
        for row in rows_data:
            self._track_width(row[0] if row else "")
        self.row_count = len(self.rows)
        self._update_virtual_size()
        self.refresh()

    def add_row(self, row_data: list[str]) -> None:
        self.rows.append(row_data)
        self._track_width(row_data[0] if row_data else "")
        self.row_count = len(self.rows)
        self._update_virtual_size()
        self.refresh()

    def add_row_at(self, index: int, row_data: list[str]) -> None:
        self.rows.insert(index, row_data)
        self._track_width(row_data[0] if row_data else "")
        self.row_count = len(self.rows)
        self._update_virtual_size()
        self.refresh()

    def remove_row(self, index: int) -> None:
        self.rows.pop(index)
        self.row_count = len(self.rows)
        # Don't recompute _max_width — it's only used for scrollbar sizing
        self._update_virtual_size()
        self.refresh()

    def clear(self, columns: bool | None = None) -> None:
        self.rows = []
        self.row_count = 0
        self.columns = [""]
        self.column_widths = [0]
        self._max_width = 0
        self.virtual_size = Size(0, 0)
        self.refresh()

    # -- Cell access ----------------------------------------------------------

    def get_cell_at(self, coordinate: Coordinate) -> str | None:
        try:
            return self.rows[coordinate.row][0]
        except (IndexError, TypeError):
            return None

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

        self.scroll_to_region(
            region=Region(
                x=self.scroll_offset.x,
                y=self.cursor_row,
                width=1,
                height=2,
            ),
            animate=bool(animate),
        )
        self.refresh()
        self.post_message(Datatable.CellHighlighted())

    def action_scroll_bottom(self) -> None:
        self.move_cursor(row=len(self.rows) - 1)

    def action_scroll_top(self) -> None:
        self.move_cursor(row=0)

    def action_page_up(self) -> None:
        page = self.size.height
        self.move_cursor(row=max(0, self.cursor_row - page))

    def action_page_down(self) -> None:
        page = self.size.height
        self.move_cursor(row=min(len(self.rows) - 1, self.cursor_row + page))

    def action_cursor_up(self) -> None:
        self.move_cursor(row=max(0, self.cursor_row - 1))

    def action_cursor_down(self) -> None:
        self.move_cursor(row=min(len(self.rows) - 1, self.cursor_row + 1))

    def action_cursor_right(self) -> None:
        self.scroll_right(animate=False)

    def action_cursor_left(self) -> None:
        self.scroll_left(animate=False)

    def action_scroll_to_end(self) -> None:
        self.scroll_to(self.max_scroll_x, self.scroll_offset.y, animate=False)

    def action_scroll_to_beginning(self) -> None:
        self.scroll_to(0, self.scroll_offset.y, animate=False)

    class RightClicked(Message):
        def __init__(self, row: int, screen_x: int, screen_y: int) -> None:
            super().__init__()
            self.row = row
            self.screen_x = screen_x
            self.screen_y = screen_y

    def on_mouse_down(self, event: events.MouseDown) -> None:
        if event.button == 3:
            row = int(self.scroll_offset.y) + event.y
            if row >= len(self.rows):
                return
            self.move_cursor(row=row)
            self.post_message(
                self.RightClicked(
                    row=row, screen_x=event.screen_x, screen_y=event.screen_y
                )
            )
            event.stop()
            return
        row = int(self.scroll_offset.y) + event.y
        if row >= len(self.rows):
            return
        self.move_cursor(row=row)

    def on_mouse_scroll_up(self, event: events.MouseScrollUp) -> None:
        self.move_cursor(row=max(0, self.cursor_row - 3))

    def on_mouse_scroll_down(self, event: events.MouseScrollDown) -> None:
        self.move_cursor(row=min(len(self.rows) - 1, self.cursor_row + 3))

    def on_mouse_move(self, event: events.MouseMove) -> None:
        row = int(self.scroll_offset.y) + event.y
        if row >= len(self.rows):
            row = -1
        if row != self._hover_row:
            self._hover_row = row
            self.refresh()

    def on_leave(self, event: events.Leave) -> None:
        if self._hover_row != -1:
            self._hover_row = -1
            self.refresh()

    # -- Rendering (virtual — only visible lines) -----------------------------

    def render_line(self, y: int) -> Strip:
        """Render a single visible line with cursor highlighting."""
        row_idx = int(self.scroll_offset.y) + y
        x = self.scroll_offset.x

        if row_idx >= len(self.rows):
            width = self.size.width
            return Strip([Segment(" " * width, self._style_blank)], width)

        line = self.rows[row_idx][0] if self.rows[row_idx] else ""
        is_cursor = row_idx == self.cursor_row
        if is_cursor:
            style = self._style_cursor
        elif row_idx == self._hover_row:
            style = self._style_hover
        else:
            style = self._style_default

        if "[" in line:
            try:
                text = Text.from_markup(line)
            except Exception:
                text = Text(line)
        else:
            text = Text(line)

        # Render to segments, then apply horizontal scroll offset
        console = self.app.console
        segments = []
        consumed = 0
        remaining_trim = x
        for seg_text, seg_style, control in text.render(console):
            if remaining_trim > 0:
                if len(seg_text) <= remaining_trim:
                    remaining_trim -= len(seg_text)
                    continue
                seg_text = seg_text[remaining_trim:]
                remaining_trim = 0
            segments.append(
                Segment(seg_text, style + seg_style if seg_style else style, control)
            )
            consumed += len(seg_text)

        # Pad to full width so cursor highlight covers the line
        width = self.size.width
        if consumed < width:
            segments.append(Segment(" " * (width - consumed), style))

        return Strip(segments, width)
