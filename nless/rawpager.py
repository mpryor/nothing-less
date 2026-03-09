"""Raw text pager widget — renders lines as-is without columnar formatting.

Uses Textual's RichLog for rendering and scrolling, with a cursor overlay
for line-by-line navigation compatible with the Datatable interface that
NlessBuffer expects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.segment import Segment
from rich.style import Style
from rich.text import Text
from textual.binding import Binding
from textual.geometry import Region
from textual.strip import Strip
from textual.widgets import RichLog

from .datatable import Coordinate, Datatable

if TYPE_CHECKING:
    from .theme import NlessTheme

DEFAULT_CLASSES = "nless-view"


class RawPager(RichLog):
    """RichLog-based raw text pager with cursor navigation.

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
        super().__init__(wrap=False, highlight=False, markup=True, auto_scroll=False)
        # Datatable-compatible state
        self.rows: list[list[str]] = []
        self.columns: list[str] = [""]
        self.column_widths: list[int] = [0]
        self.cursor_row: int = 0
        self.cursor_column: int = 0
        self.cursor_coordinate = Coordinate(0, 0)
        self.row_count: int = 0
        self.fixed_columns: int = 0
        self._init_styles(theme)

    def _init_styles(self, theme: NlessTheme | None = None) -> None:
        if theme is None:
            from .theme import BUILTIN_THEMES

            theme = BUILTIN_THEMES["default"]
        self._style_cursor = Style(
            bold=True, color=theme.cursor_fg, bgcolor=theme.cursor_bg
        )
        self.styles.scrollbar_background = theme.scrollbar_bg
        self.styles.scrollbar_color = theme.scrollbar_fg

    def apply_theme(self, theme: NlessTheme) -> None:
        self._init_styles(theme)
        self.refresh()

    # -- Column interface (no-op for raw mode) --------------------------------

    def add_columns(self, columns: list[str]) -> None:
        pass

    # -- Row management -------------------------------------------------------

    def _write_line(self, line: str) -> None:
        """Write a single line to the RichLog."""
        line = line.rstrip("\n")
        if "[" in line:
            try:
                self.write(Text.from_markup(line))
            except Exception:
                self.write(Text(line))
        else:
            self.write(Text(line))

    def add_rows(self, rows_data: list[list[str]]) -> None:
        for row in rows_data:
            self.rows.append(row)
            self._write_line(row[0] if row else "")
        self.row_count = len(self.rows)

    def add_rows_precomputed(self, rows_data: list[list[str]]) -> None:
        self.add_rows(rows_data)

    def add_row(self, row_data: list[str]) -> None:
        self.rows.append(row_data)
        self._write_line(row_data[0] if row_data else "")
        self.row_count = len(self.rows)

    def add_row_at(self, index: int, row_data: list[str]) -> None:
        self.rows.insert(index, row_data)
        self.row_count = len(self.rows)
        self._rebuild_lines()

    def remove_row(self, index: int) -> None:
        self.rows.pop(index)
        self.row_count = len(self.rows)
        self._rebuild_lines()

    def clear(self, columns: bool | None = None) -> None:
        self.rows = []
        self.row_count = 0
        self.columns = [""]
        self.column_widths = [0]
        super().clear()

    def _rebuild_lines(self) -> None:
        """Re-render all lines from self.rows (for insert/remove)."""
        super().clear()
        for row in self.rows:
            self._write_line(row[0] if row else "")

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

    # -- Rendering (cursor overlay) -------------------------------------------

    def render_line(self, y: int) -> Strip:
        """Render a line, highlighting the cursor row."""
        strip = super().render_line(y)
        row_idx = int(self.scroll_offset.y) + y
        if row_idx == self.cursor_row:
            strip = Strip(
                [
                    Segment(text, self._style_cursor, control)
                    for text, _style, control in strip._segments
                ],
                strip.cell_length,
            )
        return strip
