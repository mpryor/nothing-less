from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich.text import Text
from textual import events
from textual.binding import Binding
from textual.geometry import Region, Size
from textual.message import Message
from textual.reactive import var
from textual.strip import Strip
from textual.scroll_view import ScrollView

from rich.segment import Segment
from rich.style import Style

if TYPE_CHECKING:
    from .theme import NlessTheme


@dataclass()
class Coordinate:
    row: int
    column: int


class Datatable(ScrollView):
    class CellHighlighted(Message):
        pass

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

    cursor_coordinate = var(Coordinate(0, 0))
    col_separator: str = "   "
    col_separator_width: int = len(col_separator)
    fixed_columns: int = 0

    def action_scroll_bottom(self) -> None:
        self.move_cursor(row=len(self.rows) - 1)

    def action_scroll_top(self) -> None:
        self.move_cursor(row=0)

    def action_scroll_to_beginning(self) -> None:
        """Move cursor to beginning of current row."""
        self.move_cursor(column=0)

    def action_scroll_to_end(self) -> None:
        """Move cursor to end of current row."""
        self.move_cursor(column=len(self.columns) - 1)

    def __init__(self, theme: NlessTheme | None = None) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self.columns: list[str] = []
        self.column_widths: list[int] = []

        self.cursor_row: int = 0
        self.cursor_column: int = 0
        self.row_count: int = 0

        self._init_styles(theme)

    def _init_styles(self, theme: NlessTheme | None = None) -> None:
        """Build instance-level style objects and caches from a theme."""
        if theme is None:
            from .theme import BUILTIN_THEMES

            theme = BUILTIN_THEMES["default"]

        self._style_cursor = Style(
            bgcolor=theme.cursor_bg, bold=True, color=theme.cursor_fg
        )
        self._style_header = Style(
            bold=True, bgcolor=theme.header_bg, color=theme.header_fg
        )
        self._style_fixed_column = Style(bgcolor=theme.fixed_column_bg)
        self._style_zebra_odd_row = Style(bgcolor=theme.row_odd_bg)
        self._style_zebra_even_row = Style(bgcolor=theme.row_even_bg)
        self._style_zebra_odd_col = Style(color=theme.col_odd_fg)
        self._style_zebra_even_col = Style(color=theme.col_even_fg)

        self._cell_styles: dict[tuple[bool, bool, bool], Style] = {}
        self._sep_styles: dict[tuple[bool, bool], Style] = {}
        self._build_style_cache()

        # Apply scrollbar colors via Textual CSS properties
        self.styles.scrollbar_background = theme.scrollbar_bg
        self.styles.scrollbar_color = theme.scrollbar_fg

    def _build_style_cache(self) -> None:
        for is_cursor in (True, False):
            for is_odd_row in (True, False):
                cursor_style = self._style_cursor if is_cursor else Style()
                zebra_style = (
                    self._style_cursor
                    if is_cursor
                    else self._style_zebra_odd_row
                    if is_odd_row
                    else self._style_zebra_even_row
                )
                self._sep_styles[(is_cursor, is_odd_row)] = Style.combine(
                    [zebra_style, cursor_style]
                )
                for is_odd_col in (True, False):
                    column_style = (
                        self._style_zebra_odd_col
                        if is_odd_col and not is_cursor
                        else self._style_zebra_even_col
                    )
                    self._cell_styles[(is_cursor, is_odd_row, is_odd_col)] = (
                        Style.combine([cursor_style, column_style, zebra_style])
                    )

    def on_resize(self, event: events.Resize) -> None:
        """Clamp scroll offset and re-render on terminal resize."""
        max_x = max(0, self.virtual_size.width - self.size.width)
        max_y = max(0, self.virtual_size.height - self.size.height)
        clamped_x = min(self.scroll_offset.x, max_x)
        clamped_y = min(self.scroll_offset.y, max_y)
        if clamped_x != self.scroll_offset.x or clamped_y != self.scroll_offset.y:
            self.scroll_to(clamped_x, clamped_y, animate=False)
        self.refresh()

    def apply_theme(self, theme: NlessTheme) -> None:
        """Re-apply a theme: rebuild style caches and refresh."""
        self._init_styles(theme)
        self.refresh()

    def remove_row(self, index: int) -> None:
        self.rows.pop(index)
        self.virtual_size = Size(0, len(self.rows) + 1)
        self.row_count -= 1
        self.refresh()

    def watch_cursor_coordinate(self, coordinate: Coordinate) -> None:
        self.move_cursor(row=coordinate.row, column=coordinate.column)

    def get_cell_at(self, coordinate: Coordinate) -> str | None:
        try:
            return self.rows[coordinate.row][coordinate.column]
        except (IndexError, TypeError):
            return None

    def move_cursor(
        self,
        column: int | None = None,
        row: int | None = None,
        scroll: bool | None = None,
        animate: bool | None = None,
    ) -> None:
        self.cursor_column = column if column is not None else self.cursor_column
        self.cursor_row = row if row is not None else self.cursor_row

        if len(self.rows) > 0 and self.cursor_row > len(self.rows) - 1:
            self.cursor_row = len(self.rows) - 1

        if len(self.columns) > 0 and self.cursor_column > len(self.columns) - 1:
            self.cursor_column = len(self.columns) - 1

        if len(self.columns) == 0 and len(self.rows) == 0:
            self.cursor_column = 0
            self.cursor_row = 0

        self.cursor_coordinate = Coordinate(self.cursor_row, self.cursor_column)

        total_width = sum(self.column_widths[0 : self.cursor_column]) + (
            self.col_separator_width * self.cursor_column
        )

        sum_fixed_column_widths = sum(self.column_widths[0 : self.fixed_columns]) + (
            self.col_separator_width * self.fixed_columns
        )

        if (
            self.scroll_offset.x < total_width
            and self.cursor_column <= len(self.columns) - 1
        ):  # we're left of the column, so we need to scroll the right edge into view
            total_width += (
                self.column_widths[self.cursor_column] + self.col_separator_width
            )

        if self.cursor_column == 0:
            total_width = 0

        total_width = max(total_width - sum_fixed_column_widths, 0)

        self.scroll_to_region(
            region=Region(
                x=total_width,
                y=self.cursor_row if self.cursor_row == 0 else self.cursor_row + 1,
                width=1,
                height=1,
            ),
            animate=animate if animate else False,
        )
        self.refresh()
        self.post_message(Datatable.CellHighlighted())

    def action_cursor_up(self) -> None:
        if self.cursor_row > 0:
            self.cursor_row -= 1
            self.move_cursor(row=self.cursor_row)

    def action_cursor_down(self) -> None:
        if self.cursor_row < len(self.rows) - 1:
            self.cursor_row += 1
            self.move_cursor(row=self.cursor_row)

    def action_cursor_left(self) -> None:
        if self.cursor_column > 0:
            self.cursor_column -= 1
            self.move_cursor(column=self.cursor_column)

    def action_cursor_right(self) -> None:
        if self.cursor_column < len(self.columns) - 1:
            self.cursor_column += 1
            self.move_cursor(column=self.cursor_column)

    def add_row(self, row_data: list[str]) -> None:
        self.rows.append(row_data)
        self.virtual_size = Size(0, len(self.rows) + 1)
        self.row_count += 1
        self.refresh()

    def _calc_max_width(self) -> int:
        return (
            sum(self.column_widths) + len(self.column_widths) * self.col_separator_width
        )

    def add_columns(self, columns: list[str]) -> None:
        for col in columns:
            if col in self.columns:
                continue
            self.columns.append(col)
        self.column_widths = [len(col) for col in self.columns]  # default width

    def add_rows(self, rows_data: list[list[str]]) -> None:
        for row in rows_data:
            for i, cell_str in enumerate(row):
                # Only parse markup if it contains markup characters
                if "[" in cell_str:
                    text = Text.from_markup(cell_str)
                    str_len = text.cell_len
                else:
                    str_len = len(cell_str)
                self.column_widths[i] = max(self.column_widths[i], str_len)

        self.rows.extend(rows_data)
        self.virtual_size = Size(self._calc_max_width(), len(self.rows) + 1)
        self.row_count += len(rows_data)
        self.refresh()

    def add_rows_precomputed(self, rows_data: list[list[str]]) -> None:
        """Add rows when column widths have already been updated by the caller."""
        self.rows.extend(rows_data)
        self.virtual_size = Size(self._calc_max_width(), len(self.rows) + 1)
        self.row_count += len(rows_data)
        self.refresh()

    def add_row_at(self, index: int, row_data: list[str]) -> None:
        for i, cell in enumerate(row_data):
            if "[" in cell:
                cell_len = Text.from_markup(cell).cell_len
            else:
                cell_len = len(cell)
            if cell_len > self.column_widths[i]:
                self.column_widths[i] = cell_len

        self.rows.insert(index, row_data)
        self.row_count += 1
        self.virtual_size = Size(self._calc_max_width(), len(self.rows) + 1)
        self.refresh()

    def clear(self, columns: bool | None = None) -> None:
        self.rows = []
        self.row_count = 0
        self.virtual_size = Size(0, 0)
        if columns:
            self.columns = []
            self.column_widths = []
        self.refresh()

    def _render_column_headers(self, x: int) -> Strip:
        fixed_columns_str = ""
        for i in range(self.fixed_columns):
            fixed_columns_str += (
                self.columns[i].ljust(self.column_widths[i]) + self.col_separator
            )

        segment_str = ""
        for i, col in enumerate(self.columns):
            if i < self.fixed_columns:
                continue
            segment_str += col.ljust(self.column_widths[i])
            segment_str += self.col_separator

        return Strip(
            [
                Segment(
                    fixed_columns_str + segment_str[x : x + self.size.width],
                    self._style_header,
                )
            ]
        )

    def render_line(self, y: int) -> Strip:
        y = y + self.scroll_offset.y
        x = self.scroll_offset.x
        # if we're at the top, render the column header
        if y == self.scroll_offset.y:
            return self._render_column_headers(x)

        # else render the rows
        if y - 1 < len(self.rows):
            row = self.rows[y - 1]  # -1 to account for header
            segments = []
            accumulated_x = 0  # track how far we've rendered horizontally
            is_zebra_row = (y - 1) % 2 != 0
            is_cursor_row = (y - 1) == self.cursor_row
            console = self.app.console

            for i, cell in enumerate(row):
                curr_column_width = self.column_widths[i] + self.col_separator_width
                if (
                    accumulated_x + curr_column_width <= x
                    and i > self.fixed_columns - 1
                ):
                    # skip this cell if it's before the x offset
                    accumulated_x += curr_column_width
                    continue
                elif i < self.fixed_columns:
                    is_cursor_cell = i == self.cursor_column and is_cursor_row
                    fixed_column_style = (
                        self._style_fixed_column
                        if not is_cursor_cell
                        else self._style_cursor
                    )
                    cell_text = Text.from_markup(str(cell))
                    for parsed_text, parsed_style, _ in cell_text.render(console):
                        segments.append(
                            Segment(
                                parsed_text
                                + self.col_separator.rjust(
                                    curr_column_width - len(parsed_text)
                                ),
                                fixed_column_style + parsed_style,
                            )
                        )
                else:
                    is_cursor_cell = i == self.cursor_column and is_cursor_row

                    segment_style = self._cell_styles[
                        (is_cursor_cell, is_zebra_row, i % 2 != 0)
                    ]
                    separator_style = self._sep_styles[(is_cursor_cell, is_zebra_row)]

                    trim_len = 0

                    if accumulated_x < x:
                        trim_len = x - accumulated_x

                    cell_render_len = 0
                    if "[" in cell:
                        parsed_markup_text = Text.from_markup(cell)
                        for (
                            parsed_text,
                            parsed_style,
                            _,
                        ) in parsed_markup_text.render(console):
                            if trim_len > 0:  # need to trim from start of cell
                                if len(parsed_text) <= trim_len:
                                    trim_len -= len(parsed_text)
                                    continue
                                else:
                                    parsed_text = parsed_text[trim_len:]
                            segments.append(
                                Segment(parsed_text, segment_style + parsed_style)
                            )
                            cell_render_len += len(parsed_text)
                    else:
                        if trim_len > 0:
                            cell = cell[trim_len:]
                        cell_render_len = len(cell)
                        segments.append(Segment(cell, segment_style))

                    separator_trim_amt = 0
                    rjust_amt = curr_column_width - trim_len - cell_render_len
                    if rjust_amt < self.col_separator_width:
                        separator_trim_amt = self.col_separator_width - rjust_amt

                    segments.append(
                        Segment(
                            self.col_separator[separator_trim_amt:].rjust(rjust_amt),
                            separator_style,
                        )
                    )
                    accumulated_x += (
                        curr_column_width
                        + rjust_amt
                        - separator_trim_amt
                        + self.col_separator_width
                    )
            return Strip(segments)
        else:
            return Strip([])
