from __future__ import annotations
from dataclasses import dataclass
import traceback

from rich.text import Text
from textual import reactive
from textual.geometry import Region, Size
from textual.message import Message
from textual.reactive import Reactive, var
from textual.strip import Strip
from textual.scroll_view import ScrollView

from rich.segment import Segment
from rich.style import Style


@dataclass()
class Coordinate:
    row: int
    column: int


class Datatable(ScrollView):
    class CellHighlighted(Message):
        pass

    BINDINGS = [
        ("G", "scroll_bottom", "Scroll to Bottom"),
        ("g", "scroll_top", "Scroll to Top"),
        ("ctrl+d", "page_down", "Page Down"),
        ("ctrl+u", "page_up", "Page up"),
        ("up,k", "cursor_up", "Up"),
        ("down,j", "cursor_down", "Down"),
        ("l,w", "cursor_right", "Right"),
        ("h,b,B", "cursor_left", "Left"),
        ("$", "scroll_to_end", "End of Line"),
        ("0", "scroll_to_beginning", "Start of Line"),
    ]

    cursor_coordinate = var(Coordinate(0, 0))
    col_separator: str = "   "
    col_separator_width: int = len(col_separator)

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

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self.columns: list[str] = []
        self.column_widths: list[int] = []

        self.cursor_row: int = 0
        self.cursor_column: int = 0
        self.row_count: int = 0

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
        except Exception:
            traceback.print_exc()
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

        self.cursor_coordinate = Coordinate(self.cursor_row, self.cursor_column)
        total_width = sum(self.column_widths[0 : self.cursor_column + 1]) + (
            3 * self.cursor_column
        )
        if self.cursor_column == 0:
            total_width = 0
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
        separator_width = 3  # width of " | "
        return sum(self.column_widths) + len(self.column_widths) * separator_width

    def add_columns(self, columns: list[str]) -> None:
        for col in columns:
            if col in self.columns:
                continue
            self.columns.append(col)
        self.column_widths = [15 for _ in self.columns]  # default width

    def add_rows(self, rows_data: list[list[str]]) -> None:
        for row in rows_data:
            for i, cell in enumerate(row):
                text = Text.from_markup(str(cell))  # validate markup
                str_len = 0
                for seg_text, seg_style, _ in text.render(self.app.console):
                    str_len += len(seg_text)

                if str_len > self.column_widths[i]:
                    self.column_widths[i] = str_len

        self.rows.extend(rows_data)
        self.virtual_size = Size(self._calc_max_width(), len(self.rows) + 1)
        self.row_count += len(rows_data)
        self.refresh()

    def add_row_at(self, index: int, row_data: list[str]) -> None:
        for i, cell in enumerate(row_data):
            if len(cell) > self.column_widths[i]:
                self.column_widths[i] = len(cell)

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
        segment_str = ""
        for i, col in enumerate(self.columns):
            segment_str += col.ljust(self.column_widths[i])
            segment_str += "   "
        return Strip(
            [
                Segment(
                    segment_str[x : x + self.size.width],
                    Style(bold=True, bgcolor="#005f5f", color="#d7ffff"),
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
            for i, cell in enumerate(row):
                curr_column_width = self.column_widths[i] + 3
                if accumulated_x + curr_column_width <= x:
                    # skip this cell if it's before the x offset
                    accumulated_x += curr_column_width
                    continue
                else:
                    is_cursor_cell = (
                        i == self.cursor_column and (y - 1) == self.cursor_row
                    )
                    is_zebra_row = (y - 1) % 2 != 0
                    is_zebra_column = i % 2 != 0

                    cursor_style = (
                        Style(bgcolor="#0087d7", bold=True, color="#d7ffff") if is_cursor_cell
                        else Style()
                    )

                    column_style = (
                        Style(color="#bbbbbb") if is_zebra_column and not is_cursor_cell
                        else Style(color="#dddddd")
                    )
                    zebra_style = (
                        Style(bgcolor="#0087d7", bold=True, color="#d7ffff") if is_cursor_cell
                        else Style(bgcolor="#222222") if is_zebra_row
                        else Style(bgcolor="#333333")
                    )
                    segment_style = Style.combine(
                        [cursor_style, column_style, zebra_style]
                    )

                    str_len = 0
                    trim_len = 0

                    if accumulated_x < x:
                        trim_len = (
                            x - accumulated_x
                        )  # amount to trim from start of cell, because we have scrolled to the middle of a cell

                    segment_str = Text.from_markup(
                        str(cell)
                    )  # allow cells to use rich markup, e.g. [bold]text[/bold]
                    for parsed_text, parsed_style, _ in segment_str.render(
                        self.app.console
                    ):
                        str_len += len(parsed_text)
                        if trim_len > 0:  # need to trim from start of cell
                            if len(parsed_text) <= trim_len:
                                trim_len -= len(
                                    parsed_text
                                )  # fully trimmed this segment, move to next - as it will need trimmed as well
                                continue
                            else:
                                parsed_text = parsed_text[trim_len:]
                                trim_len = 0
                        segments.append(
                            Segment(parsed_text, segment_style + parsed_style)
                        )

                    separator_trim_amt = 0  # amount to trim from separator if we had to trim cell content
                    rjust_amt = curr_column_width - str_len - trim_len
                    if (
                        rjust_amt < self.col_separator_width
                    ):  # the total space we want the separator to take up is less than the full separator width, we need to trim it
                        separator_trim_amt = self.col_separator_width - rjust_amt

                    segments.append(
                        Segment(
                            self.col_separator[separator_trim_amt:].rjust(rjust_amt),
                            Style.combine([zebra_style, cursor_style]),
                        )
                    )
                    accumulated_x += curr_column_width
            return Strip(segments)
        else:
            return Strip([])
