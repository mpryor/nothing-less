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
    DEFAULT_CLASSES = "nless-view"

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
        self._hover_row: int = -1
        self._hover_column: int = -1
        self._hover_header_col: int = -1
        self._last_click_time: float = 0.0
        self._last_click_row: int = -1

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

        self._style_hover = Style(bgcolor=theme.search_match_bg)

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
                y=self.cursor_row,
                width=1,
                height=2,
            ),
            animate=animate if animate else False,
        )
        self.refresh()
        self.post_message(Datatable.CellHighlighted())

    def _column_at_x(self, x: int) -> int:
        """Map a screen x coordinate to a column index."""
        # Walk fixed columns first
        fixed_x = 0
        for i in range(self.fixed_columns):
            w = self.column_widths[i] + self.col_separator_width
            if x < fixed_x + w:
                return i
            fixed_x += w

        # Walk scrollable columns
        adjusted_x = x - fixed_x + self.scroll_offset.x
        acc = 0
        for i in range(self.fixed_columns, len(self.columns)):
            w = self.column_widths[i] + self.col_separator_width
            if adjusted_x < acc + w:
                return i
            acc += w
        return len(self.columns) - 1

    class HeaderClicked(Message):
        def __init__(self, column: int) -> None:
            super().__init__()
            self.column = column

    class RowDoubleClicked(Message):
        def __init__(self, row: int, column: int) -> None:
            super().__init__()
            self.row = row
            self.column = column

    class RightClicked(Message):
        def __init__(
            self,
            row: int,
            column: int,
            screen_x: int,
            screen_y: int,
            is_header: bool = False,
        ) -> None:
            super().__init__()
            self.row = row
            self.column = column
            self.screen_x = screen_x
            self.screen_y = screen_y
            self.is_header = is_header

    def on_mouse_down(self, event: events.MouseDown) -> None:
        if event.button == 3:
            # Right-click: post context menu message instead of moving cursor
            if event.y == 0:
                col = self._column_at_x(event.x)
                self.post_message(
                    self.RightClicked(
                        row=-1,
                        column=col,
                        screen_x=event.screen_x,
                        screen_y=event.screen_y,
                        is_header=True,
                    )
                )
            else:
                row = int(self.scroll_offset.y) + event.y - 1
                if row >= len(self.rows):
                    return
                col = self._column_at_x(event.x)
                self.move_cursor(column=col, row=row)
                self.post_message(
                    self.RightClicked(
                        row=row,
                        column=col,
                        screen_x=event.screen_x,
                        screen_y=event.screen_y,
                        is_header=False,
                    )
                )
            event.stop()
            return
        if event.y == 0:
            col = self._column_at_x(event.x)
            self.move_cursor(column=col)
            self.post_message(self.HeaderClicked(column=col))
            return
        row = int(self.scroll_offset.y) + event.y - 1
        if row >= len(self.rows):
            return
        col = self._column_at_x(event.x)
        self.move_cursor(column=col, row=row)

        import time

        now = time.monotonic()
        if now - self._last_click_time < 0.4 and row == self._last_click_row:
            self.post_message(self.RowDoubleClicked(row=row, column=col))
            self._last_click_time = 0.0
            self._last_click_row = -1
        else:
            self._last_click_time = now
            self._last_click_row = row

    def on_mouse_scroll_up(self, event: events.MouseScrollUp) -> None:
        self.move_cursor(row=max(0, self.cursor_row - 3))

    def on_mouse_scroll_down(self, event: events.MouseScrollDown) -> None:
        self.move_cursor(row=min(len(self.rows) - 1, self.cursor_row + 3))

    def on_mouse_move(self, event: events.MouseMove) -> None:
        if event.y == 0:
            col = self._column_at_x(event.x)
            if col != self._hover_header_col:
                self._hover_header_col = col
                self.refresh()
            row = -1
            col = -1
        else:
            if self._hover_header_col != -1:
                self._hover_header_col = -1
                self.refresh()
            row = int(self.scroll_offset.y) + event.y - 1
            if row >= len(self.rows):
                row = -1
                col = -1
            else:
                col = self._column_at_x(event.x)
        if row != self._hover_row or col != self._hover_column:
            self._hover_row = row
            self._hover_column = col
            self.refresh()

    def on_leave(self, event: events.Leave) -> None:
        changed = (
            self._hover_row != -1
            or self._hover_column != -1
            or self._hover_header_col != -1
        )
        self._hover_row = -1
        self._hover_column = -1
        self._hover_header_col = -1
        if changed:
            self.refresh()

    def action_page_up(self) -> None:
        page = self.size.height - 1  # -1 for header
        new_row = max(0, self.cursor_row - page)
        self.move_cursor(row=new_row)

    def action_page_down(self) -> None:
        page = self.size.height - 1
        new_row = min(len(self.rows) - 1, self.cursor_row + page)
        self.move_cursor(row=new_row)

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
        hover = self._hover_header_col
        header_style = self._style_header
        hover_style = header_style + Style(reverse=True)
        segments: list[Segment] = []

        # Fixed columns
        for i in range(self.fixed_columns):
            style = hover_style if i == hover else header_style
            text = self.columns[i].ljust(self.column_widths[i]) + self.col_separator
            segments.append(Segment(text, style))

        # Scrollable columns: build full string, then slice to viewport
        scroll_segments: list[Segment] = []
        for i, col in enumerate(self.columns):
            if i < self.fixed_columns:
                continue
            style = hover_style if i == hover else header_style
            text = col.ljust(self.column_widths[i]) + self.col_separator
            scroll_segments.append(Segment(text, style))

        # Slice scrollable portion to viewport
        scroll_strip = Strip(scroll_segments).crop(x, x + self.size.width)
        segments.extend(scroll_strip._segments)

        return Strip(segments)

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
            is_hover_row = (y - 1) == self._hover_row
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
                    is_hover_cell = is_hover_row and i == self._hover_column
                    if is_cursor_cell:
                        fixed_column_style = self._style_cursor
                    elif is_hover_cell:
                        fixed_column_style = (
                            self._style_fixed_column + self._style_hover
                        )
                    else:
                        fixed_column_style = self._style_fixed_column
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
                    if is_hover_row and i == self._hover_column and not is_cursor_cell:
                        segment_style = segment_style + self._style_hover
                        separator_style = separator_style + self._style_hover

                    trim_len = 0

                    if accumulated_x < x:
                        trim_len = x - accumulated_x

                    cell_render_len = 0
                    original_trim_len = trim_len
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
                                    trim_len = 0
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
                    rjust_amt = curr_column_width - original_trim_len - cell_render_len
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
