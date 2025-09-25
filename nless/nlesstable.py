from itertools import zip_longest
from typing import Literal
from rich.text import Text, TextType
from textual.widgets import DataTable
from textual.widgets.data_table import CellType, CursorType, DuplicateKey, Row, RowKey


class NlessDataTable(DataTable):
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

    def __init__(
        self,
        *,
        show_header: bool = True,
        show_row_labels: bool = True,
        fixed_rows: int = 0,
        fixed_columns: int = 0,
        zebra_stripes: bool = False,
        header_height: int = 1,
        show_cursor: bool = True,
        cursor_foreground_priority: Literal["renderable", "css"] = "css",
        cursor_background_priority: Literal["renderable", "css"] = "renderable",
        cursor_type: CursorType = "cell",
        cell_padding: int = 1,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            show_header=show_header,
            show_row_labels=show_row_labels,
            fixed_rows=fixed_rows,
            fixed_columns=fixed_columns,
            zebra_stripes=zebra_stripes,
            header_height=header_height,
            show_cursor=show_cursor,
            cursor_foreground_priority=cursor_foreground_priority,
            cursor_background_priority=cursor_background_priority,
            cursor_type=cursor_type,
            cell_padding=cell_padding,
            name=name,
            id=id,
            classes=classes,
            disabled=disabled,
        )

    def action_scroll_to_end(self) -> None:
        """Move cursor to end of current row."""
        last_column = len(self.columns) - 1
        self.cursor_coordinate = self.cursor_coordinate._replace(
            column=last_column
        )

    def action_scroll_to_beginning(self) -> None:
        """Move cursor to beginning of current row."""
        self.cursor_coordinate = self.cursor_coordinate._replace(column=0)

    def add_row_at(
        self,
        *cells: CellType,
        row_index: int,
        height: int | None = 1,
        key: str | None = None,
        label: TextType | None = None,
    ):
        """Add a row at the bottom of the DataTable.

        Args:
            *cells: Positional arguments should contain cell data.
            height: The height of a row (in lines). Use `None` to auto-detect the optimal
                height.
            key: A key which uniquely identifies this row. If None, it will be generated
                for you and returned.
            label: The label for the row. Will be displayed to the left if supplied.

        Returns:
            Unique identifier for this row. Can be used to retrieve this row regardless
                of its current location in the DataTable (it could have moved after
                being added due to sorting or insertion/deletion of other rows).
        """
        row_key = RowKey(key)
        if row_key in self._row_locations:
            raise DuplicateKey(f"The row key {row_key!r} already exists.")

        if len(cells) > len(self.ordered_columns):
            raise ValueError("More values provided than there are columns.")

        for curr_index in range(self.row_count, row_index, -1):
            old_key = self._row_locations.get_key(curr_index - 1)
            self._row_locations[old_key] = curr_index

        self._row_locations[row_key] = row_index
        self._data[row_key] = {
            column.key: cell
            for column, cell in zip_longest(self.ordered_columns, cells)
        }

        label = Text.from_markup(label, end="") if isinstance(label, str) else label

        # Rows with auto-height get a height of 0 because 1) we need an integer height
        # to do some intermediate computations and 2) because 0 doesn't impact the data
        # table while we don't figure out how tall this row is.
        self.rows[row_key] = Row(
            row_key,
            height or 0,
            label,
            height is None,
        )
        self._new_rows.add(row_key)
        self._require_update_dimensions = True
        self.cursor_coordinate = self.cursor_coordinate

        # If a position has opened for the cursor to appear, where it previously
        # could not (e.g. when there's no data in the table), then a highlighted
        # event is posted, since there's now a highlighted cell when there wasn't
        # before.
        cell_now_available = self.row_count == 1 and len(self.columns) > 0
        visible_cursor = self.show_cursor and self.cursor_type != "none"
        if cell_now_available and visible_cursor:
            self._highlight_cursor()

        self._update_count += 1
        self.check_idle()
        return row_key
