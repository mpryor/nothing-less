"""Column operations: JSON headers, column delimiters, visibility filters, and arrival toggle for NlessApp."""

from __future__ import annotations

import csv
import json
import re
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .delimiter import split_line
from .nlessselect import NlessSelect
from .suggestions import PipeSeparatedSuggestionProvider, StaticSuggestionProvider
from .types import Column, MetadataColumn

if TYPE_CHECKING:
    from .app import NlessApp
    from .autocomplete import AutocompleteInput


class ColumnOpsMixin:
    """Mixin providing column operation methods for NlessApp."""

    def _apply_json_header(self: NlessApp, col_ref_value: str) -> None:
        curr_buffer = self._get_current_buffer()
        with curr_buffer._try_lock(
            "json header",
            deferred=lambda: self._apply_json_header(col_ref_value),
        ) as acquired:
            if not acquired:
                return
            data_table = curr_buffer.query_one(".nless-view")
            cursor_column = data_table.cursor_column
            curr_column = curr_buffer._get_column_at_position(cursor_column)
            if not curr_column:
                curr_buffer.notify(
                    "No column selected to add JSON key to", severity="error"
                )
                return
            curr_column_name = strip_markup(curr_column.name)

            col_ref = col_ref_value
            if not col_ref.startswith("."):
                col_ref = f".{col_ref}"

            new_column_name = f"{curr_column_name}{col_ref}"

            # Insert before trailing ARRIVAL metadata so it stays last
            arrival_col = next(
                (
                    c
                    for c in curr_buffer.current_columns
                    if c.name == MetadataColumn.ARRIVAL.value
                ),
                None,
            )
            new_data_position = (
                arrival_col.data_position
                if arrival_col
                else len(curr_buffer.current_columns)
            )
            new_render_position = len(
                [c for c in curr_buffer.current_columns if not c.hidden]
            )

            new_col = Column(
                name=new_column_name,
                labels=set(),
                computed=True,
                render_position=new_render_position,
                data_position=new_data_position,
                hidden=False,
                json_ref=f"{curr_column_name}{col_ref}",
                delimiter="json",
            )

            curr_buffer.current_columns.append(new_col)
            # Push ARRIVAL to the end
            if arrival_col:
                arrival_col.data_position = new_data_position + 1
                arrival_col.render_position = (
                    max(c.render_position for c in curr_buffer.current_columns) + 1
                )
            old_row = data_table.cursor_row

        curr_buffer._deferred_update_table(
            restore_position=False,
            callback=lambda: data_table.move_cursor(
                column=new_render_position, row=old_row
            ),
            reason="Adding column",
        )

    def action_json_header(self: NlessApp) -> None:
        """Set the column headers from JSON in the selected cell."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = strip_markup(cell_value)
            json_data = json.loads(cell_value)
            if not isinstance(json_data, (dict, list)):
                curr_buffer.notify(
                    "Cell does not contain a JSON object.", severity="error"
                )
                return
            new_columns = []

            # iterate through the full JSON heirarchy of keys, building up a list of keys
            def extract_keys(obj, prefix=""):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_prefix = f"{prefix}.{k}" if prefix else k
                        new_columns.append((new_prefix, v))
                        extract_keys(v, new_prefix)
                elif isinstance(obj, list) and len(obj) > 0:
                    for i in range(len(obj)):
                        extract_keys(obj[i], prefix + f".{i}")

            extract_keys(json_data)

            select = NlessSelect(
                options=[
                    (f"[bold]{col}[/bold] - {json.dumps(v)}", col)
                    for (col, v) in new_columns
                ],
                classes="dock-bottom",
                id="json_header_select",
            )
            self.mount(select)
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            curr_buffer.notify(f"Error parsing JSON: {str(e)}", severity="error")

    _DELIMITER_OPTIONS = [",", "\\t", "space", "space+", "|", ";", ":", "raw"]

    def action_column_delimiter(self: NlessApp) -> None:
        """Change the column delimiter."""
        history = [
            h["val"] for h in self.input_history if h["id"] == "column_delimiter_input"
        ]
        self._create_prompt(
            "Type column delimiter (e.g. , or \\t or 'space' or 'raw')",
            "column_delimiter_input",
            provider=StaticSuggestionProvider(self._DELIMITER_OPTIONS, history=history),
        )

    def action_filter_columns(self: NlessApp) -> None:
        """Filter columns by user input."""
        data_table = self._get_current_buffer().query_one(".nless-view")
        column_names = [strip_markup(c) for c in data_table.columns]
        self._create_prompt(
            "Type pipe delimited column names to show (e.g. col1|col2) or 'all' to reset",
            "column_filter_input",
            provider=PipeSeparatedSuggestionProvider(column_names),
        )

    @staticmethod
    def _add_computed_columns(buffer, column_names, make_column_fn):
        """Add computed columns to a buffer, skipping duplicates.

        Args:
            buffer: The NlessBuffer to add columns to.
            column_names: Iterable of candidate column names.
            make_column_fn: Callable(index, name, position) -> Column.
        """
        existing = {c.name for c in buffer.current_columns}
        # Insert before trailing metadata (ARRIVAL) so it stays last
        arrival_col = next(
            (
                c
                for c in buffer.current_columns
                if c.name == MetadataColumn.ARRIVAL.value
            ),
            None,
        )
        base_pos = (
            arrival_col.data_position if arrival_col else len(buffer.current_columns)
        )
        added = 0
        for i, name in enumerate(column_names):
            if name not in existing:
                buffer.current_columns.append(make_column_fn(i, name, base_pos + added))
                added += 1
        # Push ARRIVAL to the end
        if arrival_col and added > 0:
            arrival_col.data_position = base_pos + added
            arrival_col.render_position = (
                max(c.render_position for c in buffer.current_columns) + 1
            )

    def handle_column_delimiter_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        event.input.remove()
        self._apply_column_delimiter(event.value)

    def _apply_column_delimiter(self: NlessApp, new_col_delimiter: str) -> None:
        if not new_col_delimiter:
            return
        current_buffer = self._get_current_buffer()
        should_update = False
        with current_buffer._try_lock(
            "column delimiter",
            deferred=lambda: self._apply_column_delimiter(new_col_delimiter),
        ) as acquired:
            if not acquired:
                return
            data_table = current_buffer.query_one(".nless-view")
            cursor_coordinate = data_table.cursor_coordinate
            cell = data_table.get_cell_at(cursor_coordinate)
            selected_column = current_buffer._get_column_at_position(
                cursor_coordinate.column
            )
            if not selected_column:
                current_buffer.notify(
                    "No column selected for delimiting", severity="error"
                )
                return

            if new_col_delimiter == "json":
                try:
                    cell_json = json.loads(strip_markup(cell))
                    if not isinstance(cell_json, (dict, list)):
                        current_buffer.notify(
                            "Selected cell does not contain a JSON object or array",
                            severity="error",
                        )
                        return
                    cell_json_keys = (
                        list(cell_json.keys())
                        if isinstance(cell_json, dict)
                        else list(range(len(cell_json)))
                    )
                    self._add_computed_columns(
                        current_buffer,
                        [f"{selected_column.name}.{key}" for key in cell_json_keys],
                        lambda i, name, pos: Column(
                            name=name,
                            labels=set(),
                            render_position=pos,
                            data_position=pos,
                            hidden=False,
                            computed=True,
                            json_ref=name,
                            delimiter=new_col_delimiter,
                        ),
                    )
                    should_update = True
                except json.JSONDecodeError:
                    current_buffer.notify(
                        "Selected cell does not contain a JSON object or array",
                        severity="error",
                    )
                    return
            else:
                if new_col_delimiter == "\\t":
                    new_col_delimiter = "\t"
                elif new_col_delimiter == "space":
                    new_col_delimiter = " "
                elif new_col_delimiter == "space+":
                    new_col_delimiter = "  "

                try:
                    pattern = re.compile(new_col_delimiter)
                    if pattern.groups == 0:
                        raise re.error("no named groups")
                    group_names = list(pattern.groupindex.keys())
                    self._add_computed_columns(
                        current_buffer,
                        group_names,
                        lambda i, name, pos, _pat=pattern, _sel=selected_column: Column(
                            name=name,
                            labels=set(),
                            render_position=pos,
                            data_position=pos,
                            hidden=False,
                            computed=True,
                            col_ref=f"{_sel.name}",
                            col_ref_index=i,
                            delimiter=_pat,
                        ),
                    )
                    should_update = True
                except (re.error, ValueError):
                    pass

                if not should_update:
                    try:
                        cell_parts = split_line(
                            strip_markup(cell),
                            new_col_delimiter,
                            [],
                        )
                        if len(cell_parts) < 2:
                            current_buffer.notify(
                                "Delimiter did not split cell into multiple parts",
                                severity="error",
                            )
                            return
                        self._add_computed_columns(
                            current_buffer,
                            [
                                f"{selected_column.name}-{i + 1}"
                                for i in range(len(cell_parts))
                            ],
                            lambda i,
                            name,
                            pos,
                            _delim=new_col_delimiter,
                            _sel=selected_column: Column(
                                name=name,
                                labels=set(),
                                render_position=pos,
                                data_position=pos,
                                hidden=False,
                                computed=True,
                                col_ref_index=i,
                                col_ref=f"{_sel.name}",
                                delimiter=_delim,
                            ),
                        )
                        should_update = True
                    except (
                        json.JSONDecodeError,
                        csv.Error,
                        ValueError,
                        IndexError,
                    ) as e:
                        current_buffer.notify(
                            f"Error splitting cell: {str(e)}", severity="error"
                        )

        if should_update:
            current_buffer._deferred_update_table(reason="Splitting column")

    def action_toggle_arrival(self: NlessApp) -> None:
        """Toggle visibility of the arrival timestamp column, pinned to the left."""
        buf = self._get_current_buffer()
        arrival_col = next(
            (c for c in buf.current_columns if c.name == MetadataColumn.ARRIVAL.value),
            None,
        )
        if arrival_col is None:
            return

        if arrival_col.hidden:
            # Show and pin to the left
            arrival_col.hidden = False
            arrival_col.pinned = True
            # Find the first non-pinned render_position to insert before
            pinned_count = sum(
                1
                for c in buf.current_columns
                if c.pinned and not c.hidden and c.name != MetadataColumn.ARRIVAL.value
            )
            # Shift non-pinned columns right by 1
            for c in buf.current_columns:
                if (
                    c.name != MetadataColumn.ARRIVAL.value
                    and not c.pinned
                    and c.render_position >= pinned_count
                ):
                    c.render_position += 1
            arrival_col.render_position = pinned_count
        else:
            # Hide and unpin
            old_pos = arrival_col.render_position
            arrival_col.hidden = True
            arrival_col.pinned = False
            arrival_col.render_position = 99999
            # Close the gap
            for c in buf.current_columns:
                if (
                    c.render_position > old_pos
                    and c.name != MetadataColumn.ARRIVAL.value
                ):
                    c.render_position -= 1

        buf.invalidate_caches()
        buf._deferred_update_table(reason="Toggling arrival column")

    def handle_column_filter_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        input_value = event.value
        event.input.remove()
        self._apply_column_filter(input_value)

    def _apply_column_filter(self: NlessApp, input_value: str) -> None:
        if not input_value.strip():
            return
        curr_buffer = self._get_current_buffer()
        with curr_buffer._try_lock(
            "column filter",
            deferred=lambda: self._apply_column_filter(input_value),
        ) as acquired:
            if not acquired:
                return
            if input_value.lower() == "all":
                metadata_names = {mc.value for mc in MetadataColumn}
                for col in curr_buffer.current_columns:
                    if col.name not in metadata_names or col.pinned:
                        col.hidden = False
            else:
                column_name_filters = [name.strip() for name in input_value.split("|")]
                column_name_filter_regexes = [
                    re.compile(rf"{name}", re.IGNORECASE)
                    for name in column_name_filters
                ]
                visible_pinned_columns = [
                    col
                    for col in curr_buffer.current_columns
                    if col.pinned and not col.hidden
                ]
                for col in curr_buffer.current_columns:
                    matched = False
                    plain_name = strip_markup(col.name)
                    for i, column_name_filter in enumerate(column_name_filter_regexes):
                        if column_name_filter.search(plain_name) and not col.pinned:
                            col.hidden = False
                            col.render_position = i + len(
                                visible_pinned_columns
                            )  # keep metadata columns at the start
                            matched = True
                            break

                    if matched:
                        continue

                    if (
                        col.name not in [mc.value for mc in MetadataColumn]
                        and not col.pinned
                    ):
                        col.hidden = True
                        col.render_position = 99999

                # Ensure at least one column is visible
                if all(col.hidden for col in curr_buffer.current_columns):
                    curr_buffer.notify(
                        "At least one column must be visible.", severity="warning"
                    )
                    for col in curr_buffer.current_columns:
                        col.hidden = False

            sorted_columns = sorted(
                curr_buffer.current_columns, key=lambda c: c.render_position
            )
            for i, col in enumerate(sorted_columns):
                col.render_position = i

        curr_buffer._deferred_update_table(reason="Filtering columns")
