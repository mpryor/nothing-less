"""Column operations: JSON headers, column delimiters, visibility filters, and arrival toggle for NlessApp."""

from __future__ import annotations

import csv
import json
import re
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .buffer_columns import HIDDEN_COLUMN_SENTINEL_POSITION
from .dataprocessing import strip_markup
from .delimiter import split_line
from .nlessselect import NlessSelect
from .suggestions import (
    ColumnDelimiterSuggestionProvider,
    PipeSeparatedSuggestionProvider,
)
from .types import Column, MetadataColumn, UpdateReason

if TYPE_CHECKING:
    from .app import NlessApp
    from .autocomplete import AutocompleteInput


@dataclass
class ColumnNamingState:
    column_refs: list[Column]
    sample_values: list[str]
    first_new_col_position: int
    split_source_name: str = ""
    split_delim_display: str = ""
    current_index: int = 0
    any_renamed: bool = False


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
            curr_buffer._column_history.append(deepcopy(curr_buffer.current_columns))
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

        if curr_buffer.raw_mode:
            curr_buffer.raw_mode = False
        curr_buffer._deferred_update_table(
            restore_position=False,
            callback=lambda: data_table.move_cursor(
                column=new_render_position, row=old_row
            ),
            reason=UpdateReason.ADDING_COLUMN,
        )

    def action_json_header(self: NlessApp) -> None:
        """Set the column headers from JSON in the selected cell."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(".nless-view")
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            # Unescape Rich bracket escapes (\[ → [) to restore valid JSON.
            # strip_markup can't be used here — its regex treats JSON
            # arrays/objects as markup tags and destroys them.
            cell_value = cell_value.replace("\\[", "[")
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

            from rich.markup import escape as rich_escape

            select = NlessSelect(
                options=[
                    (f"[bold]{col}[/bold] - {rich_escape(json.dumps(v))}", col)
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
        # Try to get cell value at cursor for context-aware suggestions
        cell_value = ""
        try:
            curr_buffer = self._get_current_buffer()
            data_table = curr_buffer.query_one(".nless-view")
            coordinate = data_table.cursor_coordinate
            cell_value = strip_markup(str(data_table.get_cell_at(coordinate)))
        except Exception:
            pass
        self._create_prompt(
            "Type column delimiter (e.g. , or \\t or 'space' or 'raw')",
            "column_delimiter_input",
            provider=ColumnDelimiterSuggestionProvider(cell_value, history),
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

        Returns:
            Tuple of (base_position, added_count, added_names) where
            base_position is the data/render position of the first new column.
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
        added_names = []
        for i, name in enumerate(column_names):
            if name not in existing:
                buffer.current_columns.append(make_column_fn(i, name, base_pos + added))
                added += 1
                added_names.append(name)
        # Push ARRIVAL to the end
        if arrival_col and added > 0:
            arrival_col.data_position = base_pos + added
            arrival_col.render_position = (
                max(c.render_position for c in buffer.current_columns) + 1
            )
        return base_pos, added, added_names

    def handle_column_delimiter_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        event.input.remove()
        value = event.value
        if value and value not in ("json", "kv", "\\t", "space", "space+"):
            try:
                pattern = re.compile(rf"{value}")
                if pattern.groups > len(pattern.groupindex):
                    self._start_regex_wizard(value, pattern, "column_delimiter")
                    return
            except re.error as e:
                self._get_current_buffer().notify(
                    f"Invalid regex: {e}", severity="error"
                )
                return
        self._apply_column_delimiter(value)

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

            column_snapshot = deepcopy(current_buffer.current_columns)

            split_result = None  # (base_pos, added_count, added_names)
            is_literal_split = False
            literal_cell_parts: list[str] = []
            selected_column_name = strip_markup(selected_column.name)

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
                    split_result = self._add_computed_columns(
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
                    should_update = split_result[1] > 0
                except json.JSONDecodeError:
                    current_buffer.notify(
                        "Selected cell does not contain a JSON object or array",
                        severity="error",
                    )
                    return
            elif new_col_delimiter == "kv":
                kv_re = re.compile(r"""(\w[\w.-]*)=([^,|;\s]+|"[^"]*"|'[^']*')""")
                plain_cell = strip_markup(cell)
                kv_matches = kv_re.findall(plain_cell)
                if len(kv_matches) < 2:
                    current_buffer.notify(
                        "Cell does not contain key=value pairs",
                        severity="error",
                    )
                    return
                keys = [m[0] for m in kv_matches]
                # Detect the separator between kv pairs
                kv_separator = " "
                for sep in [" | ", ", ", "; ", " "]:
                    # Count how many pairs are preceded by this separator
                    count = sum(1 for k in keys[1:] if f"{sep}{k}=" in plain_cell)
                    if count == len(keys) - 1:
                        kv_separator = sep
                        break
                split_result = self._add_computed_columns(
                    current_buffer,
                    keys,
                    lambda i,
                    name,
                    pos,
                    _sel=selected_column,
                    _sep=kv_separator: Column(
                        name=name,
                        labels=set(),
                        render_position=pos,
                        data_position=pos,
                        hidden=False,
                        computed=True,
                        col_ref=f"{_sel.name}",
                        kv_key=name,
                        kv_separator=_sep,
                        delimiter="kv",
                    ),
                )
                should_update = split_result[1] > 0
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
                    split_result = self._add_computed_columns(
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
                    should_update = split_result[1] > 0
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
                        is_literal_split = True
                        literal_cell_parts = cell_parts
                        split_result = self._add_computed_columns(
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
                        should_update = split_result[1] > 0
                    except (
                        json.JSONDecodeError,
                        csv.Error,
                        ValueError,
                        IndexError,
                    ) as e:
                        current_buffer.notify(
                            f"Error splitting cell: {str(e)}", severity="error"
                        )

            if should_update and split_result:
                current_buffer._column_history.append(column_snapshot)
                _, added_count, added_names = split_result

        if should_update and split_result:
            if current_buffer.raw_mode:
                current_buffer.raw_mode = False
            # Compute cursor target: first new column's render position
            first_new_col = next(
                (
                    c
                    for c in current_buffer.current_columns
                    if c.name in added_names and not c.hidden
                ),
                None,
            )
            target_col = first_new_col.render_position if first_new_col else 0
            old_row = cursor_coordinate.row
            # Build flash message
            display_delim = (
                repr(new_col_delimiter)
                if new_col_delimiter in ("\t", " ", "  ")
                else f'"{new_col_delimiter}"'
            )
            if len(added_names) <= 4:
                col_list = ", ".join(added_names)
            else:
                col_list = f"{added_names[0]}, {added_names[1]}, \u2026 ({added_count} columns)"
            flash_msg = (
                f'Split "{selected_column_name}" on {display_delim} \u2192 {col_list}'
            )
            # Capture columns for wizard before the lambda closes over them
            _wizard_columns = (
                [
                    c
                    for c in current_buffer.current_columns
                    if c.name in added_names and not c.hidden
                ]
                if is_literal_split
                else []
            )
            _wizard_samples = literal_cell_parts

            def _after_split():
                data_table.move_cursor(column=target_col, row=old_row)
                if _wizard_columns:
                    self._start_column_naming_wizard(
                        _wizard_columns,
                        _wizard_samples,
                        split_source_name=selected_column_name,
                        split_delim_display=display_delim,
                    )
                else:
                    current_buffer.notify(flash_msg)

            current_buffer._deferred_update_table(
                restore_position=False,
                callback=_after_split,
                reason=UpdateReason.SPLITTING_COLUMN,
            )

    @staticmethod
    def _build_wizard_flash_msg(state: ColumnNamingState) -> str:
        """Build the split flash message using final column names."""
        if not state.split_source_name:
            return ""
        names = [c.name for c in state.column_refs]
        if len(names) <= 4:
            col_list = ", ".join(names)
        else:
            col_list = f"{names[0]}, {names[1]}, \u2026 ({len(names)} columns)"
        return f'Split "{state.split_source_name}" on {state.split_delim_display} \u2192 {col_list}'

    def _start_column_naming_wizard(
        self: NlessApp,
        columns: list[Column],
        sample_values: list[str],
        split_source_name: str = "",
        split_delim_display: str = "",
    ) -> None:
        first_pos = columns[0].render_position if columns else 0
        self._column_naming_state = ColumnNamingState(
            column_refs=columns,
            sample_values=sample_values,
            first_new_col_position=first_pos,
            split_source_name=split_source_name,
            split_delim_display=split_delim_display,
        )
        self._prompt_next_column_name()

    def _prompt_next_column_name(self: NlessApp) -> None:
        state = self._column_naming_state
        if state is None:
            return
        idx = state.current_index
        sample = state.sample_values[idx] if idx < len(state.sample_values) else ""
        col = state.column_refs[idx]
        total = len(state.column_refs)
        placeholder = f'Column {idx + 1}/{total} — Enter: keep "{col.name}", or type new name (sample: "{sample}"):'
        self._create_prompt(
            placeholder,
            "column_naming_input",
            save_history=False,
        )
        data_table = self._get_current_buffer().query_one(".nless-view")
        data_table.move_cursor(column=col.render_position)
        data_table.highlighted_column = col.render_position

    def _reprompt_column_name(self: NlessApp, widget: AutocompleteInput) -> None:
        state = self._column_naming_state
        if state is None:
            return
        idx = state.current_index
        sample = state.sample_values[idx] if idx < len(state.sample_values) else ""
        col = state.column_refs[idx]
        total = len(state.column_refs)
        placeholder = f'Column {idx + 1}/{total} — Enter: keep "{col.name}", or type new name (sample: "{sample}"):'
        widget.placeholder = placeholder
        inner_input = widget.query_one("Input")
        inner_input.placeholder = placeholder
        inner_input.value = ""
        inner_input.focus()
        data_table = self._get_current_buffer().query_one(".nless-view")
        data_table.move_cursor(column=col.render_position)
        data_table.highlighted_column = col.render_position

    def _handle_column_naming_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        state = self._column_naming_state
        if state is None:
            event.input.remove()
            return
        name = event.value.strip()
        if not name:
            # Empty submit (Enter with no text) → accept the default prefix name
            # The prefix is the current default name, so just advance
            pass
        else:
            # Rename the column
            state.column_refs[state.current_index].name = name
            state.any_renamed = True
        state.current_index += 1
        if state.current_index < len(state.column_refs):
            self._reprompt_column_name(event.input)
        else:
            event.input.remove()
            any_renamed = state.any_renamed
            first_col_pos = state.first_new_col_position
            flash_msg = self._build_wizard_flash_msg(state)
            self._column_naming_state = None
            current_buffer = self._get_current_buffer()
            data_table = current_buffer.query_one(".nless-view")
            data_table.highlighted_column = -1

            def _after_wizard():
                data_table.move_cursor(column=first_col_pos)
                if flash_msg:
                    current_buffer.notify(flash_msg)

            if any_renamed:
                current_buffer._deferred_update_table(
                    reason=UpdateReason.SPLITTING_COLUMN,
                    callback=_after_wizard,
                )
            else:
                _after_wizard()

    def _handle_column_naming_cancelled(self: NlessApp) -> None:
        """Called when Escape is pressed during column naming."""
        state = self._column_naming_state
        self._column_naming_state = None
        if state is not None:
            current_buffer = self._get_current_buffer()
            data_table = current_buffer.query_one(".nless-view")
            data_table.highlighted_column = -1
            data_table.move_cursor(column=state.first_new_col_position)
            flash_msg = self._build_wizard_flash_msg(state)
            if flash_msg:
                current_buffer.notify(flash_msg)

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
            arrival_col.render_position = HIDDEN_COLUMN_SENTINEL_POSITION
            # Close the gap
            for c in buf.current_columns:
                if (
                    c.render_position > old_pos
                    and c.name != MetadataColumn.ARRIVAL.value
                ):
                    c.render_position -= 1

        buf.invalidate_caches()
        buf._deferred_update_table(reason=UpdateReason.TOGGLING_ARRIVAL)

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

        curr_buffer._deferred_update_table(reason=UpdateReason.FILTERING_COLUMNS)
