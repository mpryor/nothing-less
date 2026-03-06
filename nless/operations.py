"""Module-level operations that act on NlessBuffer instances."""

from __future__ import annotations

import csv
import time
from collections import defaultdict
from dataclasses import replace
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .types import Column, MetadataColumn

if TYPE_CHECKING:
    from .buffer import NlessBuffer


def handle_mark_unique(new_buffer: NlessBuffer, new_unique_column_name: str) -> None:
    if new_unique_column_name in [mc.value for mc in MetadataColumn]:
        # can't toggle count column
        return

    col_idx = new_buffer._get_col_idx_by_name(new_unique_column_name)
    new_unique_column = (
        new_buffer.current_columns[col_idx] if col_idx is not None else None
    )

    if new_unique_column is None:
        return

    new_buffer.count_by_column_key = defaultdict(lambda: 0)

    if (
        new_unique_column_name in new_buffer.unique_column_names
        and new_buffer.first_row_parsed
    ):
        new_buffer.unique_column_names.remove(new_unique_column_name)
        if new_buffer.sort_column in [metadata.value for metadata in MetadataColumn]:
            new_buffer.sort_column = None
        new_unique_column.labels.discard("U")
    else:
        new_buffer.unique_column_names.add(new_unique_column_name)
        new_unique_column.labels.add("U")

    if len(new_buffer.unique_column_names) == 0:
        # remove count column
        new_buffer.current_columns = [
            replace(
                c,
                render_position=c.render_position - 1,
                data_position=c.data_position - 1,
            )
            for c in new_buffer.current_columns
            if c.name != MetadataColumn.COUNT.value
        ]
    elif MetadataColumn.COUNT.value not in [c.name for c in new_buffer.current_columns]:
        # add count column at the start
        new_buffer.current_columns = [
            replace(
                c,
                render_position=c.render_position + 1,
                data_position=c.data_position + 1,
            )
            for c in new_buffer.current_columns
        ]
        new_buffer.current_columns.insert(
            0,
            Column(
                name=MetadataColumn.COUNT.value,
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
                pinned=True,
            ),
        )

    pinned_columns_visible = len(
        [c for c in new_buffer.current_columns if c.pinned and not c.hidden]
    )
    if new_unique_column_name in new_buffer.unique_column_names:
        old_position = new_unique_column.render_position
        for col in new_buffer.current_columns:
            col_name = strip_markup(col.name)
            if col_name == new_unique_column_name:
                col.render_position = (
                    pinned_columns_visible  # bubble to just after last pinned column
                )
                col.pinned = True
            elif (
                col_name != new_unique_column_name
                and col.render_position <= old_position
                and col.name not in [mc.value for mc in MetadataColumn]
                and not col.pinned
            ):
                col.render_position += 1  # shift right to make space
    else:
        old_position = new_unique_column.render_position
        pinned_columns_visible -= 1
        for col in new_buffer.current_columns:
            col_name = strip_markup(col.name)
            if col_name == new_unique_column_name:
                col.pinned = False
                col.render_position = (
                    pinned_columns_visible if pinned_columns_visible > 0 else 0
                )
            elif col.pinned and col.render_position >= old_position:
                col.render_position -= 1

    # Hide non-key columns when pivot is active; restore when cleared
    metadata_names = {mc.value for mc in MetadataColumn}
    if new_buffer.unique_column_names:
        for col in new_buffer.current_columns:
            col_name = strip_markup(col.name)
            if (
                col_name not in new_buffer.unique_column_names
                and col_name not in metadata_names
                and not col.hidden
            ):
                col.hidden = True
                new_buffer._pivot_hidden_columns.add(col_name)
    else:
        for col in new_buffer.current_columns:
            col_name = strip_markup(col.name)
            if col_name in new_buffer._pivot_hidden_columns:
                col.hidden = False
        new_buffer._pivot_hidden_columns.clear()


def write_buffer(current_buffer: NlessBuffer, output_path: str) -> None:
    if output_path == "-":
        output_path = "/dev/stdout"
        while current_buffer.app.is_running:
            time.sleep(0.1)
        time.sleep(0.1)

    with open(output_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(current_buffer._get_visible_column_labels())
        for row in current_buffer.displayed_rows:
            plain_row = [strip_markup(str(cell)) for cell in row]
            writer.writerow(plain_row)
