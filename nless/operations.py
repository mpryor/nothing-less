"""Module-level operations that act on NlessBuffer instances."""

from __future__ import annotations

import csv
import json
import os
import time
from collections import defaultdict
from dataclasses import replace
from typing import IO, TYPE_CHECKING

from .dataprocessing import _looks_numeric, strip_markup
from .types import Column, MetadataColumn

if TYPE_CHECKING:
    from .buffer import NlessBuffer


def handle_mark_unique(new_buffer: NlessBuffer, new_unique_column_name: str) -> None:
    if new_unique_column_name == MetadataColumn.COUNT.value:
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


def write_buffer_to_fd(
    current_buffer: NlessBuffer, fd: IO[str], output_format: str = "csv"
) -> None:
    """Write the current buffer contents to a file descriptor.

    Unlike write_buffer, this does not wait for app.is_running — it writes
    immediately, intended for use during app exit (pipe output).
    """
    headers = current_buffer._get_visible_column_labels()
    rows = current_buffer.displayed_rows

    try:
        if output_format == "json":
            for row in rows:
                obj = {h: strip_markup(str(cell)) for h, cell in zip(headers, row)}
                fd.write(json.dumps(obj) + "\n")
        elif output_format == "raw":
            for row in rows:
                fd.write("\t".join(strip_markup(str(cell)) for cell in row) + "\n")
        else:
            delim = "\t" if output_format == "tsv" else ","
            writer = csv.writer(fd, delimiter=delim)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([strip_markup(str(cell)) for cell in row])
    except BrokenPipeError:
        pass


def _infer_output_format(path: str) -> str:
    """Infer output format from file extension."""
    ext = os.path.splitext(path)[1].lower()
    return {
        ".json": "json",
        ".jsonl": "json",
        ".tsv": "tsv",
        ".csv": "csv",
        ".txt": "raw",
        ".log": "raw",
    }.get(ext, "csv")


def write_buffer(
    current_buffer: NlessBuffer,
    output_path: str,
    output_format: str | None = None,
) -> None:
    if output_path == "-":
        output_path = "/dev/stdout"
        while current_buffer.app.is_running:
            time.sleep(0.1)
        time.sleep(0.1)

    if output_format is None:
        output_format = _infer_output_format(output_path)

    with open(output_path, "w") as f:
        write_buffer_to_fd(current_buffer, f, output_format)


def compute_column_aggregations(
    current_buffer: NlessBuffer, column_index: int
) -> str | None:
    """Compute aggregations for a column and return a formatted summary string.

    Returns None if the column has no data.
    """
    rows = current_buffer.displayed_rows
    if not rows:
        return None

    raw_values = []
    for row in rows:
        if column_index < len(row):
            raw_values.append(strip_markup(str(row[column_index])))

    if not raw_values:
        return None

    total = len(raw_values)
    distinct = len(set(raw_values))

    numeric_values = []
    for v in raw_values:
        if _looks_numeric(v):
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                pass

    parts = [f"Count: {total:,}", f"Distinct: {distinct:,}"]

    if numeric_values:
        s = sum(numeric_values)
        avg = s / len(numeric_values)
        mn = min(numeric_values)
        mx = max(numeric_values)

        def _fmt(n: float) -> str:
            return f"{n:,.0f}" if n == int(n) else f"{n:,.4g}"

        parts.extend(
            [
                f"Sum: {_fmt(s)}",
                f"Avg: {_fmt(avg)}",
                f"Min: {_fmt(mn)}",
                f"Max: {_fmt(mx)}",
            ]
        )
        if len(numeric_values) < total:
            parts.append(f"({total - len(numeric_values):,} non-numeric skipped)")

    return " | ".join(parts)
