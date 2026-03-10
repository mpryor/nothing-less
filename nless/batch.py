"""Headless batch processing — applies CLI transforms and writes to stdout."""

from __future__ import annotations

import csv
import json
import re
import signal
import sys

from .dataprocessing import coerce_sort_key, matches_all_filters, strip_markup
from .delimiter import find_header_index, infer_delimiter, split_line
from .types import CliArgs, Column


def _read_all_lines(cli_args: CliArgs) -> list[str]:
    """Read all input lines from file or stdin."""
    if cli_args.filename:
        with open(cli_args.filename) as f:
            return f.readlines()
    return sys.stdin.readlines()


def _build_columns(header_cells: list[str]) -> list[Column]:
    """Build a list of Column objects from header cell strings."""
    return [
        Column(
            name=cell,
            labels=set(),
            render_position=i,
            data_position=i,
            hidden=False,
        )
        for i, cell in enumerate(header_cells)
    ]


def _col_lookup(columns: list[Column], name: str, _render: bool = False) -> int | None:
    """Look up a column index by name."""
    for c in columns:
        if strip_markup(c.name) == name:
            return c.data_position
    return None


def run_batch(cli_args: CliArgs) -> None:
    """Run headless batch processing: read, transform, write to stdout."""
    # Handle SIGPIPE gracefully (e.g. piping to `head`)
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    try:
        _run_batch_inner(cli_args)
    except BrokenPipeError:
        # Suppress traceback when downstream closes early
        sys.stderr.close()


def _run_batch_inner(cli_args: CliArgs) -> None:
    lines = _read_all_lines(cli_args)
    if not lines:
        return

    # Raw output mode: pass lines through with minimal processing
    if cli_args.output_format == "raw" or cli_args.raw:
        for line in lines:
            sys.stdout.write(line if line.endswith("\n") else line + "\n")
        return

    # Infer delimiter
    delimiter = cli_args.delimiter
    if delimiter is None:
        sample = [line.rstrip("\n\r") for line in lines[:20]]
        delimiter = infer_delimiter(sample)
    if delimiter is None or delimiter == "raw":
        # Can't parse columns — fall back to raw passthrough
        for line in lines:
            sys.stdout.write(line if line.endswith("\n") else line + "\n")
        return

    # Find header and parse rows
    if delimiter == "json":
        # JSON: header comes from keys of the first object
        header_cells = None
        data_rows = []
        for line in lines:
            stripped = line.rstrip("\n\r")
            if not stripped:
                continue
            cells = split_line(stripped, delimiter, [])
            if header_cells is None:
                header_cells = list(json.loads(stripped).keys())
            data_rows.append(cells)
        if header_cells is None:
            return
        columns = _build_columns(header_cells)
    else:
        sample = [line.rstrip("\n\r") for line in lines[:20]]
        header_idx = find_header_index(sample, delimiter)

        header_line = lines[header_idx].rstrip("\n\r")
        header_cells = split_line(header_line, delimiter, [])
        columns = _build_columns(header_cells)

        data_rows = []
        for line in lines[header_idx + 1 :]:
            stripped = line.rstrip("\n\r")
            if not stripped:
                continue
            cells = split_line(stripped, delimiter, columns)
            data_rows.append(cells)

    # Apply filters
    if cli_args.filters:

        def col_fn(name, _render=False):
            return _col_lookup(columns, name, _render)

        data_rows = [
            row
            for row in data_rows
            if matches_all_filters(row, cli_args.filters, col_fn)
        ]

    # Apply unique-key dedup
    if cli_args.unique_keys:
        seen = set()
        deduped = []
        for row in data_rows:
            key_parts = []
            for uk in cli_args.unique_keys:
                idx = _col_lookup(columns, uk)
                if idx is not None and idx < len(row):
                    key_parts.append(strip_markup(row[idx]))
            key = "|".join(key_parts)
            if key not in seen:
                seen.add(key)
                deduped.append(row)
        data_rows = deduped

    # Apply sort
    if cli_args.sort_by:
        col_name, direction = cli_args.sort_by.split("=")
        sort_idx = _col_lookup(columns, col_name)
        if sort_idx is not None:
            reverse = direction.lower() == "desc"
            data_rows.sort(
                key=lambda row: coerce_sort_key(
                    strip_markup(row[sort_idx]) if sort_idx < len(row) else ""
                ),
                reverse=reverse,
            )

    # Apply column filter
    visible_indices = list(range(len(columns)))
    visible_headers = [strip_markup(c.name) for c in columns]
    if cli_args.columns:
        col_re = re.compile(cli_args.columns, re.IGNORECASE)
        visible_indices = [
            i for i, c in enumerate(columns) if col_re.search(strip_markup(c.name))
        ]
        visible_headers = [strip_markup(columns[i].name) for i in visible_indices]

    # Write output
    _write_output(cli_args.output_format, visible_headers, data_rows, visible_indices)


def _write_output(
    fmt: str,
    headers: list[str],
    rows: list[list[str]],
    visible_indices: list[int],
) -> None:
    """Write rows to stdout in the requested format."""
    if fmt == "json":
        for row in rows:
            obj = {}
            for i, hi in enumerate(visible_indices):
                val = strip_markup(row[hi]) if hi < len(row) else ""
                obj[headers[i]] = val
            sys.stdout.write(json.dumps(obj) + "\n")
    elif fmt == "tsv":
        writer = csv.writer(sys.stdout, delimiter="\t")
        writer.writerow(headers)
        for row in rows:
            writer.writerow(
                [strip_markup(row[i]) if i < len(row) else "" for i in visible_indices]
            )
    else:
        # csv (default)
        writer = csv.writer(sys.stdout)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(
                [strip_markup(row[i]) if i < len(row) else "" for i in visible_indices]
            )
