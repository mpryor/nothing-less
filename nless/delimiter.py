import csv
import json
import re
from io import StringIO

from .types import Column, MetadataColumn


def _find_ref_column_cell(
    lookup_column: str | None,
    sorted_columns: list[Column],
    cells: list[str],
    count_metadata_columns: int,
) -> str | None:
    """Find the cell value from the referenced source column.

    Returns the cell string if found and in bounds, otherwise None.
    """
    if lookup_column is None:
        return None
    for c in sorted_columns:
        if c.name == lookup_column and c.data_position - count_metadata_columns < len(
            cells
        ):
            return cells[c.data_position - count_metadata_columns]
    return None


def split_line(
    line: str, delimiter: str | re.Pattern[str] | None, columns: list[Column]
) -> list[str]:
    """Split a line using the appropriate delimiter method.

    Args:
        line: The input line to split

    Returns:
        List of fields from the line
    """
    if delimiter == " ":
        cells = split_aligned_row(line)
    elif delimiter == "  ":
        cells = split_aligned_row_preserve_single_spaces(line)
    elif delimiter == ",":
        cells = split_csv_row(line)
    elif delimiter == "raw":
        cells = [line]
    elif delimiter == "json":
        cells = [
            json.dumps(v) if isinstance(v, dict) or isinstance(v, list) else str(v)
            for v in json.loads(line).values()
        ]
    elif isinstance(delimiter, re.Pattern):
        match = delimiter.match(line)
        if match:
            cells = [*match.groups()]
        else:
            cells = []
    else:
        cells = line.split(delimiter)

    cells = [
        txt.replace("\t", "  ").strip() for txt in cells
    ]  # Rich rendering breaks on tabs

    if not columns or not any(c.delimiter or c.json_ref or c.col_ref for c in columns):
        return cells

    sorted_columns = sorted(columns, key=lambda col: col.data_position)
    # Only count leading metadata columns (e.g. COUNT at position 0) for index
    # adjustment — trailing metadata like ARRIVAL doesn't shift data indices.
    count_metadata_columns = len(
        [
            col
            for col in sorted_columns
            if col.name in {mc.value for mc in MetadataColumn} and col.pinned
        ]
    )

    for i, col in enumerate(sorted_columns):
        if col.delimiter and col.delimiter == "json":
            json_path = col.json_ref.split(".")
            ref_cell = _find_ref_column_cell(
                json_path[0], sorted_columns, cells, count_metadata_columns
            )
            if ref_cell is None:
                continue
            try:
                json_data = json.loads(ref_cell)
                for key in json_path[1:]:
                    if isinstance(json_data, dict):
                        json_data = json_data.get(key, "")
                    elif isinstance(json_data, list):
                        try:
                            json_data = json_data[int(key)]
                        except (ValueError, IndexError):
                            json_data = ""
                    else:
                        json_data = ""
            except (json.JSONDecodeError, IndexError):
                json_data = ""
            cells.insert(
                col.data_position - count_metadata_columns,
                json.dumps(json_data)
                if isinstance(json_data, (dict, list))
                else str(json_data),
            )
        elif isinstance(col.delimiter, re.Pattern):
            ref_cell = _find_ref_column_cell(
                col.col_ref, sorted_columns, cells, count_metadata_columns
            )
            if ref_cell is None:
                continue
            match = col.delimiter.match(ref_cell)
            if match:
                subcells = [txt.replace("\t", "  ") for txt in match.groups()]
                cells.insert(
                    col.data_position - count_metadata_columns,
                    subcells[col.col_ref_index],
                )
        else:
            ref_cell = _find_ref_column_cell(
                col.col_ref, sorted_columns, cells, count_metadata_columns
            )
            if ref_cell is None:
                continue
            subcells = [
                txt.replace("\t", "  ")
                for txt in split_line(ref_cell, col.delimiter, [])
            ]
            cells.insert(
                col.data_position - count_metadata_columns,
                subcells[col.col_ref_index]
                if col.col_ref_index < len(subcells)
                else "",
            )
    return cells


_MULTI_SPACE_RE = re.compile(r" {2,}")


def split_aligned_row_preserve_single_spaces(line: str) -> list[str]:
    """Split a space-aligned row into fields by collapsing multiple spaces, but preserving single spaces within fields.

    Args:
        line: The input line to split

    Returns:
        List of fields from the line
    """
    return [field for field in _MULTI_SPACE_RE.split(line) if field]


def split_aligned_row(line: str) -> list[str]:
    """Split a space-aligned row into fields by collapsing multiple spaces.

    Args:
        line: The input line to split

    Returns:
        List of fields from the line
    """
    # Split on multiple spaces and filter out empty strings
    return [field for field in line.split() if field]


def split_csv_row(line: str) -> list[str]:
    """Split a CSV row properly handling quoted values.

    Args:
        line: The input line to split

    Returns:
        List of fields from the line
    """
    stripped = line.strip()
    if '"' not in stripped:
        return stripped.split(",")
    try:
        reader = csv.reader(StringIO(stripped))
        row = next(reader)
        return row
    except (csv.Error, StopIteration):
        return stripped.split(",")


def infer_delimiter(sample_lines: list[str]) -> str | None:
    """Infer the delimiter from a sample of lines.

    Args:
        sample_lines: A list of strings to analyze for delimiter detection.

    Returns:
        The most likely delimiter character.
    """
    # Check for JSON first — try parsing each line as a JSON object
    if sample_lines:
        json_count = 0
        non_empty = 0
        for line in sample_lines:
            stripped = line.strip()
            if not stripped:
                continue
            non_empty += 1
            if stripped.startswith("{"):
                # Handle trailing commas from JSON arrays
                candidate = stripped.rstrip(",")
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        json_count += 1
                except (json.JSONDecodeError, ValueError):
                    pass
            elif stripped in ("[]", "[", "]"):
                # JSON array delimiters — don't count against detection
                non_empty -= 1
        if non_empty > 0 and json_count == non_empty:
            return "json"

    common_delimiters = [",", "\t", "|", ";", " ", "  "]
    delimiter_scores = {d: 0 for d in common_delimiters}

    for line in sample_lines:
        # Skip empty lines
        if not line.strip():
            continue

        for delimiter in common_delimiters:
            if delimiter == " ":
                # Special handling for space-aligned tables
                parts = split_aligned_row(line)
            elif delimiter == "  ":
                parts = split_aligned_row_preserve_single_spaces(line)
            elif delimiter == ",":
                parts = split_csv_row(line)
            else:
                parts = line.split(delimiter)

            # Score based on number of fields and consistency
            if len(parts) > 1:
                # More fields = higher score
                delimiter_scores[delimiter] += len(parts)

                # Consistent non-empty fields = higher score
                non_empty = sum(1 for p in parts if p.strip())
                if non_empty == len(parts):
                    delimiter_scores[delimiter] += 2

                # If fields are roughly similar lengths = higher score
                lengths = [len(p.strip()) for p in parts]
                avg_len = sum(lengths) / len(lengths)
                if all(abs(field_len - avg_len) < avg_len for field_len in lengths):
                    delimiter_scores[delimiter] += 1

                # Special case: if tab and consistent fields, boost score
                if delimiter == "\t" and non_empty == len(parts):
                    delimiter_scores[delimiter] += 3

                # Special case: if space delimiter and parts are consistent across lines
                if delimiter == " " and len(sample_lines) > 1:
                    # Check if number of fields is consistent across lines
                    first_line_parts = split_aligned_row(sample_lines[0])
                    if len(parts) == len(first_line_parts):
                        delimiter_scores[delimiter] += 2
                    else:
                        delimiter_scores[delimiter] -= 20

    # Default to raw if no clear winner
    if not delimiter_scores or max(delimiter_scores.values()) == 0:
        return "raw"

    # Return the delimiter with the highest score
    return max(delimiter_scores.items(), key=lambda x: x[1])[0]
