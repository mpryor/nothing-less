import csv
import json
import re
from io import StringIO

from .types import Column, MetadataColumn


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
    cells = [txt.replace("\t", "  ") for txt in cells]  # Rich rendering breaks on tabs

    sorted_columns = sorted(columns, key=lambda col: col.data_position)
    count_metadata_columns = sum(1 for col in sorted_columns if col.name in [mc.value for mc in MetadataColumn])

    for i, col in enumerate(sorted_columns):
        if col.computed:
            json_ref = col.json_ref
            json_path = json_ref.split(".")
            lookup_column = json_path[0]
            for c in sorted_columns:
                if c.name == lookup_column and c.data_position - count_metadata_columns < len(cells):
                    try:
                        json_data = json.loads(cells[c.data_position - count_metadata_columns])
                        for key in json_path[1:]:
                            if isinstance(json_data, dict):
                                json_data = json_data.get(key, "")
                            elif isinstance(json_data, list):
                                try:
                                    index = int(key)
                                    json_data = json_data[index]
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
                    break
    return cells


def split_aligned_row_preserve_single_spaces(line: str) -> list[str]:
    """Split a space-aligned row into fields by collapsing multiple spaces, but preserving single spaces within fields.

    Args:
        line: The input line to split

    Returns:
        List of fields from the line
    """
    # Use regex to split on two or more spaces
    return [field for field in re.split(r" {2,}", line) if field]


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
    try:
        # Use csv module to properly parse the line
        reader = csv.reader(StringIO(line.strip()))
        row = next(reader)
        return row
    except (csv.Error, StopIteration):
        # Fallback to simple split if CSV parsing fails
        return line.split(",")


def infer_delimiter(sample_lines: list[str]) -> str | None:
    """Infer the delimiter from a sample of lines.

    Args:
        sample_lines: A list of strings to analyze for delimiter detection.

    Returns:
        The most likely delimiter character.
    """
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
                if all(abs(l - avg_len) < avg_len for l in lengths):
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
