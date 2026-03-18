import csv
import json
import re
from io import StringIO

from rich.markup import escape as rich_escape

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
        stripped = line.rstrip("\n\r")
        cells = [rich_escape(stripped) if "[" in stripped else stripped]
    elif delimiter == "json":
        cells = [
            (
                json.dumps(v) if isinstance(v, dict) or isinstance(v, list) else str(v)
            ).replace("[", "\\[")
            for v in json.loads(line).values()
        ]
    elif isinstance(delimiter, re.Pattern):
        match = delimiter.match(line)
        if match:
            cells = [v if v is not None else "" for v in match.groups()]
        else:
            cells = []
    else:
        cells = line.split(delimiter)

    if delimiter == "raw":
        pass  # Preserve whitespace and tabs for raw pager rendering
    else:
        cells = [
            txt.replace("\t", "  ").strip() for txt in cells
        ]  # Rich rendering breaks on tabs

    if not columns or not any(
        c.delimiter or c.json_ref or c.col_ref or c.substitution for c in columns
    ):
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

    # Collect computed cells with their target positions, then merge in
    # one pass to avoid O(n²) repeated list insertions.
    computed = []  # list of (insert_position, value)
    for i, col in enumerate(sorted_columns):
        pos = col.data_position - count_metadata_columns
        if col.delimiter and col.delimiter == "json":
            json_path = col.json_ref.split(".")
            ref_cell = _find_ref_column_cell(
                json_path[0], sorted_columns, cells, count_metadata_columns
            )
            if ref_cell is None:
                continue
            try:
                json_data = json.loads(ref_cell.replace("\\[", "["))
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
            value = (
                json.dumps(json_data)
                if isinstance(json_data, (dict, list))
                else str(json_data)
            ).replace("[", "\\[")
            computed.append((pos, value))
        elif isinstance(col.delimiter, re.Pattern):
            ref_cell = _find_ref_column_cell(
                col.col_ref, sorted_columns, cells, count_metadata_columns
            )
            if ref_cell is None:
                continue
            match = col.delimiter.match(ref_cell)
            if match:
                subcells = [txt.replace("\t", "  ") for txt in match.groups()]
                value = (
                    subcells[col.col_ref_index]
                    if col.col_ref_index < len(subcells)
                    else ""
                )
                computed.append((pos, value))
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
            value = (
                subcells[col.col_ref_index] if col.col_ref_index < len(subcells) else ""
            )
            computed.append((pos, value))

    # Merge computed cells into the base cells in one pass (O(n))
    if computed:
        computed.sort(key=lambda x: x[0])
        result = []
        base_idx = 0
        for insert_pos, value in computed:
            # Copy base cells up to this insertion point
            while base_idx < insert_pos and base_idx < len(cells):
                result.append(cells[base_idx])
                base_idx += 1
            result.append(value)
        # Append remaining base cells
        result.extend(cells[base_idx:])
        cells = result

    # Apply regex substitutions to cells
    for col in sorted_columns:
        if col.substitution is not None:
            pat, repl = col.substitution
            idx = col.data_position - count_metadata_columns
            if 0 <= idx < len(cells):
                cells[idx] = pat.sub(repl, cells[idx], count=1)

    return cells


def flatten_json_lines(lines: list[str]) -> list[str]:
    """Convert pretty-printed JSON into JSONL (one object per line).

    If the input is already JSONL, returns it unchanged.  Handles both
    a single JSON object and a JSON array of objects.
    """
    # Find the first non-empty line to decide what we're dealing with
    first = ""
    for line in lines:
        first = line.strip()
        if first:
            break

    if not first:
        return lines

    # Already JSONL — first line is a complete JSON object
    candidate = first.rstrip(",")
    if candidate.startswith("{"):
        try:
            json.loads(candidate)
            return lines
        except (json.JSONDecodeError, ValueError):
            pass

    # Only attempt the expensive join+parse if the first line looks like
    # the start of a JSON structure (object or array)
    if first not in ("{", "["):
        return lines

    joined = "\n".join(lines)
    try:
        parsed = json.loads(joined)
    except (json.JSONDecodeError, ValueError):
        return lines

    if isinstance(parsed, dict):
        return [json.dumps(parsed)]
    elif isinstance(parsed, list):
        return [json.dumps(item) for item in parsed]
    return lines


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


def _field_count(line: str, delimiter: str) -> int:
    """Count fields produced by splitting *line* with *delimiter*."""
    if delimiter == ",":
        return len(split_csv_row(line))
    elif delimiter == " ":
        return len(split_aligned_row(line))
    elif delimiter == "  ":
        return len(split_aligned_row_preserve_single_spaces(line))
    else:
        return len(line.split(delimiter))


def find_header_index(lines: list[str], delimiter: str) -> int:
    """Return the index of the first line matching the consensus field count.

    For files with leading non-tabular lines (e.g. a JSON preamble before CSV
    data), this skips past the noise so the correct header line is used.

    Returns 0 when no adjustment is needed.
    """
    counts: list[int] = []
    for line in lines[:15]:
        if not line.strip():
            continue
        c = _field_count(line, delimiter)
        if c > 1:
            counts.append(c)

    if not counts:
        return 0

    consensus = max(set(counts), key=counts.count)
    if consensus <= 1:
        return 0

    # First line already matches — no skip needed
    if lines and _field_count(lines[0], delimiter) == consensus:
        return 0

    for i, line in enumerate(lines):
        if not line.strip():
            continue
        if _field_count(line, delimiter) == consensus:
            return i

    return 0


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
    # Track field counts per delimiter for cross-line consistency check
    delimiter_field_counts: dict[str, list[int]] = {d: [] for d in common_delimiters}
    # Track whether the first non-empty line splits on each delimiter
    first_line_splits: dict[str, bool] = {d: False for d in common_delimiters}
    non_empty_lines = 0

    for line in sample_lines:
        # Skip empty lines
        if not line.strip():
            continue
        non_empty_lines += 1

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

            n_fields = len(parts)
            if n_fields > 1:
                delimiter_field_counts[delimiter].append(n_fields)
                if non_empty_lines == 1:
                    first_line_splits[delimiter] = True

            # Score based on number of fields and consistency
            if n_fields > 1:
                # More fields = higher score
                delimiter_scores[delimiter] += n_fields

                # Consistent non-empty fields = higher score
                non_empty = sum(1 for p in parts if p.strip())
                if non_empty == n_fields:
                    delimiter_scores[delimiter] += 2

                # If fields are roughly similar lengths = higher score
                lengths = [len(p.strip()) for p in parts]
                avg_len = sum(lengths) / len(lengths)
                if all(abs(field_len - avg_len) < avg_len for field_len in lengths):
                    delimiter_scores[delimiter] += 1

                # Special case: if tab and consistent fields, boost score
                if delimiter == "\t" and non_empty == n_fields:
                    delimiter_scores[delimiter] += 3

    # Penalize delimiters with inconsistent field counts across lines.
    # Tabular data has the same number of columns on every row; source code,
    # prose, and config files produce wildly varying split counts.
    for delimiter, counts in delimiter_field_counts.items():
        if non_empty_lines >= 2:
            # A delimiter that doesn't split the first line (header) but
            # splits later lines is not a tabular delimiter — zero it.
            if not first_line_splits[delimiter] and len(counts) > 0:
                delimiter_scores[delimiter] = 0
                continue
            # If half or fewer non-empty lines split, weak signal
            if len(counts) <= non_empty_lines * 0.5:
                delimiter_scores[delimiter] = 0
                continue
            if len(counts) >= 2:
                most_common = max(set(counts), key=counts.count)
                agreement = counts.count(most_common) / len(counts)
                if agreement >= 0.8:
                    # Strong cross-line consistency — boost
                    delimiter_scores[delimiter] += len(counts) * 2
                else:
                    # Inconsistent — zero out the score
                    delimiter_scores[delimiter] = 0

    # Default to raw if no clear winner
    if not delimiter_scores or max(delimiter_scores.values()) <= 0:
        return "raw"

    # Return the delimiter with the highest score
    return max(delimiter_scores.items(), key=lambda x: x[1])[0]
