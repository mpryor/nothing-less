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
    line: str,
    delimiter: str | re.Pattern[str] | None,
    columns: list[Column],
    column_positions: list[int] | None = None,
    has_computed: bool | None = None,
) -> list[str]:
    """Split a line using the appropriate delimiter method.

    Args:
        line: The input line to split
        column_positions: Optional fixed-width column positions for
            space-aligned data with empty interior cells (e.g. lsof).
        has_computed: Pre-computed flag for whether any column has
            computed fields (delimiter/json_ref/col_ref/substitution).
            Pass ``False`` to skip the per-row check in hot loops.

    Returns:
        List of fields from the line
    """
    if delimiter in (" ", "  ") and column_positions:
        cells = split_by_positions(line, column_positions)
    elif delimiter == " ":
        max_fields = len([c for c in columns if not c.computed]) if columns else 0
        cells = split_aligned_row(line, max_fields=max_fields)
    elif delimiter == "  ":
        max_fields = len([c for c in columns if not c.computed]) if columns else 0
        cells = split_aligned_row_preserve_single_spaces(line, max_fields=max_fields)
    elif delimiter == ",":
        cells = split_csv_row(line)
    elif delimiter == "raw":
        stripped = line.rstrip("\n\r")
        cells = [rich_escape(stripped) if "[" in stripped else stripped]
    elif delimiter == "json":
        cells = [
            (json.dumps(v) if isinstance(v, (dict, list)) else str(v)).replace(
                "[", "\\["
            )
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

    if has_computed is None:
        has_computed = bool(
            columns
            and any(
                c.delimiter or c.json_ref or c.col_ref or c.substitution
                for c in columns
            )
        )
    if not has_computed:
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


def split_aligned_row_preserve_single_spaces(
    line: str, max_fields: int = 0
) -> list[str]:
    """Split a space-aligned row into fields by collapsing multiple spaces, but preserving single spaces within fields.

    Args:
        line: The input line to split
        max_fields: When > 0, cap the number of fields so the last field
            may contain internal multi-space runs (e.g. ``"Mounted on"``).

    Returns:
        List of fields from the line
    """
    if max_fields > 0:
        return [
            field
            for field in _MULTI_SPACE_RE.split(line, maxsplit=max_fields - 1)
            if field
        ]
    return [field for field in _MULTI_SPACE_RE.split(line) if field]


def split_aligned_row(line: str, max_fields: int = 0) -> list[str]:
    """Split a space-aligned row into fields by collapsing multiple spaces.

    Args:
        line: The input line to split
        max_fields: When > 0, cap the number of fields so the last field
            may contain internal whitespace (e.g. a COMMAND column).

    Returns:
        List of fields from the line
    """
    if max_fields > 0:
        return [field for field in line.split(None, max_fields - 1) if field]
    return [field for field in line.split() if field]


def detect_column_positions(sample_lines: list[str]) -> list[int]:
    """Detect column start positions from space-aligned output.

    Examines ALL sample lines (header + data) to find positions where a
    column gap (all-space across every line) transitions to content
    (non-space in at least one line).  This handles right-aligned numeric
    columns and empty interior cells correctly (e.g. ``lsof`` TID column).
    """
    # Strip trailing whitespace — inconsistent padding (e.g. netstat
    # pads data lines with spaces) creates false column boundaries.
    non_empty = [ln.rstrip() for ln in sample_lines if ln.strip()]
    if not non_empty:
        return []
    # Exclude preamble/outlier lines that are much shorter than the
    # majority — they corrupt gap detection (e.g. netstat's
    # "Active Internet connections (only servers)").
    max_len = max(len(ln) for ln in non_empty)
    non_empty = [ln for ln in non_empty if len(ln) >= max_len * 0.5]
    if not non_empty:
        return []
    min_len = min(len(ln) for ln in non_empty)

    positions: list[int] = []
    for i in range(min_len):
        has_content = any(ln[i] != " " for ln in non_empty)
        if not has_content:
            continue
        if i == 0:
            positions.append(0)
        elif all(ln[i - 1] == " " for ln in non_empty):
            positions.append(i)
    return positions


def split_by_positions(line: str, positions: list[int]) -> list[str]:
    """Split a line using fixed character positions.

    Each field spans from ``positions[i]`` to ``positions[i+1]``
    (or end-of-line for the last field).  Fields are stripped of
    surrounding whitespace.
    """
    fields: list[str] = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(line)
        field = line[start:end].strip() if start < len(line) else ""
        fields.append(field)
    return fields


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


def _field_count(line: str, delimiter: str, max_fields: int = 0) -> int:
    """Count fields produced by splitting *line* with *delimiter*."""
    if delimiter == ",":
        return len(split_csv_row(line))
    elif delimiter == " ":
        return len(split_aligned_row(line, max_fields=max_fields))
    elif delimiter == "  ":
        return len(
            split_aligned_row_preserve_single_spaces(line, max_fields=max_fields)
        )
    else:
        return len(line.split(delimiter))


def find_header_index(lines: list[str], delimiter: str, max_fields: int = 0) -> int:
    """Return the index of the first line matching the consensus field count.

    For files with leading non-tabular lines (e.g. a JSON preamble before CSV
    data), this skips past the noise so the correct header line is used.

    Returns 0 when no adjustment is needed.
    """
    counts: list[int] = []
    for line in lines[:15]:
        if not line.strip():
            continue
        c = _field_count(line, delimiter, max_fields=max_fields)
        if c > 1:
            counts.append(c)

    if not counts:
        return 0

    consensus = max(set(counts), key=counts.count)
    if consensus <= 1:
        return 0

    # First line already matches — no skip needed.
    # For space-delimited data, allow the header to be slightly under
    # consensus: the last column often contains spaces in data values
    # (e.g. lsof NAME "127.0.0.1:... (SYN_SENT)") but not in the
    # header name, producing fewer fields in the header line.
    if lines:
        first_count = _field_count(lines[0], delimiter, max_fields=max_fields)
        if first_count == consensus:
            return 0
        if delimiter in (" ", "  ") and first_count == consensus - 1:
            return 0

    for i, line in enumerate(lines):
        if not line.strip():
            continue
        if _field_count(line, delimiter, max_fields=max_fields) == consensus:
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
    dblspace_raw_agreement = 0.0
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
                if delimiter == "  ":
                    dblspace_raw_agreement = agreement

                # For space delimiters, retry with adjustments before
                # giving up: (a) exclude a preamble first-line whose
                # count is far below consensus, (b) cap overflow lines
                # (spaces in last column) to the consensus count.
                if agreement < 0.8 and delimiter in (" ", "  ") and most_common > 2:
                    adjusted = []
                    n_below = 0
                    for i, c in enumerate(counts):
                        if i == 0 and c < most_common * 0.5:
                            continue  # preamble line (e.g. "total 40")
                        if c < most_common:
                            n_below += 1
                        # Cap overflow from spaces in the last column
                        # (e.g. ls -la filenames, ps aux COMMAND). Only
                        # cap lines moderately above consensus — very
                        # large overflows are structural (e.g. prose).
                        if most_common < c <= most_common + 4:
                            adjusted.append(most_common)
                        else:
                            adjusted.append(c)
                    # Only accept adjustment when disagreement is from
                    # overflow (above consensus) and the spread is small
                    # relative to the field count.  Prose/logs scatter
                    # widely — reject those.
                    count_range = max(counts) - min(counts) if counts else 0
                    relative_range = count_range / most_common if most_common else 1
                    if adjusted and n_below <= 1 and relative_range <= 0.35:
                        adj_agreement = adjusted.count(most_common) / len(adjusted)
                        if adj_agreement >= 0.8:
                            agreement = adj_agreement
                            counts = adjusted

                if agreement >= 0.8:
                    # Strong cross-line consistency — boost
                    delimiter_scores[delimiter] += len(counts) * 2
                else:
                    # Inconsistent — zero out the score
                    delimiter_scores[delimiter] = 0

    # When both space delimiters survive consistency checks, prefer "  "
    # (space+) if it has strictly better raw agreement.  " " splits on ALL
    # whitespace, so fields with internal single spaces (e.g. kubectl
    # RESTARTS "2000 (8s ago)") inflate its per-line field count and base
    # score even though the splits are wrong.  "  " preserves those values
    # and should win when its consistency is higher.
    sp_score = delimiter_scores.get(" ", 0)
    sp2_score = delimiter_scores.get("  ", 0)
    if sp_score > 0 and sp2_score > 0:
        sp_counts = delimiter_field_counts.get(" ", [])
        sp2_counts = delimiter_field_counts.get("  ", [])
        if sp_counts and sp2_counts:
            sp_agree = sp_counts.count(max(set(sp_counts), key=sp_counts.count)) / len(
                sp_counts
            )
            sp2_agree = sp2_counts.count(
                max(set(sp2_counts), key=sp2_counts.count)
            ) / len(sp2_counts)
            if sp2_agree > sp_agree:
                delimiter_scores[" "] = 0

    # Last resort: try position-based splitting for space-aligned output
    # with empty cells (e.g. lsof, where TID/TASKCMD columns are blank).
    # split() collapses empty cells, producing inconsistent field counts,
    # but fixed-width position slicing gives perfect consistency.
    # Skip if double-space already has naturally high agreement —
    # position-based splitting can't distinguish single-space word
    # boundaries within header names (e.g. "LAST SEEN") from column
    # boundaries. Only check raw agreement (before adjustment), since
    # adjusted agreement can rescue genuinely broken splits (e.g. lsof).
    if (
        delimiter_scores.get(" ", 0) == 0
        and dblspace_raw_agreement < 0.8
        and first_line_splits.get(" ", False)
        and non_empty_lines >= 2
    ):
        positions = detect_column_positions(sample_lines)
        if len(positions) > 2:
            pos_field_count = len(positions)
            pos_consistent = all(
                len(split_by_positions(ln, positions)) == pos_field_count
                for ln in sample_lines
                if ln.strip()
            )
            # Require positions to span a reasonable fraction of the line
            # width — spurious positions from source code span very few chars.
            max_line_len = max(
                (len(ln.rstrip()) for ln in sample_lines if ln.strip()), default=0
            )
            pos_span = positions[-1] - positions[0] if positions else 0
            if pos_consistent and pos_span >= max_line_len * 0.5:
                delimiter_scores[" "] = pos_field_count * non_empty_lines * 3

    # Default to raw if no clear winner
    if not delimiter_scores or max(delimiter_scores.values()) <= 0:
        return "raw"

    # Return the delimiter with the highest score
    return max(delimiter_scores.items(), key=lambda x: x[1])[0]


def detect_space_max_fields(sample_lines: list[str], delimiter: str) -> int:
    """Detect the correct field count for space-delimited data.

    Finds the consensus field count across all sample lines and returns
    it as a cap when some lines overflow (spaces in last column) or when
    the header has multi-word column names.

    Handles preamble lines (e.g. ``netstat``'s "Active Internet
    connections") by excluding outliers far below the consensus.

    Returns 0 when no cap is needed.
    """
    if delimiter not in (" ", "  "):
        return 0

    split_fn = (
        split_aligned_row
        if delimiter == " "
        else split_aligned_row_preserve_single_spaces
    )

    all_counts: list[int] = []
    for line in sample_lines:
        if not line.strip():
            continue
        n = len(split_fn(line))
        if n > 1:
            all_counts.append(n)

    if len(all_counts) < 2:
        return 0

    consensus = max(set(all_counts), key=all_counts.count)
    if consensus <= 2:
        return 0

    # Exclude preamble outliers (count far below consensus) and check
    # whether any remaining lines differ from the consensus.
    relevant = [c for c in all_counts if c >= consensus * 0.5]
    has_overflow = any(c > consensus for c in relevant)
    has_underflow = any(c < consensus for c in relevant)

    if has_overflow or has_underflow:
        return consensus

    return 0


def detect_space_splitting_strategy(
    sample: list[str], delimiter: str
) -> tuple[list[int] | None, int]:
    """Choose position-based or max_fields splitting for space-delimited data.

    Position-based splitting is preferred when the data has empty interior
    cells (e.g. lsof TID/TASKCMD) or multi-word column headers (e.g.
    netstat "Local Address").  Falls back to max_fields capping when data
    overflows the header (e.g. ps aux COMMAND with spaces).

    Returns:
        ``(column_positions, max_fields)`` — at most one is set.
        *column_positions* is a list of character offsets or ``None``;
        *max_fields* is ``0`` when not applicable.
    """
    if delimiter not in (" ", "  "):
        return None, 0

    positions = detect_column_positions(sample)
    if len(positions) > 2:
        expected = len(positions)
        max_line_len = max((len(ln) for ln in sample if ln.strip()), default=0)
        long_lines = [
            ln for ln in sample if ln.strip() and len(ln) >= max_line_len * 0.5
        ]
        # Use the correct split function for the delimiter so single-space
        # gaps inside field values (e.g. "4584 (3m4s ago)") are not treated
        # as column boundaries for the space+ delimiter.
        normal_split = (
            split_aligned_row
            if delimiter == " "
            else split_aligned_row_preserve_single_spaces
        )
        normal_counts = [len(normal_split(ln)) for ln in long_lines]
        normal_inconsistent = len(set(normal_counts)) > 1
        if normal_inconsistent:
            pos_consistent = all(
                len(split_by_positions(ln, positions)) == expected for ln in long_lines
            )
            first_count = normal_counts[0] if normal_counts else 0
            consensus = max(set(normal_counts), key=normal_counts.count)
            header_overflow = first_count > consensus
            # Reject positions that produce empty header fields — a sign
            # of a false column boundary inside a wide data value.
            header_fields = (
                split_by_positions(long_lines[0], positions) if long_lines else []
            )
            has_empty_header = any(f == "" for f in header_fields)
            if pos_consistent and not has_empty_header and header_overflow:
                return positions, 0
            if pos_consistent and not has_empty_header and not header_overflow:
                maxsplit_counts = [
                    len(normal_split(ln, max_fields=expected)) for ln in long_lines
                ]
                if len(set(maxsplit_counts)) > 1:
                    return positions, 0

    return None, detect_space_max_fields(sample, delimiter)


def find_preamble_end(lines: list[str]) -> int:
    """Find the index where short preamble lines end.

    Lines shorter than 50% of the max line length at the start of
    *lines* are considered preamble (e.g. netstat's ``"Active Internet
    connections"``).  Returns ``0`` if no preamble detected.
    """
    max_line_len = max((len(ln) for ln in lines if ln.strip()), default=0)
    end = 0
    for i, line in enumerate(lines):
        if line.strip() and len(line) < max_line_len * 0.5:
            end = i + 1
        else:
            break
    return end
