"""Pure data-processing helpers extracted from NlessBuffer.

All functions here are stateless — they take explicit parameters
rather than reading from ``self``.
"""

from __future__ import annotations

import bisect
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .types import ColumnType, Filter

# Type aliases for callbacks passed from NlessBuffer
ColLookupFn = Callable[[str, bool], int | None]
StripMarkupFn = Callable[[str], str]

_MARKUP_TAG_RE = re.compile(r"\[/?[^\]]*\]")
_NUMERIC_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


def strip_markup(cell_value: str) -> str:
    """Extract plain text from a cell value, removing any Rich markup tags."""
    if "[" not in cell_value:
        return cell_value
    return _MARKUP_TAG_RE.sub("", cell_value)


def _looks_numeric(value: str) -> bool:
    """Fast check whether a string looks like a number, avoiding exceptions."""
    if not value:
        return False
    # Fast path: check first char to reject obvious non-numbers
    c = value[0]
    if c not in "0123456789+-.":
        return False
    return _NUMERIC_RE.match(value) is not None


def coerce_to_numeric(value: Any) -> int | float | str:
    """Try to coerce *value* to a numeric type."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if not value:
            return value
        c = value[0]
        if c not in "0123456789+-.":
            return value
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value


_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%b %d, %Y",
]


def _try_parse_datetime(value: str) -> datetime | None:
    """Try to parse a datetime string. Returns datetime or None."""
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        pass
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except (ValueError, TypeError):
            continue
    return None


def infer_column_type(values: list[str], threshold: float = 0.8) -> ColumnType:
    """Infer the column type from a sample of values.

    Samples up to 100 non-empty values. Returns NUMERIC if >=threshold are
    numeric, DATETIME if >=threshold parse as dates, otherwise STRING.
    """
    from .types import ColumnType

    non_empty = [v for v in values if v.strip()][:100]
    if len(non_empty) < 3:
        return ColumnType.STRING

    numeric_count = sum(1 for v in non_empty if _looks_numeric(v))
    if numeric_count / len(non_empty) >= threshold:
        return ColumnType.NUMERIC

    datetime_count = sum(1 for v in non_empty if _try_parse_datetime(v) is not None)
    if datetime_count / len(non_empty) >= threshold:
        return ColumnType.DATETIME

    return ColumnType.STRING


def coerce_datetime_sort_key(value: str, fmt_hint: str | None = None) -> float | str:
    """Coerce a datetime string to epoch float for sorting.

    If fmt_hint is provided, tries that strptime format first.
    Falls back to fromisoformat then other formats. Returns original
    string on failure.
    """
    if not value or not value.strip():
        return value
    if fmt_hint is not None:
        try:
            return datetime.strptime(value, fmt_hint).timestamp()
        except (ValueError, TypeError):
            pass
    try:
        return datetime.fromisoformat(value).timestamp()
    except (ValueError, TypeError):
        pass
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt).timestamp()
        except (ValueError, TypeError):
            continue
    return value


def coerce_sort_key(
    value: str, column_type: ColumnType | None = None
) -> int | float | str:
    """Coerce a string to an appropriate sort key based on column type."""
    if not value:
        return value
    if column_type is not None:
        from .types import ColumnType as CT

        if column_type == CT.DATETIME:
            result = coerce_datetime_sort_key(value)
            if isinstance(result, float):
                return result
            return value
        if column_type == CT.STRING:
            return value
    # Default/NUMERIC: existing behavior
    c = value[0]
    if c not in "0123456789+-.":
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        pass
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value


def build_composite_key(
    cells: list[str],
    unique_column_names: set[str],
    col_lookup_fn: ColLookupFn,
    strip_markup_fn: StripMarkupFn = strip_markup,
    render_position: bool = False,
) -> str:
    """Build a composite key from unique column values in a row."""
    parts = []
    for col_name in unique_column_names:
        col_idx = col_lookup_fn(col_name, render_position)
        if col_idx is None:
            continue
        parts.append(strip_markup_fn(cells[col_idx]))
    return ",".join(parts)


def find_sorted_insert_index(
    cells: list[str],
    sort_keys: list,
    sort_column: str | None,
    sort_reverse: bool,
    col_lookup_fn: ColLookupFn,
    strip_markup_fn: StripMarkupFn = strip_markup,
    num_displayed_rows: int = 0,
    column_type: ColumnType | None = None,
) -> int:
    """Find the insertion index for a row based on current sort state."""
    if sort_column is None:
        return num_displayed_rows

    data_sort_col_idx = col_lookup_fn(sort_column, False)

    raw_key = strip_markup_fn(str(cells[data_sort_col_idx]))
    sort_key = coerce_sort_key(raw_key, column_type)

    idx = bisect.bisect_left(sort_keys, sort_key)
    if sort_reverse:
        return len(sort_keys) - idx
    return idx


def update_dedup_indices_after_removal(
    dedup_key_to_row_idx: dict[str, int], old_index: int
) -> None:
    """Shift dedup index entries down after a row removal."""
    for k, idx in dedup_key_to_row_idx.items():
        if idx > old_index:
            dedup_key_to_row_idx[k] = idx - 1


def update_dedup_indices_after_insertion(
    dedup_key_to_row_idx: dict[str, int], dedup_key: str, new_index: int
) -> None:
    """Shift dedup index entries up after a row insertion, then record the new key."""
    for k, idx in dedup_key_to_row_idx.items():
        if idx >= new_index:
            dedup_key_to_row_idx[k] = idx + 1
    dedup_key_to_row_idx[dedup_key] = new_index


def update_sort_keys_for_line(
    data_cells: list[str],
    old_row: list[str] | None,
    sort_column: str | None,
    sort_keys: list,
    col_lookup_fn: ColLookupFn,
    strip_markup_fn: StripMarkupFn = strip_markup,
    column_type: ColumnType | None = None,
) -> None:
    """Update the incremental sort keys list after insertion/removal.

    Args:
        data_cells: The new row cells in data-position order.
        old_row: The old display row (render-position order) being replaced, or None.
        sort_column: Current sort column name, or None.
        sort_keys: The maintained list of sort keys (ascending).
        col_lookup_fn: Function(col_name, render_position) → index.
        strip_markup_fn: Function(text) → plain text.
        column_type: The detected type of the sort column, if known.
    """
    if sort_column is None:
        return
    data_sort_col_idx = col_lookup_fn(sort_column, False)
    if data_sort_col_idx is None:
        return

    # Remove old sort key by value lookup
    if old_row is not None:
        render_sort_col_idx = col_lookup_fn(sort_column, True)
        if render_sort_col_idx is not None and render_sort_col_idx < len(old_row):
            old_raw = strip_markup_fn(str(old_row[render_sort_col_idx]))
            old_key = coerce_sort_key(old_raw, column_type)
            ki = bisect.bisect_left(sort_keys, old_key)
            if ki < len(sort_keys) and sort_keys[ki] == old_key:
                sort_keys.pop(ki)

    # Insert new sort key from data-position cells
    new_raw = strip_markup_fn(str(data_cells[data_sort_col_idx]))
    bisect.insort_left(sort_keys, coerce_sort_key(new_raw, column_type))


def highlight_search_matches(
    rows: list[list[str]],
    search_term: re.Pattern | None,
    fixed_columns: int,
    row_offset: int = 0,
    search_match_style: str = "reverse",
) -> tuple[list[list[str]], list[tuple[int, int]]]:
    """Apply search highlighting to rows.

    Returns (highlighted_rows, new_matches) where each match is a
    ``(row, col)`` tuple.
    """
    if not search_term:
        return rows, []
    result = []
    new_matches: list[tuple[int, int]] = []
    open_tag = f"[{search_match_style}]"
    close_tag = f"[/{search_match_style}]"
    for i, cells in enumerate(rows):
        highlighted_cells = []
        for col_idx, cell in enumerate(cells):
            if search_term.search(str(cell)) and col_idx > fixed_columns - 1:
                cell = re.sub(
                    search_term,
                    lambda m: f"{open_tag}{m.group(0)}{close_tag}",
                    cell,
                )
                highlighted_cells.append(cell)
                new_matches.append((row_offset + i, col_idx))
            else:
                highlighted_cells.append(cell)
        result.append(highlighted_cells)
    return result, new_matches


def highlight_regex_patterns(
    rows: list[list[str]],
    patterns: list[tuple[re.Pattern, str]],
    fixed_columns: int,
) -> list[list[str]]:
    """Apply multiple regex highlights with distinct colors.

    Each entry in *patterns* is a ``(compiled_regex, color)`` pair.
    Matches are wrapped in Rich markup tags with the corresponding color.
    """
    if not patterns:
        return rows
    result = []
    for cells in rows:
        highlighted_cells = list(cells)
        for col_idx, cell in enumerate(cells):
            if col_idx < fixed_columns:
                continue
            for pattern, color in patterns:
                if pattern.search(str(cell)):
                    open_tag = f"[{color}]"
                    close_tag = f"[/{color}]"
                    highlighted_cells[col_idx] = re.sub(
                        pattern,
                        lambda m, ot=open_tag, ct=close_tag: f"{ot}{m.group(0)}{ct}",
                        highlighted_cells[col_idx],
                    )
        result.append(highlighted_cells)
    return result


def matches_all_filters(
    cells: list[str],
    filters: list[Filter],
    col_lookup_fn: ColLookupFn,
    adjust_for_count: bool = False,
    has_unique_columns: bool = False,
) -> bool:
    """Check if a row matches all filters.

    Returns ``True`` if every filter matches (or there are no filters).
    """
    if not filters:
        return True
    for f in filters:
        if f.column is None:
            matched = any(f.pattern.search(cell) for cell in cells)
        else:
            col_idx = col_lookup_fn(f.column, False)
            if col_idx is None:
                return False
            if adjust_for_count and has_unique_columns:
                col_idx -= 1
            matched = bool(f.pattern.search(cells[col_idx]))
        if matched == f.exclude:
            return False
    return True


def choose_parse_strategy(delimiter, has_nested, columns, column_positions=None):
    """Return (parse_fn, needs_cleanup) for a given delimiter.

    Selecting the strategy once outside a hot loop avoids repeated
    isinstance / equality checks per row. This is a pure function —
    it depends only on the delimiter type, not on buffer state.
    """
    import csv

    from .delimiter import split_line

    if not has_nested and delimiter == ",":

        def parse_csv(line):
            s = line.strip()
            return next(csv.reader([s])) if '"' in s else s.split(",")

        return parse_csv, True

    if not has_nested and delimiter == "\t":
        return lambda line: line.split("\t"), True

    if (
        not has_nested
        and isinstance(delimiter, str)
        and delimiter not in ("raw", "json", " ", "  ")
    ):
        return lambda line: line.split(delimiter), True

    if delimiter == "raw":
        from rich.markup import escape as _rich_escape

        def parse_raw(line):
            s = line.rstrip("\n\r").expandtabs()
            return [_rich_escape(s) if "[" in s else s]

        return parse_raw, False

    # split_line already cleans cells
    return (
        lambda line: split_line(
            line, delimiter, columns, column_positions=column_positions
        ),
        False,
    )
