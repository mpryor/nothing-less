"""Pure data-processing helpers extracted from NlessBuffer.

All functions here are stateless — they take explicit parameters
rather than reading from ``self``.
"""

from __future__ import annotations

import bisect
import re
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .types import Filter

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
    if isinstance(value, str) and not _looks_numeric(value):
        return value
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value


def coerce_sort_key(value: str) -> int | float | str:
    """Coerce a string to numeric if possible, for sort comparison."""
    if not _looks_numeric(value):
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
) -> int:
    """Find the insertion index for a row based on current sort state."""
    if sort_column is None:
        return num_displayed_rows

    data_sort_col_idx = col_lookup_fn(sort_column, False)

    raw_key = strip_markup_fn(str(cells[data_sort_col_idx]))
    sort_key = coerce_sort_key(raw_key)

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
) -> None:
    """Update the incremental sort keys list after insertion/removal.

    Args:
        data_cells: The new row cells in data-position order.
        old_row: The old display row (render-position order) being replaced, or None.
        sort_column: Current sort column name, or None.
        sort_keys: The maintained list of sort keys (ascending).
        col_lookup_fn: Function(col_name, render_position) → index.
        strip_markup_fn: Function(text) → plain text.
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
            old_key = coerce_sort_key(old_raw)
            ki = bisect.bisect_left(sort_keys, old_key)
            if ki < len(sort_keys) and sort_keys[ki] == old_key:
                sort_keys.pop(ki)

    # Insert new sort key from data-position cells
    new_raw = strip_markup_fn(str(data_cells[data_sort_col_idx]))
    bisect.insort_left(sort_keys, coerce_sort_key(new_raw))


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
