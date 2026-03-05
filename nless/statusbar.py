"""Status bar text building, extracted from NlessBuffer."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .types import Filter


def build_status_text(
    sort_column: str | None,
    sort_reverse: bool,
    filters: list[Filter],
    search_term: re.Pattern | None,
    search_matches_count: int,
    current_match_index: int,
    total_rows: int,
    total_cols: int,
    current_row: int,
    current_col: int,
    is_tailing: bool,
    unique_column_names: set[str],
    is_loading: bool,
) -> str:
    """Build the status bar text from buffer/table state."""
    sort_prefix = "[bold]Sort[/bold]"
    filter_prefix = "[bold]Filter[/bold]"
    search_prefix = "[bold]Search[/bold]"

    if sort_column is None:
        sort_text = f"{sort_prefix}: None"
    else:
        sort_text = f"{sort_prefix}: {sort_column} {'desc' if sort_reverse else 'asc'}"

    if len(filters) == 0:
        filter_text = f"{filter_prefix}: None"
    else:
        filter_descriptions = []
        for f in filters:
            prefix = "!" if f.exclude else ""
            if f.column is None:
                filter_descriptions.append(f"{prefix}any='{f.pattern.pattern}'")
            else:
                filter_descriptions.append(f"{prefix}{f.column}='{f.pattern.pattern}'")
        filter_text = f"{filter_prefix}: " + ", ".join(filter_descriptions)

    if search_term is not None:
        search_text = f"{search_prefix}: '{search_term.pattern}' ({current_match_index + 1} / {search_matches_count} matches)"
    else:
        search_text = f"{search_prefix}: None"

    row_prefix = "[bold]Row[/bold]"
    col_prefix = "[bold]Col[/bold]"
    position_text = f"{row_prefix}: {current_row}/{total_rows} {col_prefix}: {current_col}/{total_cols}"

    if is_tailing:
        tailing_text = "| [bold][#00bb00]Tailing (`t` to stop)[/#00bb00][/bold]"
    else:
        tailing_text = ""

    column_text = ""
    if len(unique_column_names):
        column_names = ",".join(unique_column_names)
        column_text = f"| unique cols: ({column_names}) "

    if is_loading:
        loading_text = (
            f"| [bold][#ffaa00]Loading ({total_rows:,} rows)[/#ffaa00][/bold] "
        )
    else:
        loading_text = ""

    return f"{sort_text} | {filter_text} | {search_text} | {position_text} {column_text}{tailing_text}{loading_text}"
