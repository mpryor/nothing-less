"""Status bar text building, extracted from NlessBuffer."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .theme import NlessTheme
    from .types import Filter

SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

DEFAULT_STATUS_FORMAT = (
    "{sort} | {filter} | {search} | {position} {unique}{tailing}{loading}"
)


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
    loading_reason: str | None,
    theme: NlessTheme | None = None,
    spinner_frame: int = 0,
    format_str: str | None = None,
    keymap_name: str = "vim",
    theme_name: str = "default",
) -> str:
    """Build the status bar text from buffer/table state."""
    if theme is None:
        from .theme import BUILTIN_THEMES

        theme = BUILTIN_THEMES["default"]

    if format_str is None:
        format_str = DEFAULT_STATUS_FORMAT

    sort_prefix = "[bold]Sort[/bold]"
    filter_prefix = "[bold]Filter[/bold]"
    search_prefix = "[bold]Search[/bold]"

    if sort_column is None:
        sort_text = ""
    else:
        sort_text = f"{sort_prefix}: {sort_column} {'desc' if sort_reverse else 'asc'}"

    if len(filters) == 0:
        filter_text = ""
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
        search_text = ""

    row_prefix = "[bold]Row[/bold]"
    col_prefix = "[bold]Col[/bold]"
    position_text = f"{row_prefix}: {current_row}/{total_rows} {col_prefix}: {current_col}/{total_cols}"

    if is_tailing:
        tailing_color = theme.markup("status_tailing", "Tailing (`t` to stop)")
        tailing_text = f"| [bold]{tailing_color}[/bold]"
    else:
        tailing_text = ""

    column_text = ""
    if len(unique_column_names):
        column_names = ",".join(unique_column_names)
        column_text = f"| unique cols: ({column_names}) "

    if loading_reason:
        spinner = SPINNER_FRAMES[spinner_frame % len(SPINNER_FRAMES)]
        loading_color = theme.markup(
            "status_loading", f"{spinner} {loading_reason} ({total_rows:,} rows)"
        )
        loading_text = f"| [bold]{loading_color}[/bold] "
    else:
        loading_text = ""

    variables = {
        "sort": sort_text,
        "filter": filter_text,
        "search": search_text,
        "position": position_text,
        "row": str(current_row),
        "rows": str(total_rows),
        "col": str(current_col),
        "cols": str(total_cols),
        "unique": column_text,
        "tailing": tailing_text,
        "loading": loading_text,
        "keymap": keymap_name,
        "theme": theme_name,
    }

    # Expose all theme color slots as variables for Rich markup,
    # e.g. [{accent}]text[/{accent}] or [{cursor_bg}]text[/{cursor_bg}]
    for slot_name in theme.__dataclass_fields__:
        if slot_name != "name":
            variables[slot_name] = getattr(theme, slot_name)

    try:
        result = format_str.format(**variables)
    except KeyError:
        # Fall back to default format if user format has unknown variables
        result = DEFAULT_STATUS_FORMAT.format(**variables)

    # Strip empty Rich markup pairs left by empty variables, e.g.
    # "[#ffd866][/#ffd866]" when {sort} is "".
    result = re.sub(r"\[([^\]/]+)\]\s*\[/\1\]", "", result)

    # Clean up artifacts from empty variables: collapse repeated separators
    # and strip leading/trailing separators with optional surrounding markup.
    # This handles both plain "|" and markup-wrapped separators like
    # "[#727072]|[/#727072]".
    _sep = r"(?:\[[\w#/]+\])?\|(?:\[[\w#/]+\])?"
    result = re.sub(rf"(\s*{_sep}\s*){{2,}}", r"\1", result)
    result = re.sub(rf"^\s*{_sep}\s*", "", result)
    result = re.sub(rf"\s*{_sep}\s*$", "", result)
    return result.strip()
