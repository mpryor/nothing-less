"""Status bar text building, extracted from NlessBuffer."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .theme import NlessTheme
    from .types import Filter

SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

DEFAULT_STATUS_FORMAT = "{sort} | {filter} | {search} | {position} | {unique}{time_window}{skipped}{tailing}{loading}{lag} {throughput} {pipe}"


def _format_rate(rate: float) -> str:
    """Format a rows/sec rate into a human-readable string like ~1.2K/s or ~3.4M/s."""
    if rate < 1000:
        return f"~{rate:.0f}/s"
    elif rate < 1_000_000:
        return f"~{rate / 1000:.1f}K/s"
    else:
        return f"~{rate / 1_000_000:.1f}M/s"


def _format_pipe(pressure: tuple[int, int | None]) -> str:
    """Format pipe pressure as 'pipe: 42KB/64KB' or 'pipe: 42KB' (no capacity)."""
    pending, capacity = pressure
    pending_kb = f"{pending // 1024}KB" if pending >= 1024 else f"{pending}B"
    if capacity is not None and capacity > 0:
        capacity_kb = f"{capacity // 1024}KB" if capacity >= 1024 else f"{capacity}B"
        return f"pipe: {pending_kb}/{capacity_kb}"
    return f"pipe: {pending_kb}"


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
    flash_message: str | None = None,
    theme: NlessTheme | None = None,
    spinner_frame: int = 0,
    format_str: str | None = None,
    keymap_name: str = "vim",
    theme_name: str = "default",
    time_window: str | None = None,
    delimiter: str | None = None,
    skipped_rows: int = 0,
    lag_rows: int = 0,
    throughput: float = 0.0,
    pipe_pressure: tuple[int, int | None] | None = None,
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
        if loading_reason == "Searching":
            search_text = f"{search_prefix}: '{search_term.pattern}'"
        else:
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
        column_text = f"[bold]Unique[/bold]: {column_names} "

    time_window_text = ""
    if time_window:
        time_window_text = f"[bold]Window[/bold]: {time_window} "

    delimiter_text = ""
    if delimiter:
        delimiter_text = f"[bold]Delim[/bold]: {delimiter} "

    skipped_text = ""
    if skipped_rows > 0:
        skipped_text = f"[bold]Skipped[/bold]: {skipped_rows:,} "

    lag_text = ""
    throughput_text = ""
    pipe_text = ""
    if lag_rows >= 1000:
        lag_text = f"{lag_rows:,} rows behind"
    if throughput > 0 and lag_rows >= 1000:
        throughput_text = _format_rate(throughput)
    if pipe_pressure is not None:
        pending, capacity = pipe_pressure
        show_pipe = capacity is None or (capacity > 0 and pending / capacity > 0.5)
        if show_pipe and pending > 0:
            pipe_text = _format_pipe(pipe_pressure)

    if loading_reason:
        spinner = SPINNER_FRAMES[spinner_frame % len(SPINNER_FRAMES)]
        loading_color = theme.markup("status_loading", f"{spinner} {loading_reason}")
        loading_text = f"| [bold]{loading_color}[/bold] "
    elif flash_message:
        flash_color = theme.markup("status_loading", flash_message)
        loading_text = f"| [green]✔[/green] {flash_color} "
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
        "time_window": time_window_text,
        "delimiter": delimiter_text,
        "skipped": skipped_text,
        "tailing": tailing_text,
        "loading": loading_text,
        "lag": lag_text,
        "throughput": throughput_text,
        "pipe": pipe_text,
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
    # Normalize runs of whitespace so separator cleanup sees consistent input.
    result = re.sub(r"  +", " ", result)

    # Clean up artifacts from empty variables: collapse repeated separators
    # and strip leading/trailing separators with optional surrounding markup.
    # This handles both plain "|" and markup-wrapped separators like
    # "[#727072]|[/#727072]".
    _sep = r"(?:\[[\w#/]+\])?\|(?:\[[\w#/]+\])?"
    result = re.sub(
        rf"(\s*{_sep}\s*){{2,}}",
        lambda m: " " + re.search(_sep, m.group()).group() + " ",
        result,
    )
    result = re.sub(rf"^\s*{_sep}\s*", "", result)
    result = re.sub(rf"\s*{_sep}\s*$", "", result)
    return result.strip()
