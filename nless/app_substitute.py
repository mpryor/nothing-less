"""Regex find-and-replace substitution helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .types import UpdateReason

if TYPE_CHECKING:
    from .app import NlessApp


def _parse_substitution(
    text: str,
) -> tuple[re.Pattern, str, bool] | None:
    """Parse a vi-style substitution string.

    Format: s/pattern/replacement/[flags]
    The separator is the character after 's'.

    Returns (compiled_pattern, replacement, all_columns) or None on error.
    """
    if len(text) < 4 or text[0] != "s":
        return None

    sep = text[1]
    # Split on unescaped separator characters
    parts: list[str] = []
    current: list[str] = []
    i = 2
    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text) and text[i + 1] == sep:
            current.append(sep)
            i += 2
        elif text[i] == sep:
            parts.append("".join(current))
            current = []
            i += 1
        else:
            current.append(text[i])
            i += 1
    parts.append("".join(current))

    if len(parts) < 2:
        return None

    pattern_str = parts[0]
    replacement = parts[1]
    flags_str = parts[2] if len(parts) > 2 else ""

    if not pattern_str:
        return None

    all_columns = "g" in flags_str
    ignore_case = "i" in flags_str

    try:
        flags = re.IGNORECASE if ignore_case else 0
        compiled = re.compile(pattern_str, flags)
    except re.error:
        return None

    return compiled, replacement, all_columns


def apply_substitution(
    app: NlessApp,
    pat: re.Pattern,
    repl: str,
    all_columns: bool,
) -> None:
    """Create a new buffer with the substitution applied."""
    curr_buffer = app._get_current_buffer()

    if all_columns:
        target_columns = [
            c for c in curr_buffer.current_columns if not c.hidden and not c.computed
        ]
        buffer_name = "s:*"
    else:
        data_table = curr_buffer.query_one(".nless-view")
        col = curr_buffer._get_column_at_position(data_table.cursor_column)
        if not col:
            app.notify("No column selected", severity="error")
            return
        target_columns = [col]
        buffer_name = f"s/{strip_markup(col.name)}"

    target_names = {c.name for c in target_columns}

    def setup(new_buffer):
        for c in new_buffer.current_columns:
            if c.name in target_names:
                c.substitution = (pat, repl)
        new_buffer._rebuild_column_caches()

    app._copy_buffer_async(
        setup,
        buffer_name,
        reason=UpdateReason.SUBSTITUTION,
        done_reason="Substituted",
    )
