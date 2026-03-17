"""Regex highlight management for NlessApp."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .nlessselect import NlessSelect
from .types import UpdateReason

if TYPE_CHECKING:
    from .app import NlessApp
    from .buffer import NlessBuffer

    from textual.widgets import Select


class HighlightMixin:
    """Mixin providing regex highlight add/navigate/recolor/delete for NlessApp."""

    HIGHLIGHT_COLORS = [
        "#ff5555",  # red
        "#ffb86c",  # orange
        "#f1fa8c",  # yellow
        "#50fa7b",  # green
        "#8be9fd",  # cyan
        "#bd93f9",  # purple
        "#ff79c6",  # pink
        "#6272a4",  # blue-grey
    ]

    COLOR_NAMES = [
        "red",
        "orange",
        "yellow",
        "green",
        "cyan",
        "purple",
        "pink",
        "blue-grey",
    ]

    _pending_highlight_pattern: re.Pattern | None = None
    _pending_recolor_index: int | None = None
    _pending_delete_index: int | None = None

    def action_add_highlight(self: NlessApp) -> None:
        """Pin the current search term as a persistent colored highlight.

        If no search is active, clear all existing highlights.
        Shows a color picker to let the user choose the highlight color.
        """
        buf = self._get_current_buffer()
        if buf.query.search_term is None:
            # No active search — prompt to clear all highlights
            if buf.regex_highlights:
                n = len(buf.regex_highlights)
                select = NlessSelect(
                    options=[("Yes", "yes"), ("No", "no")],
                    prompt=f"Clear all {n} highlight{'s' if n != 1 else ''}?",
                    classes="dock-bottom",
                    id="highlight_clear_confirm",
                )
                buf.mount(select)
            else:
                buf.notify("Search first with /, then press + to pin as highlight")
            return

        self._pending_highlight_pattern = buf.query.search_term
        options = [
            (f"[{color}]{name} ███[/{color}]", color)
            for name, color in zip(self.COLOR_NAMES, self.HIGHLIGHT_COLORS)
        ]
        select = NlessSelect(
            options=options,
            prompt="Pick a highlight color",
            classes="dock-bottom",
            id="highlight_color_select",
        )
        buf.mount(select)

    def _count_highlight_matches(
        self: NlessApp, buf: NlessBuffer, pattern: re.Pattern
    ) -> int:
        count = 0
        for row in buf.displayed_rows:
            for cell in row:
                if pattern.search(strip_markup(cell)):
                    count += 1
                    break  # count rows, not individual cell matches
        return count

    def action_navigate_highlight(self: NlessApp) -> None:
        """Select a pinned highlight to navigate between its matches."""
        buf = self._get_current_buffer()
        if not buf.regex_highlights:
            buf.notify("No highlights pinned. Search with / then pin with +")
            return

        options = []
        for i, (pattern, color) in enumerate(buf.regex_highlights):
            if i > 0:
                options.append(("────", "separator"))
            count = self._count_highlight_matches(buf, pattern)
            label = f"[{color}]🔍 Navigate {pattern.pattern} ({count})[/{color}]"
            options.append((label, str(i)))
            options.append(
                (f"[{color}]🎨 Recolor {pattern.pattern}[/{color}]", f"recolor:{i}")
            )
            options.append(
                (f"[{color}]🗑  Delete {pattern.pattern}[/{color}]", f"delete:{i}")
            )
        select = NlessSelect(
            options=options,
            prompt="Select a highlight to navigate (n/p), recolor, or remove",
            classes="dock-bottom",
            id="highlight_navigate_select",
        )
        buf.mount(select)

    def _on_highlight_color_select(self: NlessApp, event: Select.Changed) -> None:
        color = str(event.value)
        pattern = self._pending_highlight_pattern
        self._pending_highlight_pattern = None
        if pattern is None:
            return
        buf = self._get_current_buffer()
        for existing_pattern, existing_color in buf.regex_highlights:
            if existing_pattern.pattern == pattern.pattern:
                buf.notify(
                    f"[{existing_color}]{pattern.pattern}[/{existing_color}] is already highlighted — use - to recolor"
                )
                return
        buf.regex_highlights.append((pattern, color))
        pattern_str = pattern.pattern
        buf._perform_search("")  # clear the search
        buf.notify(
            f"Pinned [{color}]{pattern_str}[/{color}] as highlight {len(buf.regex_highlights)}"
        )

    def _on_highlight_navigate_select(self: NlessApp, event: Select.Changed) -> None:
        value = str(event.value)
        buf = self._get_current_buffer()
        if value.startswith("delete:"):
            idx = int(value.removeprefix("delete:"))
            if idx < len(buf.regex_highlights):
                self._pending_delete_index = idx
                pattern, color = buf.regex_highlights[idx]
                select = NlessSelect(
                    options=[("Yes", "yes"), ("No", "no")],
                    prompt=f"Remove [{color}]{pattern.pattern}[/{color}] highlight?",
                    classes="dock-bottom",
                    id="highlight_delete_confirm",
                )
                buf.mount(select)
        elif value.startswith("recolor:"):
            idx = int(value.removeprefix("recolor:"))
            if idx < len(buf.regex_highlights):
                self._pending_recolor_index = idx
                options = [
                    (f"[{color}]{name} ███[/{color}]", color)
                    for name, color in zip(self.COLOR_NAMES, self.HIGHLIGHT_COLORS)
                ]
                select = NlessSelect(
                    options=options,
                    prompt="Pick a new color",
                    classes="dock-bottom",
                    id="highlight_recolor_select",
                )
                buf.mount(select)
        else:
            idx = int(value)
            if idx < len(buf.regex_highlights):
                pattern, color = buf.regex_highlights[idx]
                buf._perform_search(pattern.pattern)
                buf.notify(
                    f"Navigating [{color}]{pattern.pattern}[/{color}] — n/p to jump"
                )

    def _on_highlight_recolor_select(self: NlessApp, event: Select.Changed) -> None:
        color = str(event.value)
        idx = self._pending_recolor_index
        self._pending_recolor_index = None
        buf = self._get_current_buffer()
        if idx is not None and idx < len(buf.regex_highlights):
            pattern, _old_color = buf.regex_highlights[idx]
            buf.regex_highlights[idx] = (pattern, color)
            buf._deferred_update_table(reason=UpdateReason.HIGHLIGHT)
            buf.notify(f"Recolored [{color}]{pattern.pattern}[/{color}]")

    def _on_highlight_delete_confirm(self: NlessApp, event: Select.Changed) -> None:
        buf = self._get_current_buffer()
        idx = self._pending_delete_index
        self._pending_delete_index = None
        if (
            str(event.value) == "yes"
            and idx is not None
            and idx < len(buf.regex_highlights)
        ):
            pattern, color = buf.regex_highlights.pop(idx)
            buf._deferred_update_table(reason=UpdateReason.HIGHLIGHT)
            buf.notify(f"Removed [{color}]{pattern.pattern}[/{color}] highlight")

    def _on_highlight_clear_confirm(self: NlessApp, event: Select.Changed) -> None:
        if str(event.value) == "yes":
            buf = self._get_current_buffer()
            buf.regex_highlights.clear()
            buf._deferred_update_table(reason=UpdateReason.HIGHLIGHT)
            buf.notify("Cleared all highlights")
