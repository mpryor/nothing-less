"""Group management, switching, icons, streaming bar, and rename for NlessApp."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from textual.css.query import NoMatches
from textual.containers import Vertical
from textual.widgets import (
    Static,
    Tab,
    TabbedContent,
    TabPane,
)

from .buffer import NlessBuffer
from .buffergroup import BufferGroup
from .input import LineStream

if TYPE_CHECKING:
    from .app import NlessApp


class GroupMixin:
    """Mixin providing group management methods for NlessApp."""

    # ── Group CRUD ────────────────────────────────────────────────────

    async def add_group(
        self: NlessApp,
        name: str,
        first_buffer: NlessBuffer,
        stream: LineStream | None = None,
    ) -> None:
        group_id = self._next_group_id
        self._next_group_id += 1
        group = BufferGroup(
            group_id=group_id,
            name=name,
            buffers=[first_buffer],
            starting_stream=stream,
        )
        self.groups.append(group)

        # When going from 1 to 2 groups, rename the first buffer in the
        # original group to "original" so the user can distinguish them.
        if len(self.groups) == 2:
            self._rename_first_buffer(self.groups[0], "original")

        # Build DOM: Vertical > TabbedContent > TabPane > buffer
        container = Vertical(id=f"group_{group_id}", classes="buffer-group")
        tabbed_content = TabbedContent()
        tab_pane = TabPane(
            f"{self.nless_theme.markup('accent', '1')} {name}",
            id=f"buffer{first_buffer.pane_id}",
        )
        # Mount container before status_bar so it stays above it
        await self.mount(container, before=self.query_one("#status_bar"))
        container.display = False
        await container.mount(tabbed_content)
        await tabbed_content.add_pane(tab_pane)
        await tab_pane.mount(first_buffer)

        self._switch_to_group(len(self.groups) - 1)

    def _switch_to_group(self: NlessApp, index: int) -> None:
        if index < 0 or index >= len(self.groups):
            return
        # Hide current group container
        old_container = self.query_one(f"#group_{self._current_group.group_id}")
        old_container.display = False

        self.curr_group_idx = index

        # Show new group container
        new_container = self.query_one(f"#group_{self._current_group.group_id}")
        new_container.display = True

        # Focus active buffer in new group
        current_buffer = self._get_current_buffer()
        try:
            current_buffer.query_one(".nless-view").focus()
        except NoMatches:
            pass
        self.call_after_refresh(lambda: current_buffer._update_status_bar())
        self._update_group_bar()

    def _close_current_group(self: NlessApp) -> None:
        group = self._current_group
        # Unsubscribe all buffers
        for buf in group.buffers:
            if buf.line_stream:
                buf.line_stream.unsubscribe(buf)

        # Remove DOM container
        container = self.query_one(f"#group_{group.group_id}")
        container.remove()

        self.groups.pop(self.curr_group_idx)
        if self.curr_group_idx >= len(self.groups):
            self.curr_group_idx = len(self.groups) - 1

        # Show adjacent group
        new_container = self.query_one(f"#group_{self._current_group.group_id}")
        new_container.display = True

        current_buffer = self._get_current_buffer()
        try:
            current_buffer.query_one(".nless-view").focus()
        except NoMatches:
            pass
        self.call_after_refresh(lambda: current_buffer._update_status_bar())
        self._update_group_bar()

        # When back to 1 group, restore the first buffer name to the group name.
        if len(self.groups) == 1:
            self._rename_first_buffer(self.groups[0], self.groups[0].name)

    # ── Navigation ────────────────────────────────────────────────────

    def action_show_group_next(self: NlessApp) -> None:
        if len(self.groups) <= 1:
            return
        self._switch_to_group((self.curr_group_idx + 1) % len(self.groups))

    def action_show_group_previous(self: NlessApp) -> None:
        if len(self.groups) <= 1:
            return
        self._switch_to_group((self.curr_group_idx - 1) % len(self.groups))

    # ── Icons & tab labels ────────────────────────────────────────────

    # Unicode figure space — used as a blank icon placeholder during animation
    # so regex replacement doesn't accidentally match normal spaces.
    _ICON_BLANK = "\u2007"
    _ICON_CHARS = "⏵📄✓\u2007"

    @staticmethod
    def _get_group_source_icon(group: BufferGroup) -> str | None:
        """Return the source icon character (⏵ or 📄) from a group name."""
        for char in ("⏵", "📄"):
            if char in group.name:
                return char
        return None

    def _resolve_icon(self: NlessApp, group: BufferGroup, animate: bool = False) -> str:
        """Return the resolved icon character for a group's current state."""
        source = self._get_group_source_icon(group)
        if source is None:
            return ""
        if (
            source == "⏵"
            and group.starting_stream is not None
            and group.starting_stream.done
        ):
            return "✓"
        if source == "⏵" and animate and self._group_bar_frame % 2 != 0:
            return self._ICON_BLANK
        return source

    def _strip_tab_icon(self: NlessApp, name: str) -> str:
        """Remove any streaming/status icon prefix from a tab name."""
        for prefix in ("⏵ ", "📄 ", "✓ ", f"{self._ICON_BLANK} "):
            if name.startswith(prefix):
                return name[len(prefix) :]
        return name

    def _rename_first_buffer(self: NlessApp, group: BufferGroup, name: str) -> None:
        """Rename the first buffer tab in a group, prepending the group icon."""
        try:
            container = self.query_one(f"#group_{group.group_id}")
            tabbed_content = container.query_one(TabbedContent)
            first_tab = next(tabbed_content.query(Tab).results())
            name = self._strip_tab_icon(name)
            icon = self._resolve_icon(group)
            icon_prefix = f"{icon} " if icon else ""
            first_tab.update(
                f"{self.nless_theme.markup('accent', '1')} {icon_prefix}{name}"
            )
        except (NoMatches, StopIteration):
            pass

    def _refresh_first_buffer_icon(
        self: NlessApp, group: BufferGroup, animate: bool = False
    ) -> None:
        """Refresh the icon on the first buffer tab without changing the name."""
        try:
            container = self.query_one(f"#group_{group.group_id}")
            tabbed_content = container.query_one(TabbedContent)
            first_tab = next(tabbed_content.query(Tab).results())
            content = str(first_tab.content)
            # Strip Rich markup and index number to get the plain name
            name = re.sub(r"\[/?[^\]]*\]", "", content)
            name = re.sub(r"^\d+\s*", "", name)
            name = self._strip_tab_icon(name)
            icon = self._resolve_icon(group, animate=animate)
            icon_prefix = f"{icon} " if icon else ""
            first_tab.update(
                f"{self.nless_theme.markup('accent', '1')} {icon_prefix}{name}"
            )
        except (NoMatches, StopIteration):
            pass

    # ── Group bar (multi-group indicator) ─────────────────────────────

    _group_bar_frame: int = 0
    _group_bar_timer = None

    def _group_is_streaming(self: NlessApp, group: BufferGroup) -> bool:
        return group.starting_stream is not None and not group.starting_stream.done

    def _build_group_bar(self: NlessApp) -> str:
        t = self.nless_theme
        parts = []
        for i, group in enumerate(self.groups):
            name = group.name
            source = self._get_group_source_icon(group)
            if source is not None:
                icon = self._resolve_icon(group, animate=(i == self.curr_group_idx))
                name = name.replace(source, icon, 1)
            if i == self.curr_group_idx:
                parts.append(f"[bold {t.accent}]\\[{name}][/bold {t.accent}]")
            else:
                parts.append(f"[{t.muted}]{name}[/{t.muted}]")
        return " " + "   ".join(parts)

    def _tick_group_bar(self: NlessApp) -> None:
        self._group_bar_frame += 1
        has_active = any(self._group_is_streaming(g) for g in self.groups)
        if has_active and len(self.groups) > 1:
            bar = self.query_one("#group_bar", Static)
            bar.update(self._build_group_bar())
            # Also update the first buffer tab icon in the current group
            group = self._current_group
            if self._get_group_source_icon(group) is not None:
                self._refresh_first_buffer_icon(
                    group, animate=self._group_is_streaming(group)
                )
        else:
            self._stop_group_bar_timer()
            # Final update to show ✓ on completed streams
            if len(self.groups) > 1:
                bar = self.query_one("#group_bar", Static)
                bar.update(self._build_group_bar())
                for group in self.groups:
                    if self._get_group_source_icon(group) == "⏵":
                        self._refresh_first_buffer_icon(group)

    def _start_group_bar_timer(self: NlessApp) -> None:
        if self._group_bar_timer is None:
            self._group_bar_timer = self.set_interval(0.8, self._tick_group_bar)

    def _stop_group_bar_timer(self: NlessApp) -> None:
        if self._group_bar_timer is not None:
            self._group_bar_timer.stop()
            self._group_bar_timer = None

    def _update_group_bar(self: NlessApp) -> None:
        try:
            bar = self.query_one("#group_bar", Static)
        except NoMatches:
            return
        if len(self.groups) > 1:
            t = self.nless_theme
            bar.update(self._build_group_bar())
            bar.styles.border = ("round", t.muted)
            bar.styles.height = "auto"
            if any(self._group_is_streaming(g) for g in self.groups):
                self._start_group_bar_timer()
        else:
            bar.styles.height = 0
            bar.styles.border = None
            self._stop_group_bar_timer()

    def _handle_group_bar_click(self: NlessApp, click_x: int) -> None:
        """Switch to the group at the clicked x position in the group bar."""
        # Build plain-text ranges for each group name.
        # Format: " [name1]   name2   name3" (1 leading space, 3-space sep)
        x = 1  # leading space
        for i, group in enumerate(self.groups):
            name = group.name
            if i == self.curr_group_idx:
                label_len = len(name) + 2  # brackets: [name]
            else:
                label_len = len(name)
            if x <= click_x < x + label_len:
                if i != self.curr_group_idx:
                    self._switch_to_group(i)
                return
            x += label_len + 3  # 3-space separator

    # ── Rename ────────────────────────────────────────────────────────

    def action_rename_buffer(self: NlessApp) -> None:
        self._create_prompt("Enter new buffer name", "rename_buffer_input")

    def handle_rename_buffer_submitted(self: NlessApp, event) -> None:
        event.control.remove()
        name = event.value.strip()
        if name:
            tabbed_content = self._get_active_tabbed_content()
            idx = self.curr_buffer_idx
            tab_label = f"{self.nless_theme.markup('accent', str(idx + 1))} {name}"
            for i, tab in enumerate(tabbed_content.query(Tab).results()):
                if i == idx:
                    tab.update(tab_label)
                    break

    def action_rename_group(self: NlessApp) -> None:
        self._create_prompt("Enter new group name", "rename_group_input")

    def handle_rename_group_submitted(self: NlessApp, event) -> None:
        event.control.remove()
        name = event.value.strip()
        if name:
            self._current_group.name = name
            self._update_group_bar()
