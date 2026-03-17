"""Session and view persistence for NlessApp."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import threading
from typing import TYPE_CHECKING

from textual.css.query import NoMatches
from textual.widgets import Select, Tab, TabbedContent

from .buffer import NlessBuffer
from .input import LineStream, ShellCommandLineStream, StdinLineStream
from .nlessselect import NlessSelect
from .session import (
    Session,
    SessionGroup,
    View,
    apply_buffer_state,
    capture_buffer_state,
    capture_view_state,
    delete_session,
    delete_view,
    load_sessions,
    load_views,
    rename_session,
    rename_view,
    save_session,
    save_view,
)
from .types import UpdateReason

if TYPE_CHECKING:
    from .app import NlessApp
    from .autocomplete import AutocompleteInput

logger = logging.getLogger(__name__)


class SessionViewMixin:
    """Mixin providing session/view save, load, and restore for NlessApp."""

    _pending_session_delete_idx: int | None = None
    _pending_session_rename_idx: int | None = None
    _pending_auto_session: Session | None = None
    _active_session_name: str | None = None
    _pending_view_rename_idx: int | None = None
    _pending_view_delete_idx: int | None = None

    def _on_session_select(self: NlessApp, event: Select.Changed) -> None:
        value = str(event.value)
        if value == "quick_save" and self._active_session_name:
            session = self._capture_session(self._active_session_name)
            save_session(session)
            self.notify(f"Saved session: {self._active_session_name}")
        elif value == "save":
            self._create_prompt("Session name", "session_name_input")
        elif value.startswith("load:"):
            idx = int(value.removeprefix("load:"))
            sessions = load_sessions()
            if idx < len(sessions):
                self.run_worker(self._load_session(sessions[idx]))
                self.notify(f"Loaded session: {sessions[idx].name}")
        elif value.startswith("rename:"):
            idx = int(value.removeprefix("rename:"))
            sessions = load_sessions()
            if idx < len(sessions):
                self._pending_session_rename_idx = idx
                self._create_prompt("New session name", "session_rename_input")
        elif value.startswith("delete:"):
            idx = int(value.removeprefix("delete:"))
            sessions = load_sessions()
            if idx < len(sessions):
                self._pending_session_delete_idx = idx
                session = sessions[idx]
                buf = self._get_current_buffer()
                select = NlessSelect(
                    options=[("Yes", "yes"), ("No", "no")],
                    prompt=f"Delete session '{session.name}'?",
                    classes="dock-bottom",
                    id="session_delete_confirm",
                )
                buf.mount(select)

    def _on_session_delete_confirm(self: NlessApp, event: Select.Changed) -> None:
        idx = self._pending_session_delete_idx
        self._pending_session_delete_idx = None
        if str(event.value) == "yes" and idx is not None:
            sessions = load_sessions()
            if idx < len(sessions):
                name = sessions[idx].name
                delete_session(name)
                if self._active_session_name == name:
                    self._active_session_name = None
                self.notify(f"Deleted session: {name}")

    def _on_session_load_prompt(self: NlessApp, event: Select.Changed) -> None:
        if str(event.value) == "yes":
            session = self._pending_auto_session
            self._pending_auto_session = None
            if session:
                self.run_worker(self._load_session(session))
                self.notify(f"Loaded session: {session.name}")
        else:
            self._pending_auto_session = None

    def _on_view_select(self: NlessApp, event: Select.Changed) -> None:
        value = str(event.value)
        if value == "save":
            self._create_prompt("View name", "view_name_input")
        elif value == "undo":
            buf = self._get_current_buffer()
            if buf._pre_view_state is not None:
                # Restore raw data that was compacted by the view's filters
                if buf._pre_view_raw_rows is not None:
                    pre_view_rows = buf._pre_view_raw_rows
                    pre_view_ts = buf._pre_view_timestamps or []
                    buf.stream.replace_raw_rows(pre_view_rows, pre_view_ts)
                    buf._pre_view_raw_rows = None
                    buf._pre_view_timestamps = None
                buf.cache.parsed_rows = None
                apply_buffer_state(buf, buf._pre_view_state)
                buf._pre_view_state = None
                buf._deferred_update_table(reason=UpdateReason.VIEW_UNDONE)
                self.notify("Restored previous state")
        elif value.startswith("load:"):
            idx = int(value.removeprefix("load:"))
            views = load_views()
            if idx < len(views):
                buf = self._get_current_buffer()
                buf._pre_view_state = capture_buffer_state(buf)
                buf._pre_view_raw_rows = list(buf.raw_rows)
                buf._pre_view_timestamps = list(buf._arrival_timestamps)
                skipped = apply_buffer_state(buf, views[idx].state)
                buf._deferred_update_table(reason=UpdateReason.VIEW_LOADED)
                msg = f"Loaded view: {views[idx].name}"
                if skipped:
                    msg += f" ({len(skipped)} skipped: {', '.join(skipped)})"
                self.notify(msg)
        elif value.startswith("rename:"):
            idx = int(value.removeprefix("rename:"))
            views = load_views()
            if idx < len(views):
                self._pending_view_rename_idx = idx
                self._create_prompt("New view name", "view_rename_input")
        elif value.startswith("delete:"):
            idx = int(value.removeprefix("delete:"))
            views = load_views()
            if idx < len(views):
                self._pending_view_delete_idx = idx
                view = views[idx]
                buf = self._get_current_buffer()
                select = NlessSelect(
                    options=[("Yes", "yes"), ("No", "no")],
                    prompt=f"Delete view '{view.name}'?",
                    classes="dock-bottom",
                    id="view_delete_confirm",
                )
                buf.mount(select)

    def _on_view_delete_confirm(self: NlessApp, event: Select.Changed) -> None:
        idx = self._pending_view_delete_idx
        self._pending_view_delete_idx = None
        if str(event.value) == "yes" and idx is not None:
            views = load_views()
            if idx < len(views):
                name = views[idx].name
                delete_view(name)
                self.notify(f"Deleted view: {name}")

    def _handle_session_save_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        name = event.value.strip()
        if not name:
            return
        session = self._capture_session(name)
        save_session(session)
        self._active_session_name = name
        self.notify(f"Saved session: {name}")

    def _handle_session_rename_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        new_name = event.value.strip()
        idx = self._pending_session_rename_idx
        self._pending_session_rename_idx = None
        if not new_name or idx is None:
            return
        sessions = load_sessions()
        if idx < len(sessions):
            old_name = sessions[idx].name
            rename_session(old_name, new_name)
            if self._active_session_name == old_name:
                self._active_session_name = new_name
            self.notify(f"Renamed session: {old_name} → {new_name}")

    def _handle_view_save_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        name = event.value.strip()
        if not name:
            return
        buf = self._get_current_buffer()
        state = capture_view_state(buf)
        view = View(name=name, state=state)
        save_view(view)
        self.notify(f"Saved view: {name}")

    def _handle_view_rename_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        event.control.remove()
        new_name = event.value.strip()
        idx = self._pending_view_rename_idx
        self._pending_view_rename_idx = None
        if not new_name or idx is None:
            return
        views = load_views()
        if idx < len(views):
            old_name = views[idx].name
            rename_view(old_name, new_name)
            self.notify(f"Renamed view: {old_name} → {new_name}")

    def _get_data_source(self: NlessApp) -> str | None:
        """Return the data source identifier for the current group."""
        group = self._current_group
        name = group.name
        # Strip icon prefixes
        for prefix in ("📄 ", "⏵ ", "✓ "):
            if name.startswith(prefix):
                return name[len(prefix) :]
        if name == "stdin":
            return None
        return name

    def action_session_menu(self: NlessApp) -> None:
        """Open the session menu to save, load, or delete sessions."""
        buf = self._get_current_buffer()
        sessions = load_sessions()
        if self._active_session_name:
            options = [
                (f"💾 Save '{self._active_session_name}'", "quick_save"),
                ("💾 Save as new session…", "save"),
            ]
        else:
            options = [("💾 Save current session…", "save")]
        if sessions:
            options.append(("────", "separator"))
            for i, session in enumerate(sessions):
                if i > 0:
                    options.append(("────", "separator"))
                sources = ", ".join(session.data_sources) or "global"
                n_groups = len(session.groups)
                groups_label = f"{n_groups} group{'s' if n_groups != 1 else ''}"
                label = f"📂 Load {session.name}  [{self.nless_theme.muted}]({sources} · {groups_label})[/{self.nless_theme.muted}]"
                options.append((label, f"load:{i}"))
                options.append((f"✏️  Rename {session.name}", f"rename:{i}"))
                options.append((f"🗑  Delete {session.name}", f"delete:{i}"))
        select = NlessSelect(
            options=options,
            prompt="Sessions — save or load a session",
            classes="dock-bottom",
            id="session_select",
        )
        buf.mount(select)

    def action_view_menu(self: NlessApp) -> None:
        """Open the view menu to save, load, rename, or delete views."""
        buf = self._get_current_buffer()
        views = load_views()
        options: list[tuple[str, str]] = [("💾 Save current view…", "save")]
        if buf._pre_view_state is not None:
            options.append(("↩️  Undo last view", "undo"))
        if views:
            options.append(("────", "separator"))
            for i, view in enumerate(views):
                if i > 0:
                    options.append(("────", "separator"))
                options.append((f"📌 Load {view.name}", f"load:{i}"))
                options.append((f"✏️  Rename {view.name}", f"rename:{i}"))
                options.append((f"🗑  Delete {view.name}", f"delete:{i}"))
        select = NlessSelect(
            options=options,
            prompt="Views — save or load a view",
            classes="dock-bottom",
            id="view_select",
        )
        buf.mount(select)

    def _get_tab_names(self: NlessApp, group) -> list[str]:
        """Read tab label text for each buffer in a group."""
        try:
            container = self.query_one(f"#group_{group.group_id}")
            tabbed_content = container.query_one(TabbedContent)
            names = []
            for tab in tabbed_content.query(Tab).results():
                content = str(tab.content)
                # Strip Rich markup and index number prefix
                plain = re.sub(r"\[/?[^\]]*\]", "", content)
                plain = re.sub(r"^\d+\s*", "", plain).strip()
                names.append(plain)
            return names
        except NoMatches:
            return []

    def _capture_session(self: NlessApp, name: str) -> Session:
        """Capture the full workspace state as a session."""
        groups = []
        for group in self.groups:
            tab_names = self._get_tab_names(group)
            buf_states = []
            for i, buf in enumerate(group.buffers):
                state = capture_buffer_state(buf)
                state.tab_name = tab_names[i] if i < len(tab_names) else ""
                buf_states.append(state)
            # Derive data source — prefer full resolved path from stream
            data_source = None
            if group.starting_stream and hasattr(group.starting_stream, "_cli_args"):
                fn = (
                    group.starting_stream._cli_args.filename
                    if group.starting_stream._cli_args
                    else None
                )
                if fn:
                    data_source = os.path.abspath(fn)
            if data_source is None and hasattr(group.starting_stream, "_command"):
                data_source = f"⏵ {group.starting_stream._command}"
            if data_source is None:
                for prefix in ("📄 ", "⏵ ", "✓ "):
                    if group.name.startswith(prefix):
                        data_source = group.name[len(prefix) :]
                        break
            groups.append(
                SessionGroup(
                    name=group.name,
                    data_source=data_source,
                    buffers=buf_states,
                    active_buffer_idx=group.curr_buffer_idx,
                )
            )
        return Session(
            name=name,
            groups=groups,
            active_group_idx=self.curr_group_idx,
        )

    @staticmethod
    async def _wait_for_parse(buf: NlessBuffer, timeout_s: float = 10.0) -> None:
        """Wait until a buffer has parsed its first row, with timeout."""
        interval = 0.05
        elapsed = 0.0
        while not buf.first_row_parsed and elapsed < timeout_s:
            await asyncio.sleep(interval)
            elapsed += interval

    def _reset_buffer_for_reload(self: NlessApp, buf: NlessBuffer) -> None:
        """Reset a buffer's state so add_logs re-runs the first-parse path."""
        buf.first_row_parsed = False
        buf.stream.clear()
        buf.displayed_rows.clear()
        buf.delim.value = None
        buf.delim.inferred = False
        buf.raw_mode = False
        buf.current_columns = []
        buf.query.filters = []
        buf.query.search_term = None
        buf.query.sort_column = None
        buf.query.sort_reverse = False
        buf.query.unique_column_names = set()
        buf.regex_highlights = []
        buf.delim.preamble_lines = []
        try:
            buf.query_one(".nless-view").clear(columns=True)
        except Exception:
            logger.debug("Failed to clear view during buffer reset", exc_info=True)

    async def _create_additional_buffers(
        self: NlessApp,
        parent_buf: NlessBuffer,
        buf_states: list,
        stream: LineStream | None,
        wait_for_data: bool = False,
    ) -> int:
        """Create additional buffers (tabs) from saved session state.

        Returns the saved active_buffer_idx for the group.
        """
        if wait_for_data:
            await self._wait_for_parse(parent_buf)
        for i, buf_state in buf_states:
            tab_name = buf_state.tab_name or f"buffer {i + 1}"
            if "unparsed" in (buf_state.tab_name or ""):
                new_buffer = self._recreate_unparsed_buffer(
                    parent_buf, stream, buf_state
                )
                if new_buffer is None:
                    continue
            else:
                new_buffer = parent_buf.copy(pane_id=self._get_new_pane_id())
                new_buffer._pending_session_state = buf_state
            self.add_buffer(
                new_buffer,
                name=tab_name,
                add_prev_index=False,
                reason=UpdateReason.SESSION,
                activate=False,
            )

    async def _load_session(self: NlessApp, session: Session) -> None:
        """Restore a full session — apply buffer states to the current group,
        creating additional buffers as needed."""
        if not session.groups:
            return

        # Close all groups beyond the first — session will recreate them.
        while len(self.groups) > 1:
            self.curr_group_idx = len(self.groups) - 1
            self._close_current_group()

        # Close extra buffers (tabs) in the first group, keeping only the first.
        self.curr_group_idx = 0
        first_group = self.groups[0]
        while len(first_group.buffers) > 1:
            extra_buf = first_group.buffers[-1]
            if extra_buf.line_stream:
                extra_buf.line_stream.unsubscribe(extra_buf)
            try:
                tc = self._get_active_tabbed_content()
                tc.remove_pane(f"buffer{extra_buf.pane_id}")
            except Exception:
                logger.debug(
                    "Failed to remove tab pane during session load", exc_info=True
                )
            first_group.buffers.pop()
        first_group.curr_buffer_idx = 0

        first_group_state = session.groups[0]
        base_buf = first_group.get_current_buffer()

        # Check if the first group needs a different data source
        first_ds = first_group_state.data_source
        current_source = None
        if (
            self.groups[0].starting_stream
            and hasattr(self.groups[0].starting_stream, "_cli_args")
            and self.groups[0].starting_stream._cli_args
        ):
            current_source = self.groups[0].starting_stream._cli_args.filename
            if current_source:
                current_source = os.path.abspath(current_source)

        needs_file_load = (
            first_ds
            and not first_ds.startswith("⏵")
            and os.path.exists(first_ds)
            and current_source != first_ds
        )

        if needs_file_load:
            await self._reload_file_source(
                base_buf, first_ds, first_group_state, first_group
            )
        else:
            if first_group_state.buffers:
                first_state = first_group_state.buffers[0]
                apply_buffer_state(base_buf, first_state)
                base_buf._deferred_update_table(reason=UpdateReason.SESSION)
                if first_state.tab_name:
                    self._rename_first_buffer(self.groups[0], first_state.tab_name)

        # Create additional buffers within the first group
        active_idx = first_group_state.active_buffer_idx
        remaining_first = list(enumerate(first_group_state.buffers[1:], start=1))
        if remaining_first:
            await self._create_additional_buffers(
                base_buf,
                remaining_first,
                self.groups[0].starting_stream,
                wait_for_data=needs_file_load,
            )

        # Restore additional groups
        if len(session.groups) > 1:
            skipped = await self._restore_additional_groups(session.groups[1:])
            target_idx = min(session.active_group_idx, len(self.groups) - 1)
            if target_idx != self.curr_group_idx:
                self._switch_to_group(target_idx)
            if skipped:
                self.notify(
                    f"Skipped {len(skipped)} group(s): {', '.join(skipped)}",
                    timeout=5,
                )

        # Set curr_buffer_idx for first group
        if first_group_state.buffers:
            self.groups[0].curr_buffer_idx = min(
                active_idx, len(self.groups[0].buffers) - 1
            )

        self._active_session_name = session.name

    async def _reload_file_source(
        self: NlessApp, base_buf, data_source, group_state, first_group
    ) -> None:
        """Reload a file into the base buffer for session restoration."""
        from .types import CliArgs as CliArgsType

        new_cli = CliArgsType(
            delimiter=None,
            filters=[],
            unique_keys=set(),
            sort_by=None,
            filename=data_source,
        )
        try:
            stream = StdinLineStream(new_cli, data_source, None)
            if base_buf.line_stream:
                base_buf.line_stream.unsubscribe(base_buf)
            base_buf.line_stream = stream
            self._reset_buffer_for_reload(base_buf)
            if group_state.buffers:
                base_buf._pending_session_state = group_state.buffers[0]
            stream.subscribe(base_buf, base_buf.add_logs, lambda: base_buf.mounted)
            self.groups[0].starting_stream = stream

            t = threading.Thread(target=stream.run, daemon=True)
            t.start()
            group_name = group_state.name or f"📄 {os.path.basename(data_source)}"
            first_group.name = group_name
            if group_state.buffers and group_state.buffers[0].tab_name:
                self._rename_first_buffer(first_group, group_state.buffers[0].tab_name)
        except (FileNotFoundError, IsADirectoryError, PermissionError):
            logger.debug("Failed to reload file source %s", data_source, exc_info=True)

    async def _restore_additional_groups(self: NlessApp, group_states) -> list[str]:
        """Restore groups beyond the first from saved session state."""
        skipped = []
        for group_state in group_states:
            ds = group_state.data_source
            if ds and ds.startswith("⏵"):
                await self._restore_command_group(group_state, ds, skipped)
                continue
            if not ds:
                skipped.append(f"{group_state.name} (no source)")
                continue
            if not os.path.exists(ds):
                skipped.append(f"{ds} (missing)")
                continue
            await self._restore_file_group(group_state, ds, skipped)

        return skipped

    async def _restore_command_group(self: NlessApp, group_state, ds, skipped) -> None:
        """Re-execute a shell command group from session state."""
        command = ds[len("⏵ ") :]
        try:
            line_stream = ShellCommandLineStream(command)
            new_buf = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=self.cli_args,
                line_stream=line_stream,
            )
            if group_state.buffers:
                new_buf._pending_session_state = group_state.buffers[0]
            group_name = group_state.name or f"⏵ {command}"
            await self.add_group(group_name, new_buf, stream=line_stream)
            line_stream.start()
            remaining = list(enumerate(group_state.buffers[1:], start=1))
            if remaining:
                await self._create_additional_buffers(
                    new_buf, remaining, line_stream, wait_for_data=True
                )
            group = self.groups[-1]
            group.curr_buffer_idx = min(
                group_state.active_buffer_idx, len(group.buffers) - 1
            )
        except (OSError, ValueError, subprocess.SubprocessError) as e:
            skipped.append(f"{ds} (error: {e})")

    async def _restore_file_group(self: NlessApp, group_state, ds, skipped) -> None:
        """Open a file as a new group from session state."""
        from .types import CliArgs as CliArgsType

        new_cli = CliArgsType(
            delimiter=None,
            filters=[],
            unique_keys=set(),
            sort_by=None,
            filename=ds,
        )
        try:
            stream = StdinLineStream(new_cli, ds, None)
        except (FileNotFoundError, IsADirectoryError, PermissionError):
            skipped.append(f"{ds} (error)")
            return
        new_buf = NlessBuffer(
            pane_id=self._get_new_pane_id(),
            cli_args=new_cli,
            line_stream=stream,
        )
        if group_state.buffers:
            new_buf._pending_session_state = group_state.buffers[0]

        t = threading.Thread(target=stream.run, daemon=True)
        t.start()
        group_name = group_state.name or f"📄 {os.path.basename(ds)}"
        await self.add_group(group_name, new_buf, stream)
        remaining = list(enumerate(group_state.buffers[1:], start=1))
        if remaining:
            await self._create_additional_buffers(
                new_buf, remaining, stream, wait_for_data=True
            )
        group = self.groups[-1]
        group.curr_buffer_idx = min(
            group_state.active_buffer_idx, len(group.buffers) - 1
        )

    def _show_session_load_prompt(self: NlessApp, session: Session) -> None:
        """Show the auto-apply session prompt (called after DOM is ready)."""
        buf = self._get_current_buffer()
        select = NlessSelect(
            options=[("Yes", "yes"), ("No", "no")],
            prompt=f"Session '{session.name}' found for this file. Load it?",
            classes="dock-bottom",
            id="session_load_prompt",
        )
        buf.mount(select)
