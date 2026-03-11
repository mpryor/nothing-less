"""Session save/load — persist buffer view state across restarts."""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .buffer import NlessBuffer

SESSIONS_DIR = "~/.config/nless/sessions/"


# ── Data model ────────────────────────────────────────────────────────


@dataclass
class SessionColumn:
    name: str
    render_position: int
    hidden: bool
    pinned: bool


@dataclass
class SessionFilter:
    column: str | None
    pattern: str  # regex pattern string
    exclude: bool


@dataclass
class SessionHighlight:
    pattern: str  # regex pattern string
    color: str


@dataclass
class SessionBufferState:
    delimiter: str | None  # string delimiter, or None
    delimiter_regex: str | None  # regex pattern string, or None
    delimiter_name: str | None
    raw_mode: bool
    sort_column: str | None
    sort_reverse: bool
    filters: list[SessionFilter]
    columns: list[SessionColumn]
    unique_column_names: list[str]
    highlights: list[SessionHighlight]
    time_window: float | None
    rolling_time_window: bool
    is_tailing: bool
    tab_name: str = ""  # display name in the tab bar
    search_term: str | None = None
    cursor_row: int = 0
    cursor_column: int = 0


@dataclass
class SessionGroup:
    name: str
    data_source: str | None  # filename or command, for matching
    buffers: list[SessionBufferState]
    active_buffer_idx: int = 0


@dataclass
class Session:
    name: str
    groups: list[SessionGroup]
    active_group_idx: int = 0
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at

    @property
    def data_sources(self) -> list[str]:
        """Return non-None data sources across all groups."""
        return [g.data_source for g in self.groups if g.data_source]


# ── Capture / Apply ──────────────────────────────────────────────────


def capture_buffer_state(buf: NlessBuffer) -> SessionBufferState:
    """Snapshot the user-configured view state of a buffer."""
    # Delimiter
    delimiter_str = None
    delimiter_regex = None
    if isinstance(buf.delimiter, re.Pattern):
        delimiter_regex = buf.delimiter.pattern
    elif isinstance(buf.delimiter, str):
        delimiter_str = buf.delimiter

    # Filters
    filters = []
    for f in buf.current_filters:
        filters.append(
            SessionFilter(
                column=f.column,
                pattern=f.pattern.pattern,
                exclude=f.exclude,
            )
        )

    # Columns
    columns = []
    for col in buf.current_columns:
        columns.append(
            SessionColumn(
                name=col.name,
                render_position=col.render_position,
                hidden=col.hidden,
                pinned=col.pinned,
            )
        )

    # Highlights
    highlights = []
    for pattern, color in buf.regex_highlights:
        highlights.append(SessionHighlight(pattern=pattern.pattern, color=color))

    # Search term
    search_term = buf.search_term.pattern if buf.search_term else None

    # Cursor position from DataTable widget
    try:
        dt = buf.query_one(".nless-view")
        cursor_row, cursor_column = dt.cursor_row, dt.cursor_column
    except Exception:
        cursor_row = cursor_column = 0

    return SessionBufferState(
        delimiter=delimiter_str,
        delimiter_regex=delimiter_regex,
        delimiter_name=buf.delimiter_name,
        raw_mode=buf.raw_mode,
        sort_column=buf.sort_column,
        sort_reverse=buf.sort_reverse,
        filters=filters,
        columns=columns,
        unique_column_names=sorted(buf.unique_column_names),
        highlights=highlights,
        time_window=buf.time_window,
        rolling_time_window=buf.rolling_time_window,
        is_tailing=buf.is_tailing,
        search_term=search_term,
        cursor_row=cursor_row,
        cursor_column=cursor_column,
    )


def _get_delimiter_input(state: SessionBufferState) -> str | None:
    """Convert session delimiter state back to a switch_delimiter input string."""
    if state.delimiter_regex:
        return state.delimiter_regex
    if state.delimiter:
        return state.delimiter
    return None


def _delimiters_match(buf: NlessBuffer, state: SessionBufferState) -> bool:
    """Check if the buffer's current delimiter matches the session state."""
    if state.raw_mode and buf.raw_mode:
        return True
    if state.delimiter_regex:
        return (
            isinstance(buf.delimiter, re.Pattern)
            and buf.delimiter.pattern == state.delimiter_regex
        )
    if state.delimiter:
        return buf.delimiter == state.delimiter
    return buf.delimiter is None


def apply_buffer_state(buf: NlessBuffer, state: SessionBufferState) -> None:
    """Apply saved session state to a buffer."""
    from .types import Filter

    # Switch delimiter if it differs — this re-parses all rows
    if not _delimiters_match(buf, state):
        delimiter_input = _get_delimiter_input(state)
        if not delimiter_input and state.raw_mode:
            delimiter_input = "raw"
        if delimiter_input:
            buf.switch_delimiter(delimiter_input)
        if state.delimiter_name:
            buf.delimiter_name = state.delimiter_name

    # Sort
    buf.sort_column = state.sort_column
    buf.sort_reverse = state.sort_reverse

    # Filters
    buf.current_filters = [
        Filter(
            column=f.column,
            pattern=re.compile(f.pattern),
            exclude=f.exclude,
        )
        for f in state.filters
    ]

    # Unique columns — must come before column settings so the count
    # column exists when we apply saved render_position values.
    buf.unique_column_names = set(state.unique_column_names)
    if buf.unique_column_names:
        from .dataprocessing import strip_markup
        from .types import Column, MetadataColumn

        # Add "U" labels to the unique columns
        for col in buf.current_columns:
            if strip_markup(col.name) in buf.unique_column_names:
                col.labels.add("U")
        # Add count column if not already present (mirrors handle_mark_unique)
        if MetadataColumn.COUNT.value not in [c.name for c in buf.current_columns]:
            buf.current_columns = [
                replace(
                    c,
                    render_position=c.render_position + 1,
                    data_position=c.data_position + 1,
                )
                for c in buf.current_columns
            ]
            buf.current_columns.insert(
                0,
                Column(
                    name=MetadataColumn.COUNT.value,
                    labels=set(),
                    render_position=0,
                    data_position=0,
                    hidden=False,
                    pinned=True,
                ),
            )

    # Columns — apply visibility, pinned, and order by matching name
    col_settings = {c.name: c for c in state.columns}
    for col in buf.current_columns:
        if col.name in col_settings:
            saved = col_settings[col.name]
            col.hidden = saved.hidden
            col.pinned = saved.pinned
            col.render_position = saved.render_position

    # Highlights
    buf.regex_highlights = [(re.compile(h.pattern), h.color) for h in state.highlights]

    # Time window
    buf.time_window = state.time_window
    buf.rolling_time_window = state.rolling_time_window

    # Tailing
    buf.is_tailing = state.is_tailing

    # Search term
    if state.search_term:
        try:
            buf.search_term = re.compile(state.search_term, re.IGNORECASE)
        except re.error:
            pass

    # Cursor position — deferred until _deferred_update_table runs
    if state.cursor_row or state.cursor_column:
        buf._pending_cursor_position = (state.cursor_row, state.cursor_column)

    # Rebuild caches and re-render
    buf._rebuild_column_caches()
    buf._deferred_update_table(reason="Session loaded")


# ── Serialization ────────────────────────────────────────────────────


def _serialize_session(session: Session) -> dict:
    return asdict(session)


def _deserialize_session(data: dict) -> Session:
    groups = []
    for g in data.get("groups", []):
        buffers = []
        for b in g.get("buffers", []):
            buffers.append(
                SessionBufferState(
                    delimiter=b.get("delimiter"),
                    delimiter_regex=b.get("delimiter_regex"),
                    delimiter_name=b.get("delimiter_name"),
                    raw_mode=b.get("raw_mode", False),
                    sort_column=b.get("sort_column"),
                    sort_reverse=b.get("sort_reverse", False),
                    filters=[
                        SessionFilter(
                            column=f.get("column"),
                            pattern=f["pattern"],
                            exclude=f.get("exclude", False),
                        )
                        for f in b.get("filters", [])
                    ],
                    columns=[
                        SessionColumn(
                            name=c["name"],
                            render_position=c["render_position"],
                            hidden=c["hidden"],
                            pinned=c.get("pinned", False),
                        )
                        for c in b.get("columns", [])
                    ],
                    unique_column_names=b.get("unique_column_names", []),
                    highlights=[
                        SessionHighlight(pattern=h["pattern"], color=h["color"])
                        for h in b.get("highlights", [])
                    ],
                    time_window=b.get("time_window"),
                    rolling_time_window=b.get("rolling_time_window", False),
                    is_tailing=b.get("is_tailing", False),
                    tab_name=b.get("tab_name", ""),
                    search_term=b.get("search_term"),
                    cursor_row=b.get("cursor_row", 0),
                    cursor_column=b.get("cursor_column", 0),
                )
            )
        groups.append(
            SessionGroup(
                name=g["name"],
                data_source=g.get("data_source"),
                buffers=buffers,
                active_buffer_idx=g.get("active_buffer_idx", 0),
            )
        )
    return Session(
        name=data["name"],
        groups=groups,
        active_group_idx=data.get("active_group_idx", 0),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", data.get("created_at", "")),
    )


# ── File I/O ─────────────────────────────────────────────────────────


def _sanitize_filename(name: str) -> str:
    """Sanitize a session name for use as a filename stem."""
    return name.replace("/", "_").replace("\\", "_").replace("\0", "_")


def load_sessions() -> list[Session]:
    """Load all sessions from individual files in SESSIONS_DIR."""
    dir_path = os.path.expanduser(SESSIONS_DIR)
    if not os.path.isdir(dir_path):
        return []
    sessions = []
    import glob

    for filepath in sorted(glob.glob(os.path.join(dir_path, "*.json"))):
        try:
            with open(filepath) as f:
                data = json.load(f)
            sessions.append(_deserialize_session(data))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
    # Sort by most recently updated first
    sessions.sort(key=lambda s: s.updated_at, reverse=True)
    return sessions


def save_session(session: Session) -> None:
    """Save a single session to its own file in SESSIONS_DIR."""
    session.updated_at = datetime.now(timezone.utc).isoformat()
    dir_path = os.path.expanduser(SESSIONS_DIR)
    os.makedirs(dir_path, exist_ok=True)
    filename = _sanitize_filename(session.name) + ".json"
    target = os.path.join(dir_path, filename)
    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(_serialize_session(session), f, indent=2)
        os.replace(tmp_path, target)
    except BaseException:
        os.unlink(tmp_path)
        raise


def delete_session(name: str) -> None:
    """Delete a session file by name."""
    dir_path = os.path.expanduser(SESSIONS_DIR)
    filename = _sanitize_filename(name) + ".json"
    target = os.path.join(dir_path, filename)
    if os.path.exists(target):
        os.remove(target)


def rename_session(old_name: str, new_name: str) -> None:
    """Rename a session (delete old file, save with new name)."""
    for session in load_sessions():
        if session.name == old_name:
            delete_session(old_name)
            session.name = new_name
            save_session(session)
            return


def load_session_by_name(name: str) -> Session | None:
    """Find a session by exact name."""
    for session in load_sessions():
        if session.name == name:
            return session
    return None


def find_session_for_source(source: str) -> Session | None:
    """Find a saved session that contains a group matching the given data source."""
    for session in load_sessions():
        for group in session.groups:
            if group.data_source and _sources_match(group.data_source, source):
                return session
    return None


def _sources_match(saved: str, current: str) -> bool:
    """Check if a saved data source matches the current one.

    Matches on basename for file paths, exact match for commands.
    """
    # Exact match
    if saved == current:
        return True
    # Match on basename (e.g. "/home/user/access.log" matches "access.log")
    if os.path.basename(saved) == os.path.basename(current):
        return True
    return False
