"""Session save/load — persist buffer view state across restarts."""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .buffer import NlessBuffer

SESSIONS_DIR = "~/.config/nless/sessions/"
VIEWS_DIR = "~/.config/nless/views/"


# ── Data model ────────────────────────────────────────────────────────


@dataclass
class SessionColumn:
    name: str
    render_position: int
    hidden: bool
    pinned: bool
    substitution_pattern: str | None = None
    substitution_replacement: str | None = None


@dataclass
class SessionComputedColumn:
    name: str
    col_ref: str  # source column name
    col_ref_index: int  # index within split result (-1 for JSON)
    json_ref: str  # JSON path (empty string if not JSON)
    delimiter: str | None = None  # string delimiter
    delimiter_regex: str | None = None  # regex pattern string
    delimiter_regex_flags: int = 0  # re flags for regex delimiter


@dataclass
class SessionFilter:
    column: str | None
    pattern: str  # regex pattern string
    exclude: bool
    flags: int = 0  # re flags (e.g. re.IGNORECASE)


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
    computed_columns: list[SessionComputedColumn] = field(default_factory=list)
    tab_name: str = ""  # display name in the tab bar
    search_term: str | None = None
    cursor_row: int = 0
    cursor_column: int = 0
    source_labels: list[str] = field(default_factory=list)


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


@dataclass
class View:
    name: str
    state: SessionBufferState
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at


# ── Capture / Apply ──────────────────────────────────────────────────


def capture_buffer_state(buf: NlessBuffer) -> SessionBufferState:
    """Snapshot the user-configured view state of a buffer."""
    # Delimiter
    delimiter_str = None
    delimiter_regex = None
    if isinstance(buf.delim.value, re.Pattern):
        delimiter_regex = buf.delim.value.pattern
    elif isinstance(buf.delim.value, str):
        delimiter_str = buf.delim.value

    # Filters
    filters = []
    for f in buf.query.filters:
        filters.append(
            SessionFilter(
                column=f.column,
                pattern=f.pattern.pattern,
                exclude=f.exclude,
                flags=f.pattern.flags,
            )
        )

    # Columns
    columns = []
    computed_columns = []
    for col in buf.current_columns:
        sub_pat = col.substitution[0].pattern if col.substitution else None
        sub_repl = col.substitution[1] if col.substitution else None
        columns.append(
            SessionColumn(
                name=col.name,
                render_position=col.render_position,
                hidden=col.hidden,
                pinned=col.pinned,
                substitution_pattern=sub_pat,
                substitution_replacement=sub_repl,
            )
        )
        if col.computed and (col.col_ref or col.json_ref):
            cc_delim = None
            cc_delim_regex = None
            if isinstance(col.delimiter, re.Pattern):
                cc_delim_regex = col.delimiter.pattern
            elif isinstance(col.delimiter, str):
                cc_delim = col.delimiter
            cc_delim_regex_flags = (
                col.delimiter.flags if isinstance(col.delimiter, re.Pattern) else 0
            )
            computed_columns.append(
                SessionComputedColumn(
                    name=col.name,
                    col_ref=col.col_ref,
                    col_ref_index=col.col_ref_index,
                    json_ref=col.json_ref,
                    delimiter=cc_delim,
                    delimiter_regex=cc_delim_regex,
                    delimiter_regex_flags=cc_delim_regex_flags,
                )
            )

    # Highlights
    highlights = []
    for pattern, color in buf.regex_highlights:
        highlights.append(SessionHighlight(pattern=pattern.pattern, color=color))

    # Search term
    search_term = buf.query.search_term.pattern if buf.query.search_term else None

    # Cursor position from DataTable widget
    try:
        dt = buf.query_one(".nless-view")
        cursor_row, cursor_column = dt.cursor_row, dt.cursor_column
    except Exception:
        cursor_row = cursor_column = 0

    return SessionBufferState(
        delimiter=delimiter_str,
        delimiter_regex=delimiter_regex,
        delimiter_name=buf.delim.name,
        raw_mode=buf.raw_mode,
        sort_column=buf.query.sort_column,
        sort_reverse=buf.query.sort_reverse,
        filters=filters,
        columns=columns,
        unique_column_names=sorted(buf.query.unique_column_names),
        computed_columns=computed_columns,
        highlights=highlights,
        time_window=buf.time_window,
        rolling_time_window=buf.rolling_time_window,
        is_tailing=buf.is_tailing,
        search_term=search_term,
        cursor_row=cursor_row,
        cursor_column=cursor_column,
        source_labels=list(buf.stream.source_labels),
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
            isinstance(buf.delim.value, re.Pattern)
            and buf.delim.value.pattern == state.delimiter_regex
        )
    if state.delimiter:
        return buf.delim.value == state.delimiter
    return buf.delim.value is None


def _apply_session_computed_columns(
    buf: NlessBuffer, state: SessionBufferState, skipped: list[str]
) -> None:
    """Replay computed columns (column splits & JSON extractions)."""
    if not state.computed_columns:
        return
    from .types import Column, MetadataColumn

    existing_names = {c.name for c in buf.current_columns}
    arrival_col = next(
        (c for c in buf.current_columns if c.name == MetadataColumn.ARRIVAL.value),
        None,
    )
    base_position = (
        arrival_col.data_position if arrival_col else len(buf.current_columns)
    )
    added = 0
    for sc in state.computed_columns:
        if sc.name in existing_names:
            continue
        if sc.col_ref and sc.col_ref not in existing_names:
            skipped.append(
                f"computed column '{sc.name}' (source '{sc.col_ref}' not found)"
            )
            continue
        delim = (
            re.compile(sc.delimiter_regex, sc.delimiter_regex_flags)
            if sc.delimiter_regex
            else sc.delimiter
        )
        buf.current_columns.append(
            Column(
                name=sc.name,
                labels=set(),
                render_position=base_position + added,
                data_position=base_position + added,
                hidden=False,
                computed=True,
                delimiter=delim,
                col_ref=sc.col_ref,
                col_ref_index=sc.col_ref_index,
                json_ref=sc.json_ref,
            )
        )
        existing_names.add(sc.name)
        added += 1
    if arrival_col and added > 0:
        arrival_col.data_position = base_position + added
        arrival_col.render_position = (
            max(c.render_position for c in buf.current_columns) + 1
        )
    if buf.raw_mode:
        buf.raw_mode = False


def _apply_session_filters(
    buf: NlessBuffer, state: SessionBufferState, skipped: list[str]
) -> None:
    """Apply saved filter state."""
    from .types import Filter

    filters_to_apply = []
    for f in state.filters:
        if f.column:
            col_names = {c.name for c in buf.current_columns}
            if f.column not in col_names:
                skipped.append(f"filter on '{f.column}'")
        filters_to_apply.append(
            Filter(
                column=f.column,
                pattern=re.compile(f.pattern, f.flags),
                exclude=f.exclude,
            )
        )
    buf.query.filters = filters_to_apply


def _apply_session_unique_columns(
    buf: NlessBuffer, state: SessionBufferState, skipped: list[str]
) -> None:
    """Apply unique column settings and add count column if needed."""
    if state.unique_column_names:
        col_names = {c.name for c in buf.current_columns}
        missing_unique = [
            name for name in state.unique_column_names if name not in col_names
        ]
        for name in missing_unique:
            skipped.append(f"unique on '{name}'")
    buf.query.unique_column_names = set(state.unique_column_names)
    if buf.query.unique_column_names:
        from .dataprocessing import strip_markup
        from .types import Column, MetadataColumn

        for col in buf.current_columns:
            if strip_markup(col.name) in buf.query.unique_column_names:
                col.labels.add("U")
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


def _apply_session_sort(
    buf: NlessBuffer, state: SessionBufferState, skipped: list[str]
) -> None:
    """Apply sort state after unique columns exist."""
    if state.sort_column:
        col_names = {c.name for c in buf.current_columns}
        if state.sort_column in col_names:
            buf.query.sort_column = state.sort_column
            buf.query.sort_reverse = state.sort_reverse
        else:
            skipped.append(f"sort (column '{state.sort_column}' not found)")
    else:
        buf.query.sort_column = state.sort_column
        buf.query.sort_reverse = state.sort_reverse


def _apply_session_columns(
    buf: NlessBuffer, state: SessionBufferState, skipped: list[str]
) -> None:
    """Apply column visibility, pinning, and order."""
    col_settings = {c.name: c for c in state.columns}
    current_col_names = {c.name for c in buf.current_columns}
    unmatched_col_count = sum(
        1 for name in col_settings if name not in current_col_names
    )
    if unmatched_col_count:
        skipped.append(f"{unmatched_col_count} column setting(s)")
    for col in buf.current_columns:
        if col.name in col_settings:
            saved = col_settings[col.name]
            col.hidden = saved.hidden
            col.pinned = saved.pinned
            col.render_position = saved.render_position
            if saved.substitution_pattern is not None:
                col.substitution = (
                    re.compile(saved.substitution_pattern),
                    saved.substitution_replacement or "",
                )


def apply_buffer_state(buf: NlessBuffer, state: SessionBufferState) -> list[str]:
    """Apply saved session state to a buffer.

    Returns a list of human-readable descriptions of settings that were
    skipped (e.g. because the referenced column doesn't exist in the
    current data).
    """
    skipped: list[str] = []

    # Switch delimiter if it differs — this re-parses all rows
    if not _delimiters_match(buf, state):
        delimiter_input = _get_delimiter_input(state)
        if not delimiter_input and state.raw_mode:
            delimiter_input = "raw"
        if delimiter_input:
            buf.switch_delimiter(delimiter_input)
        if state.delimiter_name:
            buf.delim.name = state.delimiter_name

    _apply_session_computed_columns(buf, state, skipped)
    _apply_session_filters(buf, state, skipped)
    _apply_session_unique_columns(buf, state, skipped)
    _apply_session_sort(buf, state, skipped)
    _apply_session_columns(buf, state, skipped)

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
            buf.query.search_term = re.compile(state.search_term, re.IGNORECASE)
        except re.error:
            pass

    # Source labels (merged buffers)
    if state.source_labels:
        buf.stream.set_source_labels(list(state.source_labels))

    # Cursor position — deferred until _deferred_update_table runs
    if state.cursor_row or state.cursor_column:
        buf._pending_cursor_position = (state.cursor_row, state.cursor_column)

    # Rebuild caches — callers are responsible for triggering re-render
    buf._rebuild_column_caches()

    return skipped


# ── Serialization ────────────────────────────────────────────────────


def _deserialize_buffer_state(b: dict) -> SessionBufferState:
    """Deserialize a dict into a SessionBufferState."""
    return SessionBufferState(
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
                flags=f.get("flags", 0),
            )
            for f in b.get("filters", [])
        ],
        columns=[
            SessionColumn(
                name=c["name"],
                render_position=c["render_position"],
                hidden=c["hidden"],
                pinned=c.get("pinned", False),
                substitution_pattern=c.get("substitution_pattern"),
                substitution_replacement=c.get("substitution_replacement"),
            )
            for c in b.get("columns", [])
        ],
        unique_column_names=b.get("unique_column_names", []),
        computed_columns=[
            SessionComputedColumn(**cc) for cc in b.get("computed_columns", [])
        ],
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
        source_labels=b.get("source_labels", []),
    )


def _serialize_session(session: Session) -> dict:
    return asdict(session)


def _deserialize_session(data: dict) -> Session:
    groups = []
    for g in data.get("groups", []):
        buffers = [_deserialize_buffer_state(b) for b in g.get("buffers", [])]
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


class JsonStore[T]:
    """Atomic JSON file store for named, timestamped items."""

    def __init__(
        self,
        get_directory: Callable[[], str],
        serialize: Callable[[T], dict],
        deserialize: Callable[[dict], T],
    ):
        self._get_directory = get_directory
        self._serialize = serialize
        self._deserialize = deserialize

    def load_all(self) -> list[T]:
        """Load all items from individual JSON files."""
        dir_path = os.path.expanduser(self._get_directory())
        if not os.path.isdir(dir_path):
            return []
        items = []
        import glob

        for filepath in sorted(glob.glob(os.path.join(dir_path, "*.json"))):
            try:
                with open(filepath) as f:
                    data = json.load(f)
                items.append(self._deserialize(data))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
        items.sort(key=lambda item: item.updated_at, reverse=True)
        return items

    def save(self, item: T) -> None:
        """Save an item to its own file (atomic write)."""
        item.updated_at = datetime.now(timezone.utc).isoformat()
        dir_path = os.path.expanduser(self._get_directory())
        os.makedirs(dir_path, exist_ok=True)
        filename = _sanitize_filename(item.name) + ".json"
        target = os.path.join(dir_path, filename)
        fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self._serialize(item), f, indent=2)
            os.replace(tmp_path, target)
        except BaseException:
            os.unlink(tmp_path)
            raise

    def delete(self, name: str) -> None:
        """Delete an item file by name."""
        dir_path = os.path.expanduser(self._get_directory())
        filename = _sanitize_filename(name) + ".json"
        target = os.path.join(dir_path, filename)
        if os.path.exists(target):
            os.remove(target)

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename an item (delete old file, save with new name)."""
        for item in self.load_all():
            if item.name == old_name:
                self.delete(old_name)
                item.name = new_name
                self.save(item)
                return

    def load_by_name(self, name: str) -> T | None:
        """Find an item by exact name."""
        for item in self.load_all():
            if item.name == name:
                return item
        return None


# ── Views ─────────────────────────────────────────────────────────────


def _serialize_view(view: View) -> dict:
    return asdict(view)


def _deserialize_view(data: dict) -> View:
    return View(
        name=data["name"],
        state=_deserialize_buffer_state(data["state"]),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", data.get("created_at", "")),
    )


def capture_view_state(buf: "NlessBuffer") -> SessionBufferState:
    """Capture buffer state for a view, zeroing out ephemeral fields."""
    state = capture_buffer_state(buf)
    return replace(state, cursor_row=0, cursor_column=0, tab_name="")


# ── Store instances & public API ─────────────────────────────────────

_session_store = JsonStore(
    lambda: SESSIONS_DIR, _serialize_session, _deserialize_session
)
_view_store = JsonStore(lambda: VIEWS_DIR, _serialize_view, _deserialize_view)

load_sessions = _session_store.load_all
save_session = _session_store.save
delete_session = _session_store.delete
rename_session = _session_store.rename
load_session_by_name = _session_store.load_by_name

load_views = _view_store.load_all
save_view = _view_store.save
delete_view = _view_store.delete
rename_view = _view_store.rename
load_view_by_name = _view_store.load_by_name


# ── Session-specific helpers ─────────────────────────────────────────


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
