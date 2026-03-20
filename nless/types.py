import re
import threading
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, ClassVar, Protocol, overload, runtime_checkable


class ListView(Sequence[str]):
    """Read-only view over a list. Prevents accidental mutation
    through properties while avoiding the cost of a copy."""

    __slots__ = ("_data",)

    def __init__(self, data: list) -> None:
        self._data = data

    @overload
    def __getitem__(self, index: int) -> str: ...
    @overload
    def __getitem__(self, index: slice) -> list[str]: ...
    def __getitem__(self, index):
        return self._data[index]

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, item: object) -> bool:
        return item in self._data

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def index(self, value: str, *args) -> int:
        return self._data.index(value, *args)

    def __add__(self, other):
        if isinstance(other, (list, ListView)):
            return list(self._data) + list(other)
        return NotImplemented

    def __radd__(self, other):
        if isinstance(other, list):
            return other + list(self._data)
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, list):
            return self._data == other
        if isinstance(other, ListView):
            return self._data == other._data
        return NotImplemented

    def __repr__(self) -> str:
        return f"ListView({self._data!r})"


if TYPE_CHECKING:
    from collections.abc import Callable


class ColumnType(Enum):
    AUTO = "auto"
    STRING = "string"
    NUMERIC = "numeric"
    DATETIME = "datetime"


class RowLengthMismatchError(Exception):
    pass


class MetadataColumn(Enum):
    COUNT = "count"
    ARRIVAL = "_arrival"
    SOURCE = "_source"


class UpdateReason(StrEnum):
    SORT = "Sorting"
    SEARCH = "Searching"
    THEME = "Applying theme"
    DEDUP = "Deduplicating"
    FILTER = "Filtering"
    PIVOT = "Pivoting"
    HIGHLIGHT = "Highlighting"
    DELIMITER = "Changing delimiter"
    LOADING = "Loading"
    SWITCHING_DELIMITER = "Switching delimiter"
    SESSION = "Session loaded"
    CLEARING_WINDOW = "Clearing time window"
    APPLYING_WINDOW = "Applying time window"
    MOVING_COLUMN = "Moving column"
    PINNING_COLUMN = "Pinning column"
    ADDING_COLUMN = "Adding column"
    SUBSTITUTION = "Substitution"
    SPLITTING_COLUMN = "Splitting column"
    TOGGLING_ARRIVAL = "Toggling arrival column"
    FILTERING_COLUMNS = "Filtering columns"
    VIEW_UNDONE = "View undone"
    VIEW_LOADED = "View loaded"
    ROLLING_TICK = ""


@runtime_checkable
class BufferProtocol(Protocol):
    """Interface that NlessBuffer mixins expect from self.

    Any method on a mixin that accesses self attributes or calls
    self methods must be representable here. This protocol makes
    the implicit mixin contract explicit and statically checkable.
    """

    _lock: threading.RLock
    _pending_action: tuple | None
    locked: bool
    stream: "StreamState"
    query: "FilterSortState"
    cache: "CacheState"
    chain: "ChainTimerState"
    delim: "DelimiterState"
    loading_state: "LoadingState"
    first_row_parsed: bool
    raw_mode: bool
    _initial_load_done: bool
    _last_flushed_idx: int
    raw_rows: list[str]
    displayed_rows: list[list[str]]
    _arrival_timestamps: list[float]
    _source_labels: list[str]
    delimiter: str | re.Pattern | None
    delimiter_inferred: bool
    delimiter_name: str | None
    current_columns: list["Column"]
    _has_nested_delimiters: bool
    _has_source_column: bool

    def notify(self, message: str, severity: str = ...) -> None: ...
    def query_one(self, selector: str) -> object: ...
    def _try_lock(self, action: str, deferred: "Callable | None" = ...) -> object: ...
    def _deferred_update_table(
        self,
        restore_position: bool = ...,
        callback: "Callable | None" = ...,
        reason: "UpdateReason" = ...,
    ) -> None: ...
    def _rebuild_column_caches(self) -> None: ...
    def _get_col_idx_by_name(
        self, name: str, render_position: bool = ...
    ) -> int | None: ...
    def invalidate_caches(self) -> None: ...
    def _format_arrival(self, ts: float) -> str: ...
    def _get_theme(self) -> object: ...
    def _safe_widget_call(self, fn, *args) -> None: ...
    def _flash_status(self, message: str, duration: float = ...) -> None: ...
    def _update_status_bar(self) -> None: ...
    def _needs_full_rebuild(self) -> bool: ...


@dataclass
class Filter:
    column: str | None  # None means any column
    pattern: re.Pattern[str]
    exclude: bool = False


@dataclass
class CliArgs:
    delimiter: str | None
    filters: list[Filter]
    unique_keys: set[str]
    sort_by: str | None
    filename: str | None = None
    theme: str | None = None
    keymap: str | None = None
    tail: bool = False
    time_window: str | None = None
    columns: str | None = None
    raw: bool = False
    no_tui: bool = False
    tui: bool = False
    session: str | None = None
    merge: bool = False
    filenames: list[str] = field(default_factory=list)
    pipe_output: bool = False  # computed at runtime: stdout is a pipe
    output_format: str = "csv"  # csv, tsv, json, raw
    format_timestamp: str | None = None  # "colname -> format" conversion
    demo: bool = False  # show keybinding captions on every action


@dataclass
class Column:
    name: str
    labels: set[str]
    render_position: int
    data_position: int
    hidden: bool
    pinned: bool = False
    computed: bool = False  # whether this column is computed (e.g. count)
    delimiter: str | re.Pattern[str] | None = None  # delimiter for parsing JSON fields
    col_ref: str = ""  # reference to the original column name
    col_ref_index: int = -1  # reference to the original column index
    json_ref: str = ""  # reference to the original JSON field
    substitution: tuple[re.Pattern, str] | None = (
        None  # regex substitution (pattern, replacement)
    )
    detected_type: "ColumnType" = ColumnType.AUTO
    type_override: "ColumnType | None" = None
    datetime_fmt_hint: str | None = None  # cached strptime format for DATETIME columns
    datetime_display_fmt: str | None = (
        None  # target display format for converted DATETIME columns
    )

    @property
    def effective_type(self) -> "ColumnType":
        return self.type_override if self.type_override else self.detected_type


@dataclass
class StreamState:
    """Owns the parallel array invariant for streaming data.

    raw_rows, arrival_timestamps, and source_labels are parallel
    arrays. This class enforces:
      len(raw_rows) == len(arrival_timestamps) at all times
      source_labels is either empty or len(source_labels) == len(raw_rows)

    All mutations to these arrays must go through this class.
    Direct field access for reading is permitted via properties.
    """

    _raw_rows: list[str] = field(default_factory=list)
    _arrival_timestamps: list[float] = field(default_factory=list)
    _source_labels: list[str] = field(default_factory=list)

    # ── Mutation methods ───────────────────────────────────────

    def append(
        self,
        line: str,
        ts: float,
        source: str = "",
    ) -> None:
        """Append a single row. Invariant preserved by construction."""
        self._raw_rows.append(line)
        self._arrival_timestamps.append(ts)
        if self._source_labels or source:
            self._source_labels.append(source)

    def extend(
        self,
        lines: list[str],
        timestamps: list[float],
        sources: list[str] | None = None,
    ) -> None:
        """Extend with multiple rows. Asserts input lengths match."""
        if len(lines) != len(timestamps):
            raise ValueError(
                f"lines ({len(lines)}) and timestamps "
                f"({len(timestamps)}) must have equal length"
            )
        self._raw_rows.extend(lines)
        self._arrival_timestamps.extend(timestamps)
        if sources is not None:
            if len(sources) != len(lines):
                raise ValueError(
                    f"sources ({len(sources)}) must match lines ({len(lines)})"
                )
            self._source_labels.extend(sources)
        elif self._source_labels:
            # source_labels already populated - pad with empty strings
            self._source_labels.extend("" for _ in lines)

    def pop(self, index: int) -> tuple[str, float, str]:
        """Remove and return row at index as (line, ts, source)."""
        line = self._raw_rows.pop(index)
        ts = self._arrival_timestamps.pop(index)
        src = self._source_labels.pop(index) if self._source_labels else ""
        return line, ts, src

    def insert(
        self,
        index: int,
        line: str,
        ts: float,
        source: str = "",
    ) -> None:
        """Insert a row at index. Invariant preserved by construction."""
        self._raw_rows.insert(index, line)
        self._arrival_timestamps.insert(index, ts)
        if self._source_labels or source:
            self._source_labels.insert(index, source)

    def replace_raw_rows(
        self,
        rows: list[str],
        timestamps: list[float],
        sources: list[str] | None = None,
    ) -> None:
        """Replace all rows atomically. Used by compaction and
        delimiter switch. Asserts input lengths match."""
        if len(rows) != len(timestamps):
            raise ValueError(
                f"rows ({len(rows)}) and timestamps "
                f"({len(timestamps)}) must have equal length"
            )
        if sources is not None and len(sources) != len(rows):
            raise ValueError(f"sources ({len(sources)}) must match rows ({len(rows)})")
        self._raw_rows = rows
        self._arrival_timestamps = timestamps
        self._source_labels = sources if sources is not None else []

    def truncate_timestamps(self, new_length: int) -> None:
        """Truncate _arrival_timestamps to new_length.
        Used when source_labels compaction reduces length."""
        self._arrival_timestamps = self._arrival_timestamps[:new_length]

    def set_source_labels(self, labels: list[str]) -> None:
        """Replace source labels with length validation."""
        if labels and len(labels) != len(self._raw_rows):
            raise ValueError(
                f"source_labels ({len(labels)}) must match "
                f"raw_rows ({len(self._raw_rows)})"
            )
        self._source_labels = labels

    def clear(self) -> None:
        """Remove all rows."""
        self._raw_rows = []
        self._arrival_timestamps = []
        self._source_labels = []

    # ── Read properties ────────────────────────────────────────

    @property
    def raw_rows(self) -> ListView:
        """Read-only view. Mutate via append/extend/replace_raw_rows."""
        return ListView(self._raw_rows)

    @property
    def arrival_timestamps(self) -> ListView:
        """Read-only view. Mutate via append/extend/replace_raw_rows."""
        return ListView(self._arrival_timestamps)

    @property
    def source_labels(self) -> ListView:
        """Read-only view. Mutate via set_source_labels."""
        return ListView(self._source_labels)

    @property
    def has_sources(self) -> bool:
        return bool(self._source_labels)

    def __len__(self) -> int:
        return len(self._raw_rows)

    def __getitem__(self, index):
        return self._raw_rows[index]

    def __iter__(self):
        return iter(self._raw_rows)

    def __contains__(self, item):
        return item in self._raw_rows

    def index(self, value: str) -> int:
        return self._raw_rows.index(value)

    def assert_invariant(self) -> None:
        """Assert the parallel array invariant holds.
        Call in tests or debug builds."""
        assert len(self._raw_rows) == len(self._arrival_timestamps), (
            f"Invariant violated: raw_rows={len(self._raw_rows)} "
            f"!= arrival_timestamps={len(self._arrival_timestamps)}"
        )
        if self._source_labels:
            assert len(self._source_labels) == len(self._raw_rows), (
                f"Invariant violated: source_labels="
                f"{len(self._source_labels)} "
                f"!= raw_rows={len(self._raw_rows)}"
            )


@dataclass
class FilterSortState:
    """Owns filter, sort, search, and dedup query state.

    Provides clear_all() as the single point of reset so callers
    (e.g. switch_delimiter) don't need to know which fields to
    clear. Provides is_expensive as a named concept for the
    condition that requires full rebuild on new streaming rows.
    """

    filters: list[Filter] = field(default_factory=list)
    sort_column: str | None = None
    sort_reverse: bool = False
    search_term: re.Pattern | None = None
    search_matches: list = field(default_factory=list)
    current_match_index: int = -1
    unique_column_names: set[str] = field(default_factory=set)
    count_by_column_key: dict = field(default_factory=lambda: defaultdict(lambda: 0))

    def clear_all(self) -> None:
        """Reset all query state. Called on delimiter change."""
        self.filters = []
        self.sort_column = None
        self.sort_reverse = False
        self.search_term = None
        self.search_matches = []
        self.current_match_index = -1
        self.unique_column_names = set()
        self.count_by_column_key = defaultdict(lambda: 0)

    def clear_sort(self) -> None:
        """Clear sort state only."""
        self.sort_column = None
        self.sort_reverse = False

    def clear_search(self) -> None:
        """Clear search state only."""
        self.search_term = None
        self.search_matches = []
        self.current_match_index = -1

    @property
    def is_expensive(self) -> bool:
        """True if any active operation requires a full deferred
        rebuild when new streaming rows arrive, rather than
        incremental row insertion."""
        return bool(self.sort_column or self.unique_column_names)

    @property
    def has_filters(self) -> bool:
        return bool(self.filters)

    @property
    def has_search(self) -> bool:
        return self.search_term is not None


@dataclass
class CacheState:
    """Owns all caches derived from raw_rows and columns.

    Every field here can be recomputed from source data. Two
    invalidation levels:
      invalidate()       - full invalidation, forces complete reparse
      invalidate_widths  - width cache only, safe after markup changes
    """

    parsed_rows: list[list[str]] | None = None
    col_widths: list[int] | None = None
    sort_keys: list = field(default_factory=list)
    col_data_idx: dict[str, int] = field(default_factory=dict)
    col_render_idx: dict[str, int] = field(default_factory=dict)
    sorted_visible_columns: list = field(default_factory=list)
    dedup_key_to_row_idx: dict[str, int] = field(default_factory=dict)

    def invalidate(self) -> None:
        """Full invalidation. Forces reparse on next rebuild.
        Call after delimiter change, row compaction, or any
        structural change to raw_rows."""
        self.parsed_rows = None
        self.col_widths = None
        self.sort_keys = []
        self.dedup_key_to_row_idx = {}

    def invalidate_widths(self) -> None:
        """Width cache only. Safe after search highlight or
        theme changes where cell content changes but row
        structure does not."""
        self.col_widths = None

    def reset_sort_keys(self) -> None:
        """Clear sort keys without full invalidation.
        Call when sort column changes."""
        self.sort_keys = []
        self.dedup_key_to_row_idx = {}


@dataclass
class ChainTimerState:
    """Manages exponential backoff state for streaming rebuilds.

    When data arrives faster than the UI can render, rebuilds are
    coalesced with increasing delays. This class owns the four
    related fields that previously lived as NlessBuffer attributes,
    making the backoff policy explicit and testable.
    """

    INITIAL_DELAY: ClassVar[float] = 0.3
    MAX_SKIPS: ClassVar[int] = 3
    MAX_DELAY: ClassVar[float] = 1.5

    delay: float = field(default=0.3)
    timer: object | None = None  # Textual Timer
    skips: int = 0
    notified: bool = False

    def reset(self) -> None:
        """Reset to initial state after a successful rebuild."""
        self.delay = self.INITIAL_DELAY
        self.skips = 0

    def advance_backoff(self) -> float:
        """Increment skip count, double delay up to MAX_DELAY.
        Returns the new delay to use for the next timer."""
        self.skips += 1
        self.delay = min(self.delay * 2, self.MAX_DELAY)
        return self.delay

    @property
    def should_skip(self) -> bool:
        """True if we should skip this rebuild and wait longer."""
        return self.skips < self.MAX_SKIPS

    def cancel_timer(self) -> None:
        """Cancel pending timer without resetting backoff state."""
        if self.timer is not None:
            try:
                self.timer.stop()
            except Exception:
                pass
            self.timer = None

    def stop(self) -> None:
        """Cancel timer and reset all backoff state."""
        self.cancel_timer()
        self.reset()
        self.notified = False


@dataclass
class DelimiterState:
    """Owns delimiter identity and inference tracking.

    Groups the fields that describe *what* delimiter is active and
    *how* it was chosen. reset() is the single point of clear for
    delimiter switches.
    """

    value: str | re.Pattern | None = None
    inferred: bool = False
    name: str | None = None
    max_fields: int = 0
    column_positions: list[int] | None = None
    preamble_lines: list[str] = field(default_factory=list)
    suggestion_shown: bool = False
    mismatch_warned: bool = False
    total_skipped: int = 0

    def reset(self) -> None:
        """Clear inference tracking. Called on delimiter switch."""
        self.inferred = False
        self.suggestion_shown = False
        self.mismatch_warned = False
        self.total_skipped = 0
        self.max_fields = 0
        self.column_positions = None


@dataclass
class LoadingState:
    """Owns spinner animation and loading-reason display.

    All fields relate to the "currently processing" UI indicator.
    """

    reason: str | None = None
    spinner_timer: object | None = None  # Textual Timer
    spinner_frame: int = 0
    flash_message: str | None = None
    flash_timer: object | None = None  # Textual Timer


@dataclass
class StatusContext:
    """App-level context the buffer needs for status bar rendering.

    Set by NlessApp on mount so the buffer doesn't reach into
    self.app.* for theme, keymap, config, etc.
    """

    status_format: str = ""
    keymap_name: str = ""
    theme_name: str = ""
    pipe_output: bool = False
    session_name: str | None = None
    format_window: object = None  # Callable[[float|None, bool], str]
    theme: object = None  # NlessTheme
