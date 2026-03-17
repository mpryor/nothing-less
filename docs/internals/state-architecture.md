# State Architecture

## Overview

NlessBuffer's state is divided into six dataclass objects, each owning a
specific invariant. This replaces the previous flat-attribute design where
57 `self.*` fields were scattered across `__init__`.

```
NlessBuffer
├── stream: StreamState        — parallel array invariant
├── query: FilterSortState     — filter/sort/search/dedup state
├── cache: CacheState          — derived caches (recomputable)
├── chain: ChainTimerState     — streaming rebuild backoff
├── delim: DelimiterState      — delimiter identity + inference tracking
└── loading_state: LoadingState — spinner/loading UI
```

## State objects

### StreamState

Owns the three parallel arrays that store raw data:

```python
raw_rows: list[str]           # original text lines
arrival_timestamps: list[float]  # when each line arrived (epoch)
source_labels: list[str]      # which file/source (merge mode only)
```

**Invariant**: `len(raw_rows) == len(arrival_timestamps)` at all times.
`source_labels` is either empty (single source) or same length as `raw_rows`.

All mutations go through methods (`append`, `extend`, `pop`, `insert`,
`replace_raw_rows`, `clear`) that maintain the invariant by construction.
`assert_invariant()` can be called in tests or debug builds.

### FilterSortState

Owns everything related to "what the user is querying":

- `filters: list[Filter]` — content filters (column + regex pattern)
- `sort_column / sort_reverse` — current sort
- `search_term / search_matches / current_match_index` — search state
- `unique_column_names` — dedup key columns
- `count_by_column_key` — dedup counts

**Key property**: `is_expensive` — returns True if sort or dedup is active,
meaning new streaming rows require a full rebuild rather than incremental
append. This drives the decision in `_needs_full_rebuild()`.

`clear_all()` resets everything — called on delimiter switch so stale
column references don't persist.

### CacheState

Owns caches derived from raw_rows + columns. Everything here can be
recomputed from source data.

Two invalidation levels:
- `invalidate()` — full wipe (delimiter change, compaction)
- `invalidate_widths()` — width cache only (search highlight, theme change)

### ChainTimerState

Owns the exponential backoff policy for streaming rebuilds. When data
arrives faster than the UI can render, rebuilds are coalesced with
increasing delays (0.3s → 0.6s → 1.2s → 1.5s cap, max 3 skips).

### DelimiterState

Owns delimiter identity and inference tracking:
- `value` — the actual delimiter (string, regex Pattern, or None)
- `inferred` — whether it was auto-detected vs user-specified
- `name` — human label (e.g. "Apache Combined Log")
- `preamble_lines` — lines skipped before the header
- `suggestion_shown / mismatch_warned / total_skipped` — auto-switch tracking

`reset()` clears the inference tracking flags — called on every delimiter
switch.

### LoadingState

Owns the spinner animation and flash message display:
- `reason` — why we're loading ("Sorting", "Filtering", etc.)
- `spinner_timer / spinner_frame` — animation state
- `flash_message / flash_timer` — temporary status bar messages

## Mixin architecture

NlessBuffer inherits from 6 mixins plus Textual's `Static`:

```python
class NlessBuffer(
    ActionsMixin,      # user-facing key bindings
    ColumnMixin,       # column management, rebuild_column_caches
    DelimiterMixin,    # delimiter inference, switch, auto-switch
    TimeWindowMixin,   # time window filtering, rolling timer
    StreamingMixin,    # add_logs, incremental updates, _try_lock
    SearchMixin,       # search, highlight matches, navigation
    Static,            # Textual widget base
): ...
```

Each mixin uses `self: NlessBuffer` type annotations to access the full
buffer interface. `BufferProtocol` in `types.py` documents the contract —
which attributes and methods mixins expect on `self`.

The mixins are **file organization**, not independent components. They all
read and write the same state objects via `self.stream`, `self.query`,
`self.cache`, etc. The state objects provide encapsulation; the mixins
provide code organization.

### Dependency graph

```
StreamingMixin
  ├── ColumnMixin (hard: _make_columns, _rebuild_column_caches)
  ├── DelimiterMixin (hard: _try_auto_switch_delimiter)
  ├── TimeWindowMixin (hard: _apply_initial_time_window)
  └── SearchMixin (hard: _highlight_search_matches)

DelimiterMixin
  └── ColumnMixin (hard: _make_columns, _ensure_arrival_column)

TimeWindowMixin → standalone (reads arrival_timestamps)
SearchMixin → standalone (reads query.search_term)
ColumnMixin → standalone (manages column list)
ActionsMixin → calls into all other mixins via self
```

## Key files

- `types.py` — all 6 state dataclasses + `BufferProtocol`
- `buffer.py` — `NlessBuffer.__init__`, data processing pipeline, deferred update
- `buffer_streaming.py`, `buffer_delimiter.py`, `buffer_columns.py`,
  `buffer_search.py`, `buffer_timewindow.py`, `buffer_actions.py` — mixins
