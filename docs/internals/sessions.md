# Session Persistence

## What gets saved

A session captures the full workspace: all groups, all buffers within each
group, and each buffer's view state. Saved to
`~/.config/nless/sessions/<name>.json`.

Per-buffer state captured by `capture_buffer_state()`:

| Field | Source | Notes |
|-------|--------|-------|
| delimiter | `delim.value` | String or regex pattern string |
| delimiter_regex | `delim.value.pattern` | Only if regex |
| delimiter_name | `delim.name` | Human label |
| raw_mode | `raw_mode` | |
| sort_column / sort_reverse | `query.*` | |
| filters | `query.filters` | Pattern string + flags |
| columns | `current_columns` | visibility, pinned, render_position |
| computed_columns | computed cols | col_ref, json_ref, delimiter + flags |
| unique_column_names | `query.unique_column_names` | |
| highlights | `regex_highlights` | Pattern string + color |
| time_window / rolling | | |
| is_tailing | | |
| search_term | `query.search_term.pattern` | |
| cursor_row / cursor_column | DataTable widget | |
| source_labels | `stream.source_labels` | Merge mode |
| tab_name | Tab widget label | |

## Round-trip flow

```
User state → capture_buffer_state() → SessionBufferState (dataclass)
                                            │
                                       asdict() → JSON
                                            │
                                  JsonStore.save() → disk (atomic write)
                                            │
                                  JsonStore.load() → JSON
                                            │
                              _deserialize_buffer_state() → SessionBufferState
                                            │
                              apply_buffer_state() → NlessBuffer restored
```

### Serialization details

- `re.Pattern` → stored as `(pattern_string, flags_int)`, recompiled on load
- Computed column delimiters → stored as `(delimiter_regex, delimiter_regex_flags)`
- Filter flags (e.g. `re.IGNORECASE`) → preserved through the round-trip
- Atomic writes via `tempfile.mkstemp()` + `os.replace()`

## Restoring a session

`_load_session()` in `app_sessions.py` handles the full restore:

1. Close all groups beyond the first
2. Close extra buffers in the first group
3. For the first group:
   - If data source changed: reload file via `_reload_file_source()`
   - Else: apply state to existing buffer
4. Create additional buffers via `_create_additional_buffers()`
5. Restore additional groups (file or shell command)
6. Switch to saved active group

### File reload

When the session references a different file than what's currently loaded,
`_reload_file_source()` resets the buffer to a blank state and starts a new
`StdinLineStream`. The session state is stored in `_pending_session_state`
and applied automatically when the first data arrives (in `add_logs()`).

### Waiting for data

When creating additional buffers that need `copy()`, the parent buffer must
have parsed its first row. `_wait_for_parse()` polls with 50ms intervals
up to 10 seconds.

## Views

Views are single-buffer state snapshots (no group/tab information). Stored
in `~/.config/nless/views/<name>.json`.

Loading a view saves the current state in `_pre_view_state` (with a copy
of raw_rows/timestamps) so the user can undo. View undo restores raw_rows
from the snapshot and re-applies the saved state.

## Skipped settings

`apply_buffer_state()` returns a list of settings that couldn't be applied
(e.g. filter on a column that doesn't exist in the current data). These
are shown to the user as notifications.

## Key files

- `session.py` — `capture_buffer_state()`, `apply_buffer_state()`, `_apply_session_*()` helpers, `JsonStore`, serialization
- `app_sessions.py` — `SessionViewMixin._load_session()`, `_capture_session()`, `_reload_file_source()`
