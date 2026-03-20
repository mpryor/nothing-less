# Time Window Filtering

## Two modes

### Fixed window (e.g. `5m`)

Freezes a snapshot in time. Only rows within the window are shown. New
streaming rows that arrive after the window was set are excluded.

**Wall-clock mode** (default — no column specified):

```
_time_window_ceiling = time.time()  # frozen upper bound
cutoff = ceiling - duration

Show row if: cutoff <= arrival_ts <= ceiling
```

**Column-based mode** (explicit column — e.g. `@timestamp 5m`):

```
ceiling = max(parsed_timestamps)  # max value in the column
cutoff = ceiling - duration

Show row if: cutoff <= parsed_ts <= ceiling
```

### Rolling window (e.g. `5m+`)

A sliding window that continuously drops old rows. No ceiling — the
window moves forward with wall clock time.

```
_time_window_ceiling = None
cutoff = time.time() - duration  # recalculated each tick

Show row if: arrival_ts >= cutoff  (or parsed_ts >= cutoff for column mode)
```

## Wall-clock vs. column-based filtering

By default, `@5m` filters by **arrival timestamps** — wall-clock time when
nless received each row. This works well for live streaming where "recent"
means "recently arrived."

To filter by **parsed timestamps** from a column, prefix the duration with
the column name: `@timestamp 5m`. The column must be detected as a DATETIME
type. In column-based mode, the ceiling for fixed windows is the max parsed
timestamp in the data (not wall clock), so `@timestamp 5m` means "the last
5 minutes of the log" rather than "rows within 5 minutes of now."

## Timestamp format conversion

The `@` prompt also supports format conversion with the `->` syntax:

```
@colname -> target_format
```

This creates a new buffer (copy-on-write) with the column's values converted
to the target format. The conversion is applied in `split_line()` so all
downstream operations (sort, filter, search) see the converted values.

### CLI usage

Format conversion is also available from the command line via `--format-timestamp` / `-F`:

```bash
nless --no-tui -F 'timestamp -> epoch' events.csv
cat events.csv | nless --no-tui -F 'timestamp -> %H:%M' -o json
```

In batch mode, `format_datetime_value()` is applied directly to parsed cells.
In TUI mode, `_apply_cli_format_timestamp()` sets `datetime_display_fmt` on
the column after type inference runs, and `split_line()` applies the conversion
on every render pass.

### Supported target formats

| Format | Example output |
|--------|---------------|
| `iso` | `2024-01-15T10:30:00` |
| `epoch` | `1705312200.0` |
| `epoch_ms` | `1705312200000` |
| `relative` | `2h ago`, `5m ago` |
| `%H:%M:%S` | `10:30:00` (any strftime pattern) |
| `%Y-%m-%d` | `2024-01-15` |

### Timezone conversion

Optionally prefix the target format with `source>destination` timezone:

```
@timestamp -> UTC>US/Eastern %H:%M:%S
```

- Both IANA names (`US/Eastern`, `Europe/London`) and common abbreviations
  (`EST`, `PST`, `UTC`) are supported
- Source timezone is applied to naive datetimes via `replace(tzinfo=)`
- Target timezone converts via `astimezone()`
- Either source or target can be omitted: `>US/Eastern` (assume UTC source),
  `UTC>` (just attach timezone, no conversion)

## Duration parsing

`_parse_duration()` accepts flexible input:

| Input | Seconds |
|-------|---------|
| `5` | 300 (plain number = minutes) |
| `30s` | 30 |
| `5m` | 300 |
| `1h30m` | 5400 |
| `2d` | 172800 |
| `0` / `off` / `clear` | None (removes window) |

Append `+` for rolling: `5m+`, `1h+`.

## Rolling timer

For rolling windows, a periodic timer calls `_tick_rolling()` which
invalidates caches and triggers a full rebuild. The interval is:

```python
interval = min(window / 10, 5.0)  # e.g. 5m window → 30s ticks
interval = max(interval, 1.0)     # but at least 1 second
```

This means a 5-minute window redraws roughly every 30 seconds, which is
frequent enough to look smooth but not so frequent as to waste CPU.

## Interaction with streaming

During streaming with a fixed window, `_add_log_line()` checks
`_time_window_ceiling` and drops rows arriving after the ceiling. This
prevents the display from growing after the window was set.

During streaming with a rolling window, new rows always enter (no ceiling).
Old rows are aged out on the next timer tick rebuild.

## Key files

- `buffer_timewindow.py` — `TimeWindowMixin`: `apply_time_window_setting()`,
  `_apply_time_window()`, `_start_rolling_timer()`, `_tick_rolling()`
- `dataprocessing.py` — `format_datetime_value()`, `parse_tz_and_format()`,
  `_resolve_tz()`, `_format_relative_time()`
- `suggestions.py` — `TimeWindowSuggestionProvider`: context-aware autocomplete
  for durations, column names, formats, and timezones
