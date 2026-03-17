# Time Window Filtering

## Two modes

### Fixed window (e.g. `5m`)

Freezes a snapshot in time. Only rows with arrival timestamps between
`(now - duration)` and `now` are shown. New streaming rows that arrive
after the window was set are excluded.

```
_time_window_ceiling = time.time()  # frozen upper bound
cutoff = ceiling - duration

Show row if: cutoff <= arrival_ts <= ceiling
```

### Rolling window (e.g. `5m+`)

A sliding window that continuously drops old rows. No ceiling — the
window moves forward with wall clock time.

```
_time_window_ceiling = None
cutoff = time.time() - duration  # recalculated each tick

Show row if: arrival_ts >= cutoff
```

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
