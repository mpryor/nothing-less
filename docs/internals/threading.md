# Threading Model

## Three threads

```
Input thread (daemon)          Worker thread (@work)         Main thread (Textual)
─────────────────────          ─────────────────────         ────────────────────
StdinLineStream.run()          _run_deferred_update()        Event loop
  select() / readlines()         _process_deferred_data()     _apply_deferred_to_widgets()
  notify() → add_logs()          [holds lock]                 Widget mutations
  [holds lock]                                                call_from_thread() target
```

### Input thread

`StdinLineStream` in `input.py` runs on a daemon thread. For pipes, it uses
`select()` with a 0.5s timeout in a loop, reading chunks and splitting on
newlines. Lines accumulate in a buffer and flush when:

- 20ms has elapsed since last flush (`FLUSH_INTERVAL_MS`)
- Buffer is older than 200ms (`MAX_BUFFER_HOLD_MS`)
- Buffer exceeds 1MB (`MAX_BUFFER_SIZE`)

JSON input gets special treatment — the buffer holds longer (200ms) to
coalesce pretty-printed multi-line JSON objects before flushing.

On flush, `notify()` calls each subscriber's callback. For NlessBuffer,
that's `add_logs()`.

### Worker thread

`_run_deferred_update()` is decorated with `@work(thread=True, exclusive=True)`.
It runs `_process_deferred_data()` (the heavy filtering/sorting pipeline)
on a background thread, then schedules `_apply_deferred_to_widgets()` on the
main thread via `call_from_thread()`.

### Main thread

All widget mutation happens here — adding rows to the DataTable, updating
the status bar, swapping RawPager for DataTable, etc.

## Lock protocol

A single `RLock` (`self._lock`) serializes access to:

- `StreamState` (raw_rows, timestamps, source_labels)
- Column definitions and filter/sort/search state
- Derived caches

**`add_logs()`** holds the lock for the entire ingestion call. This means
the input thread blocks briefly while processing a batch, but the lock is
released before any widget operations.

**`_process_deferred_data()`** acquires the lock for the filter/sort/dedup
pipeline, then releases it before computing column widths (the most
expensive non-locked operation).

**`_try_lock()`** is non-blocking. If the lock is busy (input thread is
adding rows), user actions like sort/delimiter switch get queued as a
`_pending_action` and run automatically when `add_logs()` finishes.

## Generation counter

`_update_generation` is a monotonic integer incremented at the start of
every `_deferred_update_table()` call. Both `_process_deferred_data()` and
`_apply_deferred_to_widgets()` check `gen == self._update_generation` before
proceeding — if the generation advanced (because a newer sort/filter was
triggered), the stale result is silently discarded.

This prevents rapid successive operations (sort -> sort -> sort) from
applying intermediate states.

## Catch-up protocol

After `_apply_deferred_to_widgets()` pushes data to the widget, it checks
whether new rows arrived during processing:

```
if len(raw_rows) > _last_flushed_idx:
    # Gap detected — new data arrived while we were rebuilding
    if stream_active and _needs_full_rebuild():
        _schedule_chain_rebuild()  # exponential backoff
    else:
        _deferred_update_table()   # immediate re-rebuild
```

`_last_flushed_idx` records how far `displayed_rows` has been synchronized
with `raw_rows`. It's updated at the end of every successful render.

### Chain rebuild backoff

When data arrives faster than rebuilds complete, `ChainTimerState` implements
exponential backoff:

- Initial delay: 0.3s
- Each skip doubles the delay (0.3 -> 0.6 -> 1.2 -> 1.5 cap)
- Maximum 3 skips before forcing a rebuild
- Total worst-case delay: ~2 seconds

This prevents thrashing on fast streams while guaranteeing eventual progress.

## Key files

- `input.py` — `StdinLineStream`, publisher-subscriber pattern
- `buffer_streaming.py` — `add_logs()`, `_try_lock()`, `_add_logs_inner()`
- `buffer.py` — `_deferred_update_table()`, `_process_deferred_data()`,
  `_apply_deferred_to_widgets()`, `_schedule_chain_rebuild()`
- `types.py` — `ChainTimerState`
