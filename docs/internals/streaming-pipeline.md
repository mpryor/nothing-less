# Streaming & Data Processing Pipeline

## Row lifecycle

```
stdin/file → StdinLineStream → add_logs() → _add_logs_inner()
                                                    │
                              ┌─────────────────────┼──────────────────────┐
                              │                     │                      │
                         First batch           Incremental            Expensive ops
                         (parse header,        (fast path)            (sort/dedup/
                          infer delimiter)                             time window)
                              │                     │                      │
                         _parse_first_row()    _add_rows_incremental() _deferred_update_table()
                              │                     │                      │
                         _deferred_update_table()   │               _process_deferred_data()
                                                    │                      │
                                              displayed_rows         _apply_deferred_to_widgets()
                                                    │                      │
                                                    └──────────────────────┘
                                                              │
                                                         DataTable widget
```

## Two paths: incremental vs full rebuild

### Incremental (fast path)

Used when there's no sort, no dedup, and no time window. New rows are parsed,
filtered, and appended directly to `displayed_rows` and the DataTable widget
without rebuilding the entire table.

`_add_rows_incremental()` in `buffer_streaming.py`:

1. Choose parse strategy once (CSV fast path, tab split, regex, etc.)
2. Parse each line, validate column count, append metadata
3. Track column widths from first 10K rows only (avoids O(n*cols) on large files)
4. Cache parsed cells in `cache.parsed_rows` (skip re-parse on first sort)
5. Apply search/regex/green highlights via `_apply_row_highlighting()`
6. Push to DataTable with `add_rows_precomputed()`

Large batches (>50K rows) are chunked into `STREAMING_CHUNK_SIZE` blocks
for progressive display.

### Full rebuild (deferred path)

Used when sort, dedup, or time window is active — the entire dataset must be
reprocessed to maintain order/uniqueness invariants.

`_process_deferred_data()` in `buffer.py` runs on a worker thread:

1. Acquire lock
2. `_filter_rows()` — parse raw_rows, apply content filters, compact
3. `_apply_time_window()` — drop rows outside the time window
4. `_dedup_rows()` — collapse by unique key, prepend count
5. `_sort_rows()` — in-place sort by sort column
6. Rebuild dedup indices and sort keys (for incremental inserts during streaming)
7. Align cells to visible column order
8. Apply search + regex + green highlights
9. Release lock
10. Compute column widths (outside lock, expensive)

`_apply_deferred_to_widgets()` runs on the main thread:
- Clear DataTable, re-add columns and rows
- Check for catch-up gap, schedule chain rebuild if needed

## Parsed row cache

`cache.parsed_rows` avoids re-parsing raw text on every sort/filter operation.

Three states:
- **Full hit**: `len(cache.parsed_rows) == len(raw_rows)` — skip all parsing
- **Partial hit**: `len(cache.parsed_rows) < len(raw_rows)` — parse only new rows
- **Miss**: `cache.parsed_rows is None` — full reparse (after delimiter switch)

The cache is populated during `_add_rows_incremental()` (no skipped lines = cache valid)
and during `_partition_rows()` (on rebuild). It's invalidated on delimiter change,
raw_rows compaction, or any non-cache-safe update reason.

## Filter compaction

When filters are active, `_filter_rows()` compacts `raw_rows` to only contain
matching + unparseable rows. This reduces scan volume on subsequent rebuilds.

The compaction is atomic — `stream.replace_raw_rows()` swaps all three parallel
arrays (rows, timestamps, sources) at once.

## Key constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `STREAMING_CHUNK_SIZE` | 50,000 | Rows per incremental batch |
| `COLUMN_WIDTH_SAMPLE_SIZE` | 10,000 | Stop tracking widths after this many rows |
| `MAX_SKIPPED_LINES_SAMPLE` | 200 | Cap retained unparseable lines for ~ view |

## Key files

- `buffer_streaming.py` — `add_logs()`, `_add_logs_inner()`, `_add_rows_incremental()`
- `buffer.py` — `_process_deferred_data()`, `_filter_rows()`, `_partition_rows()`
- `dataprocessing.py` — `choose_parse_strategy()`, pure helper functions
