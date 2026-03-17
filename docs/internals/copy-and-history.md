# Copy-on-Write Buffer History

## How buffer copy works

Every user operation that transforms data (filter, sort, dedup, search-to-filter)
creates a new buffer via `copy()`. This gives undo-like behavior — the user
can close the new tab to go back.

### copy() flow

```
1. Acquire lock
   - Snapshot raw_rows, timestamps, source_labels (list copies)
   - Create new NlessBuffer with copied state (columns, filters, sort, etc.)
   - Subscribe new buffer to line_stream (future lines only)
   - Release lock

2. Filter outside lock (on snapshot, not live data)
   - _filter_lines() applies current filters to the snapshot
   - Returns only matching rows + their timestamps/sources

3. Populate new buffer
   - stream.extend(filtered_rows, filtered_ts, filtered_sources)

4. Cache transfer
   - If cache.parsed_rows valid AND same length as snapshot:
     copy the cache (avoids re-parsing on first rebuild)
   - If filters removed rows: cache is now longer than raw_rows,
     _partition_rows() safely ignores the stale portion

5. Store unfiltered history
   - _all_source_lines = raw_rows_snapshot (shared reference, not copied)
   - Allows ~ view to find excluded lines even without a live stream
```

### Why filter outside the lock?

Filtering can be expensive on large datasets. Holding the lock during
filtering would block the input thread from adding new rows. By snapshotting
under the lock and filtering outside, the input thread stays responsive.

### Cache edge cases

When the copy has active filters, fewer rows survive than the original.
The parsed row cache (which was built from the full dataset) is now
"too long" — `len(cache.parsed_rows) > len(raw_rows)`. This is safe:
`_partition_rows()` checks `len(parsed) <= len(raw_rows)` and falls
through to a full reparse if the cache is stale.

When dedup is active, `_dedup_rows()` prepends a count column to each row.
The cache stores rows *before* the count is prepended, so `_partition_rows()`
copies rows from the cache (`list(parsed[i])`) instead of using them
directly, preventing the count from accumulating on repeated rebuilds.

## Merge

`init_as_merged()` creates a new buffer from two existing buffers,
interleaving their rows by arrival timestamp:

1. Zip `(timestamp, row, source_label)` from both buffers
2. Sort by timestamp (ascending)
3. Unzip into merged arrays
4. Add a `_source` column to identify which buffer each row came from

The merged buffer gets a pinned `_source` column at render position 0.

## Filter compaction

During `_filter_rows()`, if content filters are active, raw_rows is
*compacted* — non-matching rows are removed, keeping only:
- Rows that passed all filters
- Rows that failed to parse (kept for delimiter auto-switch and ~ view)

This is an optimization: subsequent rebuilds (sort, search, dedup) scan
fewer rows. The compaction is atomic via `stream.replace_raw_rows()`.

## Key files

- `buffer.py` — `copy()`, `init_as_merged()`, `_filter_rows()`, `_partition_rows()`, `_apply_raw_rows_compaction()`
