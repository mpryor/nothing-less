# Column Operations

## Splittable column detection

`_detect_splittable_columns()` scans STRING/AUTO columns for values that
consistently contain `|`, `;`, or `=` delimiters (≥80% of sampled rows).
Columns that pass get a `⑃` label in the header, hinting the user can
press `d` to split them.

## Column splitting (`_apply_column_delimiter`)

When the user presses `d` on a column, nless splits that column's values
by a sub-delimiter to create new computed columns. This is how you go from
a single `request` column containing `GET /api/users HTTP/1.1` to three
columns: `method`, `path`, `protocol`.

### Flow

1. User selects a column and enters a delimiter (string or regex)
2. Sample the column's values to determine how many fields the split produces
3. Generate column names: `{original}_{1}`, `{original}_{2}`, etc.
4. Create `Column` objects with `computed=True`, `col_ref` pointing to
   the source column, and `col_ref_index` indicating which split field
5. Insert before the `_arrival` metadata column (so it stays last)
6. Trigger a full rebuild — `split_line()` will use `col.delimiter` and
   `col.col_ref_index` to extract the right field during parsing

### JSON extraction

If the column contains JSON strings, the user can press `J` instead. This
works differently:

1. Parse the JSON from the cell under cursor
2. Present top-level keys as a select menu
3. Create a computed column with `json_ref` set to the selected key
4. During parsing, `split_line()` does `json.loads(cell)[json_ref]`

Nested JSON extraction repeats this process — the user can `J` on an
already-extracted JSON column to go deeper.

### Computed column persistence

Computed columns survive session save/load. `SessionComputedColumn` stores:
- `col_ref` — source column name
- `col_ref_index` — which split field (-1 for JSON)
- `json_ref` — JSON key path
- `delimiter` / `delimiter_regex` / `delimiter_regex_flags` — how to split

## Dedup / pivot (`handle_mark_unique`)

Pressing `U` on a column marks it as a unique key. Multiple `U` presses
build a composite key. The result is a pivot-like view showing distinct
values with counts.

### How it works

1. `copy()` the buffer (new tab, preserves history)
2. Add the column name to `query.unique_column_names`
3. Insert a `count` metadata column at position 0, shifting all other
   columns right by 1 (both `data_position` and `render_position`)
4. Add `"U"` label to the selected column
5. If sort was on, and the sort column is now a unique key, clear it
   (dedup changes the row structure, sort may no longer make sense)
6. Default sort: count descending (most common values first)

### During rebuild

`_dedup_rows()` in `buffer.py` collapses rows by composite key:
- Build key from all unique column values (stripped of markup)
- Keep last-seen row for each key
- Prepend count to each row
- Track counts in `query.count_by_column_key`

### Incremental streaming updates

When new rows arrive with dedup active, `_add_log_line()` checks if the
composite key already exists:
- **Existing key**: increment count, highlight the row green, remove old
  row and re-insert at the correct sorted position
- **New key**: insert with count=1

This gives live-updating aggregation during streaming.

## Column pinning

Pinning (`m` key) locks a column to the left side of the DataTable. Pinned
columns don't scroll horizontally. The DataTable renders them with
`fixed_columns` count.

Pin/unpin reorders `render_position` values — pinned columns get the lowest
positions, unpinned columns shift right. The `"P"` label is added/removed
from the column.

## Key files

- `app_columns.py` — `_apply_column_delimiter()`, `_apply_json_header()`,
  `action_toggle_arrival()`, `_apply_column_filter()`
- `operations.py` — `handle_mark_unique()`, `compute_column_aggregations()`
- `buffer_actions.py` — `action_pin_column()`, `action_move_column()`,
  `action_sort()`
- `buffer_columns.py` — `ColumnMixin._make_columns()`, `_rebuild_column_caches()`
