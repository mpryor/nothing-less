# Custom DataTable Rendering

## Why a custom widget

nless uses a custom `Datatable` (in `datatable.py`) rather than Textual's
built-in `DataTable` for two reasons:

1. **Performance**: `add_rows_precomputed()` skips per-cell width calculation
   — widths are precomputed on the worker thread during `_process_deferred_data()`.
   This makes the main-thread widget update O(1) per row instead of O(cols).

2. **Fixed columns**: Pinned columns stay visible during horizontal scroll.
   The built-in DataTable doesn't support this.

## render_line()

The `render_line()` method (109 lines) is the hot path — called for every
visible row on every frame. It handles:

1. **Column slicing**: Only render columns that are visible in the current
   scroll viewport. Skip columns entirely off-screen.

2. **Fixed columns**: Render pinned columns first (always visible), then
   scrollable columns offset by `scroll_x`.

3. **Rich markup**: Cells can contain Rich markup (`[bold]`, `[#ff5555]`,
   etc.) for search highlights, regex highlights, and new-row green
   coloring. The renderer uses `Text.from_markup()` for cells containing
   `[` and plain `Text()` for others (fast path).

4. **Cursor highlight**: The cell under cursor gets the theme's cursor
   style applied.

5. **Column separators**: Vertical bars between columns, styled with the
   theme's separator color.

## RawPager

For `raw` delimiter mode (no column parsing), nless swaps the DataTable
for a `RawPager` widget. This is a simpler renderer that shows one line
per row with no column structure. It supports the same search highlighting
and cursor navigation.

The swap happens in `_ensure_correct_view_widget()` — it checks
`raw_mode` vs the current widget type and hot-swaps if they differ.
`_deferred_raw_swap()` handles the case where incremental loading started
with a DataTable but the delimiter was later determined to be `raw`.

## Key files

- `datatable.py` — `Datatable` (custom widget), `render_line()`,
  `add_rows_precomputed()`, `move_cursor()`
- `rawpager.py` — `RawPager`, `render_line()`
- `buffer.py` — `_ensure_correct_view_widget()`, `_deferred_raw_swap()`
