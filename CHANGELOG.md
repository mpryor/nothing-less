# Changelog

## 1.10.4 (2026-03-19)

### Fix

- improve space-delimiter inference for prose and edge cases

### Refactor

- performance optimizations, DRY extractions, and code cleanup

## 1.10.3 (2026-03-19)

### Fix

- ex-mode :open uses add_group instead of missing _open_new_group

## 1.10.2 (2026-03-19)

### Fix

- correct cli_args import path in ex-mode
- include CHANGELOG.md in package for release notes screen

## 1.10.1 (2026-03-19)

### Fix

- add missing packaging dependency

## 1.10.0 (2026-03-19)

### Feat

- infer space-aligned command output (ps aux, lsof, df -h, netstat, ls -la)
- ex-mode command dispatcher with autocomplete
- regex find-and-replace substitution on cell values
- themed getting started screen — For jerr
- README overhaul, demo GIFs, --demo mode, and caption overlay
- PyPI update check, double-click pivot drill-in, and tab-click buffer sync
- themed status format, group context menu, menu hover colors, positioning fixes
- click-to-sort headers, hover highlights
- context menu, menu bar, mouse support, and buffer.py reorg
- log pattern inference, context menu, mixin refactors, and datatable enhancements
- auto-detect log formats on initial load
- **wip**: log pattern inference, context menu, and datatable enhancements

### Fix

- status bar empty on new buffer, add missing menu actions

## 1.9.0 (2026-03-13)

### Feat

- multi-file buffer groups, merge delimiter conflict detection, and JSON extraction fixes
- add views with undo, report skipped settings, and fix filter case sensitivity
- persist computed columns in sessions and fix session loading bugs

## 1.8.0 (2026-03-11)

### Feat

- menu UX — icons, labels, and separators
- session save/restore for multi-group sessions
- highlight menu enhancements — recolor, match counts, duplicate prevention
- column aggregations — press `a` to show count, distinct, sum, avg, min, max
- infer output format from file extension in W (write to file)
- explicit pipe-and-exit with Q, --tui flag, and docs update
- improved pipe support — interactive pipe mode, batch mode, and output formats (#35)
- column pinning — press m to freeze columns to the left during horizontal scrolling
- scan for matching lines when manually switching delimiter
- prompt to save custom log format before applying delimiter

### Fix

- column unpin breaking render positions for < and > movement
- use full terminal size when stdout is piped
- preserve preamble lines skipped by find_header_index on delimiter switch
- column selection by cursor index after filtering + splitting

## 1.7.0 (2026-03-10)

### Feat

- prompt to save custom log format before applying delimiter

## 1.6.0 (2026-03-10)

### Feat

- **Improved pipe support** — nless now works as a middle stage in Unix pipelines. Interactive pipe mode auto-writes the current buffer to stdout on quit; `--no-tui` batch mode applies CLI transforms (`-f`, `-s`, `-u`, `-c`) without opening the TUI; `--output-format` / `-o` controls output format (`csv`, `tsv`, `json`, `raw`)
- **Column pinning** — press `m` to pin/unpin columns to the left side of the screen; pinned columns stay visible during horizontal scrolling, with a visual separator and `P` label in the header
- auto-detect log formats with P, custom format persistence, and regex wizard docs
- regex delimiter builder wizard for unnamed capture groups
- add autocomplete suggestions to time window prompt

### Fix

- preserve preamble lines skipped by find_header_index on delimiter switch
- scan for matching lines when manually switching delimiter
- delimiter inference picks space over comma for CSV with spaces in data

## 1.5.0 (2026-03-09)

### Features

- **Raw pager mode** — `--raw` flag or auto-detected when no delimiter is found; renders unstructured text in a virtual-rendering ScrollView optimized for million-line files
- **Fast incremental raw loading** — raw mode uses chunked incremental loading, showing data immediately instead of buffering everything
- **Header detection** — files with leading non-tabular lines (e.g. a JSON preamble before CSV data) automatically skip to the correct header row
- **Pretty-printed JSON flattening** — multi-line JSON objects and arrays are collapsed into JSONL for tabular display
- **Delimiter consistency scoring** — improved delimiter inference with cross-line field count agreement, reducing false positives on source code and config files
- **Cross-version performance benchmarks** — automated performance history tracking across releases
- **Back pressure detection** — status bar indicators when the streaming pipeline is under load
- **Coalesced deferred rebuilds** — chained table rebuilds during streaming are batched for smoother updates
- Show delimiter history in autocomplete dropdown

### Performance

- Populate parsed-row cache during incremental loading instead of recomputing on scroll
- Optimize streaming rebuild pipeline to reduce redundant work

### Fixes

- `~` (unparsed logs) no longer crashes when the source buffer has been closed
- `@` time window ceiling resets correctly when switching windows
- Raw pager uses themed background color (`row_even_bg`) to visually distinguish from normal `less`
- Escape Rich markup in raw pager lines so tags like `[INFO]` render as text
- Remove zebra striping from raw pager mode for cleaner appearance

## 1.4.0 (2026-03-09)

*This was a short-lived release; its planned features shipped in v1.5.0 instead.*

## 1.3.0 (2026-03-06)

### Features

- **Arrival timestamps** — every row records when it was received; toggle the `_arrival` metadata column with `A`
- **Time window filtering** — press `@` to show only rows from the last N seconds/minutes/hours (e.g. `5m`, `1h`, `30s`); append `+` for rolling windows that continuously drop expired rows
- **CLI arguments** — `--tail` to start in tail mode, `--time-window`/`-w` to set a time window on startup, `--columns`/`-c` to filter visible columns with a regex
- **Excluded lines rework** — `~` now shows all excluded rows (both parse failures and filter removals, not just parse failures); chained `~` buffers accumulate exclusions from all ancestor buffers; unparsed buffer stays updated with streaming data
- **Smarter delimiter inference** — auto-switch when a mismatch is detected mid-stream; show skipped row count in status bar
- **Double-space delimiter** (`space+`) — `D` → `space+` or `  ` for data like kubectl output where single spaces appear within field values
- **Action queueing** — user actions (filter, sort, etc.) during a loading operation are queued and applied when loading completes, instead of being rejected
- Flash notification when a delimiter change clears active filters

### Fixes

- Prevent double lines when switching delimiter to raw
- Filters now match text that looks like Rich markup tags (e.g. `[INFO]`, `[error]`)
- JSON delimiter switch from raw/regex uses first data row as header instead of losing it
- `view_unparsed_logs` handles parse errors gracefully for JSON/CSV delimiters
- One-shot time window no longer re-evaluates; cursor no longer scrolls behind the header row; page up/down works correctly at boundaries

## 1.2.0 (2026-03-05)

### Features

- **Buffer groups** — open multiple files or shell commands in separate groups, switch with `{`/`}`
- **Open file** — press `O` to open a file from within the app with path autocomplete
- **Configurable keymaps** — 3 built-in presets (vim, less, emacs); custom keymaps via `~/.config/nless/keymaps/`; switch interactively with `K`
- **Theming system** — 10 built-in color themes (Dracula, Nord, Gruvbox, Solarized, Catppuccin, and more); custom themes via `~/.config/nless/themes/`; switch interactively with `T`
- **Customizable status bar** — format string with Rich markup and theme color variables in `config.json`
- **Config viewer** — press `?` and navigate to the Config tab to see current settings and file path
- **Stream status icons** — animated `⏵` for running commands, `✓` on completion, `📄` for files
- **Loading indicators** — spinner and progress feedback for heavy operations (filtering, sorting, pivoting)

### Performance

- Optimize sort pipeline for 100K+ row datasets

### Fixes

- Fix resize handling and status bar spacing
- Fix shell command stream race condition causing doubled lines

## 1.1.1 (2026-03-04)

### Fixes

- Auto-detect JSON delimiter for JSONL and JSON array input

## 1.1.0 (2026-03-04)

### Features

- **Pivot focused view** — pivoting now hides non-key columns for a cleaner summary; columns reappear automatically when new data streams in
- **Exclude filters** — press `e`/`E` to exclude rows matching a value (inverse of `f`/`F`)
- **Reset highlights** — press `x` to clear new-line highlighting after reviewing streamed data
- **Documentation site** — full docs at [mpryor.github.io/nothing-less](https://mpryor.github.io/nothing-less/)

## 1.0.1 (2026-03-04)

### Fixes

- Fix compatibility with newer versions of Textual

## 1.0.0 (2026-03-03)

Initial stable release.

- **Tabular data pager** with vi-like keybindings built on the Textual framework
- **Delimiter inference** — auto-detects CSV, TSV, pipe, space-aligned, and JSON formats
- **Regex delimiters** — parse unstructured data with `D` using Python named capture groups
- **Column delimiters** — split a column into sub-columns with `d` using JSON, regex, or string delimiters
- **JSON support** — `D` → `json` for object log lines; `J` to extract nested JSON fields as columns
- **Filtering** — `f`/`F` to filter by column, `|` to filter across all columns, `&` to apply search as filter
- **Searching** — `/` to search, `*` to search by cursor word, `n`/`p` to navigate matches
- **Sorting** — `s` to toggle ascending/descending sort with numeric-aware ordering
- **Pivoting** — `U` to group by column with a `count` column; ++enter++ to drill into grouped data
- **Streaming** — real-time data from stdin pipes with new-line highlighting and tail mode (`t`)
- **Shell commands** — `!` to run a command and pipe output into a new buffer
- **Multiple buffers** — `N` to create, `L`/`H` to switch, `1`–`9` to jump, `q` to close
- **Clipboard** — `y` to copy cell contents
- **Export** — `W` to write the current view to a file or stdout (`-`)
- **CLI flags** — `--delimiter`, `--filters`, `--exclude-filters`, `--unique`, `--sort-by`

## 0.7.0 (2026-03-03)

### Performance

- Move data processing and column-width computation off the main thread so the UI stays responsive during large loads
- Non-blocking heavy operations (filter, sort, pivot) with loading indicator
- Filter rows early during buffer copy instead of copying everything then filtering
- Subscribe to streams without replaying history, reducing memory and startup time
- Add automated performance regression tests with baseline tracking

## 0.6.0 (2025-10-08)

### Features

- **Numeric-aware sorting** — columns containing numbers sort numerically instead of lexicographically (e.g. `2` before `10`)

## 0.5.3 (2025-10-07)

### Fixes

- Fix config file initialization when the file is empty

## 0.5.2 (2025-10-07)

### Fixes

- Fix crash when initializing an empty config file

## 0.5.1 (2025-10-03)

### Fixes

- Fix external command (`!`) streaming not delivering output

## 0.5.0 (2025-10-02)

### Features

- Permanently dismiss the getting started screen with `Ctrl+c`

## 0.4.0 (2025-10-02)

### Features

- Shell command buffers (`!`) use the command as the buffer name instead of appending an index

## 0.3.0 (2025-10-02)

### Features

- Copy cell contents to clipboard with `y`

## 0.2.7 (2025-10-02)

### Fixes

- Fix getting started screen rendering — overflow scrolling and spacing

## 0.2.0 (2025-10-02)

### Features

- Run external shell commands with `!` and pipe output into a new buffer
- Add CI/CD pipeline with GitHub Actions
- Add ruff pre-commit hooks for formatting and linting
- Add `NlessSelect` filterable dropdown widget

## 0.1.12 (2025-09-30)

### Features

- Add `NlessSelect` filterable dropdown widget

## 0.1.11 (2025-09-29)

### Features

- CLI flags: `--delimiter`/`-d`, `--unique`/`-u`, `--filters`/`-f`, `--sort-by`/`-s`

## 0.1.10 (2025-09-25)

### Features

- **Column delimiters** — split a column into sub-columns with `d` using JSON, regex, or string delimiters

## 0.1.9

### Fixes

- Fix duplicate column indexes causing data position errors

## 0.1.7

### Features

- **JSON support** — `D` delimiter supports `json` for object log lines and full JSON files; column delimiter `d` supports JSON; `J` to extract nested JSON fields as columns
- Page up/down with `ctrl+u`/`ctrl+d` (`d` moved to column delimiter)
- `--version` flag
- Improved automatic buffer naming
- Select buffers by number with `1`–`9`
