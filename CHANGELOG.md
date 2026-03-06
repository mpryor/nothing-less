# Changelog

## 1.4.0 (2026-03-06)

### Feat

- show history in delimiter autocomplete dropdown

### Fix

- ~ operator with deleted buffers and @ time window ceiling

## 1.3.0 (2026-03-06)

### Feat

- add --tail, --time-window, and --columns CLI arguments
- ~ shows all excluded logs (filters + parse failures, not just parse)
- chained ~ buffers exclude lines from all ancestor delimiters
- unparsed buffer (~) stays updated with streaming data
- ~ creates a new raw buffer from unparsed logs instead of a popup screen
- show skipped row count in status bar, suppress repeated mismatch warnings
- smarter delimiter inference with auto-switch on mismatch
- delimiter fixes, empty-input guards, space+ option, status bar
- queue user actions during data loading instead of rejecting them
- flash message when delimiter change clears active filters
- arrival timestamps, time window filter, and rolling window

### Fix

- prevent double lines when switching delimiter to raw
- filters now match text that looks like Rich markup tags (e.g. [INFO])
- update 5 stale tests to match current behavior
- catch parse errors in view_unparsed_logs for JSON/CSV delimiters
- JSON delimiter switch from raw/regex uses first data row as header
- one-shot time window, cursor scroll behind header, page up/down

### Refactor

- extract mixins from god objects, fix bugs, and improve test suite
- improve state management and fix code smells across codebase
- consolidate delimiter auto-switch logic and fix review findings
- extract shared auto-switch method, remove redundant state

## 1.2.0 (2026-03-05)

### Features

- **Buffer groups** — open multiple files or shell commands in separate groups, switch with `{`/`}`
- **Open file** — press `O` to open a file from within the app with path autocomplete
- **Configurable keymaps** — ship with vim, less, and emacs presets; custom keymaps via `~/.config/nless/keymaps/`; switch with `K`
- **Theming system** — 10 built-in themes with custom theme support via `~/.config/nless/themes/`; switch with `T`
- **Customizable status bar** — format string with Rich markup and theme color variables
- **Config viewer** — press `?` and navigate to the Config tab to see current settings
- **Stream status icons** — animated `⏵` for running commands, `✓` on completion, `📄` for files
- **Loading indicators** — spinner and progress feedback for heavy operations (filtering, sorting)

### Performance

- Optimize sort pipeline for 100K+ rows

### Fixes

- Fix resize handling and status bar spacing
- Fix shell command stream race condition causing doubled lines

### Refactor

- Extract data processing, operations, status bar, and unparsed logs into separate modules

## 1.1.1 (2026-03-04)

### Fixes

- Auto-detect JSON delimiter for JSONL and JSON array input

## 1.1.0 (2026-03-04)

### Features

- **Pivot focused view** — pivoting now hides non-key columns for a cleaner summary; columns reappear when new data streams in
- **Exclude filters** — press `e`/`E` to exclude rows matching a value (inverse of `f`/`F`)
- **Reset highlights** — press `x` to clear new-line highlighting after reviewing streamed data
- **Documentation site** — full docs at [mpryor.github.io/nothing-less](https://mpryor.github.io/nothing-less/)

## 1.0.1 (2026-03-04)

### Fixes

- Fix compatibility with newer versions of Textual

## 1.0.0 (2026-03-03)

Initial stable release with all MVP features.

## 0.7.0 (2026-03-03)

### Performance

- Move data processing and column-width computation off the main thread
- Non-blocking heavy operations with loading indicator
- Filter early on buffer copy and subscribe without replay
- Add automated performance regression tests

## 0.6.0 (2025-10-08)

### Features

- Numeric-aware sorting — columns with numbers sort numerically instead of lexicographically

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
- Add `NlessSelect` widget with type-based completion and arrow selection

## 0.1.12 (2025-09-30)

### Features

- Add `NlessSelect` filterable dropdown widget

## 0.1.11 (2025-09-29)

### Features

- CLI flags: `--delimiter`/`-d`, `--unique`/`-u`, `--filters`/`-f`, `--sort-by`/`-s`

## 0.1.10 (2025-09-25)

### Features

- Column delimiters — split a column into sub-columns with `d` using JSON, regex, or string delimiters

## 0.1.9

### Fixes

- Fix duplicate column indexes causing data position errors

## 0.1.7

### Features

- **JSON support** — `D` delimiter supports `json` for object log lines and full JSON files; column delimiter `d` supports JSON; `J` to extract JSON fields as columns
- Page up/down with `ctrl+u`/`ctrl+d` (`d` moved to column delimiter)
- `--version` flag
- Improved automatic buffer naming
- Select buffers by number with `1`–`9`
