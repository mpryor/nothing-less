<h1 align="center">nless</h1>

<p align="center">
  <strong>excel for your logs</strong> — pipe in **anything**, wrangle it into columns.
</p>

<p align="center">
  <img src="./docs/assets/demo.gif" width="800px" alt="nless demo — search, filter, sort, and pivot streaming K8s events"/>
</p>

<p align="center">
  <a href="https://pypi.org/project/nothing-less/"><img src="https://img.shields.io/pypi/v/nothing-less" alt="PyPI"/></a>
  <a href="https://pypi.org/project/nothing-less/"><img src="https://img.shields.io/pypi/pyversions/nothing-less" alt="Python"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"/></a>
  <a href="https://github.com/mpryor/nothing-less/actions/workflows/ci.yml"><img src="https://github.com/mpryor/nothing-less/actions/workflows/ci.yml/badge.svg" alt="CI"/></a>
</p>

<p align="center">
  <a href="https://mpryor.github.io/nothing-less/">Documentation</a> ·
  <a href="https://mpryor.github.io/nothing-less/tutorials/">Tutorials</a> ·
  <a href="https://mpryor.github.io/nothing-less/keybindings/">Keybindings</a> ·
  <a href="#installation">Install</a>
</p>

**nless** is a TUI pager for exploring and analyzing tabular data with vi-like keybindings, built on [Textual](https://textual.textualize.io/). Pipe in anything and nless infers the structure so you can filter, sort, pivot, and reshape without config. Works with CSV, TSV, JSON, logs, and any delimited output.

## Installation

```bash
pip install nothing-less
```

or

```bash
brew install mpryor/tap/nless
```

Requires Python 3.13+.

## Usage

```bash
kubectl get events -w | nless    # stream K8s events
cat access.log | nless           # explore log files
nless data.csv                   # open CSV directly
nless < output.json              # redirect a file
```

Press `?` inside nless to view all keybindings.

## Demos

<details>
<summary>CSV — open, search, filter, sort, pivot</summary>

<p align="center">
  <img src="./docs/assets/demo-csv.gif" width="800px" alt="nless CSV demo — open a file, search, filter, sort, and pivot"/>
</p>

</details>

<details>
<summary>JSON — pipe in API responses, auto-detect keys as columns</summary>

<p align="center">
  <img src="./docs/assets/demo-json.gif" width="800px" alt="nless JSON demo — auto-detect JSON keys as columns"/>
</p>

</details>

<details>
<summary>Regex — parse raw logs into columns with named capture groups</summary>

<p align="center">
  <img src="./docs/assets/demo-regex.gif" width="800px" alt="nless regex demo — parse raw logs into structured columns"/>
</p>

</details>

<details>
<summary>Pipe mode — use nless as a pipeline stage</summary>

<p align="center">
  <img src="./docs/assets/demo-pipe.gif" width="800px" alt="nless pipe mode — filter, transform, and output as JSON"/>
</p>

</details>

## Why nless?

I frequently need to explore streaming tabular data: server logs, kubectl output, CSV exports, CI pipelines. None of the existing tools had exactly the feature set I wanted, so I built nless.

- **Zero config** — pipe in anything and nless infers the structure.
- **Stream-native** — built for data that's still arriving, with streaming features like tail mode, arrival timestamps, and time window filtering.
- **Vi-native** — if you know Vim, nless should feel familiar. If you don't, choose from the other built-in keymaps or build your own. 
- **Mouse-friendly** — click column headers to sort, double-click pivot rows to drill in, right-click for context menus, navigate via the menu bar.
- **One tool, many formats** — CSV, TSV, JSON, space-aligned, regex, raw. Switch between them without leaving the pager.

## Features

- **Delimiter inference & swapping** — auto-detects CSV, TSV, space-aligned, JSON, and more. Swap with `D`, or use regex with named capture groups.
- **Filtering & searching** — filter by column, cell value, or search match. Exclude rows. Full-text search with match navigation.
- **Sorting & pivoting** — one-key sort on any column. Group by composite key with summary view and drill-in.
- **Streaming & tail mode** — live-updating as new data arrives, with arrival timestamps and time window filtering.
- **JSON & log parsing** — extract nested JSON fields into columns. Parse unstructured logs with regex.
- **Pipe mode** — use nless as a pipeline stage with `Q` to pipe and exit, or `--no-tui` for batch mode.

<details>
<summary>All features</summary>

- **Buffers** — mutating actions create a new buffer, letting you jump up and down your analysis history.
- **Delimiter swapping** — swap between CSV, TSV, space-aligned, JSON, regex with named capture groups, and raw mode with `D`.
- **Column delimiters** — split a column into more columns using JSON, regex, or string delimiters with `d`.
- **Filtering** — filter by column (`f`/`F`), exclude (`e`/`E`), across all columns (`|`), or from a search (`&`).
- **Sorting** — toggle ascending/descending sort on any column with `s`.
- **Searching** — search (`/`), search by cell value (`*`), navigate matches (`n`/`p`).
- **Pivoting** — group records by composite key with `U`, focused summary view, dive into grouped data with `enter`.
- **Column management** — show/hide columns (`C`), reorder columns (`<`/`>`).
- **JSON extraction** — promote nested JSON fields to columns with `J`.
- **Shell commands** — run a shell command and pipe its output into a new buffer with `!`.
- **Tail mode** — keep the cursor at the bottom as new data arrives with `t`.
- **Output** — write buffer contents to a file or stdout (`W`), copy cell values (`y`).
- **Themes** — 10 built-in color themes (Dracula, Nord, Gruvbox, etc.) plus custom theme support, switch with `T`.
- **Arrival timestamps** — every row records when it was received. Toggle the `_arrival` column with `A`.
- **Time window filtering** — show only recent rows with `@` (e.g. `5m`, `1h`). Append `+` for rolling windows.
- **Raw pager mode** — `--raw` or auto-detected. A fast virtual-rendering pager for unstructured text, handling million-line files without columnar overhead.
- **Excluded lines** — view lines that failed to parse or were removed by filters with `~`, with chained accumulation across buffers.
- **Pipe mode** — use nless as a pipeline stage. Interactive exploration with `Q` to pipe and exit, batch mode with `--no-tui`, or `--tui` to force interactive mode.
- **Mouse support** — click column headers to sort, double-click pivot rows to drill in, right-click for context menus, navigate via the menu bar.
- **Merge files** — combine multiple files into a single view with a `_source` column using `--merge`.

</details>

<details>
<summary>Full keybinding reference</summary>

**Buffers**:
- `[1-9]` - select the buffer at the corresponding index
- `L` - select the next buffer
- `H` - select the previous buffer
- `q` - close the current active buffer, or the program if all buffers are closed
- `Q` - pipe current buffer to stdout and exit immediately (pipe mode shortcut)
- `N` - create a new buffer from the original data
- `r` - rename the current buffer
- `M` - merge the current buffer with another buffer

**Groups**:
- `}` - switch to the next buffer group
- `{` - switch to the previous buffer group
- `R` - rename the current group
- `O` - open a file in a new buffer group

**Navigation**:
- `h` - move cursor left
- `l` - move cursor right
- `j` - move cursor down
- `k` - move cursor up
- `0` - jump to first column
- `$` - jump to final column
- `g` - jump to first row
- `G` - jump to final row
- `w` - move cursor right
- `b`/`B` - move cursor left
- `ctrl+u` - page up
- `ctrl+d` - page down
- `c` - select a column to jump the cursor to

**Column visibility**:
- `C` - prompt for a regex filter to selectively display columns, or `all` to see all columns
- `m` - pin or unpin the current column to the left side of the screen
- `>` - move the current column one to the right
- `<` - move the current column one to the left
- `A` - toggle the `_arrival` metadata column showing when each row was received

**Pivoting**:
- `U` - mark the selected column as part of a composite key to group records by, adding a `count` column pinned to the left
- `enter` - while over a composite key column, dive into the data behind the pivot

**Filtering**:
- `f` - filter the current column and prompt for a filter
- `F` - filter the current column by the highlighted cell
- `e` - exclude from the current column and prompt for a value
- `E` - exclude the current column by the highlighted cell
- `|` - filter ALL columns and prompt for a filter
- `&` - apply the current search as a filter across all columns
- `@` - set a time window to show only recent rows (e.g. `5m`, `1h`); append `+` for rolling

**Searching & Highlighting**:
- `/` - prompt for a search value and jump to the first match
- `*` - search all columns for the current highlighted cell value
- `n` - jump to the next match
- `p` - jump to previous match
- `+` - pin the current search as a persistent highlight
- `-` - navigate or manage pinned highlights

**Output**:
- `W` - prompt for a file to write the current buffer to (`-` writes to stdout)
- `y` - copy the contents of the currently highlighted cell to the clipboard

**Shell Commands**:
- `!` - run a shell command and pipe its output into a new buffer

**Tail Mode**:
- `t` - toggle tail mode
- `x` - reset new-line highlights

**Sessions & Views**:
- `S` - open the session menu (save, load, rename, delete)
- `v` - open the view menu (save, load, rename, delete)

**Themes & Keymaps**:
- `T` - open the theme selector
- `K` - open the keymap selector

**Excluded Lines**:
- `~` - view excluded lines (parse failures + filtered rows), with chained accumulation

**Sorting & Aggregations**:
- `s` - toggle ascending/descending sort on the current column
- `a` - show column aggregations (count, distinct, sum, avg, min, max)

**JSON**:
- `J` - select a JSON field under the current cell to add as a column

**Delimiter/file parsing**:
- `D` - swap the delimiter on the fly (common delimiters, regex with named capture groups, `raw`, `json`, or `  ` for double-space aligned output like kubectl)
- `d` - split a column into more columns using a columnar delimiter (`json`, regex with named capture groups, or any string)
- `P` - auto-detect a known log format and apply it as a regex delimiter

**Help**:
- `?` - show the help screen with all keybindings

**Mouse**:
- Left-click column headers to sort
- Double-click pivot rows to drill in
- Right-click cells, headers, tabs, or groups for context menus
- Click buffer tabs or group bar to switch
- Menu bar for mouse-driven access to all actions

See the [full keybinding reference](https://mpryor.github.io/nothing-less/keybindings/) and [tutorials](https://mpryor.github.io/nothing-less/tutorials/) for more.

</details>

## Contributing

Contributions are welcome! See the [contributing guidelines](CONTRIBUTING.md) for details.

## Alternatives

If nless doesn't have what you need, check out these other great tools:

| | [nless](https://github.com/mpryor/nothing-less) | [VisiData](https://www.visidata.org/) | [csvlens](https://github.com/YS-L/csvlens) | [lnav](https://github.com/tstack/lnav) | [Toolong](https://github.com/Textualize/toolong) |
|---|:---:|:---:|:---:|:---:|:---:|
| **Focus** | Tabular data pager | Data multitool | CSV viewer | Log navigator | Log viewer |
| **Language** | Python | Python | Rust | C++ | Python |
| Streaming / stdin | :white_check_mark: | :heavy_minus_sign: | :heavy_minus_sign: | :white_check_mark: | :white_check_mark: |
| Delimiter inference | :white_check_mark: | :heavy_minus_sign: | :heavy_minus_sign: | :x: | :x: |
| Vi keybindings | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x: |
| Filtering | :white_check_mark: | :white_check_mark: | :heavy_minus_sign: | :white_check_mark: | :x: |
| Sorting | :white_check_mark: | :white_check_mark: | :white_check_mark: | :heavy_minus_sign: | :x: |
| Pivoting / grouping | :white_check_mark: | :white_check_mark: | :x: | :heavy_minus_sign: | :x: |
| JSON parsing | :white_check_mark: | :white_check_mark: | :x: | :white_check_mark: | :heavy_minus_sign: |
| Log format detection | :white_check_mark: | :x: | :x: | :white_check_mark: | :heavy_minus_sign: |
| Regex column parsing | :white_check_mark: | :white_check_mark: | :x: | :white_check_mark: | :x: |
| Pipe mode | :white_check_mark: | :white_check_mark: | :heavy_minus_sign: | :heavy_minus_sign: | :x: |
| Raw text pager | :white_check_mark: | :heavy_minus_sign: | :x: | :white_check_mark: | :white_check_mark: |
| Themes | :white_check_mark: | :white_check_mark: | :heavy_minus_sign: | :white_check_mark: | :x: |
| SQL queries | :x: | :x: | :x: | :white_check_mark: | :x: |
| Python expressions | :x: | :white_check_mark: | :x: | :x: | :x: |
| Timestamp parsing | :x: | :x: | :x: | :white_check_mark: | :white_check_mark: |
| Multi-file merge | :white_check_mark: | :white_check_mark: | :x: | :white_check_mark: | :white_check_mark: |

:white_check_mark: full support · :heavy_minus_sign: partial · :x: not supported
