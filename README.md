# Nothing-less (nless)

<p align="center">
  <img src="./docs/assets/nless-logo.png" width="400px" alt="nless logo"/>
</p>

[![PyPI](https://img.shields.io/pypi/v/nothing-less)](https://pypi.org/project/nothing-less/)
[![Python](https://img.shields.io/pypi/pyversions/nothing-less)](https://pypi.org/project/nothing-less/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/mpryor/nothing-less/actions/workflows/ci.yml/badge.svg)](https://github.com/mpryor/nothing-less/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-mpryor.github.io%2Fnothing--less-blue)](https://mpryor.github.io/nothing-less/)

**Nless** is a TUI paging application (based on the awesome [Textual](https://textual.textualize.io/) library) with vi-like keybindings.

> **[Read the full documentation](https://mpryor.github.io/nothing-less/)** — tutorials, keybinding reference, configuration, and more.

Nless has enhanced functionality for parsing tabular data:
- inferring file delimiters
- delimiter swapping on the fly
- regex-based parsing of raw logs into tabular data using Python's regex engine
- filtering
- sorting
- searching
- real-time event parsing.

## Getting started
### Dependencies
- python>=3.13
OR
- [brew](https://brew.sh/)
### Installation
`pip install nothing-less`
OR
`brew install mpryor/tap/nless`
### Usage
- pipe the output of a command to nless to parse the output `$COMMAND | nless`
- read a file with nless `nless $FILE_NAME`
- redirect a file into nless `nless < $FILE_NAME`
- Once output is loaded, press `?` to view the keybindings

## Demos
### Basic functionality
The below demo shows basic functionality:
- starting with a search `/`
- applying that search `&`
- filtering the selected column by the value within the selected cell `F`
- swapping the delimiter `D` (`raw` and `,`)

[![asciicast](https://asciinema.org/a/k8MOUx01XxnK7Lo9iTcM9QOpg.svg)](https://asciinema.org/a/k8MOUx01XxnK7Lo9iTcM9QOpg)

### Streaming functionality
The below demo showcases some of nless's features for handling streaming input, and interacting with unknown delimitation:
- The nless view stays up-to-date as new log lines arrive on stdin (allows pipeline commands, or redirecting a file into nless)
- Showcases using a custom (Python engine) regex, example - `{(?P<severity>.*)}\((?P<user>.*)\) - (?P<message>.*)` - to parse raw logs into tabular fields.
- Sorts, filters, and searches on those fields.
- Flips the delimiter back to raw, sorts, searches, and filters on the raw logs

[![asciicast](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5.svg)](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5)

## Why nless?

As a kubernetes engineer, I frequently need to interact with streaming tabular data. `k get pods -w`, `k get events -w`, etc. I wanted a TUI tool to quickly dissect and analyze this data - and none of the existing alternatives had exactly what I wanted. So I decided to build my own tool, integrating some of my favorite features from other similar tools.

This project is not meant to replace any of the tools mentioned in the [alternatives](#alternatives) section. Instead, it's meant to bring its own unique set of features to complement your workflow:

- **Streaming support** - stay up-to-date as new data arrives on stdin
- **Delimiter inference** - no configuration needed; nless infers the delimiter from your data
- **Vi-like keybindings** - familiar to any Vim user, minimize keypresses to analyze a dataset
- **Kubernetes-friendly** - built for K8s use-cases like parsing streams from kubectl
- **Tabular data toolkit** - filter, sort, search, pivot, and reshape data on the fly
- **JSON & log parsing** - convert unstructured data streams into tabular data

## Features

- **Buffers** - mutating actions create a new buffer, letting you jump up and down your analysis history (`r` to rename)
- **Buffer groups** - open multiple files or shell command outputs side-by-side, switch between groups with `{`/`}`, open new files with `O`
- **Delimiter swapping** - swap between CSV, TSV, space-aligned, JSON, regex with named capture groups, and raw mode on the fly with `D`
- **Column delimiters** - split a column into more columns using JSON, regex, or string delimiters with `d`
- **Filtering** - filter by column (`f`/`F`), exclude (`e`/`E`), across all columns (`|`), or from a search (`&`)
- **Sorting** - toggle ascending/descending sort on any column with `s`
- **Searching** - search (`/`), search by cell value (`*`), navigate matches (`n`/`p`)
- **Pivoting** - group records by composite key with `U`, focused summary view, dive into grouped data with `enter`
- **Column management** - show/hide columns (`C`), reorder columns (`<`/`>`)
- **JSON extraction** - promote nested JSON fields to columns with `J`
- **Shell commands** - run a shell command and pipe its output into a new buffer with `!`
- **Tail mode** - keep the cursor at the bottom as new data arrives with `t`
- **Output** - write buffer contents to a file or stdout (`W`), copy cell values (`y`)
- **Themes & keymaps** - 10 built-in color themes (Dracula, Nord, Gruvbox, etc.) plus custom theme support (`T`), configurable keymaps (`K`)
- **Unparsed lines** - view lines that didn't match the current delimiter with `~`

<details>
<summary>Full keybinding reference</summary>

**Buffers**:
- `[1-9]` - select the buffer at the corresponding index
- `L` - select the next buffer
- `H` - select the previous buffer
- `q` - close the current active buffer, or the program if all buffers are closed
- `N` - create a new buffer from the original data
- `r` - rename the current buffer

**Groups**:
- `}` - switch to the next buffer group
- `{` - switch to the previous buffer group
- `R` - rename the current group
- `O` - open a file in a new group

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
- `>` - move the current column one to the right
- `<` - move the current column one to the left

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

**Searching**:
- `/` - prompt for a search value and jump to the first match
- `*` - search all columns for the current highlighted cell value
- `n` - jump to the next match
- `p` - jump to previous match

**Output**:
- `W` - prompt for a file to write the current buffer to (`-` writes to stdout)
- `y` - copy the contents of the currently highlighted cell to the clipboard

**Shell Commands**:
- `!` - run a shell command and pipe its output into a new buffer

**Tail Mode**:
- `t` - toggle tail mode
- `x` - reset new-line highlights

**Appearance**:
- `T` - open the theme selector
- `K` - open the keymap selector

**Unparsed Logs**:
- `~` - view logs that did not match the current delimiter

**Sorting**:
- `s` - toggle ascending/descending sort on the current column

**JSON**:
- `J` - select a JSON field under the current cell to add as a column

**Delimiter/file parsing**:
- `D` - swap the delimiter on the fly (common delimiters, regex with named capture groups, `raw`, `json`, or `  ` for double-space aligned output like kubectl)
- `d` - split a column into more columns using a columnar delimiter (`json`, regex with named capture groups, or any string)

**Help**:
- `?` - show the help screen with all keybindings

See the [full keybinding reference](https://mpryor.github.io/nothing-less/keybindings/) and [tutorials](https://mpryor.github.io/nothing-less/tutorials/) for more.

</details>

## Contributing
Contributions are welcome! Please open an issue or a pull request - check out the [contributing guidelines](CONTRIBUTING.md) for more information.

## Alternatives
Shout-outs to all of the below wonderful tools! If my tool doesn't have what you need, they likely will:
- [visidata](https://www.visidata.org/)
- [csvlens](https://github.com/YS-L/csvlens)
- [lnav](https://github.com/tstack/lnav)
- [toolong](https://github.com/Textualize/toolong)
