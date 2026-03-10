# Keybindings

nless uses vi-like keybindings. Press `?` inside the app to view this reference.

## Buffers

| Key | Action |
|-----|--------|
| `1`–`9` | Select the buffer at the corresponding index |
| `L` | Select the next buffer |
| `H` | Select the previous buffer |
| `q` | Close the current active buffer, or quit if all buffers are closed |
| `N` | Create a new buffer from the original data |
| `r` | Rename the current buffer |

## Groups

| Key | Action |
|-----|--------|
| `}` | Switch to the next buffer group |
| `{` | Switch to the previous buffer group |
| `R` | Rename the current group |
| `O` | Open a file in a new buffer group |

## Navigation

| Key | Action |
|-----|--------|
| `h` | Move cursor left |
| `l` | Move cursor right |
| `j` | Move cursor down |
| `k` | Move cursor up |
| `0` | Jump to first column |
| `$` | Jump to final column |
| `g` | Jump to first row |
| `G` | Jump to final row |
| `w` | Move cursor right |
| `b` / `B` | Move cursor left |
| `ctrl+u` | Page up |
| `ctrl+d` | Page down |
| `c` | Select a column to jump the cursor to |

## Column Visibility

| Key | Action |
|-----|--------|
| `C` | Prompt for a regex filter to selectively display columns, or `all` to see all columns |
| `m` | Pin or unpin the current column to the left side of the screen — pinned columns stay visible during horizontal scrolling |
| `>` | Move the current column one to the right |
| `<` | Move the current column one to the left |
| `A` | Toggle the `_arrival` metadata column showing when each row was received |

## Pivoting

| Key | Action |
|-----|--------|
| `U` | Mark the selected column as part of a composite key to group records by, adding a `count` column pinned to the left |
| `enter` | While over a composite key column, dive into the data behind the pivot |

When a pivot is active, the view focuses on just the key columns and `count`. If new data streams in, all columns are automatically revealed so you can see the full row detail alongside the updated counts.

## Filtering

| Key | Action |
|-----|--------|
| `f` | Filter the current column and prompt for a filter |
| `F` | Filter the current column by the highlighted cell |
| `e` | Exclude from the current column and prompt for a value |
| `E` | Exclude the current column by the highlighted cell |
| `\|` | Filter ALL columns and prompt for a filter |
| `&` | Apply the current search as a filter across all columns |
| `@` | Set a time window to show only recent rows (e.g. `5m`, `1h`, `30s`). Append `+` for rolling mode (e.g. `5m+`) |

## Searching

| Key | Action |
|-----|--------|
| `/` | Prompt for a search value and jump to the first match |
| `*` | Search all columns for the current highlighted cell value |
| `n` | Jump to the next match |
| `p` | Jump to previous match |

## Sorting

| Key | Action |
|-----|--------|
| `s` | Toggle ascending/descending sort on the current column |

## Output

| Key | Action |
|-----|--------|
| `W` | Prompt for a file to write the current buffer to (`-` writes to stdout) |
| `y` | Copy the contents of the currently highlighted cell to the clipboard |

## Shell Commands

| Key | Action |
|-----|--------|
| `!` | Run a shell command and pipe its output into a new buffer |

## Delimiter / File Parsing

| Key | Action |
|-----|--------|
| `D` | Swap the delimiter on the fly (common delimiters, regex with named capture groups, `raw`, `json`, or `  ` for double-space aligned output like kubectl) |
| `d` | Split a column into more columns using a columnar delimiter (`json`, regex with named capture groups, or any string) |
| `P` | Auto-detect a known log format (syslog, Apache, Spring Boot, etc.) and apply it as a regex delimiter |

## JSON

| Key | Action |
|-----|--------|
| `J` | Select a JSON field under the current cell to add as a column |

## Tail Mode

| Key | Action |
|-----|--------|
| `t` | Toggle tail mode — keep the cursor at the bottom as new data arrives |
| `x` | Reset new-line highlights — clear the highlighting on streamed rows |

## Excluded Lines

| Key | Action |
|-----|--------|
| `~` | View excluded lines — rows that failed to parse or were removed by filters. Chained `~` buffers accumulate exclusions from all ancestors |

## Themes & Keymaps

| Key | Action |
|-----|--------|
| `T` | Open the theme selector to switch color schemes |
| `K` | Open the keymap selector to switch keybinding presets |

## Help

| Key | Action |
|-----|--------|
| `?` | Show the help screen with all keybindings |
