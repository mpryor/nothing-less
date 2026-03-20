# Keybindings

nless uses vi-like keybindings. Press `?` inside the app to view this reference.

## Buffers

| Key | Action |
|-----|--------|
| `1`–`9` | Select the buffer at the corresponding index |
| `L` | Select the next buffer |
| `H` | Select the previous buffer |
| `q` | Close the current active buffer, or quit if all buffers are closed. When stdout is a pipe, the current buffer is auto-written to stdout on quit. |
| `Q` | Quit immediately — exit without closing tabs one-by-one. In pipe mode, pipes the current buffer to stdout. |
| `N` | Create a new buffer from the original data |
| `r` | Rename the current buffer |
| `M` | Merge the current buffer with another buffer — creates a new tab with interleaved rows and a `_source` column |

## Groups

| Key | Action |
|-----|--------|
| `}` | Switch to the next buffer group |
| `{` | Switch to the previous buffer group |
| `R` | Rename the current group |
| `O` | Open a file in a new buffer group |

> **Tip:** You can also pass multiple files from the command line — `nless file1.csv file2.csv` opens each as a separate group.

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
| `X` | Hide the current column from the view |
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

## Searching & Highlighting

| Key | Action |
|-----|--------|
| `/` | Prompt for a search value and jump to the first match |
| `*` | Search all columns for the current highlighted cell value |
| `n` | Jump to the next match |
| `p` | Jump to previous match |
| `+` | Pin the current search as a persistent highlight — opens a color picker to choose the highlight color. Duplicate patterns are blocked. Press `+` with no active search to clear all highlights. |
| `-` | Select a pinned highlight to navigate between its matches with `n`/`p`, select the 🎨 option to recolor it, or select the 🗑 option to remove it. Each highlight shows its match count. |

## Sorting

| Key | Action |
|-----|--------|
| `s` | Toggle ascending/descending sort on the current column |
| `a` | Show aggregations for the current column (count, distinct, sum, avg, min, max) |

## Output

| Key | Action |
|-----|--------|
| `W` | Prompt for a file to write the current buffer to — format is inferred from extension (`.csv`, `.tsv`, `.json`, `.jsonl`, `.txt`, `.log`); `-` writes CSV to stdout |
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

## Sessions & Views

| Key | Action |
|-----|--------|
| `S` | Open the session menu — save, load, rename, or delete sessions. Sessions capture your entire workspace (all buffer groups and their state) tied to a specific data source. When opening a file that matches a saved session, nless prompts to restore it. |
| `v` | Open the view menu — save, load, rename, or delete views. Views capture a single buffer's analysis settings (filters, sort, columns, highlights) and can be applied to any dataset. Undo restores the buffer to its previous state. |

## Help

| Key | Action |
|-----|--------|
| `?` | Show the help screen with all keybindings |
| `#` | Show the caption overlay — displays action descriptions inline as you press keys |

## Ex Mode

Press `:` to open the ex-mode command prompt. Ex mode supports the following commands:

| Command | Alias | Action |
|---------|-------|--------|
| `s/pat/rep/` | | Substitute pattern in the current column |
| `s/pat/rep/g` | | Substitute pattern in all columns |
| `sort <col>` | | Sort by column name |
| `filter <col> <pat>` | `f` | Filter column by pattern |
| `exclude <col> <pat>` | `e` | Exclude matches from column |
| `w [path]` | `write` | Write buffer to file (prompts if no path) |
| `o <path>` | `open` | Open file in a new group |
| `q` | `quit` | Close buffer or quit |
| `q!` | `quit!` | Pipe to stdout and exit |
| `set theme <name>` | | Switch theme |
| `set keymap <name>` | | Switch keymap |
| `delim <d>` | `delimiter` | Change delimiter |
| `help` | | Show the help screen |

---

## Mouse Interactions

nless supports mouse interactions alongside keyboard controls.

### Click Actions

| Target | Click | Action |
|--------|-------|--------|
| Column header | Left-click | Sort by that column (click again to reverse) |
| Pivot row | Double-click | Drill into the data behind the pivot (same as `enter`) |
| Buffer tab | Left-click | Switch to that buffer |
| Group bar | Left-click | Switch to that buffer group |
| Help hint | Left-click | Open the help screen |
| Menu bar item | Left-click | Open the menu dropdown |

### Hover Effects

| Target | Effect |
|--------|--------|
| Column header | Header cell highlights on hover |
| Group bar | Group label highlights on hover |
| Menu bar | Switches to hovered menu when a dropdown is already open |

### Scroll

| Action | Effect |
|--------|--------|
| Scroll up | Move cursor up 3 rows |
| Scroll down | Move cursor down 3 rows |

### Context Menus (Right-Click)

Right-click different elements to open a context menu with relevant actions.

**Data cell:**

| Action |
|--------|
| Copy cell |
| Search cursor word |
| Filter by value |
| Exclude value |
| Add highlight |
| Sort column |

**Column header:**

| Action |
|--------|
| Sort column |
| Pin/unpin column |
| Hide column |
| Move left / Move right |
| Split column |
| Pivot |

**Buffer tab:**

| Action |
|--------|
| Rename buffer |
| Close buffer |

**Group bar:**

| Action |
|--------|
| Rename group |
| Close group |

---

## Menu Bar

The menu bar provides mouse-driven access to all major actions. Click a menu label to open it, or hover to switch between menus when one is already open. Each menu item shows its keyboard shortcut.

### File

| Item |
|------|
| Open file |
| New buffer |
| Rename buffer |
| Merge buffers |
| Rename group |
| Write to file |
| Run command |
| Close buffer |

### View

| Item |
|------|
| Show/hide columns |
| Jump to column |
| Toggle arrival timestamps |
| Toggle tail mode |
| Column aggregations |
| View excluded lines |
| Reset highlights |
| Select theme |
| Select keymap |
| Sessions |
| Views |

### Data

| Item |
|------|
| Change delimiter |
| Split column |
| Extract JSON key |
| Auto-detect log format |
| Time window |

### Search

| Item |
|------|
| Search |
| Next match |
| Previous match |
| Search to filter |
| Filter column |
| Exclude from column |
| Filter all columns |
| Add highlight |
| Navigate highlights |
