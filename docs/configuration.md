# Configuration

nless stores configuration and history files in `~/.config/nless/`.

## Directory Structure

```
~/.config/nless/
├── config.json      # User preferences
├── history.json     # Input history (managed automatically)
├── themes/          # Custom theme files
│   └── my-theme.json
└── keymaps/         # Custom keymap files
    └── my-keymap.json
```

All files are created automatically on first use. If a file is missing or contains invalid JSON, nless falls back to defaults.

## Config File

**Location:** `~/.config/nless/config.json`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `show_getting_started` | `bool` | `true` | Show the getting started modal on first launch |
| `theme` | `string` | `"default"` | Color theme name (built-in or custom) |
| `keymap` | `string` | `"vim"` | Keymap preset name (built-in or custom) |
| `status_format` | `string` | `"{sort} \| {filter} \| {search} \| {position} \| {unique}{tailing}{loading}"` | Status bar format string |

Example:

```json
{
    "show_getting_started": false,
    "theme": "dracula",
    "keymap": "less"
}
```

---

## Themes

nless ships with 10 built-in color themes and supports custom themes. There are three ways to set a theme, listed by priority:

1. **CLI flag** (highest priority):
    ```bash
    nless --theme dracula file.csv
    nless -t nord file.csv
    ```
    The CLI flag overrides the saved config, so you can try a theme without changing your default.

2. **Interactive selector** — press `T` inside the app to pick a theme. Your selection is saved to config automatically.

3. **Config file** — set `"theme"` in `~/.config/nless/config.json`.

### Built-in Themes

| Theme | Description |
|-------|-------------|
| `default` | Green accents on dark background |
| `dracula` | Dracula color scheme |
| `monokai` | Monokai Pro colors |
| `nord` | Cool blue/teal palette |
| `solarized-dark` | Solarized dark mode |
| `solarized-light` | Solarized light mode |
| `gruvbox` | Warm retro groove colors |
| `tokyo-night` | Modern dark blues/purples |
| `catppuccin-mocha` | Dark mode with pastels |
| `catppuccin-latte` | Light mode with pastels |

### Custom Themes

Create a JSON file in `~/.config/nless/themes/` to define your own theme:

```bash
mkdir -p ~/.config/nless/themes
```

```json title="~/.config/nless/themes/ocean.json"
{
    "name": "ocean",
    "cursor_bg": "#264f78",
    "cursor_fg": "#e0e0e0",
    "header_bg": "#1b3a5c",
    "header_fg": "#c8dce8",
    "row_odd_bg": "#0d1b2a",
    "row_even_bg": "#1b2838",
    "highlight": "#00d4aa",
    "accent": "#5dadec",
    "border": "#5dadec",
    "brand": "#5dadec"
}
```

Only `"name"` is required. Omitted color slots inherit from the default theme. Custom themes appear in the `T` selector immediately.

### Color Slots Reference

| Slot | Controls |
|------|----------|
| `cursor_bg` | Selected row background |
| `cursor_fg` | Selected row text |
| `header_bg` | Column header bar background |
| `header_fg` | Column header bar text |
| `fixed_column_bg` | Pinned column background (e.g. `count` in pivots) |
| `row_odd_bg` | Odd row background |
| `row_even_bg` | Even row background |
| `col_odd_fg` | Odd column text color |
| `col_even_fg` | Even column text color |
| `scrollbar_bg` | Scrollbar track |
| `scrollbar_fg` | Scrollbar thumb |
| `search_match_bg` | Search result highlight background |
| `search_match_fg` | Search result highlight text |
| `highlight` | New-line highlighting for streaming data |
| `accent` | UI accents (borders, active indicators) |
| `status_tailing` | "Tailing" indicator in the status bar |
| `status_loading` | Loading/filtering indicator in the status bar |
| `muted` | De-emphasized text (separators, inactive elements) |
| `border` | Border color for UI elements |
| `brand` | Brand accent color |

---

## Keymaps

nless ships with 3 built-in keymap presets and supports custom keymaps. There are three ways to set a keymap, listed by priority:

1. **CLI flag** (highest priority):
    ```bash
    nless --keymap less file.csv
    nless -k emacs file.csv
    ```

2. **Interactive selector** — press `K` inside the app to pick a keymap. Your selection is saved to config automatically.

3. **Config file** — set `"keymap"` in `~/.config/nless/config.json`.

### Built-in Keymaps

| Keymap | Description |
|--------|-------------|
| `vim` | Vi-like keybindings (default) — `h`/`j`/`k`/`l` navigation, `/` search, `f` filter |
| `less` | Matches less(1) conventions — `space` pages down, `b` pages up, `h` opens help |
| `emacs` | Ctrl/Alt-based — `ctrl+n`/`ctrl+p` navigation, `ctrl+s` search, `alt+f` filter |

### Custom Keymaps

Create a JSON file in `~/.config/nless/keymaps/` to define your own keymap:

```bash
mkdir -p ~/.config/nless/keymaps
```

```json title="~/.config/nless/keymaps/custom.json"
{
    "name": "custom",
    "extends": "vim",
    "bindings": {
        "app.search": "ctrl+slash",
        "table.page_down": "space",
        "table.page_up": "shift+space",
        "app.filter": "ctrl+f"
    }
}
```

- **`name`** — required, appears in the `K` selector
- **`extends`** — base preset to inherit from (`vim`, `less`, or `emacs`). Defaults to `vim`. You only need to specify the bindings you want to change.
- **`bindings`** — maps binding IDs to key strings. Use commas to bind multiple keys: `"space,f"`.

### Binding IDs Reference

#### Navigation

| ID | Default (vim) | Action |
|----|---------------|--------|
| `table.cursor_down` | `j` / `down` | Move cursor down |
| `table.cursor_up` | `k` / `up` | Move cursor up |
| `table.cursor_right` | `l` / `w` | Move cursor right |
| `table.cursor_left` | `h` / `b` / `B` | Move cursor left |
| `table.page_down` | `ctrl+d` | Page down |
| `table.page_up` | `ctrl+u` | Page up |
| `table.scroll_top` | `g` | Jump to first row |
| `table.scroll_bottom` | `G` | Jump to last row |
| `table.scroll_to_beginning` | `0` | Jump to first column |
| `table.scroll_to_end` | `$` | Jump to last column |

#### Searching & Filtering

| ID | Default (vim) | Action |
|----|---------------|--------|
| `app.search` | `/` | Search |
| `buffer.search_cursor_word` | `*` | Search cursor word |
| `buffer.next_search` | `n` | Next search match |
| `buffer.previous_search` | `p` | Previous search match |
| `app.filter` | `f` | Filter column |
| `app.filter_cursor_word` | `F` | Filter by cursor word |
| `app.exclude_filter` | `e` | Exclude from column |
| `app.exclude_filter_cursor_word` | `E` | Exclude by cursor word |
| `app.filter_any` | `\|` | Filter all columns |
| `app.search_to_filter` | `&` | Apply search as filter |

#### Columns & Data

| ID | Default (vim) | Action |
|----|---------------|--------|
| `buffer.jump_columns` | `c` | Jump to column |
| `app.filter_columns` | `C` | Show/hide columns |
| `buffer.move_column_right` | `>` | Move column right |
| `buffer.move_column_left` | `<` | Move column left |
| `buffer.sort` | `s` | Sort column |
| `app.mark_unique` | `U` | Mark column as pivot key |
| `app.delimiter` | `D` | Change delimiter |
| `app.column_delimiter` | `d` | Split column |
| `app.json_header` | `J` | Extract JSON key |

#### Buffers & Groups

| ID | Default (vim) | Action |
|----|---------------|--------|
| `app.add_buffer` | `N` | New buffer |
| `app.show_tab_next` | `L` | Next buffer |
| `app.show_tab_previous` | `H` | Previous buffer |
| `app.close_active_buffer` | `q` | Close buffer / quit |
| `app.rename_buffer` | `r` | Rename buffer |
| `app.show_group_next` | `}` | Next group |
| `app.show_group_previous` | `{` | Previous group |
| `app.rename_group` | `R` | Rename group |
| `app.open_file` | `O` | Open file |

#### Output & Misc

| ID | Default (vim) | Action |
|----|---------------|--------|
| `app.write_to_file` | `W` | Write to file |
| `buffer.copy` | `y` | Copy cell |
| `app.run_command` | `!` | Run shell command |
| `buffer.toggle_tail` | `t` | Toggle tail mode |
| `buffer.reset_highlights` | `x` | Reset new-line highlights |
| `buffer.view_unparsed_logs` | `~` | View unparsed lines |
| `app.select_theme` | `T` | Select theme |
| `app.select_keymap` | `K` | Select keymap |
| `app.help` | `?` | Show help |

---

## Status Bar Format

The status bar format can be customized using a format string with named variables in `~/.config/nless/config.json`:

| Variable | Example output |
|----------|---------------|
| `{sort}` | `Sort: col0 asc` or empty when inactive |
| `{filter}` | `Filter: col0='val'` or empty when inactive |
| `{search}` | `Search: 'term' (1 / 5 matches)` or empty when inactive |
| `{position}` | `Row: 42/1000 Col: 3/10` |
| `{row}` | `42` |
| `{rows}` | `1000` |
| `{col}` | `3` |
| `{cols}` | `10` |
| `{unique}` | `Unique: col1,col2` or empty |
| `{tailing}` | `\| Tailing` (themed) or empty |
| `{loading}` | `\| ⠋ Filtering 1,000 rows` or `\| ✔ Filtered 1,000 → 500 rows` (themed) or empty |
| `{keymap}` | `vim` |
| `{theme}` | `dracula` |
| `{<color_slot>}` | Any theme color slot (e.g. `{accent}`, `{muted}`, `{cursor_bg}`, `{header_fg}`, etc.) |

Inactive sections (`{sort}`, `{filter}`, `{search}`) produce empty strings, and any resulting consecutive `|` separators (including Rich-markup-wrapped ones) are automatically cleaned up.

Rich markup is supported. Use theme color variables inside markup tags to get colors that automatically follow your theme:

```json
{
    "status_format": "[{cursor_fg}]{sort}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{filter}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{search}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{position}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{unique}[/{cursor_fg}]{tailing}{loading}"
}
```

---

## History File

**Location:** `~/.config/nless/history.json`

Stores command input history (search terms, filter values, etc.) across sessions. This file is managed automatically — you don't need to edit it manually.
