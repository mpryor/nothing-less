# Configuration

nless stores configuration and history files in `~/.config/nless/`.

## Config File

**Location:** `~/.config/nless/config.json`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `show_getting_started` | `bool` | `true` | Show the getting started modal on first launch |
| `theme` | `string` | `"default"` | Color theme name (built-in or custom) |
| `keymap` | `string` | `"vim"` | Keymap preset name (built-in or custom) |
| `status_format` | `string` | `"{sort} \| {filter} \| {search} \| {position} {unique}{tailing}{loading}"` | Status bar format string |

Example:

```json
{
    "show_getting_started": false,
    "theme": "dracula",
    "keymap": "less"
}
```

## Themes

nless ships with 10 built-in color themes and supports custom themes.

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

### Selecting a Theme

There are three ways to set a theme:

1. **CLI flag** (highest priority):
    ```bash
    nless --theme dracula file.csv
    nless -t nord file.csv
    ```

2. **Interactive selector** — press `T` inside the app to pick a theme. Your selection is saved to config automatically.

3. **Config file** — set `"theme"` in `~/.config/nless/config.json` (see above).

### Custom Themes

Create JSON files in `~/.config/nless/themes/` to define your own themes. Each file must have a `"name"` key. All other keys are optional — omitted keys inherit from the default theme.

```json
{
    "name": "my-theme",
    "cursor_bg": "#ff5555",
    "cursor_fg": "#f8f8f2",
    "header_bg": "#44475a",
    "header_fg": "#f8f8f2",
    "highlight": "#50fa7b",
    "accent": "#bd93f9"
}
```

Available color slots: `cursor_bg`, `cursor_fg`, `header_bg`, `header_fg`, `fixed_column_bg`, `row_odd_bg`, `row_even_bg`, `col_odd_fg`, `col_even_fg`, `scrollbar_bg`, `scrollbar_fg`, `search_match_bg`, `search_match_fg`, `highlight`, `accent`, `status_tailing`, `status_loading`, `muted`, `border`, `brand`.

## Keymaps

nless ships with 3 built-in keymap presets and supports custom keymaps.

### Built-in Keymaps

| Keymap | Description |
|--------|-------------|
| `vim` | Vi-like keybindings (default) |
| `less` | Keybindings matching less(1) conventions |
| `emacs` | Ctrl/Alt-based Emacs keybindings |

### Selecting a Keymap

There are three ways to set a keymap:

1. **CLI flag** (highest priority):
    ```bash
    nless --keymap less file.csv
    nless -k emacs file.csv
    ```

2. **Interactive selector** — press `K` inside the app to pick a keymap. Your selection is saved to config automatically.

3. **Config file** — set `"keymap"` in `~/.config/nless/config.json` (see above).

### Custom Keymaps

Create JSON files in `~/.config/nless/keymaps/` to define your own keymaps. Each file must have a `"name"` key. Use `"extends"` to inherit from a built-in preset (default: `"vim"`). The `"bindings"` object maps binding IDs to key strings.

```json
{
    "name": "my-keymap",
    "extends": "vim",
    "bindings": {
        "app.search": "ctrl+slash",
        "table.page_down": "space"
    }
}
```

Binding IDs use the format `component.action_name` (e.g. `app.search`, `buffer.sort`, `table.cursor_down`).

## Status Bar Format

The status bar format can be customized using a format string with named variables:

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
| `{unique}` | `\| unique cols: (col1,col2)` or empty |
| `{tailing}` | `\| Tailing` (themed) or empty |
| `{loading}` | `\| ⠋ Loading (1,000 rows)` (themed) or empty |
| `{keymap}` | `vim` |
| `{theme}` | `dracula` |
| `{<color_slot>}` | Any theme color slot (e.g. `{accent}`, `{muted}`, `{cursor_bg}`, `{header_fg}`, etc.) |

Inactive sections (`{sort}`, `{filter}`, `{search}`) produce empty strings, and any resulting consecutive `|` separators (including Rich-markup-wrapped ones) are automatically cleaned up.

Rich markup is supported. Use theme color variables inside markup tags to get colors that automatically follow your theme:

```json
{
    "status_format": "[{cursor_fg}]{sort}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{filter}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{search}[/{cursor_fg}] [{muted}]|[/{muted}] [{cursor_fg}]{position}[/{cursor_fg}] {unique}{tailing}{loading}"
}
```

## History File

**Location:** `~/.config/nless/history.json`

Stores command input history (search terms, filter values, etc.) across sessions. This file is managed automatically — you don't need to edit it manually.

## Directory Structure

```
~/.config/nless/
├── config.json      # User preferences
├── history.json     # Input history
├── themes/          # Custom theme files
│   └── my-theme.json
└── keymaps/         # Custom keymap files
    └── my-keymap.json
```

Both files are created automatically on first use. If a file is missing or contains invalid JSON, nless falls back to defaults.
