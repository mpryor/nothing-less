# Configuration

nless stores configuration and history files in `~/.config/nless/`.

## Config File

**Location:** `~/.config/nless/config.json`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `show_getting_started` | `bool` | `true` | Show the getting started modal on first launch |
| `theme` | `string` | `"default"` | Color theme name (built-in or custom) |

Example:

```json
{
    "show_getting_started": false,
    "theme": "dracula"
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

## History File

**Location:** `~/.config/nless/history.json`

Stores command input history (search terms, filter values, etc.) across sessions. This file is managed automatically — you don't need to edit it manually.

## Directory Structure

```
~/.config/nless/
├── config.json      # User preferences
├── history.json     # Input history
└── themes/          # Custom theme files
    └── my-theme.json
```

Both files are created automatically on first use. If a file is missing or contains invalid JSON, nless falls back to defaults.
