# Configuration

nless stores configuration and history files in `~/.config/nless/`.

## Directory Structure

```
~/.config/nless/
├── config.json          # User preferences
├── history.json         # Input history (managed automatically)
├── sessions/            # Saved sessions (managed via S menu)
├── views/               # Saved views (managed via v menu)
├── log_formats.json     # Custom log format patterns for P auto-detection
├── themes/              # Custom theme files
│   └── my-theme.json
└── keymaps/             # Custom keymap files
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
| `status_format` | `string` | `"{sort} \| {filter} \| {search} \| {position} \| {unique}{time_window}{skipped}{pipe}{tailing}{loading}{behind}"` | Status bar format string |

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
| `row_even_bg` | Even row background; also used as raw pager background |
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
| `app.add_highlight` | `+` | Pin search as highlight |
| `app.navigate_highlight` | `-` | Navigate pinned highlights |
| `app.filter` | `f` | Filter column |
| `app.filter_cursor_word` | `F` | Filter by cursor word |
| `app.exclude_filter` | `e` | Exclude from column |
| `app.exclude_filter_cursor_word` | `E` | Exclude by cursor word |
| `app.filter_any` | `\|` | Filter all columns |
| `app.search_to_filter` | `&` | Apply search as filter |
| `app.time_window` | `@` | Time window filter |

#### Columns & Data

| ID | Default (vim) | Action |
|----|---------------|--------|
| `buffer.jump_columns` | `c` | Jump to column |
| `app.filter_columns` | `C` | Show/hide columns |
| `buffer.move_column_right` | `>` | Move column right |
| `buffer.move_column_left` | `<` | Move column left |
| `app.toggle_arrival` | `A` | Toggle arrival timestamps |
| `buffer.sort` | `s` | Sort column |
| `buffer.aggregations` | `a` | Show column aggregations |
| `app.mark_unique` | `U` | Mark column as pivot key |
| `app.delimiter` | `D` | Change delimiter |
| `app.column_delimiter` | `d` | Split column |
| `app.detect_log_format` | `P` | Auto-detect log format |
| `app.json_header` | `J` | Extract JSON key |

#### Buffers & Groups

| ID | Default (vim) | Action |
|----|---------------|--------|
| `app.add_buffer` | `N` | New buffer |
| `app.show_tab_next` | `L` | Next buffer |
| `app.show_tab_previous` | `H` | Previous buffer |
| `app.close_active_buffer` | `q` | Close buffer / quit |
| `app.pipe_and_exit` | `Q` | Quit immediately (pipe & exit) |
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
| `buffer.view_unparsed_logs` | `~` | View excluded lines |
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
| `{time_window}` | `Window: 5m` or empty |
| `{delimiter}` | `Delim: csv` or empty |
| `{skipped}` | `Skipped: 42` or empty |
| `{session}` | `Session: my-session` or empty |
| `{pipe}` | `⇥ Pipe (150 rows) · Q to send` or empty |
| `{behind}` | `⚠` when input is arriving faster than processing, or empty |
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

## Custom Log Formats

**Location:** `~/.config/nless/log_formats.json`

Define custom log format patterns for the `P` (auto-detect log format) feature. Custom formats are checked before built-in formats and have higher priority, so they will be preferred when they match your data.

### Creating custom formats

There are two ways to add custom log formats:

1. **From within nless** — press `D`, enter a regex with named capture groups, and when prompted, enter a name to save it. The format is saved automatically.

2. **Edit the file directly** — create or edit `~/.config/nless/log_formats.json`:

```json title="~/.config/nless/log_formats.json"
[
  {
    "name": "My App Log",
    "pattern": "(?P<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) \\[(?P<service>\\w+)\\] (?P<level>\\w+): (?P<message>.*)"
  },
  {
    "name": "Internal Audit",
    "pattern": "AUDIT (?P<action>\\w+) (?P<user>\\S+) (?P<resource>\\S+) (?P<result>\\w+)"
  }
]
```

Each entry requires:

| Key | Required | Description |
|-----|----------|-------------|
| `name` | Yes | Display name shown in notifications and the status bar delimiter field |
| `pattern` | Yes | Python regex with at least one named capture group (`(?P<name>...)`) |
| `priority` | No | Tiebreaker for detection scoring (default: `100`). Higher values are preferred when multiple formats match equally |

Patterns must be valid Python regular expressions. Entries with invalid regex or no named groups are silently skipped.

### Built-in formats

nless ships with 19 built-in log format patterns that `P` can detect:

| Format | Example |
|--------|---------|
| Apache/nginx Combined | `93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /path HTTP/1.1" 200 2326 "http://ref" "Mozilla"` |
| Apache/nginx Common | Same without referer/useragent |
| Syslog RFC 5424 | `<165>1 2023-08-24T05:14:15Z host app 1234 ID47 - Message` |
| Syslog RFC 3164 | `Jan  5 14:23:01 myhost sshd[12345]: message` |
| NGINX Error | `2024/01/15 14:23:01 [error] 12345#0: message` |
| AWS CloudWatch/Lambda | `2024-01-15T14:23:01.123Z request-id INFO message` |
| Spring Boot / Logback | `2024-01-15T14:23:01.123+00:00  INFO 12345 --- [main] c.e.MyApp : message` |
| Ruby/Rails Logger | `I, [2024-01-15T14:23:01.123 #12345]  INFO -- app: message` |
| Laravel / Monolog | `[2024-01-15 14:23:01] production.ERROR: message` |
| Rust env_logger | `[2024-01-15T14:23:01Z INFO  myapp::server] message` |
| .NET Core Logger | `info: Microsoft.Hosting[14] message` |
| Go Log (stdlib) | `2024/01/15 14:23:01 message` |
| Logrus / slog Text | `time="2024-01-15T14:23:01Z" level=info msg="message"` |
| Elixir Logger | `14:23:01.123 [info] message` |
| Python Logging Default | `WARNING:root:message` |
| Python Logging Dash | `2024-01-15 14:23:01,123 - myapp - INFO - message` |
| ISO 8601 + Level + Logger | `2024-01-15 14:23:01,123 INFO com.example.Main message` |
| ISO 8601 + Level | `2024-01-15T14:23:01 INFO message` |
| Bracket Timestamp + Level | `[2024-01-15 14:23:01] [INFO] message` |

---

## Sessions

**Location:** `~/.config/nless/sessions/`

Sessions capture your full workspace state — all buffer groups, their filters, sort order, column visibility, highlights, delimiter, time windows, and more — so you can pick up exactly where you left off. Each session is stored as an individual JSON file (e.g. `my-session.json`). Sessions are tied to a data source — when you reopen the same file, nless can auto-restore the matching session.

**Managed via the `S` menu** — save, load, or delete sessions interactively. You don't need to edit these files manually.

When you open a file that matches a saved session's data source, nless prompts you to restore it automatically.

Each session stores:

| Field | Description |
|-------|-------------|
| `name` | Session name (user-provided) |
| `groups` | List of buffer groups, each with a name, data source, and per-buffer state |
| `active_group_idx` | Which group was active when saved |
| `created_at` | ISO 8601 timestamp |
| `updated_at` | ISO 8601 timestamp (auto-updated on save) |

Per-buffer state includes: delimiter, sort column/direction, filters, column order/visibility/pinned, unique keys, regex highlights, time window, tail mode, search term, and cursor position.

Sessions are sorted by most recently updated first.

You can load a session directly from the CLI:

```bash
nless --session my-session file.csv
nless -S my-session file.csv
```

---

## Views

**Location:** `~/.config/nless/views/`

Views capture a **single buffer's analysis settings** — filters, sort order, column visibility, highlights, computed columns, and more — as a reusable template. Unlike sessions, views are **not tied to a specific data source**. You can save a view while analyzing one file and apply it to a completely different dataset.

This makes views ideal for reusable analysis patterns: "show me only errors sorted by timestamp", "hide all columns except name and status", etc.

**Managed via the `v` menu** — save, load, rename, or delete views interactively.

Each view stores:

| Field | Description |
|-------|-------------|
| `name` | View name (user-provided) |
| `state` | Single buffer state: delimiter, sort, filters, columns, highlights, computed columns, unique keys, time window |
| `created_at` | ISO 8601 timestamp |
| `updated_at` | ISO 8601 timestamp (auto-updated on save) |

Views are sorted by most recently updated first.

**Undo:** When you load a view, nless snapshots the buffer's current state. Select **Undo last view** from the `v` menu to restore it — including any data that was compacted by the view's filters.

### Sessions vs. Views

| | Sessions (`S`) | Views (`v`) |
|---|---|---|
| **Scope** | Entire workspace (all buffer groups) | Single buffer |
| **Data source** | Tied to a specific file or command | Portable across any data |
| **Auto-restore** | Prompted when reopening a matching file | Never — always manual |
| **Use case** | "Pick up where I left off on this file" | "Apply this analysis pattern to any data" |
| **Undo** | No | Yes — restores previous buffer state |

---

## History File

**Location:** `~/.config/nless/history.json`

Stores command input history (search terms, filter values, etc.) across sessions. This file is managed automatically — you don't need to edit it manually.
