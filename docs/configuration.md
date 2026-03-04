# Configuration

nless stores configuration and history files in `~/.config/nless/`.

## Config File

**Location:** `~/.config/nless/config.json`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `show_getting_started` | `bool` | `true` | Show the getting started modal on first launch |

Example:

```json
{
    "show_getting_started": false
}
```

## History File

**Location:** `~/.config/nless/history.json`

Stores command input history (search terms, filter values, etc.) across sessions. This file is managed automatically — you don't need to edit it manually.

## Directory Structure

```
~/.config/nless/
├── config.json     # User preferences
└── history.json    # Input history
```

Both files are created automatically on first use. If a file is missing or contains invalid JSON, nless falls back to defaults.
