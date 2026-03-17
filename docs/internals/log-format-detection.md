# Log Format Detection

## Overview

Pressing `P` auto-detects log formats by matching sample lines against
known patterns (Apache, nginx, syslog, etc.) and optionally inferring a
regex from the data structure itself.

## Known format matching

`detect_log_formats()` in `logformats.py` tests each sample line against
a library of compiled regex patterns. Each format has named capture groups
that become columns (e.g. `(?P<remote_addr>...)`, `(?P<timestamp>...)`).

Scoring: `(match_ratio * 100) + (num_named_groups * 2)`. A format that
matches 90% of lines with 8 named groups scores 116. The highest-scoring
format wins; ties show a selection menu.

## Inferred pattern detection

When no known format matches, `infer_log_pattern()` attempts to build a
regex from the data:

### Token classification (`_classify_token`)

Each "word" in a sample line is classified as one of:

| Type | Examples | Regex fragment |
|------|----------|---------------|
| IPv4 | `192.168.1.1` | `\d+\.\d+\.\d+\.\d+` |
| ISO timestamp | `2024-01-15T10:30:00` | `\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}` |
| Syslog timestamp | `Jan 15 10:30:00` | `\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}` |
| Log level | `INFO`, `ERROR` | `[A-Z]+` |
| Numeric | `200`, `3.14` | `\d+` or `\d+\.\d+` |
| Bracketed | `[error]`, `[pid 123]` | `\[[^\]]+\]` |
| Quoted | `"GET /path"` | `"[^"]*"` |
| Word | `apache`, `myapp` | `\S+` |

### Regex construction (`_build_named_regex`)

1. Tokenize the first sample line
2. Classify each token
3. Build a regex with named groups: `(?P<field_1>...)(?P<field_2>...)`
4. Use literal separators (spaces, brackets, quotes) between groups
5. Test against all sample lines — if match ratio is high, accept

### Name inference

Group names are derived from token type and position:
- IPv4 → `ip` or `remote_addr`
- Timestamp → `timestamp`
- Log level → `level`
- Quoted strings → `request`, `referrer`, `user_agent` (by position)
- Numeric after quoted request → `status`, `bytes`

### Custom format saving

If the user accepts an inferred pattern, they can save it with a name.
Saved formats go to `~/.config/nless/logformats/` and are loaded on
startup alongside builtins.

## Key files

- `logformats.py` — `detect_log_formats()`, `infer_log_pattern()`,
  `_classify_token()`, `_build_named_regex()`, `LogFormat` dataclass,
  builtin patterns, custom format I/O
- `app.py` — `action_detect_log_format()`, `_apply_log_format()`
