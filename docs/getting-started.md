# Getting Started

## Dependencies

- Python >= 3.13 (for pip install)
- OR [Homebrew](https://brew.sh/) (for brew install)

## Installation

=== "pip"

    ```bash
    pip install nothing-less
    ```

=== "brew"

    ```bash
    brew install mpryor/tap/nless
    ```

## Usage

Pipe the output of a command to nless:

```bash
kubectl get pods -w | nless
```

Read a file directly:

```bash
nless data.csv
```

Redirect a file into nless:

```bash
nless < access.log
```

Once data is loaded, press `?` to view all keybindings.

## CLI Arguments

| Argument | Short | Description |
|----------|-------|-------------|
| `filename` | | File to read input from (defaults to stdin) |
| `--version` | | Show version and exit |
| `--delimiter` | `-d` | Delimiter to use for splitting fields |
| `--filters` | `-f` | Initial filter(s), format: `column=value` or `any=value`. Can be repeated. |
| `--exclude-filters` | `-x` | Initial exclude filter(s), same format as `-f`. Can be repeated. |
| `--unique` | `-u` | Initial unique key(s). Can be repeated. |
| `--sort-by` | `-s` | Column to sort by initially, format: `column=asc` or `column=desc` |
| `--theme` | `-t` | Color theme to use (e.g. `dracula`, `nord`, `monokai`) |
| `--keymap` | `-k` | Keymap preset to use (e.g. `vim`, `less`, `emacs`) |
| `--tail` | | Start in tail mode (cursor follows new data) |
| `--time-window` | `-w` | Show only rows within a time window (e.g. `5m`, `1h`, `30s`). Append `+` for rolling (e.g. `5m+`) |
| `--columns` | `-c` | Regex to filter visible columns on startup (e.g. `name\|status`) |

### Examples

Start with a specific delimiter:

```bash
nless -d ',' data.txt
```

Pre-filter data on load:

```bash
nless -f 'status=Running' -f 'namespace=default' pods.txt
```

Exclude rows matching a pattern:

```bash
nless -x 'severity=DEBUG' app.log
```

Sort by a column on load:

```bash
nless -s 'timestamp=desc' events.csv
```

Pivot by unique keys:

```bash
nless -u 'namespace' -u 'status' pods.txt
```

Start in tail mode with a rolling time window:

```bash
kubectl get events -w | nless --tail -w '5m+'
```

Show only specific columns:

```bash
nless -c 'name|status|age' pods.txt
```

Combine options with piped input:

```bash
kubectl get pods -w | nless -d '  ' -f 'STATUS=Running' -s 'NAME=asc'
```
