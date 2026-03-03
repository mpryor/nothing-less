# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**nless** is a Python TUI pager for exploring and analyzing tabular data with vi-like keybindings, built on the Textual framework. It reads from stdin, files, or shell command output.

## Development Commands

```bash
# Install dependencies
poetry install

# Run the app
poetry run nless < file.csv
cat file.txt | poetry run nless
poetry run nless file.txt

# Lint and format (ruff via pre-commit)
poetry run pre-commit run --all-files

# Install pre-commit hooks
poetry run pre-commit install

# Run tests
poetry run pytest
```

Test files cover CLI arg parsing, buffer operations, delimiter inference/splitting, and input stream handling.

## Architecture

### Data Flow

```
Input (stdin/file/command) → StdinLineStream (async, threaded)
    → NlessApp (manages buffers & UI) → NlessBuffer (state & transforms)
    → delimiter.split_line() (parse rows) → DataTable (render)
```

### Key Modules

- **cli.py** — Entry point (`main()`). Parses CLI args, sets up input stream on a background thread, launches the Textual app. Redirects `/dev/tty` for terminal input when reading piped stdin.
- **app.py** (`NlessApp`) — Main Textual App. Manages multiple buffers (tab-like), keybindings, actions (filter, search, sort, pivot, JSON extraction, shell commands).
- **buffer.py** (`NlessBuffer`) — Core state for each view: columns, filters, sorting, unique keys, search state, row data. Uses `copy()` for buffer duplication (copy-on-write pattern for history).
- **delimiter.py** — Delimiter inference (`infer_delimiter()`) and line splitting (`split_line()`). Supports CSV, TSV, space-aligned, JSON, regex with named capture groups, and nested delimiters.
- **input.py** — `StdinLineStream` (non-blocking I/O with `select()`, handles files and pipes, JSON-specific buffering) and `ShellCommandLineStream`. Publisher pattern with subscribers.
- **types.py** — Core dataclasses: `Filter`, `CliArgs`, `Column`, `MetadataColumn`.
- **datatable.py** — Custom performance-optimized ScrollView-based table rendering.
- **config.py** — User preferences stored in `~/.config/nless/` as JSON.
- **autocomplete.py** (`AutocompleteInput`) — Custom Input widget with command history navigation.
- **help.py** (`HelpScreen`) — Keybindings help screen.
- **gettingstarted.py** (`GettingStartedScreen`) — First-run getting started modal.
- **nlessselect.py** (`NlessSelect`) — Custom filterable select/dropdown widget.
- **version.py** — Version retrieval via `importlib.metadata`.

### Patterns

- **Publisher-Subscriber**: LineStream notifies app of new data via callbacks
- **Copy-on-Write**: Buffers duplicate on mutation to preserve history
- **Threading**: I/O runs on daemon threads; UI stays responsive on main thread

## Conventions

- Python 3.13+ required
- Conventional Commits for commit messages (commitizen enforced)
- Ruff for linting and formatting
- Poetry for dependency management
- Textual CSS in `nless/nless.tcss`
