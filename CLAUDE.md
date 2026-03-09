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

# Run tests excluding slow perf tests
poetry run pytest -m "not perf"

# Run only perf tests
poetry run pytest -m perf
```

Test files cover CLI arg parsing, buffer operations, delimiter inference/splitting, input stream handling, and performance regression detection (`test_perf.py`).

## Architecture

### Data Flow

```
Input (stdin/file/command) → StdinLineStream (async, threaded)
    → NlessApp (manages buffers & UI) → NlessBuffer (state & transforms)
    → delimiter.split_line() (parse rows) → DataTable or RawPager (render)
```

### Key Modules

- **cli.py** — Entry point (`main()`). Parses CLI args, sets up input stream on a background thread, launches the Textual app. Redirects `/dev/tty` for terminal input when reading piped stdin.
- **app.py** (`NlessApp`) — Main Textual App. Manages multiple buffers (tab-like), keybindings, actions (filter, search, sort, pivot, JSON extraction, shell commands).
- **buffer.py** (`NlessBuffer`) — Core state for each view: columns, filters, sorting, unique keys, search state, row data. Uses `copy()` for buffer duplication (copy-on-write pattern for history).
- **delimiter.py** — Delimiter inference (`infer_delimiter()`) and line splitting (`split_line()`). Supports CSV, TSV, space-aligned, JSON, regex with named capture groups, and nested delimiters.
- **input.py** — `StdinLineStream` (non-blocking I/O with `select()`, handles files and pipes, JSON-specific buffering) and `ShellCommandLineStream`. Publisher pattern with subscribers.
- **types.py** — Core dataclasses: `Filter`, `CliArgs`, `Column`, `MetadataColumn`.
- **datatable.py** — Custom performance-optimized ScrollView-based table rendering.
- **rawpager.py** (`RawPager`) — ScrollView-based raw text pager with virtual rendering for unstructured/raw mode. Same interface as Datatable so NlessBuffer can swap transparently.
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

## Roadmap & Project Management

The GitHub repo tracks all planned work:

- **Project board**: [nless Roadmap](https://github.com/users/mpryor/projects/2) (linked to this repo)
- **Issues**: All feature requests and bugs are tracked as GitHub issues with labels (`priority: critical/high/medium/low`, `area: *`)
- **Milestones**: Issues are organized into release milestones:
  - `v1.2 — Foundation` — Core infrastructure (bug fixes, raw pager, arrival timestamp, pipe support, column pinning)
  - `v1.3 — Analysis & Streaming` — Data analysis and log tooling (highlights, log formats, aggregations, multi-stream, saved views)
  - `v1.4 — Extensibility & UX` — Config profiles, session persistence, alerting, mouse support
  - `v2.0 — Platform` — nless as a framework (multi-pane, data editing, joins)

### "What's next?" workflow

When asked "what's next?", follow this process:

1. Run `gh project item-list 2 --owner mpryor --format json` to check the project board for items marked "In Progress"
2. If something is in progress, resume that work
3. If nothing is in progress, find the current milestone via `gh api repos/mpryor/nothing-less/milestones --jq '.[] | select(.open_issues > 0) | {title, open_issues}' | head -5`
4. List issues in that milestone sorted by priority: `gh issue list --milestone "<milestone>" --label "priority: critical" --state open` (then high, medium, low)
5. Pick the highest priority open issue that has no unresolved dependencies
6. Move it to "In Progress" on the project board and begin work
