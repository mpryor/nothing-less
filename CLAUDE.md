# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**nless** is a Python TUI pager for exploring and analyzing tabular data with vi-like keybindings, built on the Textual framework. It reads from stdin, files, or shell command output.

## Working Style

- When fixing bugs, try the SIMPLEST approach first. If the first fix doesn't work after 2 attempts, stop and ask for guidance.
- Run `poetry run pytest -m "not perf"` after making changes to verify nothing is broken.

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

- **app.py** (`NlessApp`) — Main Textual App. Manages multiple buffers (tab-like), keybindings, actions (filter, search, sort, pivot, JSON extraction, shell commands).
- **buffer.py** (`NlessBuffer`) — Core state for each view: columns, filters, sorting, unique keys, search state, row data. Copy-on-write for history.
- **delimiter.py** — Delimiter inference and line splitting. Supports CSV, TSV, space-aligned, JSON, regex with named capture groups, and nested delimiters.
- **input.py** — `StdinLineStream` (non-blocking I/O with `select()`, files/pipes, JSON buffering) and `ShellCommandLineStream`. Publisher pattern with subscribers.

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

## Recording Demos

Demos are recorded with [VHS](https://github.com/charmbracelet/vhs) from `.tape` files in `docs/assets/`, then converted to WebP via `gif2webp`.

### Setup

VHS needs chromium, ffmpeg, and webp tools. On this WSL machine, the recording infra lives in `/tmp/` (not persistent across reboots):

1. **Chromium**: `npx @puppeteer/browsers install chromium@latest --path /tmp/chromium/chromium`
2. **Symlinks in `/tmp/vhs-bin/`**: chromium, ffmpeg, ttyd, plus `kubectl` → `scripts/fake-kubectl` and `nless` → `scripts/fake-nless`
3. **webp**: `brew install webp` (provides `gif2webp`)

### Recording

```bash
# Record all tapes and convert to WebP
./scripts/record-demos.sh

# Record a single tape
./scripts/record-demos.sh docs/assets/demo.tape
```

The script handles: VHS recording (GIF) → `gif2webp` conversion → cleanup of intermediate GIF.

### How it works

- `scripts/fake-kubectl` dispatches to `scripts/fake-k8s-events.py` (simulates streaming K8s events)
- `scripts/fake-nless` wraps `poetry run nless --demo -t monokai` (shows caption overlays for each action)
- VHS symlinks in `/tmp/vhs-bin/` let the tape reference `kubectl` and `nless` by name
- VHS doesn't support WebP natively, so tapes output GIF and `scripts/record-demos.sh` converts to WebP

### Tape files

- `docs/assets/demo.tape` — hero demo (K8s events: search, filter, sort, pivot)
- `docs/assets/demo-csv.tape` — CSV demo
- `docs/assets/demo-deep-dive.tape` — deep-dive demo (drill into pod logs)
- `docs/assets/demo-json.tape` — JSON demo
- `docs/assets/demo-regex.tape` — regex parsing demo
- `docs/assets/demo-pipe.tape` — pipe mode demo

## Roadmap & Project Management

The GitHub repo tracks all planned work:

- **Project board**: [nless Roadmap](https://github.com/users/mpryor/projects/2) (linked to this repo)
- **Issues**: All feature requests and bugs are tracked as GitHub issues with labels (`priority: critical/high/medium/low`, `area: *`)
- **Milestones**: Issues are organized into release milestones (check via `gh api repos/mpryor/nothing-less/milestones`)

### "What's next?" workflow

When asked "what's next?", follow this process:

1. Run `gh project item-list 2 --owner mpryor --format json` to check the project board for items marked "In Progress"
2. If something is in progress, resume that work
3. If nothing is in progress, find the current milestone via `gh api repos/mpryor/nothing-less/milestones --jq '.[] | select(.open_issues > 0) | {title, open_issues}' | head -5`
4. List issues in that milestone sorted by priority: `gh issue list --milestone "<milestone>" --label "priority: critical" --state open` (then high, medium, low)
5. Pick the highest priority open issue that has no unresolved dependencies
6. Move it to "In Progress" on the project board and begin work
