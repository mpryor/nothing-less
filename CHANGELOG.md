# Changelog
## 0.2.7 (2025-10-02)

### Fix

- getting-started-screen rendering - overflow scrolling, and spacing (#16)

## 0.2.6 (2025-10-02)

### Fix

- github release tagging to match cz tagging pattern

## 0.2.5 (2025-10-02)

### Fix

- markdown output in workflow
- changelog message for GH release

## 0.2.4 (2025-10-02)

### Fix

- remove v from changelog version

## 0.2.3 (2025-10-02)

### Fix

- remove env vars for twine

## 0.2.2 (2025-10-02)

### Fix

- add environment

## 0.2.1 (2025-10-02)

### Fix

- python version in GH workflow

## 0.2.0 (2025-10-02)

### Feat

- run external commands with ! (#15)
- add github workflow
- adds ruff pre-commit hooks for formatting and linting
- added a new select widget, NlessSelect, which offers type-based completion and arrow selection

### Fix

- cz adds, commits, and tags for us
- run git status for debugging
- move git config earlier in the flow
- testing cz

## 0.1.12 (2025-09-30)

### Feat

- added a new select widget, NlessSelect, which offers type-based completion (#14)

## 0.1.11 (2025-09-29)

### Feat

- CLI flags for delimiter, unique, filters, and sort (#13)

## 0.1.10 (2025-09-25)

### Feat

- arbitrary column delimiters (#12)

## 0.1.9
- Fix bug with duplicate column indexes
## 0.1.8 - duplicate of 0.1.7 for pypi release
## 0.1.7
- JSON support
  - `D` delimiter supports `json`, now:
    - will convert json object log lines into columns
    - will parse full json files
  - column delimiters with `d` supporting json
  - `J` command to select json fields as columns
- Ctrl-d and Ctrl-u for paging (`d` moved to column delimiter command)
- `--version` command
- Better automatic pane naming
- Pressing numeric keys (1-9) focuses corresponding "buffer"
