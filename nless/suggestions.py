import os
import stat
from abc import ABC, abstractmethod
from pathlib import Path
from threading import Thread


class SuggestionProvider(ABC):
    """Base class for providing autocomplete suggestions."""

    @abstractmethod
    def get_suggestions(self, value: str) -> list[str]:
        """Return suggestions matching the given input value."""


class HistorySuggestionProvider(SuggestionProvider):
    """Provides suggestions from command history with substring matching.

    Prefix matches appear first, then substring matches. Case-insensitive.
    """

    def __init__(self, history: list[str]) -> None:
        self.history = history

    def get_suggestions(self, value: str) -> list[str]:
        if not value:
            return list(reversed(self.history))
        lower = value.lower()
        prefix = []
        substring = []
        for item in self.history:
            item_lower = item.lower()
            if item_lower.startswith(lower):
                prefix.append(item)
            elif lower in item_lower:
                substring.append(item)
        return prefix + substring


class StaticSuggestionProvider(SuggestionProvider):
    """Provides suggestions from a fixed list of options.

    Prefix matches appear first, then substring matches. Case-insensitive.
    Shows all options when input is empty.  History items (if provided) are
    included and appear before static options in results.
    """

    MAX_RESULTS = 20

    def __init__(self, options: list[str], history: list[str] | None = None) -> None:
        static_set = set(options)
        # History items not already in static options, most recent first
        unique_history = list(dict.fromkeys(reversed(history or [])))
        self.history = [h for h in unique_history if h not in static_set]
        self.options = options

    def get_suggestions(self, value: str) -> list[str]:
        all_items = self.history + self.options
        if not value:
            return all_items[: self.MAX_RESULTS]
        lower = value.lower()
        prefix = []
        substring = []
        for item in all_items:
            item_lower = item.lower()
            if item_lower.startswith(lower):
                prefix.append(item)
            elif lower in item_lower:
                substring.append(item)
        return (prefix + substring)[: self.MAX_RESULTS]


class PipeSeparatedSuggestionProvider(SuggestionProvider):
    """Provides suggestions for pipe-separated multi-value input.

    Matches against the portion after the last '|', excludes already-selected
    values, and returns full prefixed suggestions.
    """

    MAX_RESULTS = 20

    def __init__(self, options: list[str]) -> None:
        self.options = options

    def get_suggestions(self, value: str) -> list[str]:
        if "|" in value:
            prefix = value.rsplit("|", 1)[0] + "|"
            partial = value.rsplit("|", 1)[1]
            already_selected = {
                v.strip().lower() for v in prefix.split("|") if v.strip()
            }
        else:
            prefix = ""
            partial = value
            already_selected = set()

        available = [o for o in self.options if o.lower() not in already_selected]

        if not partial:
            return [prefix + o for o in available][: self.MAX_RESULTS]

        lower = partial.lower()
        starts = []
        contains = []
        for item in available:
            item_lower = item.lower()
            if item_lower.startswith(lower):
                starts.append(prefix + item)
            elif lower in item_lower:
                contains.append(prefix + item)
        return (starts + contains)[: self.MAX_RESULTS]


class ColumnValueSuggestionProvider(SuggestionProvider):
    """Provides suggestions from unique values in a data table column.

    Shows most frequent values first when input is empty.
    Prefix matches appear first, then substring matches. Case-insensitive.
    Cap at 20 results.
    """

    MAX_RESULTS = 20

    def __init__(self, values: list[str]) -> None:
        self.values = values

    def get_suggestions(self, value: str) -> list[str]:
        if not value:
            return self.values[: self.MAX_RESULTS]
        lower = value.lower()
        prefix = []
        substring = []
        for item in self.values:
            item_lower = item.lower()
            if item_lower.startswith(lower):
                prefix.append(item)
            elif lower in item_lower:
                substring.append(item)
            if len(prefix) + len(substring) >= self.MAX_RESULTS:
                break
        return (prefix + substring)[: self.MAX_RESULTS]


class FilePathSuggestionProvider(SuggestionProvider):
    """Provides filesystem path completions.

    If input ends with '/', lists directory contents.
    Otherwise, matches partial filenames in the parent directory.
    Cap at 20 results.
    """

    MAX_RESULTS = 20

    def get_suggestions(self, value: str) -> list[str]:
        if not value:
            # List current directory contents
            try:
                entries = sorted(Path(".").iterdir())
                return [str(e) + ("/" if e.is_dir() else "") for e in entries][
                    : self.MAX_RESULTS
                ]
            except PermissionError:
                return []
        path = Path(value)
        try:
            if value.endswith("/"):
                # List directory contents
                if path.is_dir():
                    entries = sorted(path.iterdir())
                    return [str(e) + ("/" if e.is_dir() else "") for e in entries][
                        : self.MAX_RESULTS
                    ]
                return []
            # Match partial filename in parent directory
            parent = path.parent
            partial = path.name.lower()
            if not parent.is_dir():
                return []
            entries = sorted(parent.iterdir())
            results = []
            for e in entries:
                if e.name.lower().startswith(partial):
                    results.append(str(e) + ("/" if e.is_dir() else ""))
                    if len(results) >= self.MAX_RESULTS:
                        break
            return results
        except PermissionError:
            return []


class ExModeSuggestionProvider(SuggestionProvider):
    """Provides context-aware completions for ex-mode commands.

    - No space yet → suggest command names (prefix match)
    - After ``sort `` → column names
    - After ``filter ``/``f `` → column names; after second space → no suggestions
    - After ``set theme `` → theme names; ``set keymap `` → keymap names
    - After ``w ``/``write ``/``o ``/``open `` → file paths
    - Fallback → ex-mode history
    """

    MAX_RESULTS = 20

    COMMANDS = [
        "sort",
        "filter",
        "f",
        "exclude",
        "e",
        "s/",
        "w",
        "write",
        "o",
        "open",
        "q",
        "q!",
        "delim",
        "delimiter",
        "set theme",
        "set keymap",
        "help",
    ]

    def __init__(self, app) -> None:
        self._app = app
        self._file_provider = FilePathSuggestionProvider()

    def _column_names(self) -> list[str]:
        from .dataprocessing import strip_markup

        try:
            buf = self._app._get_current_buffer()
            return [strip_markup(c.name) for c in buf.current_columns if not c.hidden]
        except Exception:
            return []

    def _theme_names(self) -> list[str]:
        from .theme import get_all_themes

        return sorted(get_all_themes().keys())

    def _keymap_names(self) -> list[str]:
        from .keymap import get_all_keymaps

        return sorted(get_all_keymaps().keys())

    def get_suggestions(self, value: str) -> list[str]:
        if not value:
            # Show command list + history
            history = [
                h["val"] for h in self._app.input_history if h["id"] == "exmode_input"
            ]
            seen = set()
            result = []
            for item in reversed(history):
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            return result[: self.MAX_RESULTS] if result else self.COMMANDS

        # Check if we're completing a command or its arguments
        parts = value.split(None, 1)
        cmd = parts[0].lower()

        # Still typing the command (no space yet)
        if len(parts) == 1 and not value.endswith(" "):
            lower = value.lower()
            return [c for c in self.COMMANDS if c.startswith(lower)][: self.MAX_RESULTS]

        # Command with args
        args = parts[1] if len(parts) > 1 else ""

        if cmd in ("sort",):
            cols = self._column_names()
            if not args:
                return cols[: self.MAX_RESULTS]
            lower = args.lower()
            return [c for c in cols if c.lower().startswith(lower)][: self.MAX_RESULTS]

        if cmd in ("filter", "f", "exclude", "e"):
            arg_parts = args.split(None, 1)
            if len(arg_parts) <= 1 and not args.endswith(" "):
                # Completing column name
                cols = self._column_names()
                partial = args.lower()
                if not partial:
                    return [f"{cmd} {c}" for c in cols][: self.MAX_RESULTS]
                return [f"{cmd} {c}" for c in cols if c.lower().startswith(partial)][
                    : self.MAX_RESULTS
                ]
            # After column name, no suggestions (user types pattern)
            return []

        if cmd == "set":
            set_parts = args.split(None, 1)
            subcmd = set_parts[0].lower() if set_parts else ""
            subarg = set_parts[1] if len(set_parts) > 1 else ""

            if not subcmd or (len(set_parts) == 1 and not args.endswith(" ")):
                options = ["theme", "keymap"]
                if subcmd:
                    options = [o for o in options if o.startswith(subcmd)]
                return [f"set {o}" for o in options]

            if subcmd == "theme":
                names = self._theme_names()
                if not subarg:
                    return [f"set theme {n}" for n in names][: self.MAX_RESULTS]
                lower = subarg.lower()
                return [f"set theme {n}" for n in names if n.lower().startswith(lower)][
                    : self.MAX_RESULTS
                ]
            if subcmd == "keymap":
                names = self._keymap_names()
                if not subarg:
                    return [f"set keymap {n}" for n in names][: self.MAX_RESULTS]
                lower = subarg.lower()
                return [
                    f"set keymap {n}" for n in names if n.lower().startswith(lower)
                ][: self.MAX_RESULTS]
            return []

        if cmd in ("w", "write", "o", "open"):
            return self._file_provider.get_suggestions(args)

        if cmd in ("delim", "delimiter"):
            if not args:
                return ["\\t", ",", "|", "space", "raw", "json"]
            return [
                d
                for d in ["\\t", ",", "|", "space", "space+", "raw", "json"]
                if d.startswith(args)
            ]

        return []


class ShellCommandSuggestionProvider(SuggestionProvider):
    """Provides shell command completions.

    First word: matches executable names from PATH (lazily scanned, cached).
    After a space: delegates to HistorySuggestionProvider.
    Cap at 20 results.
    """

    MAX_RESULTS = 20

    def __init__(self, history: list[str]) -> None:
        self._history_provider = HistorySuggestionProvider(history)
        self._executables: list[str] = []
        self._scan_thread = Thread(target=self._scan_path, daemon=True)
        self._scan_thread.start()

    def _scan_path(self) -> None:
        """Scan PATH for executable names, deduplicated and sorted."""
        seen: set[str] = set()
        executables: list[str] = []
        for dir_path in os.get_exec_path():
            try:
                entries = os.scandir(dir_path)
            except (OSError, PermissionError):
                continue
            for entry in entries:
                if entry.name in seen:
                    continue
                try:
                    if entry.is_file() and entry.stat().st_mode & (
                        stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
                    ):
                        seen.add(entry.name)
                        executables.append(entry.name)
                except (OSError, PermissionError):
                    continue
        executables.sort()
        self._executables = executables

    def get_suggestions(self, value: str) -> list[str]:
        if not value:
            return self._history_provider.get_suggestions("")
        # After a space, delegate to history
        if " " in value:
            return self._history_provider.get_suggestions(value)
        # If PATH scan is still running, fall back to history matches
        if self._scan_thread.is_alive():
            return self._history_provider.get_suggestions(value)
        # First word: match executables
        lower = value.lower()
        results = []
        for exe in self._executables:
            if exe.lower().startswith(lower):
                results.append(exe)
                if len(results) >= self.MAX_RESULTS:
                    break
        return results
