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
            return []
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


class FilePathSuggestionProvider(SuggestionProvider):
    """Provides filesystem path completions.

    If input ends with '/', lists directory contents.
    Otherwise, matches partial filenames in the parent directory.
    Cap at 20 results.
    """

    MAX_RESULTS = 20

    def get_suggestions(self, value: str) -> list[str]:
        if not value:
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
            return []
        # After a space, delegate to history
        if " " in value:
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
