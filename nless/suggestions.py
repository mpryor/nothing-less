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


class TimeWindowSuggestionProvider(SuggestionProvider):
    """Context-aware suggestions for the @ time window prompt.

    Modes based on input state:
    - No '->': time window durations and column combos
    - After '->': format options, or timezone completions when typing a tz spec
    """

    MAX_RESULTS = 20

    FORMAT_OPTIONS = [
        "iso",
        "epoch",
        "epoch_ms",
        "relative",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%b %d, %Y",
        "%d/%m/%Y",
        "%Y/%m/%d %H:%M:%S",
        "%b %d %H:%M:%S",
    ]

    FORMAT_DESCRIPTIONS: dict[str, str] = {
        "iso": "2024-01-15T10:30:00",
        "epoch": "1705312200",
        "epoch_ms": "1705312200000",
        "relative": "2m ago, 1h ago, 3d ago",
        "%Y-%m-%d %H:%M:%S": "2024-01-15 10:30:00",
        "%Y-%m-%d": "2024-01-15",
        "%H:%M:%S": "10:30:00",
        "%m/%d/%Y %H:%M": "01/15/2024 10:30",
        "%b %d, %Y": "Jan 15, 2024",
        "%d/%m/%Y": "15/01/2024",
        "%Y/%m/%d %H:%M:%S": "2024/01/15 10:30:00",
        "%b %d %H:%M:%S": "Jan 15 10:30:00",
    }

    DURATION_DESCRIPTIONS: dict[str, str] = {
        "off": "clear time window",
        "30s": "last 30 seconds",
        "1m": "last 1 minute",
        "5m": "last 5 minutes",
        "5m+": "last 5 minutes (rolling)",
        "15m": "last 15 minutes",
        "15m+": "last 15 minutes (rolling)",
        "1h": "last 1 hour",
        "1h+": "last 1 hour (rolling)",
    }

    # Common timezones shown first, before the full IANA list
    COMMON_TZ = [
        "UTC",
        "US/Eastern",
        "US/Central",
        "US/Mountain",
        "US/Pacific",
        "Europe/London",
        "Europe/Paris",
        "Europe/Berlin",
        "Asia/Tokyo",
        "Asia/Shanghai",
        "Asia/Kolkata",
        "Australia/Sydney",
        "EST",
        "CST",
        "MST",
        "PST",
    ]

    _all_tz: list[str] | None = None  # lazily populated

    def __init__(
        self,
        options: list[str],
        dt_col_names: list[str],
    ) -> None:
        self._static = StaticSuggestionProvider(options)
        self._dt_col_names = dt_col_names

    @classmethod
    def _get_all_tz(cls) -> list[str]:
        if cls._all_tz is None:
            from zoneinfo import available_timezones

            common_set = set(cls.COMMON_TZ)
            rest = sorted(tz for tz in available_timezones() if tz not in common_set)
            cls._all_tz = list(cls.COMMON_TZ) + rest
        return cls._all_tz

    def _match_tz(self, partial: str) -> list[str]:
        """Match timezone names by prefix (case-insensitive)."""
        lower = partial.lower()
        all_tz = self._get_all_tz()
        prefix = [tz for tz in all_tz if tz.lower().startswith(lower)]
        # Also match on the last component (e.g. "Eastern" matches "US/Eastern")
        if "/" not in partial:
            substring = [
                tz for tz in all_tz if tz not in prefix and lower in tz.lower()
            ]
            return (prefix + substring)[: self.MAX_RESULTS]
        return prefix[: self.MAX_RESULTS]

    def _suggest_after_arrow(self, col_name: str, fmt_part: str) -> list[str]:
        """Suggest formats or timezones for the portion after '->'."""
        # Use untrimmed fmt_part to detect trailing spaces
        raw = fmt_part.lstrip()

        # Typing a timezone spec: contains '>'
        if ">" in raw:
            src, rest = raw.split(">", 1)
            parts = rest.strip().split(None, 1)
            if len(parts) == 2:
                # "src>dst format_partial" — suggest formats
                dst, fmt_partial = parts
                prefix = f"{src}>{dst}"
                fl = fmt_partial.lower()
                fmts = [f for f in self.FORMAT_OPTIONS if f.lower().startswith(fl)]
                return [f"{col_name} -> {prefix} {f}" for f in fmts][: self.MAX_RESULTS]
            if rest.strip() and rest.rstrip() != rest:
                # "src>dst " (trailing space) — dst is complete, suggest formats
                dst = rest.strip()
                prefix = f"{src}>{dst}"
                return [f"{col_name} -> {prefix} {f}" for f in self.FORMAT_OPTIONS][
                    : self.MAX_RESULTS
                ]
            # Typing target timezone after '>'
            matches = self._match_tz(rest.strip())
            return [f"{col_name} -> {src}>{tz}" for tz in matches][: self.MAX_RESULTS]

        trimmed = raw.strip()
        if not trimmed:
            return [f"{col_name} -> {f}" for f in self.FORMAT_OPTIONS][
                : self.MAX_RESULTS
            ]

        # Check if partial matches any format option
        fl = trimmed.lower()
        fmt_matches = [f for f in self.FORMAT_OPTIONS if f.lower().startswith(fl)]

        # Also check if it looks like the start of a timezone name (for src>dst syntax)
        tz_matches = self._match_tz(trimmed)
        tz_suggestions = [f"{col_name} -> {tz}>" for tz in tz_matches]

        fmt_suggestions = [f"{col_name} -> {f}" for f in fmt_matches]
        return (fmt_suggestions + tz_suggestions)[: self.MAX_RESULTS]

    def get_suggestions(self, value: str) -> list[str]:
        if " -> " in value:
            col_part, fmt_part = value.split(" -> ", 1)
            return self._suggest_after_arrow(col_part.strip(), fmt_part)

        base = self._static.get_suggestions(value)

        # Inject "colname -> " suggestions for matching datetime columns
        stripped = value.strip()
        if stripped and self._dt_col_names:
            lower = stripped.lower()
            arrow_items = [
                f"{col} -> "
                for col in self._dt_col_names
                if col.lower().startswith(lower)
            ]
            if arrow_items:
                # If user typed exact column name + space, put arrow first
                if stripped in self._dt_col_names and value.endswith(" "):
                    return (arrow_items + base)[: self.MAX_RESULTS]
                # Otherwise append after duration matches
                return (base + arrow_items)[: self.MAX_RESULTS]

        return base

    def get_description(self, item: str) -> str | None:
        """Return a description for a suggestion item."""
        if " -> " in item:
            # Format conversion suggestion — extract the format part
            _, fmt_part = item.split(" -> ", 1)
            fmt = fmt_part.strip()
            # Strip timezone prefix to find the base format
            if ">" in fmt:
                parts = fmt.split(None, 1)
                if len(parts) == 2:
                    fmt = parts[1]
                else:
                    return "select target timezone"
            return self.FORMAT_DESCRIPTIONS.get(fmt, "convert timestamp format")

        # Duration/time window suggestion — extract the duration
        stripped = item.strip()
        # Could be "colname duration" or just "duration"
        parts = stripped.rsplit(None, 1)
        dur = parts[-1] if parts else stripped
        desc = self.DURATION_DESCRIPTIONS.get(dur)
        if desc:
            return f"filter by column: {desc}" if len(parts) > 1 else desc
        if stripped.endswith(" -> "):
            return "convert timestamp format"
        return None


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
        "type",
        "clear",
        "cols",
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

    DESCRIPTIONS: dict[str, str] = {
        "sort": "sort by column",
        "filter": "filter column by pattern",
        "f": "filter (short alias)",
        "exclude": "exclude matches from column",
        "e": "exclude (short alias)",
        "s/": "substitute in column(s)",
        "type": "set column type (numeric/date/string/auto)",
        "clear": "reset sort, search, and columns",
        "cols": "show/hide columns",
        "w": "write buffer to file",
        "write": "write buffer to file",
        "o": "open file in new group",
        "open": "open file in new group",
        "q": "close buffer or quit",
        "q!": "pipe to stdout and exit",
        "delim": "change delimiter",
        "delimiter": "change delimiter",
        "set theme": "switch theme",
        "set keymap": "switch keymap",
        "help": "show help screen",
    }

    def get_description(self, item: str) -> str | None:
        """Return a short description for a command, or None."""
        return self.DESCRIPTIONS.get(item)

    def __init__(self, app) -> None:
        self._app = app
        self._file_provider = FilePathSuggestionProvider()

    @staticmethod
    def _quote_col(name: str) -> str:
        """Quote a column name if it contains spaces."""
        return f'"{name}"' if " " in name else name

    def _column_names(self) -> list[str]:
        from .dataprocessing import strip_markup

        try:
            buf = self._app._get_current_buffer()
            return [strip_markup(c.name) for c in buf.current_columns if not c.hidden]
        except Exception:
            return []

    def _column_values(self, col_name: str) -> list[str]:
        """Return sorted distinct values for a column from the current buffer."""
        from .dataprocessing import strip_markup

        try:
            buf = self._app._get_current_buffer()
            col_idx = None
            for i, c in enumerate(buf.current_columns):
                if strip_markup(c.name).lower() == col_name.lower():
                    col_idx = i
                    break
            if col_idx is None:
                return []
            seen: set[str] = set()
            for row in buf.displayed_rows:
                if col_idx < len(row):
                    val = strip_markup(str(row[col_idx])).strip()
                    if val:
                        seen.add(val)
            return sorted(seen)
        except Exception:
            return []

    def _theme_names(self) -> list[str]:
        from .theme import get_all_themes

        return sorted(get_all_themes().keys())

    def _keymap_names(self) -> list[str]:
        from .keymap import get_all_keymaps

        return sorted(get_all_keymaps().keys())

    def get_suggestions(self, value: str) -> list[str]:
        if not value or not value.strip():
            return list(self.COMMANDS)

        # Check if we're completing a command or its arguments
        parts = value.split(None, 1)
        cmd = parts[0].lower()

        # Still typing the command (no space yet)
        if len(parts) == 1 and not value.endswith(" "):
            lower = value.lower()
            return [c for c in self.COMMANDS if c.startswith(lower)][: self.MAX_RESULTS]

        # Command with args
        args = parts[1] if len(parts) > 1 else ""

        if cmd in ("type",):
            arg_parts = args.split(None, 1)
            col_part = arg_parts[0] if arg_parts else ""
            cols = self._column_names()
            col_matched = col_part and any(c.lower() == col_part.lower() for c in cols)
            if col_matched and (len(arg_parts) > 1 or args.endswith(" ")):
                type_part = arg_parts[1].lower() if len(arg_parts) > 1 else ""
                type_options = ["numeric", "date", "string", "auto"]
                return [
                    f"type {self._quote_col(col_part)} {t}"
                    for t in type_options
                    if t.startswith(type_part)
                ]
            if not args:
                return [f"type {self._quote_col(c)}" for c in cols][: self.MAX_RESULTS]
            lower = args.lower()
            return [
                f"type {self._quote_col(c)}"
                for c in cols
                if c.lower().startswith(lower)
            ][: self.MAX_RESULTS]

        if cmd in ("sort",):
            arg_parts = args.split(None, 1)
            col_part = arg_parts[0] if arg_parts else ""
            cols = self._column_names()
            # Check if the first token matches a column name exactly
            col_matched = col_part and any(c.lower() == col_part.lower() for c in cols)
            if col_matched and (len(arg_parts) > 1 or args.endswith(" ")):
                # Column selected, suggest direction
                dir_part = arg_parts[1].lower() if len(arg_parts) > 1 else ""
                return [
                    f"sort {self._quote_col(col_part)} {d}"
                    for d in ("asc", "desc")
                    if d.startswith(dir_part)
                ]
            if not args:
                return [f"sort {self._quote_col(c)}" for c in cols][: self.MAX_RESULTS]
            lower = args.lower()
            return [
                f"sort {self._quote_col(c)}"
                for c in cols
                if c.lower().startswith(lower)
            ][: self.MAX_RESULTS]

        if cmd in ("filter", "f", "exclude", "e"):
            arg_parts = args.split(None, 1)
            if len(arg_parts) <= 1 and not args.endswith(" "):
                # Completing column name
                cols = self._column_names()
                partial = args.lower()
                if not partial:
                    return [f"{cmd} {self._quote_col(c)}" for c in cols][
                        : self.MAX_RESULTS
                    ]
                return [
                    f"{cmd} {self._quote_col(c)}"
                    for c in cols
                    if c.lower().startswith(partial)
                ][: self.MAX_RESULTS]
            # After column name, suggest distinct values from that column
            col_name = arg_parts[0]
            pattern = arg_parts[1] if len(arg_parts) > 1 else ""
            values = self._column_values(col_name)
            if pattern:
                lower = pattern.lower()
                values = [v for v in values if v.lower().startswith(lower)]
            return [f"{cmd} {self._quote_col(col_name)} {v}" for v in values][
                : self.MAX_RESULTS
            ]

        if cmd in ("cols", "columns"):
            cols = self._column_names()
            if not args:
                return [f"{cmd} {c}" for c in ["all"] + cols][: self.MAX_RESULTS]
            # Use pipe-separated provider logic
            provider = PipeSeparatedSuggestionProvider(cols)
            raw = provider.get_suggestions(args)
            return [f"{cmd} {r}" for r in raw][: self.MAX_RESULTS]

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
