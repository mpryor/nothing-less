"""Ex-mode command dispatcher for NlessApp."""

from __future__ import annotations

import difflib
from typing import TYPE_CHECKING

from .app_substitute import _parse_substitution, apply_substitution
from .dataprocessing import strip_markup
from .types import UpdateReason

if TYPE_CHECKING:
    from .app import NlessApp
    from .autocomplete import AutocompleteInput


# Command alias map: alias → canonical name
_COMMAND_ALIASES: dict[str, str] = {
    "sort": "sort",
    "filter": "filter",
    "f": "filter",
    "exclude": "exclude",
    "e": "exclude",
    "w": "write",
    "write": "write",
    "o": "open",
    "open": "open",
    "q": "quit",
    "q!": "quit!",
    "quit": "quit",
    "quit!": "quit!",
    "delim": "delim",
    "delimiter": "delim",
    "set": "set",
    "help": "help",
    "clear": "clear",
    "reset": "clear",
    "cols": "cols",
    "columns": "cols",
    "type": "type",
}


def _is_substitution(text: str) -> bool:
    """Check if text looks like a substitution command (s + non-alnum separator)."""
    if len(text) < 4 or text[0] != "s":
        return False
    sep = text[1]
    return not sep.isalnum() and sep != " "


class ExModeMixin:
    """Mixin providing ex-mode command dispatching for NlessApp."""

    def action_exmode(self: NlessApp) -> None:
        """Open the ex-mode command prompt."""
        from .suggestions import ExModeSuggestionProvider

        self._create_prompt(
            "Ex command (s/, sort, filter, w, q, help, ...)",
            "exmode_input",
            provider=ExModeSuggestionProvider(self),
            prefix=":",
        )

    def _keep_exmode_open(
        self: NlessApp, event: AutocompleteInput.Submitted, input_value: str
    ) -> None:
        """Keep ex mode open with the current value after an error."""
        ac = event.input
        ac.value = ":" + input_value
        ac._input.cursor_position = len(ac._input.value)
        ac._input.focus()

    def handle_exmode_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        input_value = event.value.strip()

        if not input_value:
            event.input.remove()
            return

        # Jump to line number
        if input_value.isdigit():
            event.input.remove()
            row = int(input_value) - 1
            curr_buffer = self._get_current_buffer()
            data_table = curr_buffer.query_one(".nless-view")
            data_table.move_cursor(row=max(0, row))
            return

        # Check for substitution syntax first
        if _is_substitution(input_value):
            parsed = _parse_substitution(input_value)
            if parsed is None:
                self.notify(
                    "Invalid substitution. Use s/pattern/replacement/[gi]",
                    severity="error",
                )
                self._keep_exmode_open(event, input_value)
                return
            event.input.remove()
            pat, repl, all_columns = parsed
            apply_substitution(self, pat, repl, all_columns)
            return

        # Split into command and args
        parts = input_value.split(None, 1)
        cmd_str = parts[0]
        args_str = parts[1] if len(parts) > 1 else ""

        canonical = _COMMAND_ALIASES.get(cmd_str)
        if canonical is None:
            matches = difflib.get_close_matches(
                cmd_str, _COMMAND_ALIASES.keys(), n=2, cutoff=0.6
            )
            if matches:
                hint = ", ".join(matches)
                self.notify(
                    f"Unknown command: {cmd_str}. Did you mean: {hint}?",
                    severity="error",
                )
            else:
                self.notify(f"Unknown command: {cmd_str}", severity="error")
            self._keep_exmode_open(event, input_value)
            return

        handler = _COMMAND_HANDLERS.get(canonical)
        if handler:
            result = handler(self, args_str)
            if result is False:
                self._keep_exmode_open(event, input_value)
                return

        event.input.remove()

    @staticmethod
    def _split_col_args(args: str) -> tuple[str, str]:
        """Split args into (column_name, rest) supporting quoted column names.

        Handles: `"multi word" pattern` and `col pattern`.
        """
        stripped = args.strip()
        if stripped.startswith('"'):
            close = stripped.find('"', 1)
            if close > 0:
                col = stripped[1:close]
                rest = stripped[close + 1 :].strip()
                return col, rest
        parts = stripped.split(None, 1)
        return parts[0] if parts else "", parts[1] if len(parts) > 1 else ""

    def _find_column(self: NlessApp, col_name: str) -> object | None:
        """Find a column by name, returning the Column object or None."""
        curr_buffer = self._get_current_buffer()
        col_name_lower = col_name.lower()
        for col in curr_buffer.current_columns:
            if strip_markup(col.name).lower() == col_name_lower:
                return col
        return None

    def _column_names_list(self: NlessApp) -> list[str]:
        """Return visible column names for fuzzy matching."""
        curr_buffer = self._get_current_buffer()
        return [
            strip_markup(c.name) for c in curr_buffer.current_columns if not c.hidden
        ]

    def _notify_column_not_found(self: NlessApp, col_name: str) -> None:
        """Notify column not found with fuzzy suggestions."""
        names = self._column_names_list()
        matches = difflib.get_close_matches(col_name, names, n=2, cutoff=0.6)
        if matches:
            hint = ", ".join(matches)
            self.notify(
                f"Column not found: {col_name}. Did you mean: {hint}?",
                severity="error",
            )
        else:
            self.notify(f"Column not found: {col_name}", severity="error")

    def _cmd_sort(self: NlessApp, args: str) -> bool | None:
        """Sort by column name, optionally with asc/desc direction."""
        stripped = args.strip()
        if not stripped:
            self.notify("Usage: sort <column> [asc|desc]", severity="error")
            return False

        # Handle quoted column names
        if stripped.startswith('"'):
            col_name, rest = self._split_col_args(stripped)
            direction = rest.lower() if rest.lower() in ("asc", "desc") else None
        else:
            parts = stripped.split()
            direction = None
            if len(parts) >= 2 and parts[-1].lower() in ("asc", "desc"):
                direction = parts[-1].lower()
                col_name = " ".join(parts[:-1])
            else:
                col_name = " ".join(parts)

        curr_buffer = self._get_current_buffer()
        target_col = self._find_column(col_name)

        if target_col is None:
            self._notify_column_not_found(col_name)
            return False

        sort_name = strip_markup(target_col.name)

        with curr_buffer._try_lock("sort") as acquired:
            if not acquired:
                return None

            if direction is not None:
                # Explicit direction
                curr_buffer.query.sort_column = sort_name
                curr_buffer.query.sort_reverse = direction == "desc"
                target_col.labels.discard("▲")
                target_col.labels.discard("▼")
                target_col.labels.add("▼" if direction == "desc" else "▲")
            else:
                # Cycle: none → asc → desc → none
                if (
                    curr_buffer.query.sort_column == sort_name
                    and curr_buffer.query.sort_reverse
                ):
                    curr_buffer.query.sort_column = None
                    target_col.labels.discard("▲")
                    target_col.labels.discard("▼")
                elif (
                    curr_buffer.query.sort_column == sort_name
                    and not curr_buffer.query.sort_reverse
                ):
                    curr_buffer.query.sort_reverse = True
                    target_col.labels.discard("▲")
                    target_col.labels.add("▼")
                else:
                    curr_buffer.query.sort_column = sort_name
                    curr_buffer.query.sort_reverse = False
                    target_col.labels.discard("▼")
                    target_col.labels.add("▲")

            # Remove sort indicators from other columns
            for col in curr_buffer.current_columns:
                if col.name != target_col.name:
                    col.labels.discard("▲")
                    col.labels.discard("▼")

        curr_buffer._deferred_update_table(reason=UpdateReason.SORT)
        return None

    def _cmd_filter(self: NlessApp, args: str) -> bool | None:
        """Filter by column and pattern."""
        col_name, pattern = self._split_col_args(args)
        if not col_name or not pattern:
            self.notify("Usage: filter <column> <pattern>", severity="error")
            return False
        col = self._find_column(col_name)
        if col is None:
            self._notify_column_not_found(col_name)
            return False
        self._perform_filter(pattern, strip_markup(col.name))
        return None

    def _cmd_exclude(self: NlessApp, args: str) -> bool | None:
        """Exclude by column and pattern."""
        col_name, pattern = self._split_col_args(args)
        if not col_name or not pattern:
            self.notify("Usage: exclude <column> <pattern>", severity="error")
            return False
        col = self._find_column(col_name)
        if col is None:
            self._notify_column_not_found(col_name)
            return False
        self._perform_filter(pattern, strip_markup(col.name), exclude=True)
        return None

    def _cmd_clear(self: NlessApp, args: str) -> None:
        """Reset sort, search, and column visibility."""
        from .types import MetadataColumn

        curr_buffer = self._get_current_buffer()
        with curr_buffer._try_lock("clear") as acquired:
            if not acquired:
                return
            curr_buffer.query.sort_column = None
            curr_buffer.query.sort_reverse = False
            curr_buffer.query.search_term = None
            curr_buffer.query.search_matches = []
            curr_buffer.query.current_match_index = -1
            # Remove sort indicators, restore column visibility and positions
            metadata_names = {mc.value for mc in MetadataColumn}
            for col in curr_buffer.current_columns:
                col.labels.discard("▲")
                col.labels.discard("▼")
                if col.name not in metadata_names or col.pinned:
                    col.hidden = False
                col.render_position = col.data_position
        curr_buffer._deferred_update_table(reason=UpdateReason.SORT)
        self.notify("Cleared sort, search, and column visibility")

    def _cmd_cols(self: NlessApp, args: str) -> None:
        """Show/hide columns. Uses pipe-separated names or 'all' to reset."""
        input_value = args.strip()
        if not input_value:
            self.action_filter_columns()
            return
        self._apply_column_filter(input_value)

    def _cmd_write(self: NlessApp, args: str) -> None:
        """Write buffer to file, or open prompt if no path given."""
        path = args.strip()
        if not path:
            self.action_write_to_file()
            return

        from threading import Thread

        from .operations import write_buffer

        current_buffer = self._get_current_buffer()

        def _write_and_notify():
            try:
                write_buffer(current_buffer, path)
                if path != "-":
                    self.call_from_thread(
                        lambda: current_buffer.notify(f"Wrote current view to {path}")
                    )
            except Exception as e:
                msg = str(e)
                self.call_from_thread(
                    lambda: current_buffer.notify(msg, severity="error")
                )

        Thread(target=_write_and_notify, daemon=True).start()

    def _cmd_open(self: NlessApp, args: str) -> None:
        """Open a file in a new group."""
        path = args.strip()
        if not path:
            self.action_open_file()
            return

        import os
        from threading import Thread

        from .buffer import NlessBuffer
        from .input import StdinLineStream
        from .types import CliArgs

        try:
            cli_args = CliArgs(
                delimiter=None,
                filters=[],
                unique_keys=set(),
                sort_by=None,
                filename=path,
            )
            line_stream = StdinLineStream(cli_args, file_name=path, new_fd=None)
            new_buffer = NlessBuffer(
                pane_id=self._get_new_pane_id(),
                cli_args=cli_args,
                line_stream=line_stream,
            )
            t = Thread(target=line_stream.run, daemon=True)
            t.start()
            file_icon = "\uf0f6" if self.demo_mode else "📄"
            self.run_worker(
                self.add_group(
                    f"{file_icon} {os.path.basename(path)}",
                    new_buffer,
                    stream=line_stream,
                )
            )
        except Exception as exc:
            self.notify(str(exc), severity="error")

    def _cmd_quit(self: NlessApp, args: str) -> None:
        """Close current buffer or quit."""
        self.action_close_active_buffer()

    def _cmd_quit_force(self: NlessApp, args: str) -> None:
        """Pipe and exit."""
        self.action_pipe_and_exit()

    def _cmd_delim(self: NlessApp, args: str) -> None:
        """Change delimiter."""
        delim = args.strip()
        if not delim:
            self.action_delimiter()
            return
        curr_buffer = self._get_current_buffer()
        curr_buffer.switch_delimiter(delim)

    def _cmd_set(self: NlessApp, args: str) -> None:
        """Handle 'set' subcommands: theme, keymap."""
        parts = args.split(None, 1)
        if len(parts) < 2:
            self.notify("Usage: set theme <name> | set keymap <name>", severity="error")
            return
        subcmd, value = parts[0].lower(), parts[1].strip()
        if subcmd == "theme":
            self.apply_theme(value)
        elif subcmd == "keymap":
            self.apply_keymap(value)
        else:
            self.notify(f"Unknown setting: {subcmd}", severity="error")

    def _cmd_type(self: NlessApp, args: str) -> bool | None:
        """Set column type: type <column> <numeric|date|string|auto>."""
        from .types import ColumnType

        col_name, type_str = self._split_col_args(args)
        if not col_name:
            self.notify(
                "Usage: type <column> <numeric|date|string|auto>", severity="error"
            )
            return False

        col = self._find_column(col_name)
        if col is None:
            self._notify_column_not_found(col_name)
            return False

        type_map = {
            "numeric": ColumnType.NUMERIC,
            "date": ColumnType.DATETIME,
            "datetime": ColumnType.DATETIME,
            "string": ColumnType.STRING,
            "auto": None,
        }

        type_str_lower = type_str.lower() if type_str else ""
        if type_str_lower not in type_map:
            self.notify(
                "Type must be: numeric, date, string, or auto",
                severity="error",
            )
            return False

        new_type = type_map[type_str_lower]
        col.type_override = new_type
        if new_type is None:
            col.detected_type = ColumnType.AUTO

        from .buffer import NlessBuffer

        NlessBuffer._update_type_label(col)

        curr_buffer = self._get_current_buffer()
        type_name = new_type.value if new_type else "auto"
        self.notify(f"Column '{strip_markup(col.name)}' type set to {type_name}")

        if curr_buffer.query.sort_column == strip_markup(col.name):
            curr_buffer.cache.reset_sort_keys()
            curr_buffer._deferred_update_table(reason=UpdateReason.SORT)

        return None

    def _cmd_help(self: NlessApp, args: str) -> None:
        """Show help screen."""
        self.action_help()


# Map canonical command names to handler methods
_COMMAND_HANDLERS: dict[str, callable] = {
    "sort": ExModeMixin._cmd_sort,
    "filter": ExModeMixin._cmd_filter,
    "exclude": ExModeMixin._cmd_exclude,
    "write": ExModeMixin._cmd_write,
    "open": ExModeMixin._cmd_open,
    "quit": ExModeMixin._cmd_quit,
    "quit!": ExModeMixin._cmd_quit_force,
    "delim": ExModeMixin._cmd_delim,
    "set": ExModeMixin._cmd_set,
    "help": ExModeMixin._cmd_help,
    "clear": ExModeMixin._cmd_clear,
    "cols": ExModeMixin._cmd_cols,
    "type": ExModeMixin._cmd_type,
}
