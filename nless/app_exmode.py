"""Ex-mode command dispatcher for NlessApp."""

from __future__ import annotations

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
        )

    def handle_exmode_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        input_value = event.value.strip()
        event.input.remove()

        if not input_value:
            return

        # Check for substitution syntax first
        if _is_substitution(input_value):
            parsed = _parse_substitution(input_value)
            if parsed is None:
                self.notify(
                    "Invalid substitution. Use: s/pattern/replacement/[g]",
                    severity="error",
                )
                return
            pat, repl, all_columns = parsed
            apply_substitution(self, pat, repl, all_columns)
            return

        # Split into command and args
        parts = input_value.split(None, 1)
        cmd_str = parts[0]
        args_str = parts[1] if len(parts) > 1 else ""

        canonical = _COMMAND_ALIASES.get(cmd_str)
        if canonical is None:
            self.notify(f"Unknown command: {cmd_str}", severity="error")
            return

        handler = _COMMAND_HANDLERS.get(canonical)
        if handler:
            handler(self, args_str)

    def _cmd_sort(self: NlessApp, args: str) -> None:
        """Sort by column name."""
        col_name = args.strip()
        if not col_name:
            self.notify("Usage: sort <column>", severity="error")
            return

        curr_buffer = self._get_current_buffer()
        col_name_lower = col_name.lower()

        target_col = None
        for col in curr_buffer.current_columns:
            if strip_markup(col.name).lower() == col_name_lower:
                target_col = col
                break

        if target_col is None:
            self.notify(f"Column not found: {col_name}", severity="error")
            return

        sort_name = strip_markup(target_col.name)

        with curr_buffer._try_lock("sort") as acquired:
            if not acquired:
                return

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

    def _cmd_filter(self: NlessApp, args: str) -> None:
        """Filter by column and pattern."""
        parts = args.split(None, 1)
        if len(parts) < 2:
            self.notify("Usage: filter <column> <pattern>", severity="error")
            return
        col_name, pattern = parts
        # Verify column exists
        curr_buffer = self._get_current_buffer()
        col_name_lower = col_name.lower()
        matched = None
        for col in curr_buffer.current_columns:
            if strip_markup(col.name).lower() == col_name_lower:
                matched = strip_markup(col.name)
                break
        if matched is None:
            self.notify(f"Column not found: {col_name}", severity="error")
            return
        self._perform_filter(pattern, matched)

    def _cmd_exclude(self: NlessApp, args: str) -> None:
        """Exclude by column and pattern."""
        parts = args.split(None, 1)
        if len(parts) < 2:
            self.notify("Usage: exclude <column> <pattern>", severity="error")
            return
        col_name, pattern = parts
        curr_buffer = self._get_current_buffer()
        col_name_lower = col_name.lower()
        matched = None
        for col in curr_buffer.current_columns:
            if strip_markup(col.name).lower() == col_name_lower:
                matched = strip_markup(col.name)
                break
        if matched is None:
            self.notify(f"Column not found: {col_name}", severity="error")
            return
        self._perform_filter(pattern, matched, exclude=True)

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

        from .types import CliArgs
        from .input import StdinLineStream

        try:
            cli_args = CliArgs(
                delimiter=None,
                filters=[],
                unique_keys=set(),
                sort_by=None,
                filename=path,
            )
            line_stream = StdinLineStream(cli_args, file_name=path, new_fd=None)
            self.run_worker(
                self._open_new_group(line_stream, group_name=path),
                thread=True,
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
}
