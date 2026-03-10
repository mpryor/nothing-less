import argparse
import os
import re
import sys
from threading import Thread

from nless.app import NlessApp
from nless.batch import run_batch
from nless.version import get_version

from .input import StdinLineStream
from .types import CliArgs, Filter


def parse_args(argv=None) -> CliArgs:
    """Parse CLI arguments and return a CliArgs object.

    Args:
        argv: Argument list to parse. Defaults to sys.argv[1:].

    Returns:
        Parsed CliArgs.

    Raises:
        SystemExit: On invalid arguments.
    """
    parser = argparse.ArgumentParser(description="nless - A terminal log viewer")
    parser.add_argument(
        "filename", nargs="?", help="File to read input from (defaults to stdin)"
    )
    parser.add_argument("--version", action="version", version=f"{get_version()}")
    parser.add_argument(
        "--delimiter", "-d", help="Delimiter to use for splitting fields", default=None
    )
    parser.add_argument(
        "--filters", "-f", action="append", help="Initial filter(s)", default=[]
    )
    parser.add_argument(
        "--exclude-filters",
        "-x",
        action="append",
        help="Initial exclude filter(s) (same format as -f, excludes matching rows)",
        default=[],
    )
    parser.add_argument(
        "--unique", "-u", action="append", help="Initial unique key(s)", default=[]
    )
    parser.add_argument(
        "--sort-by", "-s", help="Column to sort by initially", default=None
    )
    parser.add_argument(
        "--theme",
        "-t",
        help="Color theme to use (e.g. dracula, nord, monokai)",
        default=None,
    )
    parser.add_argument(
        "--keymap",
        "-k",
        help="Keymap preset to use (e.g. vim, less, emacs)",
        default=None,
    )
    parser.add_argument(
        "--tail",
        action="store_true",
        help="Start in tail mode (cursor follows new data)",
        default=False,
    )
    parser.add_argument(
        "--time-window",
        "-w",
        help="Show only rows within a time window (e.g. 5m, 1h, 30s). Append + for rolling (e.g. 5m+)",
        default=None,
    )
    parser.add_argument(
        "--columns",
        "-c",
        help="Regex to filter visible columns (e.g. 'name|status')",
        default=None,
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Start in raw pager mode (no delimiter parsing)",
        default=False,
    )
    parser.add_argument(
        "--no-tui",
        action="store_true",
        help="Skip the TUI — apply transforms and write to stdout",
        default=False,
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Force TUI mode even when stdout is piped with transforms",
        default=False,
    )
    parser.add_argument(
        "--output-format",
        "-o",
        choices=["csv", "tsv", "json", "raw"],
        help="Output format for pipe/batch output (default: csv)",
        default="csv",
    )

    args = parser.parse_args(argv)

    if args.sort_by and len(args.sort_by.split("=")) != 2:
        print(
            f"Invalid sort-by format: {args.sort_by}. Expected format is column=asc|desc"
        )
        sys.exit(1)

    filters = []
    if len(args.filters) > 0:
        for arg_filter in args.filters:
            try:
                column, value = arg_filter.split("=")
            except ValueError:
                print(
                    f"Invalid filter format: {arg_filter}. Expected format is column=value or any=value"
                )
                sys.exit(1)
            filters.append(
                Filter(
                    column=column if column != "any" else None,
                    pattern=re.compile(value, re.IGNORECASE),
                )
            )

    if len(args.exclude_filters) > 0:
        for arg_filter in args.exclude_filters:
            try:
                column, value = arg_filter.split("=")
            except ValueError:
                print(
                    f"Invalid exclude filter format: {arg_filter}. Expected format is column=value or any=value"
                )
                sys.exit(1)
            filters.append(
                Filter(
                    column=column if column != "any" else None,
                    pattern=re.compile(value, re.IGNORECASE),
                    exclude=True,
                )
            )

    unique_keys = set()
    if len(args.unique) > 0:
        for unique_key in args.unique:
            unique_keys.add(unique_key)

    cli_args = CliArgs(
        delimiter=args.delimiter if not args.raw else "raw",
        filters=filters,
        unique_keys=unique_keys,
        sort_by=args.sort_by,
        theme=args.theme,
        keymap=args.keymap,
        tail=args.tail,
        time_window=args.time_window,
        columns=args.columns,
        raw=args.raw,
        no_tui=args.no_tui,
        tui=args.tui,
        output_format=args.output_format,
    )
    cli_args.filename = args.filename
    return cli_args


def main():
    cli_args = parse_args()

    stdout_is_pipe = not sys.stdout.isatty()
    has_transforms = bool(
        cli_args.filters or cli_args.unique_keys or cli_args.sort_by or cli_args.columns
    )
    batch_mode = cli_args.no_tui or (
        stdout_is_pipe and has_transforms and not cli_args.tui
    )

    if batch_mode:
        run_batch(cli_args)
        sys.exit(0)

    cli_args.pipe_output = stdout_is_pipe

    # Save the original stdout pipe fd BEFORE redirecting stdout to stderr,
    # so Textual renders the TUI to the terminal, not the pipe.
    pipe_fd = None
    if stdout_is_pipe:
        try:
            size = os.get_terminal_size(sys.stderr.fileno())
            os.environ["COLUMNS"] = str(size.columns)
            os.environ["LINES"] = str(size.lines)
        except OSError:
            pass  # stderr isn't a terminal either; let defaults apply
        pipe_fd = os.fdopen(os.dup(sys.stdout.fileno()), "w")
        sys.stdout = sys.stderr

    new_fd = sys.stdin.fileno()

    if cli_args.filename:
        filename = cli_args.filename
        new_fd = None
    else:
        filename = None

    stdin_contains_data = not sys.stdin.isatty()
    if stdin_contains_data or filename:
        try:
            stdin_line_stream = StdinLineStream(
                cli_args,
                filename,
                new_fd,
            )
        except (FileNotFoundError, IsADirectoryError, PermissionError) as e:
            print(f"nless: {e}", file=sys.stderr)
            sys.exit(1)
        app = NlessApp(cli_args=cli_args, starting_stream=stdin_line_stream)
        t = Thread(target=stdin_line_stream.run, daemon=True)
        t.start()
        tty_file = open("/dev/tty")  # noqa: SIM115
        sys.__stdin__ = tty_file  # allow textual to read terminal input while piped stdin is read on a thread
    else:
        tty_file = None
        app = NlessApp(cli_args=cli_args, show_help=True, starting_stream=None)
    try:
        app.run()
    finally:
        if pipe_fd:
            from nless.operations import write_buffer_to_fd

            write_buffer_to_fd(
                app._get_current_buffer(), pipe_fd, cli_args.output_format
            )
            pipe_fd.close()
        if tty_file is not None:
            tty_file.close()
        os._exit(0)  # Hard exit: daemon I/O threads may block atexit join


if __name__ == "__main__":
    main()
