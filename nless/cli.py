import argparse
import re
import sys
from threading import Thread


from nless.app import NlessApp
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
        delimiter=args.delimiter,
        filters=filters,
        unique_keys=unique_keys,
        sort_by=args.sort_by,
    )
    cli_args.filename = args.filename
    return cli_args


def main():
    cli_args = parse_args()

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
        sys.__stdin__ = open(
            "/dev/tty"
        )  # hack to allow textual to read input from terminal, while still reading piped data from stdin
    else:
        app = NlessApp(cli_args=cli_args, show_help=True, starting_stream=None)
    app.run()


if __name__ == "__main__":
    main()
