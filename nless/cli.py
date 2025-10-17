import argparse
import re
import sys
from threading import Thread
from typing import List

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import (
    RichLog,
    Static,
)

from nless.app import NlessApp
from nless.version import get_version

from .input import StdinLineStream
from .types import CliArgs, Filter


class RowLengthMismatchError(Exception):
    pass


class UnparsedLogsScreen(Screen):
    BINDINGS = [("q", "app.pop_screen", "Close")]

    def __init__(self, unparsed_rows: List[str], delimiter: str):
        super().__init__()
        self.unparsed_rows = unparsed_rows
        self.delimiter = delimiter

    def compose(self) -> ComposeResult:
        yield Static(
            f"{len(self.unparsed_rows)} logs not matching columns (delimiter '{self.delimiter}'), press 'q' to close.",
        )
        rl = RichLog()
        for row in self.unparsed_rows:
            rl.write(row.strip())
        yield rl


def main():
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
        "--unique", "-u", action="append", help="Initial unique key(s)", default=[]
    )
    parser.add_argument(
        "--sort-by", "-s", help="Column to sort by initially", default=None
    )

    args = parser.parse_args()

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

    new_fd = sys.stdin.fileno()

    if args.filename:
        filename = args.filename
        new_fd = None
    else:
        filename = None

    stdin_contains_data = not sys.stdin.isatty()
    if stdin_contains_data or filename:
        ic = StdinLineStream(
            cli_args,
            filename,
            new_fd,
        )
        app = NlessApp(cli_args=cli_args, starting_stream=ic)
        t = Thread(target=ic.run, daemon=True)
        t.start()
        sys.__stdin__ = open("/dev/tty")
    else:
        app = NlessApp(cli_args=cli_args, show_help=True, starting_stream=None)
    app.run()


if __name__ == "__main__":
    main()
