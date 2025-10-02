import sys
import argparse
from nless.input import InputConsumer
from nless.types import CliArgs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="nless - A terminal log viewer")
    parser.add_argument(
        "filename", nargs="?", help="File to read input from (defaults to stdin)"
    )
    parser.add_argument(
        "--delimiter", "-d", help="Delimiter to use for splitting fields", default=None
    )

    args = parser.parse_args()
    cli_args = CliArgs(
        delimiter=args.delimiter, filters=[], unique_keys=set(), sort_by=None
    )

    if args.filename:
        ic = InputConsumer(
            cli_args,
            args.filename,
            None,
            lambda: True,
            lambda lines: print(f"new lines added: {lines}"),
        )
        ic.run()
    else:
        ic = InputConsumer(
            cli_args,
            None,
            sys.stdin.fileno(),
            lambda: True,
            lambda lines: print(f"new lines added, {len(lines)}"),
        )
        ic.run()
