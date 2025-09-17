import sys
import argparse
from input import InputConsumer


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test InputConsumer with stdin.")
    parser.add_argument("filename", nargs="?", help="File to read input from (defaults to stdin)")
    args = parser.parse_args()
    if args.filename:
        with open(args.filename, "r") as f:
            ic = InputConsumer(f.fileno(), lambda: True, lambda lines: print(lines))
            ic.run()
    else:
        ic = InputConsumer(sys.stdin.fileno(), lambda: True, lambda lines: print(lines))
        ic.run()
