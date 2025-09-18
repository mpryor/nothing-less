import sys
import argparse
from nless.input import InputConsumer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test InputConsumer with stdin.")
    parser.add_argument("filename", nargs="?", help="File to read input from (defaults to stdin)")
    args = parser.parse_args()
    if args.filename:
        ic = InputConsumer(args.filename, None, lambda: True, lambda lines: print(lines))
        ic.run()
    else:
        ic = InputConsumer(None, sys.stdin.fileno(), lambda: True, lambda lines: print(lines))
        ic.run()
