#!/usr/bin/env python3
"""Cross-version performance benchmark suite.

Runs all perf benchmarks and outputs JSON results. Designed to work
across nless versions 0.7.0+ by gracefully handling API differences.

Usage:
    poetry run python bench_all.py
"""

import asyncio
import json
import subprocess
import sys
import time
from threading import Thread

# Monkey-patch config for older versions
try:
    from nless import config as _config_mod

    _OrigConfig = _config_mod.NlessConfig

    def _safe_load_config():
        return _OrigConfig(show_getting_started=False)

    _config_mod.load_config = _safe_load_config
except Exception:
    pass

from nless.app import NlessApp
from nless.types import CliArgs

N_ROWS = 50_000
N_ROWS_100K = 100_000
N_COLS = 5
RUNS = 3


def _csv_lines(n_rows, n_cols):
    header = ",".join(f"col{c}" for c in range(n_cols))
    rows = [",".join(f"r{r}c{c}" for c in range(n_cols)) for r in range(n_rows)]
    return [header, *rows]


def _csv_lines_numeric(n_rows, n_cols):
    header = ",".join(f"col{c}" for c in range(n_cols))
    rows = [
        ",".join(str((r * 7 + c) % 99991) for c in range(n_cols)) for r in range(n_rows)
    ]
    return [header, *rows]


def _csv_lines_with_repeats(n_rows, n_cols, unique_values):
    header = ",".join(f"col{c}" for c in range(n_cols))
    rows = [
        ",".join(
            [f"group{r % unique_values}"] + [f"r{r}c{c}" for c in range(1, n_cols)]
        )
        for r in range(n_rows)
    ]
    return [header, *rows]


async def _wait(pilot, app, timeout=30.0):
    deadline = time.monotonic() + timeout
    settled = 0
    while time.monotonic() < deadline:
        await pilot.pause(delay=0.05)
        if all(not getattr(b, "_loading_reason", None) for b in app.buffers):
            settled += 1
            if settled >= 5:
                return
        else:
            settled = 0
    raise TimeoutError(f"Did not settle within {timeout}s")


async def bench_load():
    """Load 50K rows via add_logs (no sort/filter)."""
    lines = _csv_lines(N_ROWS, N_COLS)
    cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        t0 = time.monotonic()
        app.buffers[0].add_logs(lines)
        await _wait(pilot, app)
        return time.monotonic() - t0


async def bench_sort():
    """Load 50K rows then sort."""
    lines = _csv_lines(N_ROWS, N_COLS)
    cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait(pilot, app)

        t0 = time.monotonic()
        app.buffers[0].action_sort()
        await _wait(pilot, app)
        return time.monotonic() - t0


async def bench_filter():
    """Load 50K rows then filter."""
    lines = _csv_lines(N_ROWS, N_COLS)
    cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait(pilot, app)

        t0 = time.monotonic()
        app._perform_filter("r0c0", "col0")
        await _wait(pilot, app)
        return time.monotonic() - t0


async def bench_unique():
    """Load 50K rows with repeats then deduplicate."""
    lines = _csv_lines_with_repeats(N_ROWS, N_COLS, unique_values=100)
    cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait(pilot, app)

        t0 = time.monotonic()
        app.action_mark_unique()
        await _wait(pilot, app)
        return time.monotonic() - t0


async def bench_sort_100k():
    """Load 100K numeric rows then sort."""
    lines = _csv_lines_numeric(N_ROWS_100K, N_COLS)
    cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait(pilot, app)

        t0 = time.monotonic()
        app.buffers[0].action_sort()
        await _wait(pilot, app)
        return time.monotonic() - t0


async def bench_stream_sort_100k():
    """Stream 100K rows with sort active."""
    lines = _csv_lines_numeric(N_ROWS_100K, N_COLS)
    header = lines[0]
    data = lines[1:]
    batch_size = 5000

    cli_args = CliArgs(
        delimiter=None, filters=[], unique_keys=set(), sort_by="col0=desc"
    )
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        buf = app.buffers[0]
        buf.add_logs([header])
        await pilot.pause(delay=0.1)

        def feed():
            for i in range(0, len(data), batch_size):
                buf.add_logs(data[i : i + batch_size])
                time.sleep(0.005)

        t0 = time.monotonic()
        feeder = Thread(target=feed, daemon=True)
        feeder.start()

        deadline = time.monotonic() + 120.0
        settled = 0
        while time.monotonic() < deadline:
            await pilot.pause(delay=0.05)
            loading = getattr(buf, "_loading_reason", None)
            feeder_done = not feeder.is_alive()
            if (
                feeder_done
                and len(buf.displayed_rows) >= N_ROWS_100K
                and not loading
                and not buf.locked
            ):
                settled += 1
                if settled >= 5:
                    break
            else:
                settled = 0
        else:
            print(f"    TIMEOUT: {len(buf.displayed_rows)}/{N_ROWS_100K} rows")

        return time.monotonic() - t0


BENCHMARKS = [
    ("load", bench_load),
    ("sort", bench_sort),
    ("filter", bench_filter),
    ("unique", bench_unique),
    ("sort_100k", bench_sort_100k),
    ("stream_sort_100k", bench_stream_sort_100k),
]


async def main():
    commit = subprocess.check_output(["git", "log", "--oneline", "-1"]).decode().strip()
    print(f"Performance benchmarks ({RUNS} runs each)")
    print(f"Commit: {commit}\n")

    results = {}
    for name, fn in BENCHMARKS:
        print(f"  {name}:")
        times = []
        skip = False
        for i in range(RUNS):
            try:
                elapsed = await fn()
                times.append(round(elapsed, 3))
                print(f"    Run {i + 1}: {elapsed:.3f}s")
            except Exception as e:
                print(f"    Run {i + 1}: SKIP ({type(e).__name__}: {e})")
                skip = True
                break
        if skip or not times:
            print("    SKIPPED\n")
            continue
        avg = round(sum(times) / len(times), 3)
        best = round(min(times), 3)
        results[name] = {"runs": times, "avg": avg, "best": best}
        print(f"    avg={avg:.3f}s  best={best:.3f}s\n")

    # Write JSON to stdout for capture by the runner script
    if "--json" in sys.argv:
        print("---JSON---")
        print(json.dumps(results))


if __name__ == "__main__":
    asyncio.run(main())
