"""Performance regression tests.

Each test uses 50K rows to catch accidental O(n^2) regressions while staying
fast enough for CI.  Thresholds are derived from measured baselines (max of 3
runs) with a MULTIPLIER applied to absorb CI jitter, subject to a FLOOR so
that very fast operations (sub-100ms) aren't flaky under load.

Baselines were measured on the development machine (2026-03-03):

    load       0.30s
    sort       0.44s
    filter     0.41s
    unique     0.53s
    copy       0.001s
    add_rows   0.027s

Run only perf tests:   pytest -m perf
Skip perf tests:       pytest -m "not perf"
"""

import time

import pytest

from nless.app import NlessApp
from nless.datatable import Datatable
from nless.types import CliArgs

N_ROWS = 50_000
N_COLS = 5

# Measured baselines (max of 3 runs, seconds).
_BASELINES = {
    "load": 0.30,
    "sort": 0.44,
    "filter": 0.41,
    "unique": 0.53,
    "copy": 0.001,
    "add_rows": 0.027,
}

# Multiplier applied to baselines to set thresholds.  2x is tight enough to
# catch real regressions while allowing for normal CI variance (~10%).
MULTIPLIER = 2

# Floor in seconds — operations faster than this get at least this much
# headroom, since sub-100ms timings have high relative variance under load.
FLOOR = 0.15


def _threshold(name: str) -> float:
    return max(_BASELINES[name] * MULTIPLIER, FLOOR)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_csv_lines(n_rows: int, n_cols: int) -> list[str]:
    header = ",".join(f"col{c}" for c in range(n_cols))
    rows = [",".join(f"r{r}c{c}" for c in range(n_cols)) for r in range(n_rows)]
    return [header, *rows]


def _generate_csv_lines_with_repeats(
    n_rows: int, n_cols: int, unique_col0_values: int
) -> list[str]:
    header = ",".join(f"col{c}" for c in range(n_cols))
    rows = [
        ",".join(
            [f"group{r % unique_col0_values}"] + [f"r{r}c{c}" for c in range(1, n_cols)]
        )
        for r in range(n_rows)
    ]
    return [header, *rows]


async def _wait_perf(pilot, app, timeout: float = 30.0):
    """Pump the event loop until all buffers finish loading (perf-safe timeout)."""
    deadline = time.monotonic() + timeout
    settled = 0
    while time.monotonic() < deadline:
        await pilot.pause(delay=0.05)
        if all(not b._is_loading for b in app.buffers):
            settled += 1
            if settled >= 5:
                return
        else:
            settled = 0
    raise TimeoutError(f"App did not settle within {timeout}s")


def _assert_perf(name: str, elapsed: float):
    limit = _threshold(name)
    assert elapsed < limit, (
        f"{name} took {elapsed:.3f}s, limit {limit:.2f}s "
        f"(baseline {_BASELINES[name]:.3f}s x{MULTIPLIER})"
    )


@pytest.fixture
def cli_args():
    return CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.perf
@pytest.mark.asyncio
async def test_load_50k(cli_args):
    """add_logs → _add_rows_incremental fast path."""
    lines = _generate_csv_lines(N_ROWS, N_COLS)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        t0 = time.monotonic()
        app.buffers[0].add_logs(lines)
        await _wait_perf(pilot, app)
        elapsed = time.monotonic() - t0

    _assert_perf("load", elapsed)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_sort_50k(cli_args):
    """action_sort → _deferred_update_table pipeline."""
    lines = _generate_csv_lines(N_ROWS, N_COLS)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait_perf(pilot, app)

        buf = app.buffers[0]
        t0 = time.monotonic()
        buf.action_sort()
        await _wait_perf(pilot, app)
        elapsed = time.monotonic() - t0

    _assert_perf("sort", elapsed)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_filter_50k(cli_args):
    """_perform_filter → _copy_buffer_async → copy() + deferred."""
    lines = _generate_csv_lines(N_ROWS, N_COLS)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait_perf(pilot, app)

        t0 = time.monotonic()
        app._perform_filter("r0c0", "col0")
        await _wait_perf(pilot, app)
        elapsed = time.monotonic() - t0

    _assert_perf("filter", elapsed)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_unique_50k(cli_args):
    """action_mark_unique → dedup pipeline."""
    lines = _generate_csv_lines_with_repeats(N_ROWS, N_COLS, unique_col0_values=100)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        app.buffers[0].add_logs(lines)
        await _wait_perf(pilot, app)

        t0 = time.monotonic()
        app.action_mark_unique()
        await _wait_perf(pilot, app)
        elapsed = time.monotonic() - t0

    _assert_perf("unique", elapsed)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_buffer_copy_50k(cli_args):
    """buffer.copy() synchronous path (deepcopy + filter_lines)."""
    lines = _generate_csv_lines(N_ROWS, N_COLS)
    app = NlessApp(cli_args=cli_args, starting_stream=None)

    async with app.run_test(size=(120, 40)) as pilot:
        buf = app.buffers[0]
        buf.add_logs(lines)
        await _wait_perf(pilot, app)

        t0 = time.monotonic()
        buf.copy(pane_id=99)
        elapsed = time.monotonic() - t0

    _assert_perf("copy", elapsed)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_datatable_add_rows_50k():
    """Datatable.add_rows() width computation loop."""
    columns = [f"col{c}" for c in range(N_COLS)]
    rows = [[f"r{r}c{c}" for c in range(N_COLS)] for r in range(N_ROWS)]

    app = NlessApp(
        cli_args=CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None),
        starting_stream=None,
    )

    async with app.run_test(size=(120, 40)):
        dt = Datatable()
        await app.mount(dt)
        dt.add_columns(columns)

        t0 = time.monotonic()
        dt.add_rows(rows)
        elapsed = time.monotonic() - t0

    _assert_perf("add_rows", elapsed)
