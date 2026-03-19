import pytest

from nless.types import CliArgs


@pytest.fixture
def cli_args_factory():
    """Factory for creating CliArgs with sensible defaults."""

    def _make(
        delimiter=None,
        filters=None,
        unique_keys=None,
        sort_by=None,
    ):
        return CliArgs(
            delimiter=delimiter,
            filters=filters or [],
            unique_keys=unique_keys or set(),
            sort_by=sort_by,
        )

    return _make


@pytest.fixture
def csv_lines():
    return [
        "name,age,city",
        "Alice,30,New York",
        "Bob,25,San Francisco",
        'Charlie,35,"Los Angeles"',
    ]


@pytest.fixture
def tsv_lines():
    return [
        "name\tage\tcity",
        "Alice\t30\tNew York",
        "Bob\t25\tSan Francisco",
    ]


@pytest.fixture
def json_lines():
    return [
        '{"name": "Alice", "age": 30, "city": "New York"}',
        '{"name": "Bob", "age": 25, "city": "San Francisco"}',
    ]


@pytest.fixture
def space_aligned_lines():
    return [
        "NAME       STATUS    RESTARTS   AGE",
        "nginx      Running   0          5d",
        "redis      Running   1          3d",
    ]


@pytest.fixture
def ps_aux_lines():
    return [
        "USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND",
        "root           1  0.0  0.0 169128 13120 ?        Ss   Mar14   0:02 /sbin/init",
        "matt        1234  1.2  0.5 123456 56789 pts/0    Sl+  10:30   0:45 python /home/matt/app.py arg1 arg2",
        "www-data    5678  0.3  0.2  98765 23456 ?        S    09:00   1:23 /usr/sbin/apache2 -k start",
    ]
