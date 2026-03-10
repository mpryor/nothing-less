import re
from dataclasses import dataclass
from enum import Enum


class RowLengthMismatchError(Exception):
    pass


class MetadataColumn(Enum):
    COUNT = "count"
    ARRIVAL = "_arrival"


@dataclass
class Filter:
    column: str | None  # None means any column
    pattern: re.Pattern[str]
    exclude: bool = False


@dataclass
class CliArgs:
    delimiter: str | None
    filters: list[Filter]
    unique_keys: set[str]
    sort_by: str | None
    filename: str | None = None
    theme: str | None = None
    keymap: str | None = None
    tail: bool = False
    time_window: str | None = None
    columns: str | None = None
    raw: bool = False
    no_tui: bool = False
    pipe_output: bool = False  # computed at runtime: stdout is a pipe
    output_format: str = "csv"  # csv, tsv, json, raw


@dataclass
class Column:
    name: str
    labels: set[str]
    render_position: int
    data_position: int
    hidden: bool
    pinned: bool = False
    computed: bool = False  # whether this column is computed (e.g. count)
    delimiter: str | re.Pattern[str] | None = None  # delimiter for parsing JSON fields
    col_ref: str = ""  # reference to the original column name
    col_ref_index: int = -1  # reference to the original column index
    json_ref: str = ""  # reference to the original JSON field
