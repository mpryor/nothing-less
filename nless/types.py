import re
from dataclasses import dataclass
from enum import Enum

class MetadataColumn(Enum):
    COUNT = "count"

@dataclass
class Filter:
    column: str | None  # None means any column
    pattern: re.Pattern[str]

@dataclass
class Column:
    name: str
    labels: set[str]
    render_position: int
    data_position: int
    hidden: bool
    pinned: bool = False
    computed: bool = False  # whether this column is computed (e.g. count)
    json_ref: str = ""  # reference to the original JSON field
