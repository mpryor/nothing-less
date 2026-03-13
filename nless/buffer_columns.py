"""Column management, creation, and caching for NlessBuffer."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from .dataprocessing import strip_markup
from .delimiter import split_line
from .types import Column, MetadataColumn

if TYPE_CHECKING:
    from .buffer import NlessBuffer


class ColumnMixin:
    """Mixin providing column management methods for NlessBuffer."""

    @staticmethod
    def _make_columns(names: list) -> list[Column]:
        """Create a list of Column objects from a list of names."""
        return [
            Column(
                name=str(n),
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, n in enumerate(names)
        ]

    @staticmethod
    def _ensure_arrival_column(columns: list[Column]) -> None:
        """Ensure the hidden _arrival metadata column is present at the end."""
        if any(c.name == MetadataColumn.ARRIVAL.value for c in columns):
            return
        arrival_pos = len(columns)
        columns.append(
            Column(
                name=MetadataColumn.ARRIVAL.value,
                labels=set(),
                render_position=arrival_pos,
                data_position=arrival_pos,
                hidden=True,
                computed=True,
            )
        )

    @staticmethod
    def _ensure_source_column(columns: list[Column]) -> None:
        """Ensure the hidden _source metadata column is present at the end."""
        if any(c.name == MetadataColumn.SOURCE.value for c in columns):
            return
        source_pos = len(columns)
        columns.append(
            Column(
                name=MetadataColumn.SOURCE.value,
                labels=set(),
                render_position=source_pos,
                data_position=source_pos,
                hidden=True,
                computed=True,
            )
        )

    def _parse_first_line_columns(self: NlessBuffer, first_log_line: str) -> list:
        """Determine column names from the first line based on the delimiter."""
        if self.delimiter == "raw":
            return ["log"]
        elif isinstance(self.delimiter, re.Pattern):
            return list(self.delimiter.groupindex.keys())
        elif self.delimiter == "json":
            try:
                json_data = json.loads(first_log_line)
                if isinstance(json_data, dict):
                    return list(json_data.keys())
                elif isinstance(json_data, list) and len(json_data) > 0:
                    return list(range(len(json_data)))
            except json.JSONDecodeError:
                pass
            return ["value"]
        else:
            return split_line(first_log_line, self.delimiter, self.current_columns)

    def _rebuild_column_caches(self: NlessBuffer) -> None:
        """Rebuild all column-derived caches. Call when columns change."""
        self._col_data_idx = {}
        self._col_render_idx = {}
        for col in self.current_columns:
            plain = strip_markup(col.name)
            self._col_data_idx[plain] = col.data_position
            self._col_render_idx[plain] = col.render_position
        self._sorted_visible_columns = sorted(
            [c for c in self.current_columns if not c.hidden],
            key=lambda c: c.render_position,
        )
        self._has_nested_delimiters = any(
            c.delimiter or c.json_ref or c.col_ref for c in self.current_columns
        )
        self._has_source_column = any(
            c.name == MetadataColumn.SOURCE.value for c in self.current_columns
        )

    def _get_col_idx_by_name(
        self: NlessBuffer, col_name: str, render_position: bool = False
    ) -> int | None:
        cache = self._col_render_idx if render_position else self._col_data_idx
        return cache.get(col_name)

    def _get_column_at_position(self: NlessBuffer, position: int) -> Column | None:
        """Get the visible column at a given cursor index, or None."""
        visible = [
            col
            for col in sorted(self.current_columns, key=lambda c: c.render_position)
            if not col.hidden
        ]
        if 0 <= position < len(visible):
            return visible[position]
        return None

    def _get_visible_column_labels(self: NlessBuffer) -> list[str]:
        labels = []
        for col in sorted(self.current_columns, key=lambda c: c.render_position):
            if not col.hidden:
                labels.append(f"{col.name} {' '.join(col.labels)}".strip())
        return labels

    def _align_cells_to_visible_columns(
        self: NlessBuffer, rows: list[list[str]]
    ) -> list[list[str]]:
        visible_cols = self._sorted_visible_columns
        new_rows = []
        for row in rows:
            new_rows.append([row[col.data_position] for col in visible_cols])
        return new_rows

    def _apply_initial_column_filter(self: NlessBuffer, column_regex: str) -> None:
        """Apply a column visibility filter from CLI args."""
        filters = [name.strip() for name in column_regex.split("|")]
        regexes = [re.compile(rf"{name}", re.IGNORECASE) for name in filters]
        metadata_names = {mc.value for mc in MetadataColumn}
        for col in self.current_columns:
            if col.name in metadata_names or col.pinned:
                continue
            plain_name = strip_markup(col.name)
            matched = False
            for i, regex in enumerate(regexes):
                if regex.search(plain_name):
                    col.hidden = False
                    col.render_position = i
                    matched = True
                    break
            if not matched:
                col.hidden = True
                col.render_position = 99999
        self._rebuild_column_caches()
