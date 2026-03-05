from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .buffer import NlessBuffer
    from .input import LineStream


@dataclass
class BufferGroup:
    group_id: int
    name: str
    buffers: list[NlessBuffer] = field(default_factory=list)
    curr_buffer_idx: int = 0
    starting_stream: LineStream | None = None

    def get_current_buffer(self) -> NlessBuffer:
        return self.buffers[self.curr_buffer_idx]

    def add_buffer(self, buf: NlessBuffer) -> None:
        self.buffers.append(buf)

    def remove_buffer(self, idx: int) -> None:
        self.buffers.pop(idx)
        if self.curr_buffer_idx >= len(self.buffers):
            self.curr_buffer_idx = max(0, len(self.buffers) - 1)
