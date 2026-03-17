# Internals

Developer documentation for the non-obvious parts of nless. Start here
if you're about to modify the streaming pipeline, delimiter logic, or
session persistence.

## Architecture
- [State Architecture](state-architecture.md) — the six state objects,
  mixin structure, and how they interact
- [Threading Model](threading.md) — three threads, lock protocol,
  generation counter, chain rebuild backoff

## Data flow
- [Streaming Pipeline](streaming-pipeline.md) — how rows flow from
  stdin to the DataTable, incremental vs full rebuild, parsed row cache
- [Delimiter Inference](delimiter-inference.md) — scoring algorithm,
  preamble handling, auto-switch logic
- [Copy & History](copy-and-history.md) — copy-on-write buffers, merge,
  filter compaction

## Features
- [Column Operations](column-operations.md) — column splitting, JSON
  extraction, dedup/pivot, pinning
- [Log Format Detection](log-format-detection.md) — known format
  matching, inferred pattern construction, token classification
- [Time Windows](time-windows.md) — fixed vs rolling, timer mechanism,
  interaction with streaming
- [Session Persistence](sessions.md) — capture/apply round-trip,
  serialization, file reload, views

## Rendering
- [Custom DataTable](custom-rendering.md) — why a custom widget,
  render_line hot path, RawPager, fixed columns
