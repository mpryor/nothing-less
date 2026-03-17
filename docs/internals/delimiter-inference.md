# Delimiter Inference & Switching

## How nless picks a delimiter

When data first arrives, `infer_delimiter()` in `delimiter.py` scores six
candidate delimiters (`,`, `\t`, `|`, `;`, single space, double space) plus
JSON detection.

### Scoring

JSON is checked first — if every non-empty line parses as a JSON object,
return `"json"` immediately.

For each candidate delimiter, the score starts at zero and accumulates:

| Criterion | Points | Why |
|-----------|--------|-----|
| Splits into > 1 field | +n_fields | More fields = stronger signal |
| All fields non-empty | +2 | Empty fields suggest wrong split |
| Field lengths roughly uniform | +1 | Aligned columns (space delimiters) |
| Tab with all non-empty fields | +3 | Tab is almost always intentional |

Then a cross-line consistency check filters out false positives:

- If the delimiter covers < 50% of sample lines, zero it out
- Count how many lines produce the same field count (the "agreement ratio")
- Agreement >= 80%: boost by `len(distinct_counts) * 2`
- Agreement < 80%: zero out entirely
- The header line must split on the chosen delimiter, or it's zeroed

The candidate with the highest score wins. Ties go to the first in the list
(comma beats tab beats pipe).

### Preamble handling

Some log files have non-data lines at the top (timestamps, comments, blank
lines). `find_header_index()` in `delimiter.py` scans for the first line
that splits into the consensus field count. Lines before it become
`delim.preamble_lines` — they're preserved so a delimiter switch can
restore them.

## Switching delimiters

`switch_delimiter()` in `buffer_delimiter.py` handles all state transitions
when the user presses `D`:

```
1. Restore preamble lines (if any) to raw_rows
2. Clear all filters/sort/search (query.clear_all())
3. Determine new columns:
   - Regex: named capture groups become columns
   - JSON: scan for first parseable JSON dict
   - Standard: split first line to get header
4. Decide if old header should re-enter as data
   (e.g. switching csv -> raw: the header "name,age" is now a data row)
5. Trigger _deferred_update_table() for full rebuild
```

### Auto-switching

During initial load, if > 30% of rows fail to parse with the inferred
delimiter, `_try_auto_switch_delimiter()` kicks in:

1. Sample the failing lines (`_majority_sample` picks lines with matching word counts)
2. Infer a new delimiter from the sample
3. If it differs from current: switch, re-add bad lines as data, use the
   majority sample's first line as the new header

This handles cases like log files where the first few lines look
space-delimited but the bulk is actually CSV.

## Key files

- `delimiter.py` — `infer_delimiter()`, `split_line()`, `find_header_index()`
- `buffer_delimiter.py` — `DelimiterMixin.switch_delimiter()`, auto-switch logic
- `types.py` — `DelimiterState` dataclass
