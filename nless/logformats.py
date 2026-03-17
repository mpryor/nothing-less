"""Library of common log format patterns and auto-detection logic."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

LOG_FORMATS_FILE = "~/.config/nless/log_formats.json"


@dataclass(frozen=True)
class LogFormat:
    name: str
    pattern: re.Pattern[str]
    priority: int = 0


LOG_FORMATS: list[LogFormat] = [
    # Apache/nginx Combined Log Format
    LogFormat(
        name="Apache/nginx Combined",
        pattern=re.compile(
            r"(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<timestamp>[^\]]+)\] "
            r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" '
            r"(?P<status>\d{3}) (?P<size>\S+) "
            r'"(?P<referer>[^"]*)" "(?P<useragent>[^"]*)"'
        ),
        priority=10,
    ),
    # Apache/nginx Common Log Format
    LogFormat(
        name="Apache/nginx Common",
        pattern=re.compile(
            r"(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<timestamp>[^\]]+)\] "
            r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" '
            r"(?P<status>\d{3}) (?P<size>\S+)"
        ),
        priority=5,
    ),
    # Syslog RFC 5424
    LogFormat(
        name="Syslog (RFC 5424)",
        pattern=re.compile(
            r"<(?P<priority>\d+)>(?P<version>\d+) "
            r"(?P<timestamp>\S+) (?P<host>\S+) (?P<app>\S+) "
            r"(?P<procid>\S+) (?P<msgid>\S+) \S+ "
            r"(?P<message>.*)"
        ),
        priority=10,
    ),
    # NGINX error log
    LogFormat(
        name="NGINX Error",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) "
            r"\[(?P<level>\w+)\] "
            r"(?P<pid>\d+)#(?P<tid>\d+): "
            r"(?P<message>.*)"
        ),
        priority=5,
    ),
    # AWS CloudWatch / Lambda
    LogFormat(
        name="AWS CloudWatch/Lambda",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\t"
            r"(?P<request_id>[0-9a-f-]+)\t"
            r"(?P<level>\S+)\t"
            r"(?P<message>.*)"
        ),
        priority=5,
    ),
    # Syslog BSD / RFC 3164
    LogFormat(
        name="Syslog (RFC 3164)",
        pattern=re.compile(
            r"(?P<timestamp>[A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}) "
            r"(?P<host>\S+) "
            r"(?P<process>[^\[:]+)(?:\[(?P<pid>\d+)\])?: "
            r"(?P<message>.*)"
        ),
        priority=3,
    ),
    # ISO 8601 + Level + Logger (Java-style)
    LogFormat(
        name="ISO 8601 + Level + Logger",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,\.]\d+) "
            r"(?P<level>[A-Z]+) +(?P<logger>\S+) "
            r"(?P<message>.*)"
        ),
        priority=2,
    ),
    # ISO 8601 + Level + Message
    LogFormat(
        name="ISO 8601 + Level",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\S*) "
            r"(?P<level>[A-Z]+) "
            r"(?P<message>.*)"
        ),
        priority=1,
    ),
    # Bracket timestamp + level
    LogFormat(
        name="Bracket Timestamp + Level",
        pattern=re.compile(
            r"\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^\]]*)\] "
            r"\[(?P<level>\w+)\] "
            r"(?P<message>.*)"
        ),
        priority=1,
    ),
    # Spring Boot / Logback default:
    # 2024-01-15T14:23:01.123+00:00  INFO 12345 --- [main] c.e.MyApp : message
    LogFormat(
        name="Spring Boot / Logback",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.\d+\S*) +"
            r"(?P<level>[A-Z]+) +"
            r"(?P<pid>\d+) +--- +\[ *(?P<thread>[^\]]+)\] "
            r"(?P<logger>\S+) +: "
            r"(?P<message>.*)"
        ),
        priority=8,
    ),
    # Go stdlib log: 2024/01/15 14:23:01 message
    LogFormat(
        name="Go Log",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) "
            r"(?P<message>.*)"
        ),
        priority=0,
    ),
    # Logrus / slog text: time="..." level=info msg="..." key=value
    LogFormat(
        name="Logrus / slog Text",
        pattern=re.compile(
            r'time="(?P<timestamp>[^"]+)" '
            r"level=(?P<level>\w+) "
            r'msg="(?P<message>[^"]*)"'
            r"(?P<fields>.*)"
        ),
        priority=3,
    ),
    # Elixir Logger: "HH:MM:SS.mmm [level] message" or with date prefix
    LogFormat(
        name="Elixir Logger",
        pattern=re.compile(
            r"(?P<timestamp>(?:\d{4}-\d{2}-\d{2} )?\d{2}:\d{2}:\d{2}\.\d+) "
            r"\[(?P<level>\w+)\] "
            r"(?P<message>.*)"
        ),
        priority=1,
    ),
    # Ruby/Rails Logger: I, [2024-01-15T14:23:01.123456 #12345]  INFO -- : message
    LogFormat(
        name="Ruby/Rails Logger",
        pattern=re.compile(
            r"(?P<severity_char>[DIWEF]), "
            r"\[(?P<timestamp>[^\s]+) #(?P<pid>\d+)\] +"
            r"(?P<level>\w+) -- "
            r"(?P<progname>[^:]*): "
            r"(?P<message>.*)"
        ),
        priority=5,
    ),
    # PHP/Laravel Monolog: [2024-01-15 14:23:01] channel.LEVEL: message {"ctx":{}} []
    LogFormat(
        name="Laravel / Monolog",
        pattern=re.compile(
            r"\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] "
            r"(?P<channel>\w+)\.(?P<level>[A-Z]+): "
            r"(?P<message>.*)"
        ),
        priority=5,
    ),
    # Rust env_logger: [2024-01-15T14:23:01Z INFO  myapp::module] message
    LogFormat(
        name="Rust env_logger",
        pattern=re.compile(
            r"\[(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\S*) "
            r"(?P<level>\w+) +(?P<target>[^\]]+)\] "
            r"(?P<message>.*)"
        ),
        priority=5,
    ),
    # .NET / ASP.NET Core: info: Microsoft.Hosting[14] message
    LogFormat(
        name=".NET Core Logger",
        pattern=re.compile(
            r"(?P<level>\w+): "
            r"(?P<category>[^\[]+)\[(?P<event_id>\d+)\]"
            r"(?P<message>.*)"
        ),
        priority=2,
    ),
    # Python logging default: LEVEL:logger:message
    LogFormat(
        name="Python Logging Default",
        pattern=re.compile(r"(?P<level>[A-Z]+):(?P<logger>[^:]+):(?P<message>.*)"),
        priority=0,
    ),
    # Python logging with timestamp and dash separators:
    # 2024-01-15 14:23:01,123 - myapp - INFO - message
    LogFormat(
        name="Python Logging Dash",
        pattern=re.compile(
            r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,\.]\d+) - "
            r"(?P<logger>\S+) - "
            r"(?P<level>[A-Z]+) - "
            r"(?P<message>.*)"
        ),
        priority=2,
    ),
]


_LOG_LEVELS = frozenset(
    {
        "TRACE",
        "DEBUG",
        "INFO",
        "WARN",
        "WARNING",
        "ERROR",
        "FATAL",
        "CRITICAL",
        "NOTICE",
        "ALERT",
        "EMERG",
    }
)

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TIME_RE = re.compile(r"\d{2}:\d{2}:\d{2}(?:[.,]\d+)?")
_BRACKETED_RE = re.compile(r"\[([^\]]+)\]")
_QUOTED_RE = re.compile(r'"([^"]*)"')
_DURATION_RE = re.compile(r"\d+(?:\.\d+)?(?:ms|s|us|ns|µs)")
_KV_RE = re.compile(r"(\w+)=(\S+)")
_HTTP_METHODS = frozenset({"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"})

# Additional token patterns (most-specific first)
_ISO8601_FULL_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:\d{2})?"
)
_IPV4_RE = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)
_ANGLE_PRIORITY_RE = re.compile(r"<\d+>")
_PID_HASH_RE = re.compile(r"#\d+")
_DOTTED_PACKAGE_RE = re.compile(r"[a-zA-Z]\w*(?:\.[a-zA-Z]\w*){1,}")
_SINGLE_SEVERITY_RE = re.compile(r"[IWEFD]")
_DATE_TIME_DIGITS_RE = re.compile(r".*\d{2}[:/.-]\d{2}.*")
# Composite date + time (space-separated inside a single token from non-space delimiters)
_COMPOSITE_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[.,]\d+)?")


def _classify_token(token: str) -> tuple[str, str] | tuple[None, None]:
    """Classify a single token.

    Returns ``(regex_pattern, suggested_name)`` for structured tokens,
    or ``(None, None)`` for free-text / unrecognisable tokens.
    """
    # Composite date + time (from non-space separators like " - ")
    if _COMPOSITE_TIMESTAMP_RE.fullmatch(token):
        return r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[.,]\d+)?", "timestamp"
    # Full ISO 8601 timestamp (must come before bare date)
    if _ISO8601_FULL_RE.fullmatch(token):
        return (
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:\d{2})?",
            "timestamp",
        )
    if _DATE_RE.fullmatch(token):
        return r"\d{4}-\d{2}-\d{2}", "date"
    if _TIME_RE.fullmatch(token):
        return r"\d{2}:\d{2}:\d{2}(?:[.,]\d+)?", "time"
    # Bracketed content — smart naming
    m = _BRACKETED_RE.fullmatch(token)
    if m:
        inner = m.group(1)
        if _DATE_TIME_DIGITS_RE.match(inner):
            return "bracket", "timestamp"
        if inner.upper() in _LOG_LEVELS:
            return "bracket", "level"
        if re.match(r"[\w.-]+-\d+|Thread-\d+|main|pool-", inner):
            return "bracket", "thread"
        return "bracket", "tag"
    # Quoted string
    qm = _QUOTED_RE.fullmatch(token)
    if qm:
        return "quoted", "request"
    # Angle-bracket priority
    if _ANGLE_PRIORITY_RE.fullmatch(token):
        return r"<\d+>", "priority"
    # UUID
    if _UUID_RE.fullmatch(token):
        return (
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            "request_id",
        )
    # IPv4
    if _IPV4_RE.fullmatch(token):
        return r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", "ip"
    # PID hash
    if _PID_HASH_RE.fullmatch(token):
        return r"#\d+", "pid"
    if token in _HTTP_METHODS:
        return r"(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)", "method"
    if token.startswith("/"):
        return r"\S+", "path"
    if _DURATION_RE.fullmatch(token):
        return r"\d+(?:\.\d+)?(?:ms|s|us|ns|µs)", "duration"
    kv_m = _KV_RE.fullmatch(token)
    if kv_m:
        return rf"{re.escape(kv_m.group(1))}=\S+", kv_m.group(1)
    if token.upper() in _LOG_LEVELS:
        return r"\w+", "level"
    # Dotted package name (logger)
    if _DOTTED_PACKAGE_RE.fullmatch(token):
        return r"\S+", "logger"
    # Single severity character
    if _SINGLE_SEVERITY_RE.fullmatch(token):
        return r"[IWEFD]", "severity"
    if re.fullmatch(r"\d{3}", token):
        return r"\d{3}", "status"
    if re.fullmatch(r"\d+", token):
        return r"\d+", "number"
    return None, None


def _detect_separator(lines: list[str]) -> str:
    """Detect the field separator used across sample lines.

    Tries candidates in order of specificity: tab, `` - ``, space.
    """
    if len(lines) < 2:
        return " "

    # Tab: all lines must contain tabs
    if all("\t" in line for line in lines):
        return "\t"

    # Space-dash-space: >=80% of lines have 3+ occurrences
    dash_counts = [line.count(" - ") for line in lines]
    if sum(1 for c in dash_counts if c >= 3) / len(lines) >= 0.8:
        return " - "

    # Pipe: >=80% of lines have 3+ occurrences
    pipe_counts = [line.count(" | ") for line in lines]
    if sum(1 for c in pipe_counts if c >= 3) / len(lines) >= 0.8:
        return " | "

    return " "


def _tokenize_line(line: str, separator: str) -> list[str]:
    """Split *line* on *separator*, respecting brackets and quotes.

    Content inside ``[...]`` or ``"..."`` is kept as a single token
    even if it contains the separator.
    """
    if separator == " ":
        # For space separator, do character-level walk
        return _tokenize_line_space(line)

    # For multi-char separators (tab, " - "), simple split but merge
    # bracketed / quoted segments first.
    # Pre-merge brackets and quotes by replacing separator inside them
    placeholder = "\x00"
    out: list[str] = []
    i = 0
    buf: list[str] = []
    depth = 0
    in_quote = False
    while i < len(line):
        ch = line[i]
        if ch == "[" and not in_quote:
            depth += 1
            buf.append(ch)
        elif ch == "]" and not in_quote and depth > 0:
            depth -= 1
            buf.append(ch)
        elif ch == '"' and depth == 0:
            in_quote = not in_quote
            buf.append(ch)
        elif depth == 0 and not in_quote and line[i : i + len(separator)] == separator:
            out.append("".join(buf).replace(placeholder, separator))
            buf = []
            i += len(separator)
            continue
        else:
            buf.append(ch)
        i += 1
    if buf:
        out.append("".join(buf).replace(placeholder, separator))
    return out


def _tokenize_line_space(line: str) -> list[str]:
    """Space-aware tokenizer that keeps brackets and quotes intact."""
    tokens: list[str] = []
    buf: list[str] = []
    i = 0
    depth = 0
    in_quote = False
    while i < len(line):
        ch = line[i]
        if ch == "[" and not in_quote:
            depth += 1
            buf.append(ch)
        elif ch == "]" and not in_quote and depth > 0:
            depth -= 1
            buf.append(ch)
        elif ch == '"' and depth == 0:
            in_quote = not in_quote
            buf.append(ch)
        elif ch == " " and depth == 0 and not in_quote:
            if buf:
                tokens.append("".join(buf))
                buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        tokens.append("".join(buf))
    return tokens


def _token_shape(token: str) -> str:
    """Return a shape fingerprint: digits→D, letters→L, keep punctuation."""
    out: list[str] = []
    prev = ""
    for ch in token:
        if ch.isdigit():
            cat = "D"
        elif ch.isalpha():
            cat = "L"
        else:
            cat = ch
        # Collapse runs of same category
        if cat != prev:
            out.append(cat)
            prev = cat
    return "".join(out)


def _classify_position(
    tokens_at_pos: list[str],
) -> tuple[str, str] | None:
    """Majority-vote classification for tokens at a given position.

    Returns ``(pattern, name)`` if >=70% agree, or if all tokens share
    the same shape fingerprint (captured as generic ``\\S+``).
    Returns ``None`` if neither condition is met (message boundary).
    """
    votes: dict[tuple[str, str], int] = {}
    for token in tokens_at_pos:
        result = _classify_token(token)
        if result[0] is not None:
            key = (result[0], result[1])  # type: ignore[index]
            votes[key] = votes.get(key, 0) + 1

    total = len(tokens_at_pos)
    if votes:
        best_key = max(votes, key=lambda k: votes[k])
        if votes[best_key] / total >= 0.7:
            return best_key

    # Check shape consistency — if all tokens have the same shape, capture.
    # Exclude trivial shapes (pure letters/digits) that indicate free-text.
    shapes = {_token_shape(t) for t in tokens_at_pos}
    if len(shapes) == 1:
        shape = next(iter(shapes))
        # Only use shape consistency for structured tokens (with punctuation)
        if shape not in ("L", "D") and len(shape) > 1:
            return r"\S+", f"field_{hash(shape) % 10000}"

    return None


def _build_named_regex(
    classifications: list[tuple[str, str]],
    separator: str = " ",
    message_position: str = "end",
) -> str:
    """Build a full regex string with named groups from classified tokens.

    Merges adjacent date + time into a single ``timestamp`` group,
    deduplicates names, and appends a ``message`` tail capture.
    """
    # Merge adjacent date + time into timestamp
    merged: list[tuple[str, str]] = []
    i = 0
    while i < len(classifications):
        pat, name = classifications[i]
        if (
            name == "date"
            and i + 1 < len(classifications)
            and classifications[i + 1][1] == "time"
        ):
            date_pat = classifications[i][0]
            time_pat = classifications[i + 1][0]
            # Use the actual separator between date and time
            merged.append(
                (
                    rf"{date_pat}{re.escape(separator)}{time_pat}",
                    "timestamp",
                )
            )
            i += 2
        else:
            merged.append((pat, name))
            i += 1

    # Deduplicate names
    counts: dict[str, int] = {}
    for _pat, name in merged:
        counts[name] = counts.get(name, 0) + 1
    seen: dict[str, int] = {}
    for j, (pat, name) in enumerate(merged):
        if counts[name] > 1:
            idx = seen.get(name, 0) + 1
            seen[name] = idx
            merged[j] = (pat, f"{name}{idx}")
        else:
            seen[name] = 1

    # Build regex parts with named groups
    sep_re = re.escape(separator) if separator != " " else " "
    # For non-space separators, tokens may contain spaces.
    # Use a lookahead to match "everything up to the separator" instead of \S+
    if separator == " ":
        generic_capture = r"\S+"
    elif separator == "\t":
        generic_capture = r"[^\t]+"
    else:
        # e.g. " - " → match everything that isn't followed by " - "
        generic_capture = rf"(?:(?!{sep_re}).)+?"

    regex_parts: list[str] = []
    for pat, name in merged:
        if pat == "bracket":
            # Bracket token — capture inner content (supports spaces/special chars)
            regex_parts.append(rf"\[(?P<{name}>[^\]]+)\]")
        elif pat == "quoted":
            regex_parts.append(rf'"(?P<{name}>[^"]*)"')
        elif r"=\S+" in pat:
            # key=value — wrap value portion in named group
            regex_parts.append(pat.replace(r"=\S+", rf"=(?P<{name}>\S+)"))
        elif pat == r"\S+" and separator != " ":
            # Generic capture — use separator-aware pattern
            regex_parts.append(rf"(?P<{name}>{generic_capture})")
        else:
            regex_parts.append(rf"(?P<{name}>{pat})")

    # Join structured parts, then append/prepend message capture
    main_regex = sep_re.join(regex_parts)
    if message_position == "start":
        return r"(?P<message>.*?)" + sep_re + main_regex
    else:
        return main_regex + r"(?:" + sep_re + r"(?P<message>.*))?"


def infer_log_pattern(sample_lines: list[str]) -> LogFormat | None:
    """Try to infer a log format regex from sample lines.

    Uses separator detection, quote/bracket-aware tokenization, and
    majority-vote classification per position.  Everything after the
    last classified position is captured as ``message``.

    Returns a ``LogFormat`` with named groups, or ``None`` if the lines
    don't have enough recognisable structure.
    """
    non_empty = [line.strip() for line in sample_lines if line.strip()]
    if len(non_empty) < 3:
        return None

    separator = _detect_separator(non_empty)
    tokenized = [_tokenize_line(line, separator) for line in non_empty]
    min_tokens = min(len(t) for t in tokenized)
    if min_tokens < 2:
        return None

    classifications: list[tuple[str, str]] = []
    message_position = "end"

    for pos in range(min_tokens):
        tokens_at_pos = [t[pos] for t in tokenized]
        result = _classify_position(tokens_at_pos)
        if result is None:
            break
        classifications.append(result)

    if len(classifications) < 2:
        # Try right-to-left: message may be at the start
        rtl_classifications: list[tuple[str, str]] = []
        for pos in range(min_tokens - 1, -1, -1):
            tokens_at_pos = [t[pos] for t in tokenized]
            result = _classify_position(tokens_at_pos)
            if result is None:
                break
            rtl_classifications.append(result)
        if len(rtl_classifications) >= 2:
            classifications = list(reversed(rtl_classifications))
            message_position = "start"

    if len(classifications) < 2:
        return None

    full_regex = _build_named_regex(classifications, separator, message_position)

    try:
        compiled = re.compile(full_regex)
    except re.error:
        return None

    # Validate match rate
    matches = sum(1 for line in non_empty if compiled.match(line))
    if matches / len(non_empty) < 0.6:
        return None

    return LogFormat(name="Auto-detected", pattern=compiled, priority=0)


def load_custom_formats() -> list[LogFormat]:
    """Load user-defined log formats from ~/.config/nless/log_formats.json.

    Expected format:
        [{"name": "My Format", "pattern": "(?P<ts>\\\\d+) (?P<msg>.*)"}]

    Each pattern must be a valid regex with at least one named group.
    Invalid entries are silently skipped.
    """
    path = os.path.expanduser(LOG_FORMATS_FILE)
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []
    if not isinstance(data, list):
        return []
    formats: list[LogFormat] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        raw_pattern = entry.get("pattern")
        if not name or not raw_pattern:
            continue
        try:
            compiled = re.compile(raw_pattern)
        except re.error as e:
            logger.debug("Skipping custom log format %r: %s", name, e)
            continue
        if not compiled.groupindex:
            logger.debug("Skipping custom log format %r: no named groups", name)
            continue
        priority = entry.get("priority", 100)
        formats.append(LogFormat(name=name, pattern=compiled, priority=priority))
    return formats


def save_custom_format(name: str, pattern: str) -> None:
    """Append a log format to the user's custom formats file."""
    path = os.path.expanduser(LOG_FORMATS_FILE)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    existing: list[dict] = []
    if os.path.exists(path):
        try:
            with open(path) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            existing = []
    # Replace if same name exists
    existing = [e for e in existing if e.get("name") != name]
    existing.append({"name": name, "pattern": pattern})
    with open(path, "w") as f:
        json.dump(existing, f, indent=2)


def detect_log_formats(
    sample_lines: list[str], min_match_ratio: float = 0.6
) -> list[tuple[LogFormat, float]]:
    """Detect matching log formats from a sample of lines, ranked by score.

    For each format, counts lines matching via pattern.match(line).
    Score: match_ratio * 100 + num_named_groups * 2 + priority.
    Only considers formats with match_ratio >= min_match_ratio.
    Custom user formats are checked first and get priority boost.
    Returns list of ``(LogFormat, score)`` sorted by descending score.
    """
    non_empty = [line for line in sample_lines if line.strip()]
    if not non_empty:
        return []

    all_formats = load_custom_formats() + LOG_FORMATS

    candidates: list[tuple[LogFormat, float]] = []
    for fmt in all_formats:
        matches = sum(1 for line in non_empty if fmt.pattern.match(line))
        ratio = matches / len(non_empty)
        if ratio < min_match_ratio:
            continue
        score = ratio * 100 + len(fmt.pattern.groupindex) * 2 + fmt.priority
        candidates.append((fmt, score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates


def detect_log_format(
    sample_lines: list[str], min_match_ratio: float = 0.6
) -> LogFormat | None:
    """Detect the best matching log format from a sample of lines.

    Convenience wrapper around :func:`detect_log_formats` that returns
    only the top-scoring format, or ``None``.
    """
    candidates = detect_log_formats(sample_lines, min_match_ratio)
    return candidates[0][0] if candidates else None
