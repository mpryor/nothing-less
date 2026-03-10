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


def detect_log_format(
    sample_lines: list[str], min_match_ratio: float = 0.6
) -> LogFormat | None:
    """Detect the best matching log format from a sample of lines.

    For each format, counts lines matching via pattern.match(line).
    Score: match_ratio * 100 + num_named_groups * 2 + priority.
    Only considers formats with match_ratio >= min_match_ratio.
    Custom user formats are checked first and get priority boost.
    Returns highest-scoring format, or None.
    """
    non_empty = [line for line in sample_lines if line.strip()]
    if not non_empty:
        return None

    all_formats = load_custom_formats() + LOG_FORMATS

    best: LogFormat | None = None
    best_score = -1.0

    for fmt in all_formats:
        matches = sum(1 for line in non_empty if fmt.pattern.match(line))
        ratio = matches / len(non_empty)
        if ratio < min_match_ratio:
            continue
        score = ratio * 100 + len(fmt.pattern.groupindex) * 2 + fmt.priority
        if score > best_score:
            best_score = score
            best = fmt

    return best
