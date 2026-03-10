import json

from nless.logformats import (
    LOG_FORMATS,
    LogFormat,
    detect_log_format,
    load_custom_formats,
    save_custom_format,
)


class TestLogFormatPatterns:
    """Verify each format's regex matches representative lines and captures expected groups."""

    def test_apache_combined(self):
        line = '93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /downloads/product_1 HTTP/1.1" 304 0 "-" "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"'
        fmt = _find_format("Apache/nginx Combined")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("ip") == "93.180.71.3"
        assert m.group("method") == "GET"
        assert m.group("status") == "304"
        assert m.group("useragent") == "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"

    def test_apache_common(self):
        line = '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326'
        fmt = _find_format("Apache/nginx Common")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("ip") == "127.0.0.1"
        assert m.group("user") == "frank"
        assert m.group("size") == "2326"

    def test_syslog_rfc5424(self):
        line = "<165>1 2023-08-24T05:14:15.000003-07:00 myhost myapp 1234 ID47 - This is a message"
        fmt = _find_format("Syslog (RFC 5424)")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("priority") == "165"
        assert m.group("version") == "1"
        assert m.group("host") == "myhost"
        assert m.group("app") == "myapp"
        assert m.group("message") == "This is a message"

    def test_nginx_error(self):
        line = "2024/01/15 14:23:01 [error] 12345#0: *67890 upstream timed out"
        fmt = _find_format("NGINX Error")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "error"
        assert m.group("pid") == "12345"
        assert m.group("tid") == "0"

    def test_aws_cloudwatch(self):
        line = "2024-01-15T14:23:01.123Z\td4c3b2a1-e5f6-7890-abcd-ef1234567890\tINFO\tProcessing request"
        fmt = _find_format("AWS CloudWatch/Lambda")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("request_id") == "d4c3b2a1-e5f6-7890-abcd-ef1234567890"

    def test_syslog_rfc3164(self):
        line = "Jan  5 14:23:01 myhost sshd[12345]: Accepted publickey for user"
        fmt = _find_format("Syslog (RFC 3164)")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("host") == "myhost"
        assert m.group("process") == "sshd"
        assert m.group("pid") == "12345"

    def test_syslog_rfc3164_no_pid(self):
        line = "Jan  5 14:23:01 myhost kernel: some kernel message"
        fmt = _find_format("Syslog (RFC 3164)")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("process") == "kernel"
        assert m.group("pid") is None
        assert m.group("message") == "some kernel message"

    def test_iso8601_level_logger(self):
        line = "2024-01-15 14:23:01,123 INFO com.example.Main Starting application"
        fmt = _find_format("ISO 8601 + Level + Logger")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("logger") == "com.example.Main"

    def test_iso8601_level(self):
        line = "2024-01-15T14:23:01 INFO Server started on port 8080"
        fmt = _find_format("ISO 8601 + Level")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("message") == "Server started on port 8080"

    def test_bracket_timestamp_level(self):
        line = "[2024-01-15 14:23:01] [INFO] Application started"
        fmt = _find_format("Bracket Timestamp + Level")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("message") == "Application started"

    def test_go_log(self):
        line = "2024/01/15 14:23:01 Starting server on :8080"
        fmt = _find_format("Go Log")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("timestamp") == "2024/01/15 14:23:01"
        assert m.group("message") == "Starting server on :8080"

    def test_logrus_text(self):
        line = 'time="2024-01-15T14:23:01Z" level=info msg="Server started" port=8080'
        fmt = _find_format("Logrus / slog Text")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "info"
        assert m.group("message") == "Server started"

    def test_elixir_logger_time_only(self):
        line = "14:23:01.123 [info] Running MyApp.Endpoint"
        fmt = _find_format("Elixir Logger")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "info"
        assert m.group("message") == "Running MyApp.Endpoint"

    def test_elixir_logger_with_date(self):
        line = "2024-01-15 14:23:01.123 [warning] Something happened"
        fmt = _find_format("Elixir Logger")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "warning"

    def test_ruby_rails_logger(self):
        line = "I, [2024-01-15T14:23:01.123456 #12345]  INFO -- app: Started GET /users"
        fmt = _find_format("Ruby/Rails Logger")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("severity_char") == "I"
        assert m.group("pid") == "12345"
        assert m.group("level") == "INFO"
        assert m.group("progname") == "app"
        assert m.group("message") == "Started GET /users"

    def test_laravel_monolog(self):
        line = '[2024-01-15 14:23:01] production.ERROR: Something failed {"exception":"RuntimeException"} []'
        fmt = _find_format("Laravel / Monolog")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("channel") == "production"
        assert m.group("level") == "ERROR"
        assert "Something failed" in m.group("message")

    def test_rust_env_logger(self):
        line = "[2024-01-15T14:23:01Z INFO  myapp::server] Listening on 0.0.0.0:8080"
        fmt = _find_format("Rust env_logger")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("target").strip() == "myapp::server"
        assert m.group("message") == "Listening on 0.0.0.0:8080"

    def test_dotnet_core_logger(self):
        line = "info: Microsoft.Hosting.Lifetime[14] Now listening on: https://localhost:5001"
        fmt = _find_format(".NET Core Logger")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "info"
        assert m.group("category") == "Microsoft.Hosting.Lifetime"
        assert m.group("event_id") == "14"

    def test_spring_boot_logback(self):
        line = "2024-01-15T14:23:01.123+00:00  INFO 12345 --- [main] c.e.demo.MyApp                : Starting MyApp"
        fmt = _find_format("Spring Boot / Logback")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("pid") == "12345"
        assert m.group("thread") == "main"
        assert m.group("logger") == "c.e.demo.MyApp"
        assert m.group("message") == "Starting MyApp"

    def test_spring_boot_logback_with_spaces_in_timestamp(self):
        line = "2024-01-15 14:23:01.123  INFO 12345 --- [           main] c.e.MyApp                    : Started"
        fmt = _find_format("Spring Boot / Logback")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("thread").strip() == "main"

    def test_python_logging_default(self):
        line = "WARNING:root:some warning message"
        fmt = _find_format("Python Logging Default")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "WARNING"
        assert m.group("logger") == "root"
        assert m.group("message") == "some warning message"

    def test_python_logging_dash(self):
        line = "2024-01-15 14:23:01,123 - myapp.module - INFO - Processing request"
        fmt = _find_format("Python Logging Dash")
        m = fmt.pattern.match(line)
        assert m is not None
        assert m.group("level") == "INFO"
        assert m.group("logger") == "myapp.module"
        assert m.group("message") == "Processing request"


class TestDetectLogFormat:
    def test_detects_syslog(self):
        lines = [
            "Jan  5 14:23:01 myhost sshd[12345]: Accepted publickey",
            "Jan  5 14:23:02 myhost sshd[12345]: pam_unix session opened",
            "Jan  5 14:23:03 myhost cron[999]: (root) CMD (/usr/bin/foo)",
            "Jan  5 14:24:00 myhost kernel: some kernel message",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Syslog" in result.name

    def test_detects_apache(self):
        lines = [
            '93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /downloads/product_1 HTTP/1.1" 304 0 "-" "Debian APT-HTTP/1.3"',
            '93.180.71.3 - - [17/May/2015:08:05:23 +0000] "GET /downloads/product_2 HTTP/1.1" 200 490 "-" "Debian APT-HTTP/1.3"',
            '80.91.33.133 - - [17/May/2015:08:05:24 +0000] "GET /downloads/product_1 HTTP/1.1" 304 0 "-" "Debian APT-HTTP/1.3"',
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Apache" in result.name or "Combined" in result.name

    def test_detects_iso_timestamp(self):
        lines = [
            "2024-01-15T14:23:01 INFO Server started",
            "2024-01-15T14:23:02 DEBUG Loading config",
            "2024-01-15T14:23:03 WARN Low memory",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "ISO" in result.name

    def test_detects_rails(self):
        lines = [
            "I, [2024-01-15T14:23:01.123456 #12345]  INFO -- app: Started GET /users",
            "I, [2024-01-15T14:23:01.234567 #12345]  INFO -- app: Processing by UsersController#index",
            "I, [2024-01-15T14:23:01.345678 #12345]  INFO -- app: Completed 200 OK in 15ms",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Ruby" in result.name or "Rails" in result.name

    def test_detects_laravel(self):
        lines = [
            "[2024-01-15 14:23:01] production.ERROR: Something failed {} []",
            "[2024-01-15 14:23:02] production.INFO: Request processed {} []",
            "[2024-01-15 14:23:03] production.WARNING: Slow query {} []",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Laravel" in result.name or "Monolog" in result.name

    def test_detects_rust_env_logger(self):
        lines = [
            "[2024-01-15T14:23:01Z INFO  myapp::server] Listening on 0.0.0.0:8080",
            "[2024-01-15T14:23:01Z DEBUG myapp::db] Connected to database",
            "[2024-01-15T14:23:02Z WARN  myapp::cache] Cache miss for key=abc",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Rust" in result.name or "env_logger" in result.name

    def test_detects_dotnet(self):
        lines = [
            "info: Microsoft.Hosting.Lifetime[14] Now listening on: https://localhost:5001",
            "info: Microsoft.Hosting.Lifetime[0] Application started",
            "warn: Microsoft.EntityFrameworkCore[20503] Query took too long",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert ".NET" in result.name

    def test_detects_spring_boot(self):
        lines = [
            "2024-01-15T14:23:01.123+00:00  INFO 12345 --- [main] c.e.demo.MyApp                : Starting",
            "2024-01-15T14:23:01.456+00:00  INFO 12345 --- [main] c.e.demo.MyApp                : Started",
            "2024-01-15T14:23:02.789+00:00 DEBUG 12345 --- [http-nio-8080-exec-1] c.e.demo.Controller : Request received",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Spring" in result.name

    def test_detects_python_logging(self):
        lines = [
            "WARNING:root:something happened",
            "INFO:myapp:started",
            "ERROR:myapp.db:connection failed",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert "Python" in result.name

    def test_csv_returns_none(self):
        lines = ["name,age,city", "Alice,30,NYC", "Bob,25,LA"]
        result = detect_log_format(lines)
        assert result is None

    def test_noise_still_detects(self):
        """70% match + 30% noise should still detect."""
        lines = [
            "2024-01-15T14:23:01 INFO msg1",
            "2024-01-15T14:23:02 DEBUG msg2",
            "2024-01-15T14:23:03 WARN msg3",
            "2024-01-15T14:23:04 INFO msg4",
            "2024-01-15T14:23:05 ERROR msg5",
            "2024-01-15T14:23:06 INFO msg6",
            "2024-01-15T14:23:07 DEBUG msg7",
            "this is just some random noise",
            "another random line",
            "third noise line",
        ]
        result = detect_log_format(lines)
        assert result is not None

    def test_below_threshold_returns_none(self):
        """Below 60% match → returns None."""
        lines = [
            "2024-01-15T14:23:01 INFO msg1",
            "2024-01-15T14:23:02 DEBUG msg2",
            "random noise 1",
            "random noise 2",
            "random noise 3",
            "random noise 4",
            "random noise 5",
        ]
        result = detect_log_format(lines)
        assert result is None

    def test_empty_input_returns_none(self):
        assert detect_log_format([]) is None

    def test_all_empty_lines_returns_none(self):
        assert detect_log_format(["", "  ", ""]) is None


class TestCustomFormats:
    def test_save_and_load(self, tmp_path, monkeypatch):
        fmt_file = tmp_path / "log_formats.json"
        monkeypatch.setattr("nless.logformats.LOG_FORMATS_FILE", str(fmt_file))
        save_custom_format("My Format", r"(?P<ts>\d+) (?P<msg>.*)")
        formats = load_custom_formats()
        assert len(formats) == 1
        assert formats[0].name == "My Format"
        assert formats[0].priority == 100  # default high priority

    def test_save_replaces_same_name(self, tmp_path, monkeypatch):
        fmt_file = tmp_path / "log_formats.json"
        monkeypatch.setattr("nless.logformats.LOG_FORMATS_FILE", str(fmt_file))
        save_custom_format("My Format", r"(?P<a>\w+)")
        save_custom_format("My Format", r"(?P<b>\d+)")
        formats = load_custom_formats()
        assert len(formats) == 1
        assert "b" in formats[0].pattern.groupindex

    def test_custom_format_wins_detection(self, tmp_path, monkeypatch):
        fmt_file = tmp_path / "log_formats.json"
        monkeypatch.setattr("nless.logformats.LOG_FORMATS_FILE", str(fmt_file))
        save_custom_format(
            "My Custom Log",
            r"MYAPP (?P<level>\w+) (?P<code>\d+) (?P<message>.*)",
        )
        lines = [
            "MYAPP INFO 200 request handled",
            "MYAPP ERROR 500 internal error",
            "MYAPP DEBUG 201 created resource",
        ]
        result = detect_log_format(lines)
        assert result is not None
        assert result.name == "My Custom Log"

    def test_load_missing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "nless.logformats.LOG_FORMATS_FILE", str(tmp_path / "nope.json")
        )
        assert load_custom_formats() == []

    def test_load_invalid_json(self, tmp_path, monkeypatch):
        fmt_file = tmp_path / "log_formats.json"
        fmt_file.write_text("not json")
        monkeypatch.setattr("nless.logformats.LOG_FORMATS_FILE", str(fmt_file))
        assert load_custom_formats() == []

    def test_load_skips_invalid_regex(self, tmp_path, monkeypatch):
        fmt_file = tmp_path / "log_formats.json"
        fmt_file.write_text(json.dumps([{"name": "Bad", "pattern": "(?P<a>[invalid"}]))
        monkeypatch.setattr("nless.logformats.LOG_FORMATS_FILE", str(fmt_file))
        assert load_custom_formats() == []

    def test_load_skips_no_named_groups(self, tmp_path, monkeypatch):
        fmt_file = tmp_path / "log_formats.json"
        fmt_file.write_text(json.dumps([{"name": "No Groups", "pattern": r"\d+"}]))
        monkeypatch.setattr("nless.logformats.LOG_FORMATS_FILE", str(fmt_file))
        assert load_custom_formats() == []


def _find_format(name: str) -> LogFormat:
    for fmt in LOG_FORMATS:
        if fmt.name == name:
            return fmt
    raise ValueError(f"No format named {name!r}")
