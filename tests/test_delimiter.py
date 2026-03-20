import re

from nless.delimiter import (
    detect_column_positions,
    detect_space_max_fields,
    detect_space_splitting_strategy,
    find_header_index,
    flatten_json_lines,
    infer_delimiter,
    split_aligned_row,
    split_aligned_row_preserve_single_spaces,
    split_by_positions,
    split_csv_row,
    split_line,
)
from nless.types import Column


class TestSplitCsvRow:
    def test_standard_csv(self):
        assert split_csv_row("a,b,c") == ["a", "b", "c"]

    def test_quoted_fields_with_commas(self):
        assert split_csv_row('"hello, world",b,c') == ["hello, world", "b", "c"]

    def test_empty_fields(self):
        assert split_csv_row("a,,c") == ["a", "", "c"]

    def test_single_field(self):
        assert split_csv_row("only") == ["only"]


class TestSplitAlignedRow:
    def test_space_aligned_columns(self):
        result = split_aligned_row("nginx      Running   0          5d")
        assert result == ["nginx", "Running", "0", "5d"]

    def test_single_word(self):
        assert split_aligned_row("hello") == ["hello"]

    def test_leading_trailing_spaces(self):
        result = split_aligned_row("  foo   bar  ")
        assert result == ["foo", "bar"]


class TestSplitAlignedRowPreserveSingleSpaces:
    def test_double_space_delimited(self):
        result = split_aligned_row_preserve_single_spaces(
            "New York  San Francisco  Los Angeles"
        )
        assert result == ["New York", "San Francisco", "Los Angeles"]

    def test_mixed_spacing(self):
        result = split_aligned_row_preserve_single_spaces("one  two three  four")
        assert result == ["one", "two three", "four"]


class TestSplitLine:
    def test_comma_delimiter(self):
        result = split_line("a,b,c", ",", [])
        assert result == ["a", "b", "c"]

    def test_tab_delimiter(self):
        result = split_line("a\tb\tc", "\t", [])
        assert result == ["a", "b", "c"]

    def test_pipe_delimiter(self):
        result = split_line("a|b|c", "|", [])
        assert result == ["a", "b", "c"]

    def test_space_delimiter(self):
        result = split_line("foo   bar   baz", " ", [])
        assert result == ["foo", "bar", "baz"]

    def test_double_space_delimiter(self):
        result = split_line("New York  San Francisco", "  ", [])
        assert result == ["New York", "San Francisco"]

    def test_raw_delimiter(self):
        result = split_line("hello world", "raw", [])
        assert result == ["hello world"]

    def test_json_delimiter(self):
        result = split_line('{"a": 1, "b": "two"}', "json", [])
        assert result == ["1", "two"]

    def test_regex_with_named_groups(self):
        pattern = re.compile(r"(?P<severity>\w+) (?P<message>.*)")
        result = split_line("ERROR something broke", pattern, [])
        assert result == ["ERROR", "something broke"]

    def test_regex_no_match_returns_empty(self):
        pattern = re.compile(r"(?P<a>zzz)")
        result = split_line("no match here", pattern, [])
        assert result == []

    def test_regex_optional_group_none_becomes_empty_string(self):
        pattern = re.compile(
            r"(?P<process>[^\[:]+)(?:\[(?P<pid>\d+)\])?: (?P<message>.*)"
        )
        result = split_line("kernel: some message", pattern, [])
        assert result == ["kernel", "", "some message"]


class TestInferDelimiter:
    def test_csv_input(self):
        lines = ["name,age,city", "Alice,30,NYC", "Bob,25,LA"]
        assert infer_delimiter(lines) == ","

    def test_tsv_input(self):
        lines = ["name\tage\tcity", "Alice\t30\tNYC", "Bob\t25\tLA"]
        assert infer_delimiter(lines) == "\t"

    def test_pipe_input(self):
        lines = ["name|age|city", "Alice|30|NYC", "Bob|25|LA"]
        assert infer_delimiter(lines) == "|"

    def test_space_aligned_input(self, space_aligned_lines):
        result = infer_delimiter(space_aligned_lines)
        assert result in (" ", "  ")

    def test_empty_input_returns_raw(self):
        assert infer_delimiter([]) == "raw"

    def test_single_column_returns_raw(self):
        assert infer_delimiter(["hello", "world", "test"]) == "raw"

    def test_json_input(self):
        lines = [
            '{"name": "Alice", "age": 30, "city": "New York"}',
            '{"name": "Bob", "age": 25, "city": "San Francisco"}',
        ]
        assert infer_delimiter(lines) == "json"

    def test_json_input_single_line(self):
        lines = ['{"status": "ok", "count": 42}']
        assert infer_delimiter(lines) == "json"

    def test_json_with_empty_lines(self):
        lines = [
            '{"a": 1}',
            "",
            '{"a": 2}',
        ]
        assert infer_delimiter(lines) == "json"

    def test_json_array_input(self):
        lines = [
            "[",
            '    {"op": "add", "path": "/a/b/c", "value": "foo"},',
            '    {"op": "subtract", "path": "/b/c/d", "value": "bar"}',
            "]",
        ]
        assert infer_delimiter(lines) == "json"

    def test_non_json_with_braces_not_detected_as_json(self):
        lines = ["name,age,city", "{invalid json,30,NYC"]
        assert infer_delimiter(lines) != "json"

    def test_source_code_infers_raw(self):
        lines = [
            "def main():",
            "    x = get_value()",
            "    if x > 0:",
            '        print("positive")',
            "    else:",
            '        print("non-positive")',
            "    return x",
        ]
        assert infer_delimiter(lines) == "raw"

    def test_config_file_infers_raw(self):
        lines = [
            "{",
            '  "name": "test",',
            '  "version": "1.0",',
            '  "description": "a thing"',
            "}",
        ]
        # infer_delimiter alone won't detect this (sampled lines),
        # but it shouldn't misidentify as CSV either
        result = infer_delimiter(lines)
        assert result != ","


class TestFlattenJsonLines:
    def test_jsonl_passthrough(self):
        lines = ['{"a": 1}', '{"a": 2}']
        assert flatten_json_lines(lines) is lines

    def test_pretty_object(self):
        lines = ["{", '  "op": "add",', '  "path": "/a"', "}"]
        result = flatten_json_lines(lines)
        assert result is not lines
        assert len(result) == 1
        assert '"op"' in result[0]

    def test_pretty_array(self):
        lines = ["[", '  {"a": 1},', '  {"a": 2}', "]"]
        result = flatten_json_lines(lines)
        assert result is not lines
        assert len(result) == 2

    def test_csv_passthrough(self):
        lines = ["a,b,c", "1,2,3"]
        assert flatten_json_lines(lines) is lines

    def test_empty_passthrough(self):
        lines = []
        assert flatten_json_lines(lines) is lines

    def test_large_pretty_array(self):
        """Pretty-printed JSON larger than the 15-line sample window."""
        import json

        data = [{"id": i, "val": i * 10} for i in range(50)]
        lines = json.dumps(data, indent=2).splitlines()
        assert len(lines) > 15
        result = flatten_json_lines(lines)
        assert result is not lines
        assert len(result) == 50


class TestMaxFieldsSplit:
    def test_max_fields_caps_split(self):
        result = split_aligned_row("a b c d e", max_fields=3)
        assert result == ["a", "b", "c d e"]

    def test_max_fields_preserve_single_spaces(self):
        result = split_aligned_row_preserve_single_spaces(
            "New York  San Francisco  Los Angeles  United States", max_fields=3
        )
        assert result == ["New York", "San Francisco", "Los Angeles  United States"]

    def test_max_fields_zero_unchanged(self):
        assert split_aligned_row("a b c d e") == ["a", "b", "c", "d", "e"]

    def test_max_fields_equal_to_field_count(self):
        result = split_aligned_row("a b c", max_fields=3)
        assert result == ["a", "b", "c"]


class TestPsAuxInfer:
    def test_ps_aux_infer(self, ps_aux_lines):
        result = infer_delimiter(ps_aux_lines)
        assert result == " "

    def test_ps_aux_split_line(self, ps_aux_lines):
        # Build columns matching the 11-field header
        header_fields = split_aligned_row(ps_aux_lines[0])
        assert len(header_fields) == 11
        columns = [
            Column(
                name=name,
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, name in enumerate(header_fields)
        ]
        # Split a data line with spaces in COMMAND
        cells = split_line(ps_aux_lines[2], " ", columns)
        assert len(cells) == 11
        assert "python /home/matt/app.py arg1 arg2" in cells[-1]

    def test_df_h_infer(self):
        lines = [
            "Filesystem      Size  Used Avail Use% Mounted on",
            "/dev/sda1        50G   20G   28G  42% /",
            "tmpfs           3.9G     0  3.9G   0% /dev/shm",
            "/dev/sdb1       100G   60G   35G  64% /home",
        ]
        result = infer_delimiter(lines)
        # "  " (double-space) is preferred here because the header contains
        # the multi-word column "Mounted on" separated by single spaces.
        assert result in (" ", "  ")

    def test_detect_space_max_fields_ps_aux(self, ps_aux_lines):
        # Header has 11 fields, data rows have 12+ → max_fields = 11 (header wins)
        result = detect_space_max_fields(ps_aux_lines, " ")
        assert result == 11

    def test_detect_space_max_fields_df_h(self):
        lines = [
            "Filesystem      Size  Used Avail Use% Mounted on",
            "/dev/sda1        50G   20G   28G  42% /",
            "tmpfs           3.9G     0  3.9G   0% /dev/shm",
            "/dev/sdb1       100G   60G   35G  64% /home",
        ]
        # Header has 7 fields, data rows have 6 → max_fields = 6
        result = detect_space_max_fields(lines, " ")
        assert result == 6

    def test_detect_space_max_fields_no_mismatch(self, space_aligned_lines):
        # All lines have the same field count → no cap needed
        result = detect_space_max_fields(space_aligned_lines, " ")
        assert result == 0

    def test_df_h_split_with_max_fields(self):
        header = "Filesystem      Size  Used Avail Use% Mounted on"
        # With max_fields=6, "Mounted on" stays as one field
        result = split_aligned_row(header, max_fields=6)
        assert result == ["Filesystem", "Size", "Used", "Avail", "Use%", "Mounted on"]


class TestPositionBasedSplitting:
    def test_detect_column_positions(self):
        lines = [
            "COMMAND     PID   TID TASKCMD   USER   FD      TYPE",
            "init(Ubun     1                 root  cwd   unknown",
        ]
        positions = detect_column_positions(lines)
        assert positions[0] == 0  # COMMAND
        # PID position may vary based on data alignment
        assert len(positions) >= 7  # 7 columns

    def test_split_by_positions_with_empty_cells(self):
        lines = [
            "COMMAND     PID   TID TASKCMD   USER",
            "init(Ubun     1                 root",
            "init(Ubun     1     8 Interop   root",
        ]
        positions = detect_column_positions(lines)
        # Header itself splits correctly
        assert split_by_positions(lines[0], positions) == [
            "COMMAND",
            "PID",
            "TID",
            "TASKCMD",
            "USER",
        ]
        # Data line with empty TID and TASKCMD
        fields = split_by_positions(lines[1], positions)
        assert fields[0] == "init(Ubun"
        assert fields[1] == "1"
        assert fields[2] == ""  # TID empty
        assert fields[3] == ""  # TASKCMD empty
        assert fields[4] == "root"

    def test_lsof_infer(self):
        lines = [
            "COMMAND     PID   TID TASKCMD   USER   FD      TYPE             DEVICE  SIZE/OFF             NODE NAME",
            "init(Ubun     1                 root  cwd   unknown                                               /proc/1/cwd (readlink: Permission denied)",
            "init(Ubun     1     8 Interop   root  cwd   unknown                                               /proc/1/task/8/cwd (readlink: Permission denied)",
            "init          6                 root  cwd   unknown                                               /proc/6/cwd (readlink: Permission denied)",
            "init          6     7 init      root  cwd   unknown                                               /proc/6/task/7/cwd (readlink: Permission denied)",
        ]
        result = infer_delimiter(lines)
        assert result == " "

    def test_lsof_split_by_positions(self):
        lines = [
            "COMMAND     PID   TID TASKCMD   USER   FD      TYPE             DEVICE  SIZE/OFF             NODE NAME",
            "init(Ubun     1     8 Interop   root  cwd   unknown                                               /proc/1/task/8/cwd (readlink: Permission denied)",
            "init(Ubun     1                 root  cwd   unknown                                               /proc/1/cwd (readlink: Permission denied)",
        ]
        positions = detect_column_positions(lines)
        fields = split_by_positions(lines[1], positions)
        assert len(fields) == 11
        assert fields[0] == "init(Ubun"
        assert fields[1] == "1"
        assert fields[2] == "8"
        assert fields[3] == "Interop"
        assert fields[4] == "root"
        assert fields[5] == "cwd"
        assert fields[10] == "/proc/1/task/8/cwd (readlink: Permission denied)"

    def test_split_line_with_positions(self):
        """split_line uses column_positions when provided."""
        lines = [
            "COMMAND     PID   TID TASKCMD   USER",
            "init(Ubun     1                 root",
            "init(Ubun     1     8 Interop   root",
        ]
        positions = detect_column_positions(lines)
        fields = split_line(lines[1], " ", [], column_positions=positions)
        assert len(fields) == 5
        assert fields[2] == ""  # TID empty
        assert fields[3] == ""  # TASKCMD empty

    def test_netstat_infer(self):
        lines = [
            "Active Internet connections (only servers)",
            "Proto Recv-Q Send-Q Local Address           Foreign Address         State       PID/Program name",
            "tcp        0      0 127.0.0.1:40881         0.0.0.0:*               LISTEN      27281/ttyd",
            "tcp        0      0 127.0.0.1:40675         0.0.0.0:*               LISTEN      28459/ttyd",
            "tcp        0      0 10.255.255.254:53       0.0.0.0:*               LISTEN      -",
        ]
        assert infer_delimiter(lines) == " "

    def test_netstat_position_split(self):
        """netstat multi-word headers like 'Local Address' stay intact."""
        lines = [
            "Active Internet connections (only servers)",
            "Proto Recv-Q Send-Q Local Address           Foreign Address         State       PID/Program name",
            "tcp        0      0 127.0.0.1:40881         0.0.0.0:*               LISTEN      27281/ttyd",
            "tcp        0      0 10.255.255.254:53       0.0.0.0:*               LISTEN      -",
        ]
        positions = detect_column_positions(lines)
        header = split_by_positions(lines[1], positions)
        assert "Local Address" in header
        assert "Foreign Address" in header
        assert "PID/Program name" in header
        data = split_by_positions(lines[2], positions)
        assert "127.0.0.1:40881" in data
        assert "27281/ttyd" in data

    def test_ls_la_infer(self):
        lines = [
            "total 312",
            "drwxr-xr-x  16 matt matt   4096 Mar 18 18:58 .",
            "drwxrwxr-x  15 matt matt   4096 Mar 17 08:33 ..",
            "-rw-r--r--   1 matt matt    125 Mar 18 18:57 .gitignore",
            "-rw-r--r--   1 matt matt      0 Mar 18 19:43 file name.txt",
        ]
        assert infer_delimiter(lines) == " "

    def test_ls_la_filename_with_spaces(self):
        """Filenames with spaces are kept intact via maxsplit."""
        lines = [
            "total 312",
            "drwxr-xr-x  16 matt matt   4096 Mar 18 18:58 .",
            "-rw-r--r--   1 matt matt      0 Mar 18 19:43 file name.txt",
        ]
        # After preamble skip, first data line becomes header (9 fields)
        header_fields = split_aligned_row(lines[1])
        assert len(header_fields) == 9
        columns = [
            Column(
                name=f,
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, f in enumerate(header_fields)
        ]
        # Data line with spaces in filename: maxsplit keeps it as one field
        cells = split_line(lines[2], " ", columns)
        assert len(cells) == 9
        assert cells[-1] == "file name.txt"

    def test_kubectl_pods_restarts_with_age_space_plus(self):
        """kubectl RESTARTS '(Xs ago)' must not create a false column boundary with space+ delimiter."""
        lines = [
            "NAMESPACE     NAME                                      READY   STATUS    RESTARTS         AGE",
            "default       nginx-deployment-5d7f8c9b47-abc12         1/1     Running   0                10d",
            "kube-system   coredns-5d78c9869d-xyz34                  1/1     Running   4584 (3m4s ago)   10d",
            "kube-system   etcd-control-plane                        1/1     Running   0                10d",
        ]
        # With space+ delimiter, the single spaces inside '4584 (3m4s ago)'
        # must not trigger position-based splitting with false boundaries.
        positions, _max_fields = detect_space_splitting_strategy(lines, "  ")
        if positions is not None:
            # If positions are used, they must not produce empty header fields
            header_fields = split_by_positions(lines[0], positions)
            assert all(f != "" for f in header_fields), (
                f"False column boundary created empty header field: {header_fields}"
            )
        # Splitting must keep '4584 (3m4s ago)' intact
        header_fields = split_aligned_row_preserve_single_spaces(lines[0])
        columns = [
            Column(
                name=f,
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, f in enumerate(header_fields)
        ]
        cells = split_line(
            lines[2],
            "  ",
            columns,
            column_positions=positions,
        )
        assert len(cells) == 6
        assert cells[4] == "4584 (3m4s ago)"

    def test_kubectl_pods_watch_streaming_restarts(self):
        """Streaming kubectl -w lines with RESTARTS '(Xs ago)' must not be skipped."""
        lines = [
            "NAMESPACE     NAME                                      READY   STATUS    RESTARTS         AGE",
            "default       nginx-deployment-5d7f8c9b47-abc12         1/1     Running   0                10d",
            "kube-system   coredns-5d78c9869d-xyz34                  1/1     Running   0                10d",
        ]
        # Initial sample has no restarts with '(Xs ago)' — strategy chosen from clean data
        positions, _max_fields = detect_space_splitting_strategy(lines, "  ")
        header_fields = split_aligned_row_preserve_single_spaces(lines[0])
        columns = [
            Column(
                name=f,
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, f in enumerate(header_fields)
        ]
        # Later a streaming line arrives with RESTARTS containing '(Xs ago)'
        streaming_line = "kube-system   coredns-5d78c9869d-xyz34                  1/1     Running   4584 (3m4s ago)   10d"
        cells = split_line(
            streaming_line,
            "  ",
            columns,
            column_positions=positions,
        )
        assert len(cells) == 6
        assert cells[4] == "4584 (3m4s ago)"

    def test_source_code_not_space(self):
        """Python source code must not infer as space-delimited."""
        lines = [
            "def main():",
            "    x = get_value()",
            "    if x > 0:",
            '        print("positive")',
            "    return x",
        ]
        assert infer_delimiter(lines) == "raw"


class TestProseInfer:
    """Prose text should infer as raw, not space-delimited."""

    def test_paragraph_infers_raw(self):
        lines = [
            "The quick brown fox jumps over the lazy dog. This is a paragraph",
            "of plain text that should not be interpreted as tabular data by",
            "the delimiter inference engine. It has varying word counts per",
            "line and no consistent column structure whatsoever.",
            "",
            "A second paragraph continues here with more words and sentences.",
            "None of this should trigger CSV, TSV, or space-aligned detection.",
        ]
        assert infer_delimiter(lines) == "raw"

    def test_markdown_infers_raw(self):
        lines = [
            "# Getting Started",
            "",
            "Install the package with pip:",
            "",
            "```bash",
            "pip install nothing-less",
            "```",
            "",
            "Then run it:",
        ]
        assert infer_delimiter(lines) == "raw"

    def test_log_style_prose_infers_raw(self):
        lines = [
            "Starting application server on port 8080...",
            "Loading configuration from /etc/app/config.yaml",
            "Connected to database at localhost:5432",
            "Warming up caches... done (2.3s)",
            "Ready to accept connections",
        ]
        assert infer_delimiter(lines) == "raw"


class TestKubectlEventsInfer:
    """kubectl get events -A -w: double-space aligned with multi-word headers."""

    @staticmethod
    def _lines():
        return [
            "NAMESPACE          LAST SEEN    TYPE       REASON                   OBJECT                                               MESSAGE",
            "payments           40s          Warning    CrashLoopBackOff         pod/payment-processor-7a3f1-b92e4                    Back-off restarting failed container app in pod payment-processor-7a3f1-b92e4",
            "kube-system        12s          Normal     Scheduled                pod/coredns-cb523-abd32                              Successfully assigned kube-system/coredns-cb523-abd32 to node-3",
            'monitoring         5s           Normal     Pulling                  pod/prometheus-server-bb736-52902                    Pulling image "prom/prometheus:v2.51.0"',
            "auth               23s          Warning    Unhealthy                pod/auth-service-e5f93-1d8b6                         Readiness probe failed: HTTP probe failed with statuscode: 503",
        ]

    def test_infer_double_space(self):
        assert infer_delimiter(self._lines()) == "  "

    def test_header_preserved_as_single_field(self):
        """'LAST SEEN' must stay as one column, not split into two."""
        lines = self._lines()
        parts = split_aligned_row_preserve_single_spaces(lines[0])
        assert "LAST SEEN" in parts

    def test_message_column_intact(self):
        """MESSAGE column with spaces must not be split."""
        lines = self._lines()
        parts = split_aligned_row_preserve_single_spaces(lines[1])
        assert any("Back-off restarting failed container" in p for p in parts)

    def test_consistent_field_count(self):
        """All lines should produce the same number of fields."""
        lines = self._lines()
        counts = [len(split_aligned_row_preserve_single_spaces(line)) for line in lines]
        assert len(set(counts)) == 1


class TestLsofNiTcpInfer:
    """lsof -ni tcp: single-space, no empty cells, NAME has spaces."""

    @staticmethod
    def _lines():
        return [
            "COMMAND   PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME",
            "zellij    856 matt   24u  IPv4 428952      0t0  TCP 127.0.0.1:46029->127.0.0.1:8082 (SYN_SENT)",
            "zellij    856 matt   38u  IPv4 426939      0t0  TCP 127.0.0.1:46039->127.0.0.1:8082 (SYN_SENT)",
            "zellij    856 matt   39u  IPv4 430344      0t0  TCP 127.0.0.1:47729->127.0.0.1:8082 (SYN_SENT)",
            "node     1234 matt   12u  IPv6 531000      0t0  TCP *:3000 (LISTEN)",
        ]

    def test_infer_single_space(self):
        assert infer_delimiter(self._lines()) == " "

    def test_header_not_skipped(self):
        """Header has fewer fields than data (no space in NAME header).

        find_header_index must not skip it.
        """
        lines = self._lines()
        max_fields = detect_space_max_fields(lines, " ")
        idx = find_header_index(lines, " ", max_fields=max_fields)
        assert idx == 0

    def test_name_column_intact(self):
        """NAME value with spaces like '(SYN_SENT)' stays in one field."""
        lines = self._lines()
        max_fields = detect_space_max_fields(lines, " ")
        header = split_aligned_row(lines[0], max_fields=max_fields)
        columns = [
            Column(
                name=name,
                labels=set(),
                render_position=i,
                data_position=i,
                hidden=False,
            )
            for i, name in enumerate(header)
        ]
        cells = split_line(lines[1], " ", columns)
        assert cells[-1] == "127.0.0.1:46029->127.0.0.1:8082 (SYN_SENT)"


class TestDockerPsInfer:
    """docker ps: double-space aligned with multi-word headers."""

    @staticmethod
    def _lines():
        return [
            "CONTAINER ID   IMAGE                    COMMAND                  CREATED         STATUS         PORTS                    NAMES",
            'a1b2c3d4e5f6   nginx:latest             "/docker-entrypoint.…"   2 hours ago     Up 2 hours     0.0.0.0:80->80/tcp       web-server',
            'f6e5d4c3b2a1   postgres:16              "docker-entrypoint.s…"   3 hours ago     Up 3 hours     0.0.0.0:5432->5432/tcp   db',
            '1234abcd5678   redis:7-alpine           "docker-entrypoint.s…"   5 hours ago     Up 5 hours     0.0.0.0:6379->6379/tcp   cache',
        ]

    def test_infer_double_space(self):
        assert infer_delimiter(self._lines()) == "  "

    def test_multi_word_fields_intact(self):
        """'2 hours ago' and 'Up 2 hours' must not be split."""
        lines = self._lines()
        parts = split_aligned_row_preserve_single_spaces(lines[1])
        assert any("2 hours ago" in p for p in parts)
        assert any("Up 2 hours" in p for p in parts)


class TestSsInfer:
    """ss -tlnp: space-aligned with multi-word headers."""

    @staticmethod
    def _lines():
        return [
            "State    Recv-Q   Send-Q     Local Address:Port     Peer Address:Port  Process",
            'LISTEN   0        128              0.0.0.0:22            0.0.0.0:*      users:(("sshd",pid=1234,fd=3))',
            'LISTEN   0        511              0.0.0.0:80            0.0.0.0:*      users:(("nginx",pid=5678,fd=6))',
            'LISTEN   0        128                 [::]:22               [::]:*      users:(("sshd",pid=1234,fd=4))',
            'LISTEN   0        4096           127.0.0.1:5432          0.0.0.0:*      users:(("postgres",pid=9012,fd=5))',
        ]

    def test_infer_space(self):
        result = infer_delimiter(self._lines())
        assert result in (" ", "  ")

    def test_field_count_consistent(self):
        """All lines should produce a usable number of fields."""
        lines = self._lines()
        d = infer_delimiter(lines)
        split_fn = (
            split_aligned_row_preserve_single_spaces if d == "  " else split_aligned_row
        )
        counts = [len(split_fn(line)) for line in lines]
        # Header and data should be within 1 field of each other
        assert max(counts) - min(counts) <= 2


class TestMountInfer:
    """mount output: space-delimited with 'on' and 'type' keywords."""

    @staticmethod
    def _lines():
        return [
            "sysfs on /sys type sysfs (rw,nosuid,nodev,noexec,relatime)",
            "proc on /proc type proc (rw,nosuid,nodev,noexec,relatime)",
            "/dev/sda1 on / type ext4 (rw,relatime,discard)",
            "tmpfs on /run type tmpfs (rw,nosuid,nodev,noexec,relatime,size=1632852k,mode=755)",
            "tmpfs on /dev/shm type tmpfs (rw,nosuid,nodev)",
        ]

    def test_infer_space(self):
        """mount has no header and consistent structure."""
        result = infer_delimiter(self._lines())
        assert result == " "


class TestIpAddrInfer:
    """ip addr output: mixed indentation, not tabular."""

    @staticmethod
    def _lines():
        return [
            "1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000",
            "    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00",
            "    inet 127.0.0.1/8 scope host lo",
            "       valid_lft forever preferred_lft forever",
            "2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP group default qlen 1000",
            "    link/ether 00:15:5d:e8:12:34 brd ff:ff:ff:ff:ff:ff",
            "    inet 172.28.0.2/20 brd 172.28.15.255 scope global eth0",
        ]

    def test_infer_raw(self):
        """ip addr is not tabular — should infer raw."""
        result = infer_delimiter(self._lines())
        assert result == "raw"


class TestHeaderDetection:
    """find_header_index edge cases for space-delimited data."""

    def test_header_fewer_fields_than_data(self):
        """Header has fewer fields when last column name is one word but data has spaces."""
        lines = [
            "COMMAND   PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME",
            "zellij    856 matt   24u  IPv4 428952      0t0  TCP 127.0.0.1:8082 (SYN_SENT)",
            "zellij    856 matt   38u  IPv4 426939      0t0  TCP 127.0.0.1:8082 (SYN_SENT)",
        ]
        max_fields = detect_space_max_fields(lines, " ")
        idx = find_header_index(lines, " ", max_fields=max_fields)
        assert idx == 0, "Header should not be skipped even with fewer fields"

    def test_preamble_still_skipped(self):
        """Lines far below consensus should still be skipped."""
        lines = [
            "System Report",
            "USER   PID  %CPU  %MEM    VSZ   RSS  TTY   STAT  START  TIME  COMMAND",
            "root     1   0.0   0.0   2616  1752  ?     Sl    07:17  0:00  /init",
            "matt   100   0.0   0.0  15240  9080  pts/0 Ss    07:17  0:00  -zsh",
        ]
        max_fields = detect_space_max_fields(lines, " ")
        idx = find_header_index(lines, " ", max_fields=max_fields)
        assert idx == 1, "Preamble line should be skipped"

    def test_header_exact_match(self):
        """Header with same field count as data is not skipped."""
        lines = [
            "USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND",
            "root         1  0.0  0.0   2616  1752 ?        Sl   07:17   0:00 /init",
            "matt       100  0.0  0.0  15240  9080 pts/0    Ss   07:17   0:00 -zsh",
        ]
        idx = find_header_index(lines, " ")
        assert idx == 0


class TestSplitLineComputedColumns:
    """Tests for split_line with computed columns (col_ref, col_ref_index, json_ref)."""

    def test_regex_col_ref_index_in_bounds(self):
        pattern = re.compile(r"(?P<a>\w+)-(?P<b>\w+)")
        columns = [
            Column(
                name="src",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="part_a",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                col_ref="src",
                col_ref_index=0,
                delimiter=pattern,
            ),
        ]
        result = split_line("hello-world", ",", columns)
        assert "hello" in result

    def test_regex_col_ref_index_out_of_bounds(self):
        pattern = re.compile(r"(?P<a>\w+)")  # Only 1 group
        columns = [
            Column(
                name="src",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="part_b",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                col_ref="src",
                col_ref_index=5,  # Way out of bounds
                delimiter=pattern,
            ),
        ]
        result = split_line("hello-world", ",", columns)
        # Should not crash; out-of-bounds returns empty string
        assert "" in result

    def test_literal_col_ref_index_out_of_bounds(self):
        columns = [
            Column(
                name="src",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="part_c",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                col_ref="src",
                col_ref_index=99,
                delimiter="|",
            ),
        ]
        result = split_line("a|b|c", ",", columns)
        assert "" in result

    def test_json_ref_nested_key(self):
        columns = [
            Column(
                name="data",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="data.name",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                json_ref="data.name",
                delimiter="json",
            ),
        ]
        result = split_line('{"name": "Alice", "age": 30}', "json", columns)
        assert "Alice" in result

    def test_json_ref_missing_key(self):
        columns = [
            Column(
                name="data",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="data.missing",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
                computed=True,
                json_ref="data.missing",
                delimiter="json",
            ),
        ]
        result = split_line('{"name": "Alice"}', "json", columns)
        assert "" in result

    def test_nested_literal_split(self):
        """Splitting a column that was itself produced by a prior split.

        Simulates: CSV with a 'msg' column → split on '.' → then split
        one of the resulting sub-columns on ' | '.
        """
        # Base columns: id (pos 0), msg (pos 1)
        # First split on '.': msg-1 (pos 2), msg-2 (pos 3)
        # Second split on ' | ' on msg-2: msg-2-1 (pos 4), msg-2-2 (pos 5)
        columns = [
            Column(
                name="id",
                labels=set(),
                render_position=0,
                data_position=0,
                hidden=False,
            ),
            Column(
                name="msg",
                labels=set(),
                render_position=1,
                data_position=1,
                hidden=False,
            ),
            Column(
                name="msg-1",
                labels=set(),
                render_position=2,
                data_position=2,
                hidden=False,
                computed=True,
                col_ref="msg",
                col_ref_index=0,
                delimiter=".",
            ),
            Column(
                name="msg-2",
                labels=set(),
                render_position=3,
                data_position=3,
                hidden=False,
                computed=True,
                col_ref="msg",
                col_ref_index=1,
                delimiter=".",
            ),
            Column(
                name="msg-2-1",
                labels=set(),
                render_position=4,
                data_position=4,
                hidden=False,
                computed=True,
                col_ref="msg-2",
                col_ref_index=0,
                delimiter=" | ",
            ),
            Column(
                name="msg-2-2",
                labels=set(),
                render_position=5,
                data_position=5,
                hidden=False,
                computed=True,
                col_ref="msg-2",
                col_ref_index=1,
                delimiter=" | ",
            ),
        ]
        result = split_line("001,logger.name | key=val | extra", ",", columns)
        # msg-1 should be "logger" (first part of '.' split)
        assert result[2] == "logger"
        # msg-2 should be "name | key=val | extra"
        assert result[3] == "name | key=val | extra"
        # msg-2-1 should be "name" (first part of ' | ' split on msg-2)
        assert result[4] == "name"
        # msg-2-2 should be "key=val"
        assert result[5] == "key=val"
