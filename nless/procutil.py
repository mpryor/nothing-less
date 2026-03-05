"""Best-effort detection of the command piping into stdin.

Works on Linux (/proc) and macOS (ps/lsof). Returns None on any failure so
callers can fall back to a default.
Easy to revert: delete this file and remove its usage from app.py.
"""

import os
import subprocess
import sys


def get_stdin_source() -> str | None:
    """Try to identify what is connected to stdin.

    Detects two cases:
    - **File redirection** (``nless < file.csv``): resolves the path of fd 0.
    - **Pipe** (``cmd | nless``): finds a sibling process in the same
      process group (pipeline).

    Returns a human-readable string, or None on any failure so callers can
    fall back to a default.
    """
    try:
        stdin_stat = os.fstat(0)

        if _is_regular_file(stdin_stat.st_mode):
            return _resolve_stdin_file()

        if _is_pipe(stdin_stat.st_mode):
            if sys.platform == "linux":
                return _linux_find_pipe_peer(stdin_stat.st_ino)
            return _pgrp_find_pipe_peer()
    except (OSError, PermissionError):
        pass
    return None


def _is_pipe(mode: int) -> bool:
    import stat

    return stat.S_ISFIFO(mode)


def _is_regular_file(mode: int) -> bool:
    import stat

    return stat.S_ISREG(mode)


# ── File redirection ──────────────────────────────────────────────────


def _resolve_stdin_file() -> str | None:
    """Resolve the file path of fd 0."""
    # Linux: /proc/self/fd/0 is a reliable symlink to the actual file.
    try:
        target = os.readlink("/proc/self/fd/0")
        if _is_usable_path(target):
            return target
    except (OSError, PermissionError):
        pass

    # macOS: use fcntl F_GETPATH to get the file path from fd 0.
    try:
        import fcntl

        buf = fcntl.fcntl(0, fcntl.F_GETPATH, b"\0" * 1024)
        path = buf.split(b"\0", 1)[0].decode("utf-8", errors="replace")
        if _is_usable_path(path):
            return path
    except (OSError, AttributeError):
        # AttributeError: F_GETPATH not available (Linux)
        pass
    return None


def _is_usable_path(path: str) -> bool:
    """Check if a resolved path is a real user file (not a device or pseudo-file)."""
    return (
        path.startswith("/")
        and not path.startswith("/dev/")
        and "(deleted)" not in path
    )


# ── Pipe peer: Linux /proc ────────────────────────────────────────────


def _linux_find_pipe_peer(stdin_ino: int) -> str | None:
    """Scan /proc for the process writing to our stdin pipe."""
    my_pid = os.getpid()
    try:
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid == my_pid:
                continue
            cmd = _linux_check_pid(pid, stdin_ino)
            if cmd:
                return cmd
    except (OSError, PermissionError):
        pass
    return None


def _linux_check_pid(pid: int, target_ino: int) -> str | None:
    """Check if *pid* holds an fd with inode *target_ino* and return its cmdline."""
    fd_dir = f"/proc/{pid}/fd"
    try:
        for fd_entry in os.scandir(fd_dir):
            try:
                if os.stat(fd_entry.path).st_ino == target_ino:
                    return _linux_read_cmdline(pid)
            except (OSError, PermissionError):
                continue
    except (OSError, PermissionError):
        pass
    return None


def _linux_read_cmdline(pid: int) -> str | None:
    """Read /proc/<pid>/cmdline and return a human-readable string."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
        if not raw:
            return None
        return raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except (OSError, PermissionError):
        return None


# ── Pipe peer: process group (macOS + fallback) ───────────────────────


def _pgrp_find_pipe_peer() -> str | None:
    """Find a pipeline sibling via process group.

    In a shell pipeline (``cmd | nless``), all processes share the same
    process group. We list processes in our pgrp and return the command
    of one that isn't us.
    """
    try:
        my_pid = os.getpid()
        my_pgrp = os.getpgrp()
        out = subprocess.check_output(
            ["ps", "-eo", "pgid,pid,command="],
            stderr=subprocess.DEVNULL,
            timeout=2,
        ).decode("utf-8", errors="replace")
        for line in out.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            pgid, pid = parts[0], parts[1]
            if not pgid.isdigit() or not pid.isdigit():
                continue
            if int(pgid) == my_pgrp and int(pid) != my_pid:
                return parts[2]
    except (OSError, subprocess.SubprocessError):
        pass
    return None
