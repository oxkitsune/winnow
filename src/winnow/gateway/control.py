"""Runtime control files for the local daemon."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import signal

from winnow.storage.atomic import atomic_write_text


PID_FILE = "gateway.pid"
HEARTBEAT_FILE = "gateway.heartbeat"


def _pid_path(runtime_dir: Path) -> Path:
    return runtime_dir / PID_FILE


def _heartbeat_path(runtime_dir: Path) -> Path:
    return runtime_dir / HEARTBEAT_FILE


def write_pid(runtime_dir: Path, pid: int) -> None:
    atomic_write_text(_pid_path(runtime_dir), f"{pid}\n")


def read_pid(runtime_dir: Path) -> int | None:
    path = _pid_path(runtime_dir)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def clear_pid(runtime_dir: Path) -> None:
    path = _pid_path(runtime_dir)
    if path.exists():
        path.unlink()


def write_heartbeat(runtime_dir: Path) -> None:
    stamp = datetime.now(timezone.utc).isoformat()
    atomic_write_text(_heartbeat_path(runtime_dir), f"{stamp}\n")


def read_heartbeat(runtime_dir: Path) -> str | None:
    path = _heartbeat_path(runtime_dir)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    return text or None


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def stop_pid(pid: int, sig: int = signal.SIGTERM) -> None:
    os.kill(pid, sig)
