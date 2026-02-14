"""Local gateway daemon process."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from winnow.gateway.control import (
    clear_pid,
    pid_is_alive,
    read_heartbeat,
    read_pid,
    stop_pid,
    write_heartbeat,
    write_pid,
)
from winnow.gateway.scheduler import process_one_pending
from winnow.storage.state_store import DEFAULT_STATE_ROOT, ensure_state_layout


class GatewayDaemon:
    """Simple local daemon that polls filesystem queue directories."""

    def __init__(self, state_root: Path, poll_interval: float = 1.0) -> None:
        self.state_root = state_root
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()

    def _install_signal_handlers(self) -> None:
        def _handle_signal(_signum: int, _frame: object) -> None:
            self._stop_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

    def run_forever(self) -> None:
        paths = ensure_state_layout(self.state_root)
        existing_pid = read_pid(paths.runtime)
        if existing_pid and pid_is_alive(existing_pid):
            raise RuntimeError(f"Gateway is already running with pid={existing_pid}.")

        write_pid(paths.runtime, os.getpid())
        self._install_signal_handlers()

        try:
            while not self._stop_event.is_set():
                write_heartbeat(paths.runtime)
                process_one_pending(paths)
                time.sleep(self.poll_interval)
        finally:
            clear_pid(paths.runtime)


def start_background(
    state_root: Path,
    poll_interval: float,
    log_file: Path | None,
) -> int:
    """Start daemon in background and return spawned pid."""

    cmd = [
        sys.executable,
        "-m",
        "winnow.gateway.daemon",
        "--state-root",
        str(state_root),
        "--poll-interval",
        str(poll_interval),
    ]
    stdout = subprocess.DEVNULL
    stderr = subprocess.DEVNULL
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handle = log_file.open("a", encoding="utf-8")
        stdout = handle
        stderr = handle
    process = subprocess.Popen(
        cmd,
        stdout=stdout,
        stderr=stderr,
        start_new_session=True,
    )
    if log_file is not None:
        handle.close()
    return process.pid


def stop_background(state_root: Path) -> bool:
    """Stop a running daemon if found."""

    paths = ensure_state_layout(state_root)
    pid = read_pid(paths.runtime)
    if pid is None:
        return False
    if not pid_is_alive(pid):
        clear_pid(paths.runtime)
        return False
    stop_pid(pid)
    return True


def status(state_root: Path) -> dict[str, object]:
    """Return daemon status and queue counts."""

    paths = ensure_state_layout(state_root)
    pid = read_pid(paths.runtime)
    alive = bool(pid and pid_is_alive(pid))
    heartbeat = read_heartbeat(paths.runtime)
    return {
        "state_root": str(paths.root.resolve()),
        "pid": pid,
        "alive": alive,
        "heartbeat": heartbeat,
        "queue": {
            "pending": len(list(paths.queue_pending.glob("*.json"))),
            "running": len(list(paths.queue_running.glob("*.json"))),
            "done": len(list(paths.queue_done.glob("*.json"))),
            "failed": len(list(paths.queue_failed.glob("*.json"))),
        },
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Winnow gateway daemon.")
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    return parser.parse_args(argv)


def _main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    daemon = GatewayDaemon(state_root=args.state_root, poll_interval=args.poll_interval)
    daemon.run_forever()


if __name__ == "__main__":
    _main()
