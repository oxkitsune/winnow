"""Shared utility helpers for gateway TUI parsing and formatting."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _parse_iso8601(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        stamp = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        return stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def _as_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clip(value: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return f"{value[: width - 3]}..."


def _format_age(iso_ts: str | None) -> str:
    stamp = _parse_iso8601(iso_ts)
    if stamp is None:
        return "n/a"
    delta = datetime.now(timezone.utc) - stamp
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d"


def _format_ts_short(iso_ts: str | None) -> str:
    stamp = _parse_iso8601(iso_ts)
    if stamp is None:
        return "--:--:--"
    return stamp.astimezone().strftime("%H:%M:%S")


def _format_rss_mib(rss_kib: int | None) -> str:
    if rss_kib is None:
        return "-"
    return f"{rss_kib / 1024.0:.1f}"


def _latest_timestamp(job: "JobSummary") -> float:
    for candidate in (
        job.updated_at,
        job.finished_at,
        job.started_at,
        job.submitted_at,
    ):
        parsed = _parse_iso8601(candidate)
        if parsed is not None:
            return parsed.timestamp()
    return 0.0


def _is_active_job(job: "JobSummary") -> bool:
    return job.status in {"PENDING", "RUNNING"} or job.queue_lane in {
        "pending",
        "running",
    }


def _iter_json_files(directory: Path) -> list[Path]:
    try:
        files = [
            path
            for path in directory.iterdir()
            if path.is_file() and path.suffix == ".json"
        ]
    except OSError:
        return []
    files.sort()
    return files
