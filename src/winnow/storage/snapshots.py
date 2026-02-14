"""State snapshot generation and loading helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from winnow.storage.atomic import atomic_write_json, read_json
from winnow.storage.events import iter_events
from winnow.storage.state_store import StatePaths


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _queue_depth(paths: StatePaths) -> dict[str, int]:
    return {
        "pending": len(list(paths.queue_pending.glob("*.json"))),
        "running": len(list(paths.queue_running.glob("*.json"))),
        "done": len(list(paths.queue_done.glob("*.json"))),
        "failed": len(list(paths.queue_failed.glob("*.json"))),
    }


def _job_status_counts(paths: StatePaths) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job_file in paths.jobs.glob("*.json"):
        payload = read_json(job_file)
        if not isinstance(payload, dict):
            continue
        status = payload.get("status")
        if not isinstance(status, str):
            status = "UNKNOWN"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _checkpoint_summary(paths: StatePaths) -> dict[str, Any]:
    stages: dict[str, dict[str, int]] = {}
    total = 0

    for stage_dir in sorted(paths.stages.iterdir() if paths.stages.exists() else []):
        if not stage_dir.is_dir():
            continue
        stage_name = stage_dir.name
        status_counts: dict[str, int] = {}
        for checkpoint in stage_dir.rglob("*.json"):
            payload = read_json(checkpoint)
            if not isinstance(payload, dict):
                continue
            status = payload.get("status")
            if not isinstance(status, str):
                status = "UNKNOWN"
            status_counts[status] = status_counts.get(status, 0) + 1
            total += 1
        stages[stage_name] = status_counts

    return {
        "total": total,
        "stages": stages,
    }


def _event_summary(paths: StatePaths) -> dict[str, Any]:
    total = 0
    by_type: dict[str, int] = {}
    last_event_ts: str | None = None

    for event in iter_events(paths):
        total += 1
        event_name = event.get("event")
        if isinstance(event_name, str):
            by_type[event_name] = by_type.get(event_name, 0) + 1

        ts = event.get("ts")
        if isinstance(ts, str):
            if last_event_ts is None or ts > last_event_ts:
                last_event_ts = ts

    return {
        "total": total,
        "by_type": by_type,
        "last_event_ts": last_event_ts,
    }


def rebuild_snapshot(
    paths: StatePaths,
    *,
    name: str = "state-latest.json",
) -> dict[str, Any]:
    """Rebuild and persist a full state snapshot."""

    snapshot = {
        "schema": "snapshot/v1",
        "generated_at": _now(),
        "state_root": str(paths.root.resolve()),
        "queue": {
            "depth": _queue_depth(paths),
        },
        "jobs": {
            "status_counts": _job_status_counts(paths),
        },
        "checkpoints": _checkpoint_summary(paths),
        "events": _event_summary(paths),
    }

    out_path = paths.snapshots / name
    atomic_write_json(out_path, snapshot)

    return {
        "snapshot": snapshot,
        "path": str(out_path),
    }


def read_snapshot(paths: StatePaths, *, name: str = "state-latest.json") -> dict[str, Any] | None:
    """Read a named snapshot if present."""

    path = paths.snapshots / name
    if not path.exists():
        return None
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise TypeError(f"Snapshot payload must be a JSON object: {path}")
    return payload
