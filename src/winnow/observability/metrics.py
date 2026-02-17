"""Persistent operational metrics for the local Winnow runtime."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from winnow.storage.atomic import atomic_write_json, read_json
from winnow.storage.state_store import StatePaths


_METRICS_FILE = "metrics.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _metrics_path(paths: StatePaths) -> Path:
    return paths.runtime / _METRICS_FILE


def _empty_metrics() -> dict[str, Any]:
    return {
        "schema": "metrics/v1",
        "updated_at": _now(),
        "jobs": {
            "started": 0,
            "succeeded": 0,
            "failed": 0,
        },
        "stages": {},
        "queue": {
            "last_depth": {
                "pending": 0,
                "running": 0,
                "done": 0,
                "failed": 0,
            },
            "updated_at": None,
        },
    }


def _normalize(payload: dict[str, Any]) -> dict[str, Any]:
    base = _empty_metrics()

    jobs = payload.get("jobs") if isinstance(payload.get("jobs"), dict) else {}
    stages = payload.get("stages") if isinstance(payload.get("stages"), dict) else {}
    queue = payload.get("queue") if isinstance(payload.get("queue"), dict) else {}

    base["updated_at"] = payload.get("updated_at", base["updated_at"])
    base["jobs"]["started"] = int(jobs.get("started", 0))
    base["jobs"]["succeeded"] = int(jobs.get("succeeded", 0))
    base["jobs"]["failed"] = int(jobs.get("failed", 0))
    base["stages"] = stages

    depth = queue.get("last_depth") if isinstance(queue.get("last_depth"), dict) else {}
    base["queue"]["last_depth"] = {
        "pending": int(depth.get("pending", 0)),
        "running": int(depth.get("running", 0)),
        "done": int(depth.get("done", 0)),
        "failed": int(depth.get("failed", 0)),
    }
    base["queue"]["updated_at"] = queue.get("updated_at")
    return base


def read_metrics(paths: StatePaths) -> dict[str, Any]:
    """Read current runtime metrics, returning defaults when not present."""

    path = _metrics_path(paths)
    if not path.exists():
        return _empty_metrics()
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise TypeError(f"Metrics payload must be an object: {path}")
    return _normalize(payload)


def _update_metrics(
    paths: StatePaths,
    mutator: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    metrics = read_metrics(paths)
    mutator(metrics)
    metrics["updated_at"] = _now()
    atomic_write_json(_metrics_path(paths), metrics)
    return metrics


def _ensure_stage(metrics: dict[str, Any], stage_name: str) -> dict[str, Any]:
    stages = metrics.get("stages")
    if not isinstance(stages, dict):
        stages = {}
        metrics["stages"] = stages

    existing = stages.get(stage_name)
    if not isinstance(existing, dict):
        existing = {
            "runs": 0,
            "succeeded": 0,
            "failed": 0,
            "retries": 0,
            "total_latency_sec": 0.0,
            "last_latency_sec": 0.0,
            "last_status": None,
            "updated_at": None,
        }
        stages[stage_name] = existing
    return existing


def record_queue_depth(paths: StatePaths) -> dict[str, int]:
    """Capture current queue depth counters into metrics state."""

    depth = {
        "pending": len(list(paths.queue_pending.glob("*.json"))),
        "running": len(list(paths.queue_running.glob("*.json"))),
        "done": len(list(paths.queue_done.glob("*.json"))),
        "failed": len(list(paths.queue_failed.glob("*.json"))),
    }

    def _mutate(metrics: dict[str, Any]) -> None:
        queue = metrics.get("queue")
        if not isinstance(queue, dict):
            queue = {}
            metrics["queue"] = queue
        queue["last_depth"] = depth
        queue["updated_at"] = _now()

    _update_metrics(paths, _mutate)
    return depth


def record_job_started(paths: StatePaths) -> None:
    """Increment the global started-job counter."""

    def _mutate(metrics: dict[str, Any]) -> None:
        jobs = metrics.get("jobs")
        if not isinstance(jobs, dict):
            jobs = {}
            metrics["jobs"] = jobs
        jobs["started"] = int(jobs.get("started", 0)) + 1

    _update_metrics(paths, _mutate)


def record_job_finished(paths: StatePaths, status: str) -> None:
    """Increment job completion counters by status."""

    normalized = status.strip().upper()

    def _mutate(metrics: dict[str, Any]) -> None:
        jobs = metrics.get("jobs")
        if not isinstance(jobs, dict):
            jobs = {}
            metrics["jobs"] = jobs

        if normalized == "SUCCEEDED":
            jobs["succeeded"] = int(jobs.get("succeeded", 0)) + 1
        elif normalized == "FAILED":
            jobs["failed"] = int(jobs.get("failed", 0)) + 1

    _update_metrics(paths, _mutate)


def record_stage_retry(paths: StatePaths, stage_name: str) -> None:
    """Increment retry counter for one stage."""

    def _mutate(metrics: dict[str, Any]) -> None:
        stage = _ensure_stage(metrics, stage_name)
        stage["retries"] = int(stage.get("retries", 0)) + 1
        stage["updated_at"] = _now()

    _update_metrics(paths, _mutate)


def record_stage_result(
    paths: StatePaths,
    *,
    stage_name: str,
    status: str,
    latency_seconds: float,
) -> None:
    """Record one stage batch outcome and latency."""

    normalized = status.strip().upper()
    latency = max(0.0, float(latency_seconds))

    def _mutate(metrics: dict[str, Any]) -> None:
        stage = _ensure_stage(metrics, stage_name)
        stage["runs"] = int(stage.get("runs", 0)) + 1

        if normalized == "SUCCEEDED":
            stage["succeeded"] = int(stage.get("succeeded", 0)) + 1
        elif normalized == "FAILED":
            stage["failed"] = int(stage.get("failed", 0)) + 1

        stage["total_latency_sec"] = (
            float(stage.get("total_latency_sec", 0.0)) + latency
        )
        stage["last_latency_sec"] = latency
        stage["last_status"] = normalized
        stage["updated_at"] = _now()

    _update_metrics(paths, _mutate)
