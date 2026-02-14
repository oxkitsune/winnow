"""Filesystem-queue scheduler for the local daemon."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import traceback

from winnow.config.loader import pipeline_config_from_dict
from winnow.observability.logging import get_logger, log_event
from winnow.observability.metrics import record_queue_depth
from winnow.pipeline.executor import execute_pipeline_job
from winnow.storage.atomic import atomic_move, atomic_write_json, read_json
from winnow.storage.events import append_event
from winnow.storage.queue import read_job, write_job
from winnow.storage.state_store import DEFAULT_ARTIFACT_ROOT, StatePaths


_LOGGER = get_logger("winnow.scheduler")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _next_pending(paths: StatePaths) -> Path | None:
    candidates = sorted(paths.queue_pending.glob("*.json"))
    if not candidates:
        return None
    return candidates[0]


def process_one_pending(paths: StatePaths) -> int:
    """Process one queued job through the full pipeline."""

    pending_path = _next_pending(paths)
    if pending_path is None:
        return 0

    job_id = pending_path.stem
    running_path = paths.queue_running / pending_path.name
    done_path = paths.queue_done / pending_path.name
    failed_path = paths.queue_failed / pending_path.name

    atomic_move(pending_path, running_path)
    record_queue_depth(paths)

    job = read_job(paths, job_id)
    if job is None:
        payload = read_json(running_path)
        job = {
            "job_id": job_id,
            "status": "PENDING",
            "submitted_at": _now(),
            "updated_at": _now(),
            "payload": payload,
        }

    job["status"] = "RUNNING"
    job["updated_at"] = _now()
    job["started_at"] = _now()
    write_job(paths, job_id, job)
    append_event(
        paths,
        {
            "event": "job_started",
            "job_id": job_id,
            "mode": "daemon",
            "correlation_id": job_id,
        },
    )
    log_event(
        _LOGGER,
        "job_started",
        job_id=job_id,
        correlation_id=job_id,
        queue_file=str(running_path),
    )

    try:
        payload = job.get("payload", {})
        input_path = Path(payload["input"])
        cfg = pipeline_config_from_dict(payload["config"])
        artifacts_root = Path(payload.get("artifacts_root", str(DEFAULT_ARTIFACT_ROOT)))
        workers = int(payload.get("workers", 1))

        result = execute_pipeline_job(
            input_path=input_path,
            config=cfg,
            state_root=paths.root,
            artifacts_root=artifacts_root,
            job_id=job_id,
            mode="daemon",
            max_workers=workers,
            raise_on_error=False,
        )
    except Exception as exc:
        result = {
            **job,
            "status": "FAILED",
            "updated_at": _now(),
            "finished_at": _now(),
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
        write_job(paths, job_id, result)

    atomic_write_json(running_path, result)

    if result.get("status") == "SUCCEEDED":
        atomic_move(running_path, done_path)
    else:
        atomic_move(running_path, failed_path)
    record_queue_depth(paths)

    append_event(
        paths,
        {
            "event": "job_finished",
            "job_id": job_id,
            "correlation_id": job_id,
            "status": result.get("status", "UNKNOWN"),
        },
    )
    log_event(
        _LOGGER,
        "job_finished",
        job_id=job_id,
        correlation_id=job_id,
        status=result.get("status", "UNKNOWN"),
    )
    return 1
