"""Filesystem queue helpers used by `submit` and daemon scheduler."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from winnow.storage.atomic import atomic_write_json, read_json
from winnow.storage.state_store import StatePaths


def build_job_record(
    job_id: str,
    payload: dict[str, Any],
    status: str,
    message: str | None = None,
) -> dict[str, Any]:
    """Build a normalized job record structure."""

    now = datetime.now(timezone.utc).isoformat()
    record = {
        "job_id": job_id,
        "status": status,
        "submitted_at": now,
        "updated_at": now,
        "payload": payload,
    }
    if message:
        record["message"] = message
    return record


def enqueue_job(paths: StatePaths, payload: dict[str, Any]) -> str:
    """Persist a job record and queue file in pending."""

    job_id = payload.get("job_id") or uuid4().hex
    record = build_job_record(job_id=job_id, payload=payload, status="PENDING")
    atomic_write_json(paths.jobs / f"{job_id}.json", record)
    atomic_write_json(paths.queue_pending / f"{job_id}.json", record)
    return job_id


def read_job(paths: StatePaths, job_id: str) -> dict[str, Any] | None:
    """Read a stored job record by id."""

    job_path = paths.jobs / f"{job_id}.json"
    if not job_path.exists():
        return None
    return read_json(job_path)


def write_job(paths: StatePaths, job_id: str, payload: dict[str, Any]) -> Path:
    """Overwrite a job record by id."""

    out_path = paths.jobs / f"{job_id}.json"
    atomic_write_json(out_path, payload)
    return out_path
