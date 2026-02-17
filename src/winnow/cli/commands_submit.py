"""`winnow submit` command."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from winnow.config.loader import load_pipeline_config
from winnow.config.profiles import apply_profile
from winnow.ingest.scanner import scan_stream
from winnow.observability.metrics import record_queue_depth
from winnow.storage.queue import enqueue_job
from winnow.storage.state_store import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_STATE_ROOT,
    ensure_state_layout,
)
from winnow.workers.pool import normalize_worker_count


@dataclass(slots=True)
class SubmitCommand:
    """Submit a job to the local filesystem queue."""

    input: Path
    config: str | None = None
    profile: str | None = None
    state_root: Path = DEFAULT_STATE_ROOT
    artifacts_root: Path = DEFAULT_ARTIFACT_ROOT
    workers: int | None = None
    annotation_enabled: bool | None = None
    annotation_detector: str | None = None
    annotation_segmenter: str | None = None
    annotation_min_score: float | None = None
    strict_sequence: bool = True


def execute(command: SubmitCommand) -> None:
    cfg = load_pipeline_config(
        config_ref=command.config,
        input_path=command.input,
        strict_sequence=command.strict_sequence,
    )
    profile = apply_profile(cfg, command.profile) if command.profile else None
    if command.annotation_enabled is not None:
        cfg.annotation.enabled = command.annotation_enabled
    if command.annotation_detector is not None:
        cfg.annotation.detector = command.annotation_detector
    if command.annotation_segmenter is not None:
        cfg.annotation.segmenter = command.annotation_segmenter
    if command.annotation_min_score is not None:
        cfg.annotation.min_score = float(command.annotation_min_score)

    if command.workers is not None:
        worker_count = normalize_worker_count(command.workers)
    elif profile is not None:
        worker_count = normalize_worker_count(profile.suggested_workers)
    else:
        worker_count = normalize_worker_count(8)

    scan = scan_stream(command.input, strict_sequence=command.strict_sequence)
    paths = ensure_state_layout(command.state_root)
    payload = {
        "input": str(command.input.resolve()),
        "artifacts_root": str(command.artifacts_root.resolve()),
        "workers": worker_count,
        "frame_count": scan.frame_count,
        "missing_indices": scan.missing_indices,
        "config": asdict(cfg),
    }
    job_id = enqueue_job(paths, payload)
    record_queue_depth(paths)
    print(
        f"submitted job_id={job_id} workers={worker_count} "
        f"pending={paths.queue_pending / f'{job_id}.json'}"
    )
