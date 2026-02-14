"""`winnow run` command."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from winnow.config.loader import load_pipeline_config
from winnow.config.profiles import apply_profile
from winnow.pipeline.executor import execute_pipeline_job
from winnow.storage.state_store import DEFAULT_ARTIFACT_ROOT, DEFAULT_STATE_ROOT


@dataclass(slots=True)
class RunCommand:
    """Execute a local direct pipeline run."""

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


def execute(command: RunCommand) -> None:
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

    worker_count = max(1, int(command.workers)) if command.workers is not None else 1
    if command.workers is None and profile is not None:
        worker_count = profile.suggested_workers

    result = execute_pipeline_job(
        input_path=command.input,
        config=cfg,
        state_root=command.state_root,
        artifacts_root=command.artifacts_root,
        mode="direct",
        max_workers=worker_count,
    )
    print(
        f"run job_id={result['job_id']} status={result['status']} "
        f"frames={result['frame_count']} batches={result['batch_count']} "
        f"workers={result['max_workers']}"
    )
