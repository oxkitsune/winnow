"""Pipeline executor for local and daemon job execution."""

from __future__ import annotations

from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from hashlib import sha1, sha256
import json
from pathlib import Path
import time
import traceback
from typing import Any
from uuid import uuid4

from winnow.config.schema import DuplicateMetricConfig, IdleMetricConfig, PipelineConfig
from winnow.ingest.scanner import StreamScanResult, scan_stream
from winnow.metrics import duplicates as duplicate_metrics
from winnow.metrics import idle as idle_metrics
from winnow.observability.logging import get_logger, log_event
from winnow.observability.metrics import (
    record_job_finished,
    record_job_started,
    record_queue_depth,
    record_stage_result,
    record_stage_retry,
)
from winnow.pipeline.registry import resolve_stages
from winnow.pipeline.stage import BatchInput, FrameInput, StageDefinition
from winnow.storage.artifacts import (
    read_jsonl_artifact,
    store_jsonl_artifact,
    verify_jsonl_artifact,
)
from winnow.storage.atomic import atomic_write_json, atomic_write_text
from winnow.storage.checkpoints import (
    checkpoint_path,
    is_checkpoint_hit,
    read_checkpoint,
    write_checkpoint,
)
from winnow.storage.events import append_event
from winnow.storage.queue import read_job, write_job
from winnow.storage.state_store import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_STATE_ROOT,
    StatePaths,
    ensure_artifact_root,
    ensure_state_layout,
)
from winnow.workers.pool import normalize_worker_count


_LOGGER = get_logger("winnow.executor")


@dataclass(slots=True)
class _StageWorkItem:
    """Resolved work unit for one stage and batch partition."""

    batch: BatchInput
    cache_key: str
    checkpoint_path: Path
    checkpoint_base: dict[str, Any]
    started_at: str


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stream_id(stream_path: Path) -> str:
    digest = sha1(str(stream_path.resolve()).encode("utf-8")).hexdigest()
    return digest[:16]


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    return value


def _build_cache_key(stage: StageDefinition, batch: BatchInput) -> str:
    payload = {
        "stage": stage.name,
        "version": stage.version,
        "config": _to_jsonable(stage.config),
        "stream_id": batch.stream_id,
        "start_idx": batch.start_idx,
        "end_idx": batch.end_idx,
        "frames": [
            {
                "frame_idx": frame.frame_idx,
                "path": str(frame.path),
                "size_bytes": frame.size_bytes,
                "mtime_ns": frame.mtime_ns,
            }
            for frame in batch.frames
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _build_global_cache_key(stage: StageDefinition, batch_cache_keys: list[str]) -> str:
    payload = {
        "stage": stage.name,
        "version": stage.version,
        "config": _to_jsonable(stage.config),
        "batch_cache_keys": sorted(batch_cache_keys),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _collect_frame_inputs(scan: StreamScanResult) -> list[FrameInput]:
    frames: list[FrameInput] = []
    for frame in scan.frames:
        stat = frame.path.stat()
        frames.append(
            FrameInput(
                frame_idx=frame.frame_idx,
                path=frame.path.resolve(),
                size_bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )
        )
    return frames


def _make_batches(
    *,
    stream_id: str,
    stream_path: Path,
    frames: list[FrameInput],
    batch_size: int,
) -> list[BatchInput]:
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")

    batches: list[BatchInput] = []
    for offset in range(0, len(frames), batch_size):
        chunk = frames[offset : offset + batch_size]
        if not chunk:
            continue
        batches.append(
            BatchInput(
                stream_id=stream_id,
                stream_path=stream_path,
                start_idx=chunk[0].frame_idx,
                end_idx=chunk[-1].frame_idx,
                frames=chunk,
            )
        )
    return batches


def _write_stream_manifest(
    *,
    state_root: Path,
    stream_id: str,
    scan: StreamScanResult,
    frames: list[FrameInput],
) -> None:
    stream_dir = state_root / "streams" / stream_id
    stream_meta_path = stream_dir / "stream.json"
    frames_path = stream_dir / "frames.jsonl"

    stream_payload = {
        "stream_id": stream_id,
        "path": str(scan.stream_path.resolve()),
        "frame_count": len(frames),
        "missing_indices": scan.missing_indices,
        "updated_at": _now(),
    }
    atomic_write_json(stream_meta_path, stream_payload)

    lines = []
    for frame in frames:
        lines.append(
            json.dumps(
                {
                    "frame_idx": frame.frame_idx,
                    "path": str(frame.path),
                    "size_bytes": frame.size_bytes,
                    "mtime_ns": frame.mtime_ns,
                },
                sort_keys=True,
            )
        )
    atomic_write_text(frames_path, "\n".join(lines) + ("\n" if lines else ""))


def _stage_checkpoint_base(
    *,
    stage: StageDefinition,
    batch: BatchInput,
    cache_key: str,
    attempt: int,
) -> dict[str, Any]:
    return {
        "stage": stage.name,
        "stage_version": stage.version,
        "stream_id": batch.stream_id,
        "batch_start": batch.start_idx,
        "batch_end": batch.end_idx,
        "cache_key": cache_key,
        "attempt": attempt,
        "config": _to_jsonable(stage.config),
    }


def _prepare_stage_work(
    *,
    stage: StageDefinition,
    batch: BatchInput,
    paths: StatePaths,
    stage_stats: dict[str, int],
) -> _StageWorkItem | None:
    cache_key = _build_cache_key(stage, batch)
    ckpt_path = checkpoint_path(
        paths=paths,
        stage_name=stage.name,
        stream_id=batch.stream_id,
        batch_start=batch.start_idx,
        batch_end=batch.end_idx,
        cache_key=cache_key,
    )

    checkpoint = read_checkpoint(ckpt_path)
    if is_checkpoint_hit(checkpoint):
        artifact = checkpoint.get("artifact") if checkpoint else None
        artifact_uri = artifact.get("uri") if isinstance(artifact, dict) else None
        if artifact_uri and Path(artifact_uri).exists():
            stage_stats["skipped"] += 1
            return None

    attempt = int(checkpoint.get("attempt", 0)) + 1 if checkpoint else 1
    if attempt > 1:
        record_stage_retry(paths, stage.name)

    base = _stage_checkpoint_base(
        stage=stage,
        batch=batch,
        cache_key=cache_key,
        attempt=attempt,
    )
    started_at = _now()

    write_checkpoint(
        ckpt_path,
        {
            **base,
            "status": "RUNNING",
            "started_at": started_at,
        },
    )

    return _StageWorkItem(
        batch=batch,
        cache_key=cache_key,
        checkpoint_path=ckpt_path,
        checkpoint_base=base,
        started_at=started_at,
    )


def _finalize_stage_success(
    *,
    stage: StageDefinition,
    work: _StageWorkItem,
    records: list[dict[str, Any]],
    artifact_root: Path,
    stage_stats: dict[str, int],
) -> None:
    artifact = store_jsonl_artifact(
        artifact_root,
        stage_name=stage.name,
        stream_id=work.batch.stream_id,
        batch_start=work.batch.start_idx,
        batch_end=work.batch.end_idx,
        cache_key=work.cache_key,
        records=records,
    )
    integrity = verify_jsonl_artifact(artifact)
    write_checkpoint(
        work.checkpoint_path,
        {
            **work.checkpoint_base,
            "status": "SUCCEEDED",
            "started_at": work.started_at,
            "finished_at": _now(),
            "artifact": artifact,
            "record_count": artifact["record_count"],
            "integrity": integrity,
        },
    )
    stage_stats["completed"] += 1


def _finalize_stage_failure(
    *,
    work: _StageWorkItem,
    stage_stats: dict[str, int],
    error: str,
    tb: str,
) -> None:
    write_checkpoint(
        work.checkpoint_path,
        {
            **work.checkpoint_base,
            "status": "FAILED",
            "started_at": work.started_at,
            "finished_at": _now(),
            "error": error,
            "traceback": tb,
        },
    )
    stage_stats["failed"] += 1


def _run_stage_runner(
    stage: StageDefinition,
    batch: BatchInput,
) -> list[dict[str, Any]]:
    """Worker-safe stage runner invocation."""

    return stage.runner(batch, stage.config)


def _execute_stage(
    *,
    stage: StageDefinition,
    batches: list[BatchInput],
    paths: StatePaths,
    artifact_root: Path,
    stage_stats: dict[str, int],
    max_workers: int,
) -> None:
    prepared: list[_StageWorkItem] = []
    for batch in batches:
        work = _prepare_stage_work(
            stage=stage,
            batch=batch,
            paths=paths,
            stage_stats=stage_stats,
        )
        if work is not None:
            prepared.append(work)

    if not prepared:
        return

    if max_workers <= 1 or len(prepared) == 1:
        for work in prepared:
            started_perf = time.perf_counter()
            try:
                records = _run_stage_runner(stage, work.batch)
            except Exception as exc:
                _finalize_stage_failure(
                    work=work,
                    stage_stats=stage_stats,
                    error=f"{type(exc).__name__}: {exc}",
                    tb=traceback.format_exc(),
                )
                record_stage_result(
                    paths,
                    stage_name=stage.name,
                    status="FAILED",
                    latency_seconds=time.perf_counter() - started_perf,
                )
                raise
            try:
                _finalize_stage_success(
                    stage=stage,
                    work=work,
                    records=records,
                    artifact_root=artifact_root,
                    stage_stats=stage_stats,
                )
            except Exception as exc:
                _finalize_stage_failure(
                    work=work,
                    stage_stats=stage_stats,
                    error=f"{type(exc).__name__}: {exc}",
                    tb=traceback.format_exc(),
                )
                record_stage_result(
                    paths,
                    stage_name=stage.name,
                    status="FAILED",
                    latency_seconds=time.perf_counter() - started_perf,
                )
                raise

            record_stage_result(
                paths,
                stage_name=stage.name,
                status="SUCCEEDED",
                latency_seconds=time.perf_counter() - started_perf,
            )
        return

    first_error: Exception | None = None
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map: dict[Future[list[dict[str, Any]]], tuple[_StageWorkItem, float]] = {
            executor.submit(_run_stage_runner, stage, work.batch): (work, time.perf_counter())
            for work in prepared
        }
        for future in as_completed(future_map):
            work, started_perf = future_map[future]
            try:
                records = future.result()
            except Exception as exc:
                _finalize_stage_failure(
                    work=work,
                    stage_stats=stage_stats,
                    error=f"{type(exc).__name__}: {exc}",
                    tb="".join(traceback.format_exception(exc)),
                )
                record_stage_result(
                    paths,
                    stage_name=stage.name,
                    status="FAILED",
                    latency_seconds=time.perf_counter() - started_perf,
                )
                if first_error is None:
                    first_error = exc
                continue

            try:
                _finalize_stage_success(
                    stage=stage,
                    work=work,
                    records=records,
                    artifact_root=artifact_root,
                    stage_stats=stage_stats,
                )
            except Exception as exc:
                _finalize_stage_failure(
                    work=work,
                    stage_stats=stage_stats,
                    error=f"{type(exc).__name__}: {exc}",
                    tb=traceback.format_exc(),
                )
                record_stage_result(
                    paths,
                    stage_name=stage.name,
                    status="FAILED",
                    latency_seconds=time.perf_counter() - started_perf,
                )
                if first_error is None:
                    first_error = exc
                continue

            record_stage_result(
                paths,
                stage_name=stage.name,
                status="SUCCEEDED",
                latency_seconds=time.perf_counter() - started_perf,
            )

    if first_error is not None:
        raise RuntimeError(f"Stage {stage.name} failed: {first_error}")


def _collect_stage_records_for_batches(
    *,
    stage: StageDefinition,
    batches: list[BatchInput],
    paths: StatePaths,
) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    cache_keys: list[str] = []

    for batch in batches:
        cache_key = _build_cache_key(stage, batch)
        ckpt_path = checkpoint_path(
            paths=paths,
            stage_name=stage.name,
            stream_id=batch.stream_id,
            batch_start=batch.start_idx,
            batch_end=batch.end_idx,
            cache_key=cache_key,
        )
        checkpoint = read_checkpoint(ckpt_path)
        if checkpoint is None or checkpoint.get("status") != "SUCCEEDED":
            raise RuntimeError(
                f"Missing successful checkpoint for stage={stage.name} "
                f"batch={batch.start_idx}-{batch.end_idx}"
            )

        artifact = checkpoint.get("artifact")
        uri = artifact.get("uri") if isinstance(artifact, dict) else None
        if not isinstance(uri, str):
            raise RuntimeError(
                f"Checkpoint missing artifact URI for stage={stage.name} "
                f"batch={batch.start_idx}-{batch.end_idx}"
            )

        artifact_path = Path(uri)
        if not artifact_path.exists():
            raise FileNotFoundError(f"Artifact URI not found: {artifact_path}")

        records.extend(read_jsonl_artifact(artifact_path))
        cache_keys.append(cache_key)

    records.sort(key=lambda item: int(item.get("frame_idx", 0)))
    return records, cache_keys


def _globalize_stage_records(
    *,
    stage: StageDefinition,
    records: list[dict[str, Any]],
    stream_id: str,
) -> list[dict[str, Any]] | None:
    if stage.name == "duplicates" and isinstance(stage.config, DuplicateMetricConfig):
        return duplicate_metrics.globalize(records=records, config=stage.config, stream_id=stream_id)
    if stage.name == "idle" and isinstance(stage.config, IdleMetricConfig):
        return idle_metrics.globalize(records=records, config=stage.config, stream_id=stream_id)
    if stage.name == "annotation":
        ordered = [dict(record) for record in records]
        ordered.sort(key=lambda item: int(item.get("frame_idx", 0)))
        return ordered
    return None


def _build_annotation_summary(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    frame_count = 0
    annotation_count = 0
    category_counts: dict[str, int] = {}

    for record in records:
        frame_count += 1
        items = record.get("annotations", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            annotation_count += 1
            label = item.get("label")
            if isinstance(label, str):
                category_counts[label] = category_counts.get(label, 0) + 1

    categories = [
        {"label": label, "count": category_counts[label]}
        for label in sorted(category_counts)
    ]
    return [
        {
            "frame_count": frame_count,
            "annotation_count": annotation_count,
            "categories": categories,
        }
    ]


def _build_duplicate_cluster_records(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        group_id = record.get("duplicate_group_id")
        if not isinstance(group_id, str):
            continue
        groups.setdefault(group_id, []).append(record)

    clusters: list[dict[str, Any]] = []
    for group_id in sorted(groups):
        members = sorted(groups[group_id], key=lambda item: int(item["frame_idx"]))
        frame_indices = [int(item["frame_idx"]) for item in members]
        confidences = [float(item.get("duplicate_confidence", 0.0)) for item in members]
        clusters.append(
            {
                "duplicate_group_id": group_id,
                "representative_frame_idx": frame_indices[0],
                "member_count": len(members),
                "frame_indices": frame_indices,
                "mean_confidence": sum(confidences) / len(confidences) if confidences else 0.0,
            }
        )
    return clusters


def _build_idle_interval_records(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        segment_id = record.get("idle_segment_id")
        if not isinstance(segment_id, str):
            continue
        groups.setdefault(segment_id, []).append(record)

    intervals: list[dict[str, Any]] = []
    for segment_id in sorted(groups):
        members = sorted(groups[segment_id], key=lambda item: int(item["frame_idx"]))
        frame_indices = [int(item["frame_idx"]) for item in members]
        smoothed = [float(item.get("smoothed_motion_score", 0.0)) for item in members]
        intervals.append(
            {
                "idle_segment_id": segment_id,
                "start_frame_idx": frame_indices[0],
                "end_frame_idx": frame_indices[-1],
                "frame_count": len(frame_indices),
                "frame_indices": frame_indices,
                "mean_smoothed_motion": sum(smoothed) / len(smoothed) if smoothed else 0.0,
            }
        )
    return intervals


def _build_stage_summaries(
    *,
    stage: StageDefinition,
    global_records: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    if stage.name == "duplicates":
        return [("clusters", _build_duplicate_cluster_records(global_records))]
    if stage.name == "idle":
        return [("intervals", _build_idle_interval_records(global_records))]
    if stage.name == "annotation":
        return [("summary", _build_annotation_summary(global_records))]
    return []


def _write_global_stage_output(
    *,
    stage: StageDefinition,
    batches: list[BatchInput],
    paths: StatePaths,
    artifact_root: Path,
    stream_id: str,
) -> dict[str, Any] | None:
    if not batches:
        return None

    per_batch_records, cache_keys = _collect_stage_records_for_batches(
        stage=stage,
        batches=batches,
        paths=paths,
    )
    global_records = _globalize_stage_records(
        stage=stage,
        records=per_batch_records,
        stream_id=stream_id,
    )
    if global_records is None:
        return None

    global_cache_key = _build_global_cache_key(stage, cache_keys)
    first_batch = min(batches, key=lambda item: item.start_idx)
    last_batch = max(batches, key=lambda item: item.end_idx)

    artifact = store_jsonl_artifact(
        artifact_root,
        stage_name=f"{stage.name}_global",
        stream_id=stream_id,
        batch_start=first_batch.start_idx,
        batch_end=last_batch.end_idx,
        cache_key=global_cache_key,
        records=global_records,
    )
    summary_artifacts: dict[str, dict[str, Any]] = {}
    for summary_name, summary_records in _build_stage_summaries(
        stage=stage,
        global_records=global_records,
    ):
        summary_artifact = store_jsonl_artifact(
            artifact_root,
            stage_name=f"{stage.name}_{summary_name}",
            stream_id=stream_id,
            batch_start=first_batch.start_idx,
            batch_end=last_batch.end_idx,
            cache_key=global_cache_key,
            records=summary_records,
        )
        summary_artifacts[summary_name] = {
            "name": summary_name,
            "record_count": summary_artifact["record_count"],
            "artifact": summary_artifact,
        }

    return {
        "type": "global_stage_output",
        "stage": stage.name,
        "cache_key": global_cache_key,
        "record_count": artifact["record_count"],
        "artifact": artifact,
        "summary_artifacts": summary_artifacts,
    }


def execute_pipeline_job(
    *,
    input_path: Path,
    config: PipelineConfig,
    state_root: Path = DEFAULT_STATE_ROOT,
    artifacts_root: Path = DEFAULT_ARTIFACT_ROOT,
    job_id: str | None = None,
    mode: str = "direct",
    max_workers: int = 1,
    raise_on_error: bool = True,
) -> dict[str, Any]:
    """Execute full stream pipeline for one input folder."""

    paths = ensure_state_layout(state_root)
    artifact_root = ensure_artifact_root(artifacts_root)
    worker_count = normalize_worker_count(max_workers)

    if job_id is None:
        job_id = uuid4().hex

    existing_job = read_job(paths, job_id)
    submitted_at = existing_job.get("submitted_at") if existing_job else _now()

    scan = scan_stream(input_path, strict_sequence=config.ingest.strict_sequence)
    frames = _collect_frame_inputs(scan)
    stream_id = _stream_id(scan.stream_path)
    _write_stream_manifest(
        state_root=paths.root,
        stream_id=stream_id,
        scan=scan,
        frames=frames,
    )

    batches = _make_batches(
        stream_id=stream_id,
        stream_path=scan.stream_path.resolve(),
        frames=frames,
        batch_size=config.batch_size,
    )
    stages = resolve_stages(config)

    stage_stats = {
        stage.name: {"completed": 0, "skipped": 0, "failed": 0}
        for stage in stages
    }
    stage_outputs: dict[str, Any] = {}

    started_at = _now()
    running_record: dict[str, Any] = {
        "job_id": job_id,
        "status": "RUNNING",
        "mode": mode,
        "submitted_at": submitted_at,
        "updated_at": started_at,
        "started_at": started_at,
        "input": str(input_path.resolve()),
        "stream_id": stream_id,
        "frame_count": len(frames),
        "batch_size": config.batch_size,
        "batch_count": len(batches),
        "max_workers": worker_count,
        "config": asdict(config),
        "stage_stats": stage_stats,
        "stage_outputs": stage_outputs,
    }
    if existing_job and "payload" in existing_job:
        running_record["payload"] = existing_job["payload"]
    write_job(paths, job_id, running_record)
    record_job_started(paths)
    record_queue_depth(paths)
    append_event(
        paths,
        {
            "event": "pipeline_started",
            "job_id": job_id,
            "mode": mode,
            "correlation_id": job_id,
        },
    )
    log_event(
        _LOGGER,
        "pipeline_started",
        job_id=job_id,
        correlation_id=job_id,
        mode=mode,
        stream_id=stream_id,
        frame_count=len(frames),
        batch_count=len(batches),
        workers=worker_count,
    )

    try:
        for stage in stages:
            _execute_stage(
                stage=stage,
                batches=batches,
                paths=paths,
                artifact_root=artifact_root,
                stage_stats=stage_stats[stage.name],
                max_workers=worker_count,
            )

            global_output = _write_global_stage_output(
                stage=stage,
                batches=batches,
                paths=paths,
                artifact_root=artifact_root,
                stream_id=stream_id,
            )
            if global_output is not None:
                stage_outputs[stage.name] = global_output
                append_event(
                    paths,
                    {
                        "event": "stage_globalized",
                        "job_id": job_id,
                        "correlation_id": job_id,
                        "stage": stage.name,
                        "record_count": global_output["record_count"],
                    },
                )
                log_event(
                    _LOGGER,
                    "stage_globalized",
                    job_id=job_id,
                    correlation_id=job_id,
                    stage=stage.name,
                    record_count=global_output["record_count"],
                )

        success_record = {
            **running_record,
            "status": "SUCCEEDED",
            "updated_at": _now(),
            "finished_at": _now(),
            "message": "Pipeline execution completed.",
            "stage_stats": stage_stats,
            "stage_outputs": stage_outputs,
        }
        write_job(paths, job_id, success_record)
        record_job_finished(paths, "SUCCEEDED")
        record_queue_depth(paths)
        append_event(
            paths,
            {
                "event": "pipeline_finished",
                "job_id": job_id,
                "correlation_id": job_id,
                "status": "SUCCEEDED",
            },
        )
        log_event(
            _LOGGER,
            "pipeline_finished",
            job_id=job_id,
            correlation_id=job_id,
            status="SUCCEEDED",
            stage_stats=stage_stats,
        )
        return success_record

    except Exception as exc:
        failed_record = {
            **running_record,
            "status": "FAILED",
            "updated_at": _now(),
            "finished_at": _now(),
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
            "stage_stats": stage_stats,
            "stage_outputs": stage_outputs,
        }
        write_job(paths, job_id, failed_record)
        record_job_finished(paths, "FAILED")
        record_queue_depth(paths)
        append_event(
            paths,
            {
                "event": "pipeline_finished",
                "job_id": job_id,
                "correlation_id": job_id,
                "status": "FAILED",
            },
        )
        log_event(
            _LOGGER,
            "pipeline_finished",
            level=40,
            job_id=job_id,
            correlation_id=job_id,
            status="FAILED",
            error=f"{type(exc).__name__}: {exc}",
        )
        if raise_on_error:
            raise
        return failed_record
