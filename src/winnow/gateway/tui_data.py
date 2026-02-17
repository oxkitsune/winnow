"""Data loading and aggregation helpers for the gateway TUI."""

from __future__ import annotations

from collections import deque
import json
from pathlib import Path
import subprocess
from typing import Any, Callable

from winnow.storage.artifacts import iter_jsonl_artifact
from winnow.storage.atomic import read_json
from winnow.storage.events import iter_event_files
from winnow.storage.state_store import StatePaths

from .tui_models import (
    _ArtifactMetricCacheEntry,
    _METRIC_LABELS,
    _STAGE_BOOLEAN_METRICS,
    _STAGE_NUMERIC_METRICS,
    BatchStatus,
    JobSummary,
    StageImageStats,
    StageMetricStats,
    StageProgress,
    WorkerProcess,
)
from .tui_utils import _as_int, _iter_json_files, _latest_timestamp, _optional_int

def _build_job_summary(
    job_id: str,
    payload: dict[str, Any],
    queue_lane: str,
) -> JobSummary:
    nested = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}

    config = payload.get("config")
    if not isinstance(config, dict):
        nested_config = nested.get("config")
        config = nested_config if isinstance(nested_config, dict) else {}

    stage_stats_payload = payload.get("stage_stats")
    stage_stats: dict[str, dict[str, int]] = {}
    if isinstance(stage_stats_payload, dict):
        for stage_name, stage_info in stage_stats_payload.items():
            if not isinstance(stage_name, str) or not isinstance(stage_info, dict):
                continue
            stage_stats[stage_name] = {
                "completed": _as_int(stage_info.get("completed")),
                "failed": _as_int(stage_info.get("failed")),
                "skipped": _as_int(stage_info.get("skipped")),
            }

    stage_outputs = payload.get("stage_outputs")
    if not isinstance(stage_outputs, dict):
        stage_outputs = {}

    job_status = payload.get("status")
    if isinstance(job_status, str) and job_status:
        status_value = job_status.upper()
    elif queue_lane != "none":
        status_value = queue_lane.upper()
    else:
        status_value = "UNKNOWN"

    mode_value = payload.get("mode")
    if isinstance(mode_value, str) and mode_value:
        mode = mode_value
    elif queue_lane in {"pending", "running", "done", "failed"}:
        mode = "daemon"
    else:
        mode = "direct"

    input_path = payload.get("input")
    if not isinstance(input_path, str):
        nested_input = nested.get("input")
        input_path = nested_input if isinstance(nested_input, str) else None

    frame_count = payload.get("frame_count")
    if frame_count is None:
        frame_count = nested.get("frame_count")

    max_workers = payload.get("max_workers")
    if max_workers is None:
        max_workers = nested.get("workers")

    return JobSummary(
        job_id=job_id,
        status=status_value,
        mode=mode,
        queue_lane=queue_lane,
        input_path=input_path,
        frame_count=_optional_int(frame_count),
        batch_count=_optional_int(payload.get("batch_count")),
        max_workers=_optional_int(max_workers),
        submitted_at=payload.get("submitted_at")
        if isinstance(payload.get("submitted_at"), str)
        else None,
        updated_at=payload.get("updated_at")
        if isinstance(payload.get("updated_at"), str)
        else None,
        started_at=payload.get("started_at")
        if isinstance(payload.get("started_at"), str)
        else None,
        finished_at=payload.get("finished_at")
        if isinstance(payload.get("finished_at"), str)
        else None,
        stream_id=payload.get("stream_id")
        if isinstance(payload.get("stream_id"), str)
        else None,
        stage_stats=stage_stats,
        stage_outputs=stage_outputs,
        config=config,
        error=payload.get("error") if isinstance(payload.get("error"), str) else None,
    )


def _read_job_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = read_json(path)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_jobs(
    paths: StatePaths,
    *,
    read_payload: Callable[[Path], dict[str, Any] | None] = _read_job_payload,
) -> list[JobSummary]:
    queue_paths = {
        "pending": paths.queue_pending,
        "running": paths.queue_running,
        "done": paths.queue_done,
        "failed": paths.queue_failed,
    }

    queue_lookup: dict[str, tuple[str, Path]] = {}
    for lane, lane_root in queue_paths.items():
        for file_path in _iter_json_files(lane_root):
            queue_lookup[file_path.stem] = (lane, file_path)

    jobs: list[JobSummary] = []
    seen: set[str] = set()

    for job_path in _iter_json_files(paths.jobs):
        payload = read_payload(job_path)
        if payload is None:
            continue

        job_id = (
            payload.get("job_id")
            if isinstance(payload.get("job_id"), str)
            else job_path.stem
        )
        lane = queue_lookup.get(job_id, ("none", job_path))[0]
        jobs.append(_build_job_summary(job_id=job_id, payload=payload, queue_lane=lane))
        seen.add(job_id)

    for job_id, (lane, queue_file) in queue_lookup.items():
        if job_id in seen:
            continue
        payload = read_payload(queue_file)
        if payload is None:
            payload = {"job_id": job_id, "status": lane.upper()}
        jobs.append(_build_job_summary(job_id=job_id, payload=payload, queue_lane=lane))

    jobs.sort(key=_latest_timestamp, reverse=True)
    return jobs


def _read_events_file(
    path: Path, *, sink: deque[dict[str, Any]], job_id: str | None = None
) -> None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue
                if job_id is not None:
                    corr = payload.get("correlation_id")
                    event_job = payload.get("job_id")
                    if event_job != job_id and corr != job_id:
                        continue
                sink.append(payload)
    except OSError:
        return


def _load_recent_events(
    paths: StatePaths,
    *,
    limit: int,
    job_id: str | None,
) -> list[dict[str, Any]]:
    files = list(iter_event_files(paths))
    if not files:
        return []

    maxlen = max(32, limit)
    sink: deque[dict[str, Any]] = deque(maxlen=maxlen)

    hot_files = files[-3:]
    for file_path in hot_files:
        _read_events_file(file_path, sink=sink, job_id=job_id)

    if job_id is not None and not sink:
        for file_path in reversed(files[:-3]):
            _read_events_file(file_path, sink=sink, job_id=job_id)
            if len(sink) >= limit:
                break

    return list(sink)[-limit:]


def _stage_order(
    config: dict[str, Any], stage_stats: dict[str, dict[str, int]]
) -> list[str]:
    stages = ["blur", "darkness"]

    duplicate_cfg = (
        config.get("duplicate") if isinstance(config.get("duplicate"), dict) else {}
    )
    idle_cfg = config.get("idle") if isinstance(config.get("idle"), dict) else {}
    annotation_cfg = (
        config.get("annotation") if isinstance(config.get("annotation"), dict) else {}
    )

    if bool(duplicate_cfg.get("enabled", True)):
        stages.append("duplicates")
    if bool(idle_cfg.get("enabled", True)):
        stages.append("idle")
    if bool(annotation_cfg.get("enabled", False)):
        stages.append("annotation")

    for stage_name in stage_stats:
        if stage_name not in stages:
            stages.append(stage_name)
    return stages


def _checkpoint_counts(
    paths: StatePaths,
    *,
    stage_name: str,
    stream_id: str,
    read_payload: Callable[[Path], dict[str, Any] | None] = _read_job_payload,
) -> dict[str, int]:
    counts: dict[str, int] = {
        "SUCCEEDED": 0,
        "RUNNING": 0,
        "FAILED": 0,
        "UNKNOWN": 0,
    }
    stage_root = paths.stages / stage_name / stream_id
    if not stage_root.exists():
        return counts

    try:
        batch_dirs = sorted(stage_root.iterdir(), key=lambda path: path.name)
    except OSError:
        return counts

    for batch_dir in batch_dirs:
        if not batch_dir.is_dir():
            continue

        latest_status = "UNKNOWN"
        latest_stamp = ""
        latest_mtime = -1

        for checkpoint in _iter_json_files(batch_dir):
            payload = read_payload(checkpoint)
            if payload is None:
                continue

            status = payload.get("status")
            if not isinstance(status, str) or not status:
                status = "UNKNOWN"
            status = status.upper()

            stamp = payload.get("finished_at") or payload.get("started_at")
            stamp_text = stamp if isinstance(stamp, str) else ""
            try:
                mtime = checkpoint.stat().st_mtime_ns
            except OSError:
                mtime = -1

            if (stamp_text, mtime) >= (latest_stamp, latest_mtime):
                latest_status = status
                latest_stamp = stamp_text
                latest_mtime = mtime

        counts[latest_status] = counts.get(latest_status, 0) + 1

    return counts


def _build_stage_progress(
    paths: StatePaths,
    job: JobSummary,
    *,
    read_payload: Callable[[Path], dict[str, Any] | None] = _read_job_payload,
) -> list[StageProgress]:
    expected = max(0, job.batch_count or 0)
    stages = _stage_order(job.config, job.stage_stats)

    progress: list[StageProgress] = []
    for stage_name in stages:
        stage_stat = job.stage_stats.get(stage_name, {})
        completed = _as_int(stage_stat.get("completed"))
        failed = _as_int(stage_stat.get("failed"))
        skipped = _as_int(stage_stat.get("skipped"))
        running = 0

        should_use_checkpoints = job.status in {"RUNNING", "PENDING"} or (
            completed == 0 and failed == 0 and skipped == 0
        )

        if should_use_checkpoints and job.stream_id:
            counts = _checkpoint_counts(
                paths,
                stage_name=stage_name,
                stream_id=job.stream_id,
                read_payload=read_payload,
            )
            completed = counts.get("SUCCEEDED", 0)
            failed = counts.get("FAILED", 0)
            running = counts.get("RUNNING", 0)

        if expected > 0:
            completed = min(completed, expected)
            skipped = min(skipped, max(0, expected - completed))

        progress.append(
            StageProgress(
                stage=stage_name,
                expected_batches=expected,
                succeeded_batches=completed,
                running_batches=running,
                failed_batches=failed,
                skipped_batches=skipped,
            )
        )

    return progress


def _status_style(status: str) -> str:
    normalized = status.upper()
    if normalized == "RUNNING":
        return "bold green"
    if normalized == "SUCCEEDED":
        return "green"
    if normalized == "PENDING":
        return "yellow"
    if normalized == "FAILED":
        return "bold red"
    if normalized == "PARTIAL":
        return "bold cyan"
    return "cyan"


def _parse_batch_range(value: str) -> tuple[int, int] | None:
    if "_" not in value:
        return None
    left, right = value.split("_", 1)
    try:
        return int(left), int(right)
    except ValueError:
        return None


def _latest_checkpoint_in_batch_dir(
    batch_dir: Path,
    *,
    read_payload: Callable[[Path], dict[str, Any] | None],
) -> dict[str, Any] | None:
    latest_payload: dict[str, Any] | None = None
    latest_key = ("", -1)
    for checkpoint in _iter_json_files(batch_dir):
        payload = read_payload(checkpoint)
        if payload is None:
            continue
        stamp = payload.get("finished_at") or payload.get("started_at")
        stamp_text = stamp if isinstance(stamp, str) else ""
        try:
            mtime = checkpoint.stat().st_mtime_ns
        except OSError:
            mtime = -1
        key = (stamp_text, mtime)
        if key >= latest_key:
            latest_payload = payload
            latest_key = key
    return latest_payload


def _stage_output_image_count(
    paths: StatePaths,
    *,
    stage_name: str,
    stream_id: str,
    read_payload: Callable[[Path], dict[str, Any] | None],
) -> int | None:
    stage_root = paths.stages / stage_name / stream_id
    if not stage_root.exists():
        return None

    try:
        batch_dirs = sorted(stage_root.iterdir(), key=lambda path: path.name)
    except OSError:
        return None

    total = 0
    found = False
    for batch_dir in batch_dirs:
        if not batch_dir.is_dir():
            continue
        latest = _latest_checkpoint_in_batch_dir(batch_dir, read_payload=read_payload)
        if latest is None:
            continue
        status = latest.get("status")
        if not isinstance(status, str) or status.upper() != "SUCCEEDED":
            continue
        total += max(0, _as_int(latest.get("record_count")))
        found = True

    return total if found else None


def _build_stage_image_stats(
    paths: StatePaths,
    *,
    job: JobSummary,
    stage_names: list[str],
    read_payload: Callable[[Path], dict[str, Any] | None],
) -> dict[str, StageImageStats]:
    input_images = job.frame_count
    expected_batches = max(0, job.batch_count or 0)
    stats: dict[str, StageImageStats] = {}

    for stage_name in stage_names:
        output_images: int | None = None
        if job.stream_id:
            output_images = _stage_output_image_count(
                paths,
                stage_name=stage_name,
                stream_id=job.stream_id,
                read_payload=read_payload,
            )
        if output_images is None:
            stage_output = (
                job.stage_outputs.get(stage_name)
                if isinstance(job.stage_outputs.get(stage_name), dict)
                else {}
            )
            output_images = _optional_int(stage_output.get("record_count"))

        if output_images is None:
            stage_stat = job.stage_stats.get(stage_name, {})
            completed = _as_int(stage_stat.get("completed"))
            skipped = _as_int(stage_stat.get("skipped"))
            failed = _as_int(stage_stat.get("failed"))
            stage_complete = (
                expected_batches == 0
                or (completed + skipped + failed) >= expected_batches
            )
            if (
                input_images is not None
                and stage_complete
                and failed <= 0
                and job.status in {"SUCCEEDED", "DONE"}
            ):
                output_images = input_images

        stats[stage_name] = StageImageStats(
            stage=stage_name,
            input_images=input_images,
            output_images=output_images,
        )

    return stats


def _read_metric_artifact_cached(
    artifact_path: Path,
    *,
    numeric_fields: tuple[str, ...],
    boolean_fields: tuple[str, ...],
    cache: dict[Path, _ArtifactMetricCacheEntry],
) -> _ArtifactMetricCacheEntry | None:
    try:
        stat = artifact_path.stat()
    except OSError:
        cache.pop(artifact_path, None)
        return None

    cached = cache.get(artifact_path)
    if (
        cached is not None
        and cached.mtime_ns == stat.st_mtime_ns
        and cached.size == stat.st_size
        and cached.numeric_fields == numeric_fields
        and cached.boolean_fields == boolean_fields
    ):
        return cached

    numeric_sums = {field: 0.0 for field in numeric_fields}
    numeric_counts = {field: 0 for field in numeric_fields}
    true_counts = {field: 0 for field in boolean_fields}
    boolean_counts = {field: 0 for field in boolean_fields}
    record_count = 0

    try:
        for record in iter_jsonl_artifact(artifact_path):
            record_count += 1
            for field in numeric_fields:
                value = record.get(field)
                if isinstance(value, bool):
                    continue
                if isinstance(value, (int, float)):
                    numeric_sums[field] += float(value)
                    numeric_counts[field] += 1
            for field in boolean_fields:
                value = record.get(field)
                if isinstance(value, bool):
                    boolean_counts[field] += 1
                    if value:
                        true_counts[field] += 1
                    continue
                if isinstance(value, (int, float)):
                    boolean_counts[field] += 1
                    if bool(value):
                        true_counts[field] += 1
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None

    entry = _ArtifactMetricCacheEntry(
        mtime_ns=stat.st_mtime_ns,
        size=stat.st_size,
        numeric_fields=numeric_fields,
        boolean_fields=boolean_fields,
        record_count=record_count,
        numeric_sums=numeric_sums,
        numeric_counts=numeric_counts,
        true_counts=true_counts,
        boolean_counts=boolean_counts,
    )
    cache[artifact_path] = entry
    return entry


def _build_stage_metric_stats(
    paths: StatePaths,
    *,
    job: JobSummary,
    stage_names: list[str],
    read_payload: Callable[[Path], dict[str, Any] | None],
    artifact_cache: dict[Path, _ArtifactMetricCacheEntry],
) -> dict[str, StageMetricStats]:
    if not job.stream_id:
        return {}

    stats: dict[str, StageMetricStats] = {}

    for stage_name in stage_names:
        numeric_fields = _STAGE_NUMERIC_METRICS.get(stage_name, ())
        boolean_fields = _STAGE_BOOLEAN_METRICS.get(stage_name, ())
        if not numeric_fields and not boolean_fields:
            continue

        stage_root = paths.stages / stage_name / job.stream_id
        if not stage_root.exists():
            continue

        try:
            batch_dirs = sorted(stage_root.iterdir(), key=lambda path: path.name)
        except OSError:
            continue

        stage_record_count = 0
        numeric_sums = {field: 0.0 for field in numeric_fields}
        numeric_counts = {field: 0 for field in numeric_fields}
        true_counts = {field: 0 for field in boolean_fields}
        boolean_counts = {field: 0 for field in boolean_fields}
        seen_artifacts: set[Path] = set()

        for batch_dir in batch_dirs:
            if not batch_dir.is_dir():
                continue

            latest = _latest_checkpoint_in_batch_dir(
                batch_dir, read_payload=read_payload
            )
            if latest is None:
                continue
            status = latest.get("status")
            if not isinstance(status, str) or status.upper() != "SUCCEEDED":
                continue

            artifact = latest.get("artifact")
            uri = artifact.get("uri") if isinstance(artifact, dict) else None
            if not isinstance(uri, str) or not uri:
                continue

            artifact_path = Path(uri)
            if artifact_path in seen_artifacts:
                continue
            seen_artifacts.add(artifact_path)

            artifact_stats = _read_metric_artifact_cached(
                artifact_path,
                numeric_fields=numeric_fields,
                boolean_fields=boolean_fields,
                cache=artifact_cache,
            )
            if artifact_stats is None:
                continue

            stage_record_count += artifact_stats.record_count
            for field in numeric_fields:
                numeric_sums[field] += artifact_stats.numeric_sums.get(field, 0.0)
                numeric_counts[field] += artifact_stats.numeric_counts.get(field, 0)
            for field in boolean_fields:
                true_counts[field] += artifact_stats.true_counts.get(field, 0)
                boolean_counts[field] += artifact_stats.boolean_counts.get(field, 0)

        mean_values = {
            field: (numeric_sums[field] / count)
            for field, count in numeric_counts.items()
            if count > 0
        }
        true_ratios = {
            field: (true_counts[field] / count)
            for field, count in boolean_counts.items()
            if count > 0
        }

        if stage_record_count <= 0 and not mean_values and not true_ratios:
            continue

        stats[stage_name] = StageMetricStats(
            stage=stage_name,
            record_count=stage_record_count,
            mean_values=mean_values,
            true_ratios=true_ratios,
        )

    return stats


def _format_stage_metric_line(stage: str, stats: StageMetricStats) -> str:
    parts: list[str] = []
    for field in _STAGE_NUMERIC_METRICS.get(stage, ()):
        value = stats.mean_values.get(field)
        if value is None:
            continue
        label = _METRIC_LABELS.get(field, field)
        parts.append(f"{label}={value:.2f}")
    for field in _STAGE_BOOLEAN_METRICS.get(stage, ()):
        ratio = stats.true_ratios.get(field)
        if ratio is None:
            continue
        label = _METRIC_LABELS.get(field, field)
        parts.append(f"{label}={ratio * 100.0:.1f}%")
    parts.append(f"n={stats.record_count}")
    return f"  {stage}: {', '.join(parts)}"


def _collect_batch_statuses(
    paths: StatePaths,
    *,
    job: JobSummary,
    stage_names: list[str],
    read_payload: Callable[[Path], dict[str, Any] | None],
) -> list[BatchStatus]:
    if not job.stream_id:
        return []

    batch_stage_map: dict[str, dict[str, dict[str, Any]]] = {}
    for stage_name in stage_names:
        stage_root = paths.stages / stage_name / job.stream_id
        if not stage_root.exists():
            continue
        try:
            batch_dirs = sorted(stage_root.iterdir(), key=lambda path: path.name)
        except OSError:
            continue
        for batch_dir in batch_dirs:
            if not batch_dir.is_dir():
                continue
            latest = _latest_checkpoint_in_batch_dir(
                batch_dir, read_payload=read_payload
            )
            if latest is None:
                continue
            batch_stage_map.setdefault(batch_dir.name, {})[stage_name] = latest

    if not batch_stage_map:
        return []

    batches: list[BatchStatus] = []
    total_stages = len(stage_names)
    for batch_name, per_stage in batch_stage_map.items():
        parsed = _parse_batch_range(batch_name)
        if parsed is None:
            continue
        start_idx, end_idx = parsed

        completed_stages = 0
        has_failed = False
        has_running = False
        active_stage: str | None = None
        worker_pid: int | None = None
        updated_at: str | None = None

        for stage_name in stage_names:
            payload = per_stage.get(stage_name)
            status = "PENDING"
            if isinstance(payload, dict):
                raw_status = payload.get("status")
                if isinstance(raw_status, str) and raw_status:
                    status = raw_status.upper()
                stamp = payload.get("finished_at") or payload.get("started_at")
                if isinstance(stamp, str) and (
                    updated_at is None or stamp > updated_at
                ):
                    updated_at = stamp
                if worker_pid is None:
                    worker_pid = _optional_int(payload.get("worker_pid"))

            if status in {"SUCCEEDED", "SKIPPED"}:
                completed_stages += 1
            elif status == "FAILED":
                has_failed = True
            elif status == "RUNNING":
                has_running = True
                if active_stage is None:
                    active_stage = stage_name

        if has_failed:
            status_value = "FAILED"
        elif total_stages > 0 and completed_stages >= total_stages:
            status_value = "SUCCEEDED"
        elif has_running:
            status_value = "RUNNING"
        elif completed_stages > 0:
            status_value = "PARTIAL"
        else:
            status_value = "PENDING"

        batches.append(
            BatchStatus(
                range_label=f"{start_idx}-{end_idx}",
                start_idx=start_idx,
                end_idx=end_idx,
                status=status_value,
                active_stage=active_stage,
                completed_stages=completed_stages,
                total_stages=total_stages,
                worker_pid=worker_pid,
                updated_at=updated_at,
            )
        )

    batches.sort(key=lambda batch: batch.start_idx)
    return batches


def _probe_process(pid: int, *, role: str) -> WorkerProcess | None:
    cmd = [
        "ps",
        "-p",
        str(pid),
        "-o",
        "pid=,ppid=,stat=,%cpu=,rss=,etime=,command=",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        return None
    parts = lines[-1].strip().split(None, 6)
    if len(parts) < 7:
        return None

    parsed_pid = _optional_int(parts[0])
    if parsed_pid is None:
        return None
    return WorkerProcess(
        role=role,
        pid=parsed_pid,
        ppid=_optional_int(parts[1]),
        state=parts[2],
        cpu_percent=parts[3],
        rss_kib=_optional_int(parts[4]),
        elapsed=parts[5],
        command=parts[6],
    )


def _process_tree() -> dict[int, list[int]]:
    try:
        proc = subprocess.run(
            ["ps", "-axo", "pid=,ppid="],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return {}
    if proc.returncode != 0:
        return {}

    tree: dict[int, list[int]] = {}
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        pid = _optional_int(parts[0])
        ppid = _optional_int(parts[1])
        if pid is None or ppid is None:
            continue
        tree.setdefault(ppid, []).append(pid)

    for children in tree.values():
        children.sort()
    return tree


def _descendant_pids(ppid: int) -> list[int]:
    tree = _process_tree()
    if not tree:
        return []

    descendants: list[int] = []
    queue = list(tree.get(ppid, []))
    seen: set[int] = set()
    while queue:
        pid = queue.pop(0)
        if pid in seen:
            continue
        seen.add(pid)
        descendants.append(pid)
        queue.extend(tree.get(pid, []))

    descendants.sort()
    return descendants


def _collect_worker_processes(gateway: dict[str, Any]) -> list[WorkerProcess]:
    pid = _optional_int(gateway.get("pid"))
    alive = bool(gateway.get("alive"))
    if not alive or pid is None:
        return []

    workers: list[WorkerProcess] = []
    gateway_proc = _probe_process(pid, role="gateway")
    if gateway_proc is not None:
        workers.append(gateway_proc)

    for child_pid in _descendant_pids(pid):
        worker = _probe_process(child_pid, role="worker")
        if worker is not None:
            workers.append(worker)

    workers.sort(key=lambda item: (0 if item.role == "gateway" else 1, item.pid))
    return workers

