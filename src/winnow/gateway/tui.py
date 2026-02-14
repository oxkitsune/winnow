"""Interactive Textual TUI for gateway and job monitoring."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Callable

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header, Static

from winnow.gateway.daemon import start_background, status as gateway_status, stop_background
from winnow.storage.atomic import read_json
from winnow.storage.events import iter_event_files
from winnow.storage.state_store import StatePaths, ensure_state_layout


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


def _latest_timestamp(job: "JobSummary") -> float:
    for candidate in (job.updated_at, job.finished_at, job.started_at, job.submitted_at):
        parsed = _parse_iso8601(candidate)
        if parsed is not None:
            return parsed.timestamp()
    return 0.0


def _is_active_job(job: "JobSummary") -> bool:
    return job.status in {"PENDING", "RUNNING"} or job.queue_lane in {"pending", "running"}


def _iter_json_files(directory: Path) -> list[Path]:
    try:
        files = [path for path in directory.iterdir() if path.is_file() and path.suffix == ".json"]
    except OSError:
        return []
    files.sort()
    return files


@dataclass(slots=True)
class JobSummary:
    """Flattened job metadata used by the dashboard."""

    job_id: str
    status: str
    mode: str
    queue_lane: str
    input_path: str | None
    frame_count: int | None
    batch_count: int | None
    max_workers: int | None
    submitted_at: str | None
    updated_at: str | None
    started_at: str | None
    finished_at: str | None
    stream_id: str | None
    stage_stats: dict[str, dict[str, int]]
    stage_outputs: dict[str, Any]
    config: dict[str, Any]
    error: str | None


@dataclass(slots=True)
class StageProgress:
    """Best-effort stage progress for one selected job."""

    stage: str
    expected_batches: int
    succeeded_batches: int
    running_batches: int
    failed_batches: int
    skipped_batches: int


@dataclass(slots=True)
class BatchStatus:
    """Per-batch status snapshot for one job."""

    range_label: str
    start_idx: int
    end_idx: int
    status: str
    active_stage: str | None
    completed_stages: int
    total_stages: int
    worker_pid: int | None
    updated_at: str | None


@dataclass(slots=True)
class WorkerProcess:
    """Best-effort process info for gateway and workers."""

    role: str
    pid: int
    ppid: int | None
    state: str
    cpu_percent: str
    rss_kib: int | None
    elapsed: str
    command: str


@dataclass(slots=True)
class JobDetailSnapshot:
    """Cached detail view payload."""

    job_id: str
    stage_progress: list[StageProgress]
    batches: list[BatchStatus]
    workers: list[WorkerProcess]
    events: list[dict[str, Any]]
    generated_at: float


@dataclass(slots=True)
class _JsonCacheEntry:
    mtime_ns: int
    size: int
    payload: dict[str, Any] | None


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
        submitted_at=payload.get("submitted_at") if isinstance(payload.get("submitted_at"), str) else None,
        updated_at=payload.get("updated_at") if isinstance(payload.get("updated_at"), str) else None,
        started_at=payload.get("started_at") if isinstance(payload.get("started_at"), str) else None,
        finished_at=payload.get("finished_at") if isinstance(payload.get("finished_at"), str) else None,
        stream_id=payload.get("stream_id") if isinstance(payload.get("stream_id"), str) else None,
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

        job_id = payload.get("job_id") if isinstance(payload.get("job_id"), str) else job_path.stem
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


def _read_events_file(path: Path, *, sink: deque[dict[str, Any]], job_id: str | None = None) -> None:
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


def _stage_order(config: dict[str, Any], stage_stats: dict[str, dict[str, int]]) -> list[str]:
    stages = ["blur", "darkness"]

    duplicate_cfg = config.get("duplicate") if isinstance(config.get("duplicate"), dict) else {}
    idle_cfg = config.get("idle") if isinstance(config.get("idle"), dict) else {}
    annotation_cfg = config.get("annotation") if isinstance(config.get("annotation"), dict) else {}

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
            latest = _latest_checkpoint_in_batch_dir(batch_dir, read_payload=read_payload)
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
                if isinstance(stamp, str) and (updated_at is None or stamp > updated_at):
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


def _child_pids(ppid: int) -> list[int]:
    try:
        proc = subprocess.run(
            ["pgrep", "-P", str(ppid)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return []
    if proc.returncode not in (0, 1):
        return []
    children: list[int] = []
    for raw in proc.stdout.splitlines():
        candidate = _optional_int(raw.strip())
        if candidate is not None:
            children.append(candidate)
    return sorted(set(children))


def _collect_worker_processes(gateway: dict[str, Any]) -> list[WorkerProcess]:
    pid = _optional_int(gateway.get("pid"))
    alive = bool(gateway.get("alive"))
    if not alive or pid is None:
        return []

    workers: list[WorkerProcess] = []
    gateway_proc = _probe_process(pid, role="gateway")
    if gateway_proc is not None:
        workers.append(gateway_proc)

    for child_pid in _child_pids(pid):
        worker = _probe_process(child_pid, role="worker")
        if worker is not None:
            workers.append(worker)

    workers.sort(key=lambda item: (0 if item.role == "gateway" else 1, item.pid))
    return workers


class GatewayTextualApp(App[None]):
    """Textual application for observing gateway and jobs."""

    CSS = """
    Screen {
        layout: vertical;
    }

    #status_line {
        height: 1;
        padding: 0 1;
        color: $text-muted;
        background: $surface;
    }

    #main_panel {
        height: 1fr;
        margin: 0 1 1 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("enter", "open_selected", "Open Job"),
        Binding("escape", "back_to_jobs", "Back"),
        Binding("backspace", "back_to_jobs", "Back", show=False),
        Binding("s", "toggle_gateway", "Start/Stop Gateway"),
        Binding("a", "toggle_jobs_scope", "Active/All Jobs"),
        Binding("e", "toggle_events_scope", "Event Scope"),
        Binding("r", "manual_refresh", "Refresh"),
        Binding("up", "select_prev", "Prev Job", show=False),
        Binding("down", "select_next", "Next Job", show=False),
        Binding("k", "select_prev", "Prev Job", show=False),
        Binding("j", "select_next", "Next Job", show=False),
    ]

    def __init__(
        self,
        *,
        state_root: Path,
        refresh_interval: float,
        daemon_poll_interval: float,
        log_file: Path | None,
        show_all_jobs: bool,
        events_limit: int,
    ) -> None:
        super().__init__()
        self.paths = ensure_state_layout(state_root)
        self.refresh_interval = max(0.2, refresh_interval)
        self.daemon_poll_interval = max(0.05, daemon_poll_interval)
        self.log_file = log_file
        self.show_all_jobs = show_all_jobs
        self.events_limit = max(16, events_limit)
        self.events_scoped_to_selection = True

        self.view_mode = "jobs"
        self.jobs: list[JobSummary] = []
        self.gateway: dict[str, Any] = {}

        self.selected_idx = 0
        self.jobs_scroll = 0
        self.last_refresh = 0.0

        self.notice: str | None = None
        self.notice_until = 0.0

        self.status_line: Static | None = None
        self.main_panel: Static | None = None

        self._json_cache: dict[Path, _JsonCacheEntry] = {}
        self._json_cache_pruned_at = 0.0

        self._event_offsets: dict[Path, int] = {}
        self._events_buffer: deque[dict[str, Any]] = deque(maxlen=max(512, self.events_limit * 8))
        self._events_primed = False

        self._detail_snapshot: JobDetailSnapshot | None = None
        self._detail_job_id: str | None = None
        self._detail_refreshed_at = 0.0

        self._worker_process_cache: list[WorkerProcess] = []
        self._worker_process_refreshed_at = 0.0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="status_line")
        yield Static(id="main_panel")
        yield Footer()

    def on_mount(self) -> None:
        self.status_line = self.query_one("#status_line", Static)
        self.main_panel = self.query_one("#main_panel", Static)
        self._refresh(force=True)
        self.set_interval(self.refresh_interval, self._refresh)

    def _set_notice(self, message: str, *, ttl_sec: float = 3.0) -> None:
        self.notice = message
        self.notice_until = time.monotonic() + ttl_sec

    def _selected_job_id(self) -> str | None:
        if not self.jobs:
            return None
        idx = max(0, min(self.selected_idx, len(self.jobs) - 1))
        return self.jobs[idx].job_id

    def _selected_job(self) -> JobSummary | None:
        if not self.jobs:
            return None
        idx = max(0, min(self.selected_idx, len(self.jobs) - 1))
        return self.jobs[idx]

    def _read_json_cached(self, path: Path) -> dict[str, Any] | None:
        try:
            stat = path.stat()
        except OSError:
            self._json_cache.pop(path, None)
            return None

        cached = self._json_cache.get(path)
        if cached is not None and cached.mtime_ns == stat.st_mtime_ns and cached.size == stat.st_size:
            return cached.payload

        payload = _read_job_payload(path)
        self._json_cache[path] = _JsonCacheEntry(
            mtime_ns=stat.st_mtime_ns,
            size=stat.st_size,
            payload=payload,
        )
        return payload

    def _prune_json_cache(self) -> None:
        now = time.monotonic()
        if now - self._json_cache_pruned_at < 15.0:
            return
        self._json_cache_pruned_at = now
        for path in list(self._json_cache):
            if not path.exists():
                self._json_cache.pop(path, None)

    def _append_event_line(self, raw_line: str) -> None:
        line = raw_line.strip()
        if not line:
            return
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            return
        if isinstance(payload, dict):
            self._events_buffer.append(payload)

    def _prime_event_cache(self) -> None:
        files = list(iter_event_files(self.paths))
        if not files:
            self._events_primed = True
            return

        hot_files = set(files[-3:])
        for file_path in files:
            try:
                with file_path.open("r", encoding="utf-8") as handle:
                    if file_path in hot_files:
                        for raw_line in handle:
                            self._append_event_line(raw_line)
                    else:
                        handle.seek(0, 2)
                    self._event_offsets[file_path] = handle.tell()
            except OSError:
                continue

        self._events_primed = True

    def _update_event_cache(self) -> None:
        if not self._events_primed:
            self._prime_event_cache()
            return

        files = list(iter_event_files(self.paths))
        file_set = set(files)
        for path in list(self._event_offsets):
            if path not in file_set:
                self._event_offsets.pop(path, None)

        for path in files[:-3]:
            if path not in self._event_offsets:
                try:
                    self._event_offsets[path] = path.stat().st_size
                except OSError:
                    continue

        for file_path in files[-3:]:
            start_offset = self._event_offsets.get(file_path, 0)
            try:
                with file_path.open("r", encoding="utf-8") as handle:
                    handle.seek(0, 2)
                    file_size = handle.tell()
                    if start_offset > file_size:
                        start_offset = 0
                    handle.seek(start_offset)
                    for raw_line in handle:
                        self._append_event_line(raw_line)
                    self._event_offsets[file_path] = handle.tell()
            except OSError:
                continue

    def _recent_events(self, *, limit: int, job_id: str | None) -> list[dict[str, Any]]:
        if limit <= 0:
            return []

        events: list[dict[str, Any]] = []
        for event in reversed(self._events_buffer):
            if job_id is not None:
                corr = event.get("correlation_id")
                event_job = event.get("job_id")
                if event_job != job_id and corr != job_id:
                    continue
            events.append(event)
            if len(events) >= limit:
                break

        if events:
            events.reverse()
            return events

        return _load_recent_events(self.paths, limit=limit, job_id=job_id)

    def _refresh_detail_snapshot(self, *, force: bool = False) -> None:
        job = self._selected_job()
        if job is None:
            self._detail_snapshot = None
            self._detail_job_id = None
            return

        now = time.monotonic()
        active = _is_active_job(job)
        min_interval = 0.8 if active else 3.0
        should_refresh = (
            force
            or self._detail_snapshot is None
            or self._detail_job_id != job.job_id
            or (now - self._detail_refreshed_at) >= min_interval
        )
        if not should_refresh:
            return

        stage_progress = _build_stage_progress(
            self.paths,
            job,
            read_payload=self._read_json_cached,
        )
        stage_names = [row.stage for row in stage_progress]
        if not stage_names:
            stage_names = _stage_order(job.config, job.stage_stats)
        batches = _collect_batch_statuses(
            self.paths,
            job=job,
            stage_names=stage_names,
            read_payload=self._read_json_cached,
        )
        workers = self._worker_processes(force=force)
        selected_job_id = job.job_id if self.events_scoped_to_selection else None
        events = self._recent_events(limit=self.events_limit, job_id=selected_job_id)

        self._detail_snapshot = JobDetailSnapshot(
            job_id=job.job_id,
            stage_progress=stage_progress,
            batches=batches,
            workers=workers,
            events=events,
            generated_at=now,
        )
        self._detail_job_id = job.job_id
        self._detail_refreshed_at = now

    def _worker_processes(self, *, force: bool = False) -> list[WorkerProcess]:
        now = time.monotonic()
        if not force and (now - self._worker_process_refreshed_at) < 1.5:
            return self._worker_process_cache

        self._worker_process_cache = _collect_worker_processes(self.gateway)
        self._worker_process_refreshed_at = now
        return self._worker_process_cache

    def _refresh(self, *, force: bool = False) -> None:
        if not force and (time.monotonic() - self.last_refresh) < self.refresh_interval:
            return

        previous_job_id = self._selected_job_id()

        try:
            self._prune_json_cache()
            gateway = gateway_status(self.paths.root)
            all_jobs = _load_jobs(self.paths, read_payload=self._read_json_cached)

            if self.show_all_jobs:
                jobs = all_jobs
            else:
                jobs = [job for job in all_jobs if _is_active_job(job)]
                if not jobs:
                    jobs = all_jobs[:10]

            if previous_job_id is not None:
                for idx, job in enumerate(jobs):
                    if job.job_id == previous_job_id:
                        self.selected_idx = idx
                        break
            if jobs:
                self.selected_idx = max(0, min(self.selected_idx, len(jobs) - 1))
            else:
                self.selected_idx = 0
                self.jobs_scroll = 0
                if self.view_mode == "job_detail":
                    self.view_mode = "jobs"

            self.gateway = gateway
            self.jobs = jobs
            self._update_event_cache()
            self.last_refresh = time.monotonic()

            if self.view_mode == "job_detail":
                self._refresh_detail_snapshot(force=force)
        except Exception as exc:  # pragma: no cover
            self._set_notice(f"refresh failed: {type(exc).__name__}: {exc}", ttl_sec=4.0)

        self._render()

    def _jobs_visible_rows(self) -> int:
        if self.main_panel is None:
            return 14
        return max(8, self.main_panel.size.height - 10)

    def _detail_row_budgets(self) -> tuple[int, int, int, int]:
        if self.main_panel is None:
            return (5, 8, 4, 6)
        usable = max(12, self.main_panel.size.height - 17)
        stage_rows = max(3, min(9, usable // 5))
        worker_rows = max(2, min(8, usable // 6))
        batch_rows = max(4, min(24, usable // 2))
        event_rows = max(3, usable - stage_rows - worker_rows - batch_rows)
        if event_rows < 3:
            deficit = 3 - event_rows
            batch_rows = max(4, batch_rows - deficit)
            event_rows = 3
        return stage_rows, batch_rows, worker_rows, event_rows

    def _render(self) -> None:
        if self.status_line is None or self.main_panel is None:
            return

        self.status_line.update(self._render_status_line())
        self.main_panel.update(self._render_main_panel())

    def _render_status_line(self) -> Text:
        now = time.monotonic()
        if self.notice and now <= self.notice_until:
            return Text(self.notice)

        age = now - self.last_refresh if self.last_refresh > 0 else 0.0
        selected = self._selected_job_id()
        selected_text = selected[:8] if isinstance(selected, str) else "-"
        view = "jobs" if self.view_mode == "jobs" else "detail"

        alive = bool(self.gateway.get("alive"))
        pid = self.gateway.get("pid")
        queue = self.gateway.get("queue") if isinstance(self.gateway.get("queue"), dict) else {}
        return Text(
            f"view={view} selected={selected_text} refresh={self.refresh_interval:.1f}s "
            f"last={age:.1f}s | gateway={'RUNNING' if alive else 'STOPPED'} pid={pid or '-'} "
            f"| q={_as_int(queue.get('pending'))}/{_as_int(queue.get('running'))}/"
            f"{_as_int(queue.get('done'))}/{_as_int(queue.get('failed'))}"
        )

    def _render_main_panel(self) -> Panel:
        if self.view_mode == "job_detail":
            return self._render_job_detail_screen()
        return self._render_jobs_screen()

    def _render_jobs_screen(self) -> Panel:
        visible_rows = self._jobs_visible_rows()
        self.selected_idx = max(0, min(self.selected_idx, max(0, len(self.jobs) - 1)))

        if self.selected_idx < self.jobs_scroll:
            self.jobs_scroll = self.selected_idx
        if self.selected_idx >= self.jobs_scroll + visible_rows:
            self.jobs_scroll = self.selected_idx - visible_rows + 1
        self.jobs_scroll = max(0, min(self.jobs_scroll, max(0, len(self.jobs) - visible_rows)))

        alive = bool(self.gateway.get("alive"))
        pid = self.gateway.get("pid")
        heartbeat = self.gateway.get("heartbeat")
        queue = self.gateway.get("queue") if isinstance(self.gateway.get("queue"), dict) else {}
        metrics = self.gateway.get("metrics") if isinstance(self.gateway.get("metrics"), dict) else {}
        jobs_metrics = metrics.get("jobs") if isinstance(metrics.get("jobs"), dict) else {}

        table = Table(expand=True, show_header=True, pad_edge=False)
        table.add_column(" ", width=1)
        table.add_column("Status", width=9)
        table.add_column("Job", width=8)
        table.add_column("Q", width=7)
        table.add_column("Mode", width=6)
        table.add_column("Frames", width=7, justify="right")
        table.add_column("Batches", width=7, justify="right")
        table.add_column("Workers", width=7, justify="right")
        table.add_column("Upd", width=7, justify="right")
        table.add_column("Input")

        if not self.jobs:
            table.add_row("", "(none)", "-", "-", "-", "-", "-", "-", "-", "-")
        else:
            start = self.jobs_scroll
            end = min(len(self.jobs), start + visible_rows)
            for idx in range(start, end):
                job = self.jobs[idx]
                marker = ">" if idx == self.selected_idx else " "
                style = _status_style(job.status)
                table.add_row(
                    marker,
                    Text(job.status, style=style),
                    job.job_id[:8],
                    job.queue_lane,
                    job.mode,
                    str(job.frame_count) if job.frame_count is not None else "-",
                    str(job.batch_count) if job.batch_count is not None else "-",
                    str(job.max_workers) if job.max_workers is not None else "-",
                    _format_age(job.updated_at),
                    _clip(job.input_path or "-", 66),
                    style=("reverse" if idx == self.selected_idx else ""),
                )

        status_line = Text(
            f"Gateway {'RUNNING' if alive else 'STOPPED'} pid={pid if pid is not None else 'n/a'} "
            f"heartbeat={_format_age(heartbeat)}",
            style=("bold green" if alive else "bold red"),
        )
        queue_line = Text(
            f"Queue pending={_as_int(queue.get('pending'))} running={_as_int(queue.get('running'))} "
            f"done={_as_int(queue.get('done'))} failed={_as_int(queue.get('failed'))} "
            f"backlog={_as_int(self.gateway.get('backlog'))}"
        )
        metrics_line = Text(
            f"Jobs started={_as_int(jobs_metrics.get('started'))} "
            f"succeeded={_as_int(jobs_metrics.get('succeeded'))} "
            f"failed={_as_int(jobs_metrics.get('failed'))}"
        )
        controls_line = Text(
            "Enter open job | q quit | s start/stop gateway | a active/all | e event scope "
            "| r refresh | up/down or j/k move",
            style="dim",
        )

        body = Group(status_line, queue_line, metrics_line, controls_line, table)
        filter_name = "all" if self.show_all_jobs else "active"
        return Panel(body, title=f"Jobs ({len(self.jobs)} | {filter_name})", border_style="cyan")

    def _render_job_detail_screen(self) -> Panel:
        job = self._selected_job()
        if job is None:
            return Panel(Text("No selected job."), title="Job Detail", border_style="cyan")

        if self._detail_snapshot is None or self._detail_job_id != job.job_id:
            self._refresh_detail_snapshot(force=True)
        snapshot = self._detail_snapshot
        if snapshot is None:
            return Panel(Text("Loading job details..."), title="Job Detail", border_style="cyan")

        stage_rows, batch_rows, worker_rows, event_rows = self._detail_row_budgets()
        width_budget = 120
        if self.main_panel is not None:
            width_budget = max(64, self.main_panel.size.width - 8)

        running_batches = sum(1 for batch in snapshot.batches if batch.status == "RUNNING")
        worker_pids = sorted(
            {batch.worker_pid for batch in snapshot.batches if batch.worker_pid is not None}
        )
        worker_pid_text = ",".join(str(pid) for pid in worker_pids[:8]) if worker_pids else "-"
        if len(worker_pids) > 8:
            worker_pid_text = f"{worker_pid_text},..."

        header_lines: list[Text] = [
            Text("Esc to return to job list.", style="dim"),
            Text(f"job={job.job_id}", style="bold"),
            Text(
                f"status={job.status} queue={job.queue_lane} mode={job.mode} "
                f"stream={job.stream_id or '-'}",
                style=_status_style(job.status),
            ),
            Text(
                f"frames={job.frame_count if job.frame_count is not None else '-'} "
                f"batches={job.batch_count if job.batch_count is not None else '-'} "
                f"running_batches={running_batches} "
                f"max_workers={job.max_workers if job.max_workers is not None else '-'} "
                f"active_worker_pids={worker_pid_text}"
            ),
            Text(f"input={_clip(job.input_path or '-', width_budget)}"),
            Text(
                f"submitted={job.submitted_at or '-'} | started={job.started_at or '-'} | "
                f"updated={job.updated_at or '-'} | finished={job.finished_at or '-'}"
            ),
        ]

        stage_table = Table(expand=True, show_header=True, pad_edge=False)
        stage_table.add_column("Stage", width=12)
        stage_table.add_column("Done", width=18)
        stage_table.add_column("OK", width=5, justify="right")
        stage_table.add_column("Run", width=5, justify="right")
        stage_table.add_column("Fail", width=5, justify="right")
        stage_table.add_column("Skip", width=5, justify="right")
        stage_table.add_column("Status", width=9)

        if not snapshot.stage_progress:
            stage_table.add_row("-", "-", "-", "-", "-", "-", "-")
        else:
            for progress in snapshot.stage_progress[:stage_rows]:
                if progress.expected_batches > 0:
                    done = progress.succeeded_batches + progress.skipped_batches + progress.failed_batches
                    pct = (100.0 * done) / progress.expected_batches
                    done_text = f"{done}/{progress.expected_batches} ({pct:5.1f}%)"
                else:
                    done_text = "n/a"

                if progress.failed_batches > 0:
                    status = "FAILED"
                elif progress.running_batches > 0:
                    status = "RUNNING"
                elif progress.expected_batches > 0 and (
                    progress.succeeded_batches + progress.skipped_batches
                ) >= progress.expected_batches:
                    status = "SUCCEEDED"
                elif progress.succeeded_batches > 0 or progress.skipped_batches > 0:
                    status = "PARTIAL"
                else:
                    status = "PENDING"

                stage_table.add_row(
                    progress.stage,
                    done_text,
                    str(progress.succeeded_batches),
                    str(progress.running_batches),
                    str(progress.failed_batches),
                    str(progress.skipped_batches),
                    Text(status, style=_status_style(status)),
                )

        batch_table = Table(expand=True, show_header=True, pad_edge=False)
        batch_table.add_column("Batch", width=20)
        batch_table.add_column("Status", width=10)
        batch_table.add_column("Stage", width=14)
        batch_table.add_column("Progress", width=12)
        batch_table.add_column("Worker PID", width=11, justify="right")
        batch_table.add_column("Updated", width=8, justify="right")

        if not snapshot.batches:
            batch_table.add_row("-", "-", "-", "-", "-", "-")
        else:
            prioritized_batches = sorted(
                snapshot.batches,
                key=lambda batch: (
                    0 if batch.status == "RUNNING" else 1 if batch.status == "FAILED" else 2,
                    batch.start_idx,
                ),
            )
            for batch in prioritized_batches[:batch_rows]:
                batch_table.add_row(
                    batch.range_label,
                    Text(batch.status, style=_status_style(batch.status)),
                    batch.active_stage or "-",
                    f"{batch.completed_stages}/{batch.total_stages}",
                    str(batch.worker_pid) if batch.worker_pid is not None else "-",
                    _format_age(batch.updated_at),
                )

        worker_table = Table(expand=True, show_header=True, pad_edge=False)
        worker_table.add_column("Role", width=8)
        worker_table.add_column("PID", width=8, justify="right")
        worker_table.add_column("PPID", width=8, justify="right")
        worker_table.add_column("%CPU", width=6, justify="right")
        worker_table.add_column("RSS KiB", width=9, justify="right")
        worker_table.add_column("Elapsed", width=9, justify="right")
        worker_table.add_column("State", width=8)
        worker_table.add_column("Command")

        if not snapshot.workers:
            worker_table.add_row("-", "-", "-", "-", "-", "-", "-", "No gateway worker processes detected")
        else:
            for worker in snapshot.workers[:worker_rows]:
                worker_table.add_row(
                    worker.role,
                    str(worker.pid),
                    str(worker.ppid) if worker.ppid is not None else "-",
                    worker.cpu_percent,
                    str(worker.rss_kib) if worker.rss_kib is not None else "-",
                    worker.elapsed,
                    worker.state,
                    _clip(worker.command, width_budget),
                )

        events_table = Table(expand=True, show_header=True, pad_edge=False)
        events_table.add_column("Time", width=8)
        events_table.add_column("Job", width=8)
        events_table.add_column("Event", width=22)
        events_table.add_column("Details")

        scoped_events = snapshot.events[-event_rows:]
        if not scoped_events:
            events_table.add_row("-", "-", "-", "No events available for current scope")
        else:
            for event in scoped_events:
                ts = _format_ts_short(event.get("ts") if isinstance(event.get("ts"), str) else None)
                job_id = str(event.get("job_id", "-"))[:8]
                event_name = str(event.get("event", "event"))

                details: list[str] = []
                if isinstance(event.get("status"), str):
                    details.append(f"status={event['status']}")
                if isinstance(event.get("stage"), str):
                    details.append(f"stage={event['stage']}")
                if "record_count" in event:
                    details.append(f"records={event['record_count']}")

                events_table.add_row(ts, job_id, event_name, _clip(" ".join(details), width_budget))

        sections: list[Any] = []
        sections.extend(header_lines)
        sections.append(Text("Stage Progress", style="bold"))
        sections.append(stage_table)
        if len(snapshot.stage_progress) > stage_rows:
            sections.append(Text(f"showing {stage_rows}/{len(snapshot.stage_progress)} stages", style="dim"))

        sections.append(Text("Batches", style="bold"))
        sections.append(batch_table)
        if len(snapshot.batches) > batch_rows:
            sections.append(Text(f"showing {batch_rows}/{len(snapshot.batches)} batches", style="dim"))

        sections.append(Text("Worker Processes", style="bold"))
        sections.append(worker_table)
        if len(snapshot.workers) > worker_rows:
            sections.append(Text(f"showing {worker_rows}/{len(snapshot.workers)} processes", style="dim"))

        scope = "selected job" if self.events_scoped_to_selection else "all jobs"
        sections.append(Text(f"Recent Events ({scope})", style="bold"))
        sections.append(events_table)
        if len(snapshot.events) > event_rows:
            sections.append(Text(f"showing {event_rows}/{len(snapshot.events)} events", style="dim"))

        return Panel(
            Group(*sections),
            title=f"Job Detail ({job.job_id[:12]})",
            border_style="cyan",
        )

    def action_open_selected(self) -> None:
        if not self.jobs:
            return
        self.view_mode = "job_detail"
        self._refresh_detail_snapshot(force=True)
        self._render()

    def action_back_to_jobs(self) -> None:
        if self.view_mode == "jobs":
            return
        self.view_mode = "jobs"
        self._set_notice("job list")
        self._render()

    def action_select_prev(self) -> None:
        if not self.jobs:
            return
        self.selected_idx = max(0, self.selected_idx - 1)
        if self.view_mode == "job_detail":
            self._refresh_detail_snapshot(force=True)
        self._render()

    def action_select_next(self) -> None:
        if not self.jobs:
            return
        self.selected_idx = min(len(self.jobs) - 1, self.selected_idx + 1)
        if self.view_mode == "job_detail":
            self._refresh_detail_snapshot(force=True)
        self._render()

    def action_manual_refresh(self) -> None:
        self._set_notice("refreshed")
        self._refresh(force=True)

    def action_toggle_jobs_scope(self) -> None:
        self.show_all_jobs = not self.show_all_jobs
        label = "all jobs" if self.show_all_jobs else "active jobs"
        self._set_notice(f"view: {label}")
        self._refresh(force=True)

    def action_toggle_events_scope(self) -> None:
        self.events_scoped_to_selection = not self.events_scoped_to_selection
        label = "selected job" if self.events_scoped_to_selection else "all jobs"
        self._set_notice(f"events: {label}")
        self._refresh(force=True)

    def action_toggle_gateway(self) -> None:
        try:
            current = gateway_status(self.paths.root)
            if bool(current.get("alive")):
                stopped = stop_background(self.paths.root)
                if stopped:
                    self._set_notice("gateway stop signal sent")
                else:
                    self._set_notice("gateway was not running")
            else:
                pid = start_background(
                    state_root=self.paths.root,
                    poll_interval=self.daemon_poll_interval,
                    log_file=self.log_file,
                )
                self._set_notice(f"gateway started pid={pid}")
            self._refresh(force=True)
        except Exception as exc:  # pragma: no cover
            self._set_notice(f"gateway action failed: {type(exc).__name__}: {exc}", ttl_sec=4.0)
            self._refresh(force=True)


def run_gateway_tui(
    *,
    state_root: Path,
    refresh_interval: float,
    poll_interval: float,
    log_file: Path | None,
    show_all_jobs: bool,
    events_limit: int,
) -> None:
    """Run the gateway monitoring TUI."""

    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise SystemExit("winnow gateway tui requires an interactive terminal")

    app = GatewayTextualApp(
        state_root=state_root,
        refresh_interval=refresh_interval,
        daemon_poll_interval=poll_interval,
        log_file=log_file,
        show_all_jobs=show_all_jobs,
        events_limit=events_limit,
    )
    app.run()
