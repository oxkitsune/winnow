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

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Static,
    TabbedContent,
    TabPane,
)

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


@dataclass(slots=True)
class FilterState:
    status: str = "Active"
    lane: str = "All"
    mode: str = "All"
    time_window: str = "24h"
    warn_or_error_only: bool = False
    search: str = ""

    def clone(self) -> "FilterState":
        return FilterState(
            status=self.status,
            lane=self.lane,
            mode=self.mode,
            time_window=self.time_window,
            warn_or_error_only=self.warn_or_error_only,
            search=self.search,
        )


@dataclass(slots=True)
class SortState:
    column: str = "updated"
    reverse: bool = True


class PromptScreen(ModalScreen[str | None]):
    """Simple one-line input modal."""

    CSS = """
    PromptScreen {
        align: center middle;
    }
    #prompt_box {
        width: 70;
        padding: 1 2;
        border: solid $accent;
        background: $surface;
    }
    #prompt_actions {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, *, title: str, placeholder: str = "") -> None:
        super().__init__()
        self.title = title
        self.placeholder = placeholder

    def compose(self) -> ComposeResult:
        with Vertical(id="prompt_box"):
            yield Static(self.title)
            yield Input(placeholder=self.placeholder, id="prompt_input")
            with Horizontal(id="prompt_actions"):
                yield Button("Save", variant="primary", id="prompt_ok")
                yield Button("Cancel", id="prompt_cancel")

    def on_mount(self) -> None:
        self.query_one("#prompt_input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "prompt_ok":
            value = self.query_one("#prompt_input", Input).value.strip()
            self.dismiss(value or None)
            return
        self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value.strip() or None)


class ConfirmScreen(ModalScreen[bool]):
    """Simple yes/no confirm modal."""

    CSS = """
    ConfirmScreen {
        align: center middle;
    }
    #confirm_box {
        width: 72;
        padding: 1 2;
        border: solid $warning;
        background: $surface;
    }
    #confirm_actions {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, *, message: str, ok_label: str = "Confirm") -> None:
        super().__init__()
        self.message = message
        self.ok_label = ok_label

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm_box"):
            yield Static(self.message)
            with Horizontal(id="confirm_actions"):
                yield Button(self.ok_label, variant="warning", id="confirm_ok")
                yield Button("Cancel", id="confirm_cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm_ok")


class GatewayActionScreen(ModalScreen[str | None]):
    """Daemon action picker modal."""

    CSS = """
    GatewayActionScreen {
        align: center middle;
    }
    #action_box {
        width: 62;
        padding: 1 2;
        border: solid $accent;
        background: $surface;
    }
    #action_buttons {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, *, running: bool) -> None:
        super().__init__()
        self.running = running

    def compose(self) -> ComposeResult:
        label = "Gateway is running." if self.running else "Gateway is stopped."
        with Vertical(id="action_box"):
            yield Static(f"{label} Choose an action:")
            with Horizontal(id="action_buttons"):
                yield Button("Start", variant="success", id="action_start")
                yield Button("Stop", variant="error", id="action_stop")
                yield Button("Restart", variant="warning", id="action_restart")
                yield Button("Cancel", id="action_cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        choice = event.button.id or ""
        if choice == "action_cancel":
            self.dismiss(None)
            return
        self.dismiss(choice.removeprefix("action_"))


class GatewayTextualApp(App[None]):
    """Textual application for observing gateway and jobs."""

    CSS = """
    Screen {
        layout: vertical;
    }

    #header_bar {
        height: 1;
        padding: 0 1;
        color: $text-muted;
        background: $surface-darken-1;
    }

    #main_shell {
        height: 1fr;
        margin: 0 1;
    }

    #sidebar {
        width: 34;
        min-width: 28;
        border: round $panel;
        padding: 0 1;
    }

    #center_shell {
        width: 1fr;
        margin-left: 1;
    }

    #jobs_table {
        height: 14;
        border: round $panel;
    }

    #detail_tabs {
        height: 1fr;
        border: round $panel;
        margin-top: 1;
    }

    #bottom_shell {
        height: 12;
        margin: 0 1 0 1;
        border: round $panel;
        padding: 0 1;
    }

    #activity_table {
        height: 1fr;
    }

    #context_hints {
        height: 1;
        padding: 0 1;
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("?", "show_help", "Help"),
        Binding("ctrl+p", "command_palette", "Palette"),
        Binding("r", "manual_refresh", "Refresh"),
        Binding("/", "focus_search", "Search"),
        Binding("tab", "focus_next_panel", "Next Panel"),
        Binding("shift+tab", "focus_prev_panel", "Prev Panel"),
        Binding("up", "select_prev", "Prev", show=False),
        Binding("down", "select_next", "Next", show=False),
        Binding("k", "select_prev", "Prev", show=False),
        Binding("j", "select_next", "Next", show=False),
        Binding("g", "go_top", "Top", show=False),
        Binding("G", "go_bottom", "Bottom", show=False),
        Binding("enter", "default_action", "Action"),
        Binding("a", "cycle_status_filter", "Status"),
        Binding("l", "cycle_lane_filter", "Lane"),
        Binding("e", "toggle_events_scope", "Evt Scope"),
        Binding("f", "toggle_follow_events", "Follow"),
        Binding("F", "toggle_freeze_updates", "Freeze"),
        Binding("s", "daemon_control", "Daemon"),
        Binding("ctrl+s", "save_view", "Save View"),
        Binding("d", "delete_view", "Delete View"),
        Binding("[", "prev_tab", "Prev Tab"),
        Binding("]", "next_tab", "Next Tab"),
        Binding("n", "next_error", "Next Error"),
        Binding("p", "prev_error", "Prev Error"),
        Binding("1", "sort_col_1", "Sort 1", show=False),
        Binding("2", "sort_col_2", "Sort 2", show=False),
        Binding("3", "sort_col_3", "Sort 3", show=False),
        Binding("4", "sort_col_4", "Sort 4", show=False),
        Binding("5", "sort_col_5", "Sort 5", show=False),
        Binding("6", "sort_col_6", "Sort 6", show=False),
        Binding("7", "sort_col_7", "Sort 7", show=False),
        Binding("8", "sort_col_8", "Sort 8", show=False),
        Binding("!", "sort_col_1_rev", "Sort 1 Rev", show=False),
        Binding("@", "sort_col_2_rev", "Sort 2 Rev", show=False),
        Binding("#", "sort_col_3_rev", "Sort 3 Rev", show=False),
        Binding("$", "sort_col_4_rev", "Sort 4 Rev", show=False),
        Binding("%", "sort_col_5_rev", "Sort 5 Rev", show=False),
        Binding("^", "sort_col_6_rev", "Sort 6 Rev", show=False),
        Binding("&", "sort_col_7_rev", "Sort 7 Rev", show=False),
        Binding("*", "sort_col_8_rev", "Sort 8 Rev", show=False),
    ]

    SORT_COLUMNS: dict[int, str] = {
        1: "status",
        2: "job_id",
        3: "lane",
        4: "progress",
        5: "workers",
        6: "updated",
        7: "event",
        8: "input",
    }

    FOCUS_AREAS = ("filters", "jobs", "detail", "activity")

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
        self.events_limit = max(16, events_limit)
        self.gateway: dict[str, Any] = {}
        self.jobs: list[JobSummary] = []
        self.filtered_jobs: list[JobSummary] = []

        self.filters = FilterState(status="All" if show_all_jobs else "Active")
        self.sort_state = SortState()
        self.events_mode = "job"
        self.follow_events = True
        self.freeze_updates = False
        self.focus_idx = 1
        self.selected_job_id: str | None = None
        self.ghost_selection: str | None = None
        self._last_notice: str | None = None

        self.last_refresh = 0.0
        self._disconnected = False
        self._event_error_idx = -1

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
        self._seen_event_ts_by_job: dict[str, float] = {}
        self._activity_job_ids: list[str] = []

        self.saved_views: dict[str, FilterState] = {
            "Active": FilterState(status="Active"),
            "Failures": FilterState(status="Failed", time_window="24h"),
            "Recently Updated": FilterState(status="All", time_window="1h"),
        }
        self.saved_view_names: list[str] = list(self.saved_views)
        self.saved_view_idx = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="header_bar")
        with Horizontal(id="main_shell"):
            with Vertical(id="sidebar"):
                yield Static("[b]Filters[/b]")
                yield Input(placeholder="Search jobs, paths, lanes...", id="search_box")
                yield Static(id="filters_summary")
                yield Static(id="saved_views")
            with Vertical(id="center_shell"):
                yield DataTable(id="jobs_table")
                with TabbedContent(id="detail_tabs", initial="tab-overview"):
                    with TabPane("Overview", id="tab-overview"):
                        yield Static(id="overview_pane")
                    with TabPane("Stages", id="tab-stages"):
                        yield DataTable(id="stages_table")
                    with TabPane("Batches", id="tab-batches"):
                        yield DataTable(id="batches_table")
                    with TabPane("Workers", id="tab-workers"):
                        yield DataTable(id="workers_table")
                    with TabPane("Events", id="tab-events"):
                        yield Static(id="events_filter_line")
                        yield DataTable(id="detail_events_table")
                    with TabPane("Files", id="tab-files"):
                        yield DataTable(id="files_table")
        with Vertical(id="bottom_shell"):
            yield Static(id="activity_filters")
            yield DataTable(id="activity_table")
        yield Static(id="context_hints")
        yield Footer()

    def on_mount(self) -> None:
        self._configure_tables()
        self._set_focus_area("jobs")
        self._refresh(force=True)
        self.set_interval(self.refresh_interval, self._refresh)

    def _configure_tables(self) -> None:
        jobs = self.query_one("#jobs_table", DataTable)
        jobs.cursor_type = "row"
        jobs.add_columns("S", "Status", "Job", "Lane", "Progress", "Workers", "Upd", "Event", "Input")

        stages = self.query_one("#stages_table", DataTable)
        stages.cursor_type = "row"
        stages.add_columns("Stage", "Done", "OK", "Run", "Fail", "Skip", "Status")

        batches = self.query_one("#batches_table", DataTable)
        batches.cursor_type = "row"
        batches.add_columns("Batch", "Status", "Stage", "Progress", "Worker", "Updated")

        workers = self.query_one("#workers_table", DataTable)
        workers.cursor_type = "row"
        workers.add_columns("Role", "PID", "PPID", "%CPU", "RSS", "Elapsed", "State", "Command")

        detail_events = self.query_one("#detail_events_table", DataTable)
        detail_events.cursor_type = "row"
        detail_events.add_columns("Time", "Sev", "Job", "Event", "Details")

        files = self.query_one("#files_table", DataTable)
        files.cursor_type = "row"
        files.add_columns("Type", "Path")

        activity = self.query_one("#activity_table", DataTable)
        activity.cursor_type = "row"
        activity.add_columns("Time", "Sev", "Job", "Message")

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

    def _selected_job(self) -> JobSummary | None:
        if not self.filtered_jobs:
            return None
        if self.selected_job_id is None:
            return self.filtered_jobs[0]
        for job in self.filtered_jobs:
            if job.job_id == self.selected_job_id:
                return job
        return self.filtered_jobs[0]

    def _window_seconds(self) -> int | None:
        if self.filters.time_window == "15m":
            return 15 * 60
        if self.filters.time_window == "1h":
            return 60 * 60
        if self.filters.time_window == "24h":
            return 24 * 60 * 60
        return None

    def _event_severity(self, event: dict[str, Any]) -> str:
        value = str(event.get("level") or event.get("severity") or event.get("status") or "INFO").upper()
        if "ERROR" in value or "FAIL" in value:
            return "ERROR"
        if "WARN" in value:
            return "WARN"
        return "INFO"

    def _latest_event_for_job(self, job_id: str) -> dict[str, Any] | None:
        for event in reversed(self._events_buffer):
            event_job = event.get("job_id")
            corr = event.get("correlation_id")
            if event_job == job_id or corr == job_id:
                return event
        return None

    def _job_progress(self, job: JobSummary) -> tuple[int, int]:
        expected = max(0, job.batch_count or 0)
        if expected <= 0:
            return (0, 0)
        done = 0
        for stats in job.stage_stats.values():
            done = max(done, _as_int(stats.get("completed")) + _as_int(stats.get("skipped")))
        return (min(done, expected), expected)

    def _apply_filters_and_sort(self, jobs: list[JobSummary]) -> list[JobSummary]:
        window_seconds = self._window_seconds()
        now_ts = time.time()
        query = self.filters.search.lower().strip()
        lane = self.filters.lane.lower()
        mode = self.filters.mode.lower()

        filtered: list[JobSummary] = []
        for job in jobs:
            if self.filters.status == "Active" and not _is_active_job(job):
                continue
            if self.filters.status == "Queued" and job.queue_lane != "pending":
                continue
            if self.filters.status == "Completed" and job.status not in {"SUCCEEDED", "DONE"}:
                continue
            if self.filters.status == "Failed" and job.status != "FAILED":
                continue

            if lane != "all" and job.queue_lane.lower() != lane:
                continue
            if mode != "all" and job.mode.lower() != mode:
                continue

            if window_seconds is not None:
                stamp = _latest_timestamp(job)
                if stamp <= 0 or (now_ts - stamp) > window_seconds:
                    continue

            last_event = self._latest_event_for_job(job.job_id)
            if self.filters.warn_or_error_only:
                if last_event is None or self._event_severity(last_event) == "INFO":
                    continue

            if query:
                match = query in job.job_id.lower() or query in (job.input_path or "").lower()
                match = match or query in job.queue_lane.lower() or query in job.status.lower()
                if not match and last_event is not None:
                    event_text = f"{last_event.get('event', '')} {last_event.get('message', '')}".lower()
                    match = query in event_text
                if not match:
                    continue

            filtered.append(job)

        sort_col = self.sort_state.column

        def key(job: JobSummary) -> Any:
            if sort_col == "status":
                return job.status
            if sort_col == "job_id":
                return job.job_id
            if sort_col == "lane":
                return job.queue_lane
            if sort_col == "progress":
                done, total = self._job_progress(job)
                return (0 if total == 0 else done / total, done, total)
            if sort_col == "workers":
                return _as_int(job.max_workers)
            if sort_col == "event":
                event = self._latest_event_for_job(job.job_id)
                if event is None:
                    return ("", "")
                return (self._event_severity(event), str(event.get("event", "")))
            if sort_col == "input":
                return job.input_path or ""
            return _latest_timestamp(job)

        filtered.sort(key=key, reverse=self.sort_state.reverse)
        return filtered

    def _refresh_detail_snapshot(self, *, force: bool = False) -> None:
        job = self._selected_job()
        if job is None:
            self._detail_snapshot = None
            self._detail_job_id = None
            return

        now = time.monotonic()
        min_interval = 0.8 if _is_active_job(job) else 3.0
        if (
            not force
            and self._detail_snapshot is not None
            and self._detail_job_id == job.job_id
            and (now - self._detail_refreshed_at) < min_interval
        ):
            return

        stage_progress = _build_stage_progress(self.paths, job, read_payload=self._read_json_cached)
        stage_names = [row.stage for row in stage_progress] or _stage_order(job.config, job.stage_stats)
        batches = _collect_batch_statuses(
            self.paths,
            job=job,
            stage_names=stage_names,
            read_payload=self._read_json_cached,
        )
        workers = self._worker_processes(force=force)
        events_job_id = job.job_id if self.events_mode == "job" else None
        events = self._recent_events(limit=self.events_limit, job_id=events_job_id)

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

        if self.freeze_updates and not force:
            self._update_header()
            return

        self._prune_json_cache()
        try:
            self.gateway = gateway_status(self.paths.root)
            self._disconnected = False
        except Exception:  # pragma: no cover
            self._disconnected = True

        try:
            all_jobs = _load_jobs(self.paths, read_payload=self._read_json_cached)
        except Exception as exc:  # pragma: no cover
            self.notify(f"job refresh failed: {type(exc).__name__}: {exc}", severity="error")
            all_jobs = self.jobs

        self._update_event_cache()
        self.jobs = all_jobs
        filtered = self._apply_filters_and_sort(all_jobs)

        previous_selected = self.selected_job_id
        self.filtered_jobs = filtered
        if filtered:
            if previous_selected and any(job.job_id == previous_selected for job in filtered):
                self.selected_job_id = previous_selected
            elif previous_selected:
                self.selected_job_id = filtered[0].job_id
                self.ghost_selection = previous_selected
                self.notify("Selected job no longer in view (filtered/completed)", severity="warning")
            else:
                self.selected_job_id = filtered[0].job_id
        else:
            self.selected_job_id = None

        self._refresh_detail_snapshot(force=force)
        self.last_refresh = time.monotonic()
        self._render_all()

    def _render_all(self) -> None:
        self._update_header()
        self._update_sidebar()
        self._update_jobs_table()
        self._update_detail_tabs()
        self._update_activity()
        self._update_footer_hints()

    def _gateway_state_label(self) -> str:
        if self._disconnected:
            return "DISCONNECTED"
        if bool(self.gateway.get("alive")):
            return "RUNNING"
        return "STOPPED"

    def _update_header(self) -> None:
        header = self.query_one("#header_bar", Static)
        queue = self.gateway.get("queue") if isinstance(self.gateway.get("queue"), dict) else {}
        active = _as_int(queue.get("running"))
        queued = _as_int(queue.get("pending"))
        failed = _as_int(queue.get("failed"))
        selected = self._selected_job()
        refresh_mode = "LIVE (0.8s)" if selected and _is_active_job(selected) else "IDLE (3.0s)"
        age = time.monotonic() - self.last_refresh if self.last_refresh else 0.0
        stale = " STALE" if self._disconnected else ""
        frozen = " FROZEN" if self.freeze_updates else ""
        header.update(
            f"Gateway {self._gateway_state_label()}{stale}{frozen} | "
            f"Queue active={active} queued={queued} failed={failed} | "
            f"Refresh {refresh_mode} | Last update {age:.1f}s ago"
        )

    def _update_sidebar(self) -> None:
        summary = self.query_one("#filters_summary", Static)
        saved = self.query_one("#saved_views", Static)
        focus_mark = ">> " if self.FOCUS_AREAS[self.focus_idx] == "filters" else "   "
        ghost = f"\nHidden selected job: {self.ghost_selection[:12]}" if self.ghost_selection else ""
        summary.update(
            "\n".join(
                [
                    f"{focus_mark}Status: {self.filters.status}",
                    f"   Lane: {self.filters.lane}",
                    f"   Mode: {self.filters.mode}",
                    f"   Time: {self.filters.time_window}",
                    f"   Severity: {'WARN/ERROR' if self.filters.warn_or_error_only else 'All'}",
                    f"   Search: {self.filters.search or '-'}{ghost}",
                ]
            )
        )
        rows: list[str] = ["", "[b]Saved Views[/b]"]
        for idx, name in enumerate(self.saved_view_names):
            marker = ">" if idx == self.saved_view_idx else " "
            rows.append(f"{marker} {name}")
        saved.update("\n".join(rows))

    def _status_icon(self, status: str) -> str:
        normalized = status.upper()
        if normalized == "FAILED":
            return "x"
        if normalized == "RUNNING":
            return ">"
        if normalized in {"PENDING", "QUEUED"}:
            return "~"
        if normalized in {"SUCCEEDED", "DONE"}:
            return "v"
        return "-"

    def _job_last_event_cell(self, job: JobSummary) -> tuple[str, str]:
        event = self._latest_event_for_job(job.job_id)
        if event is None:
            return ("-", "-")
        severity = self._event_severity(event)
        label = str(event.get("event", "event"))
        msg = str(event.get("message", ""))[:40]
        detail = label if not msg else f"{label}: {msg}"
        return (severity, detail)

    def _update_jobs_table(self) -> None:
        table = self.query_one("#jobs_table", DataTable)
        table.clear(columns=False)

        for job in self.filtered_jobs:
            done, total = self._job_progress(job)
            progress = "-" if total <= 0 else f"{done}/{total}"
            sev, event_detail = self._job_last_event_cell(job)
            new_dot = ""
            latest_event = self._latest_event_for_job(job.job_id)
            if latest_event is not None:
                ts = _parse_iso8601(latest_event.get("ts"))
                if ts is not None and ts.timestamp() > self._seen_event_ts_by_job.get(job.job_id, 0.0):
                    new_dot = " *"
            table.add_row(
                self._status_icon(job.status),
                f"{job.status}{new_dot}",
                job.job_id[:10],
                job.queue_lane,
                progress,
                f"{_as_int(job.max_workers)}",
                _format_age(job.updated_at),
                f"{sev} {event_detail}",
                _clip(job.input_path or "-", 52),
                key=job.job_id,
            )

        if not self.filtered_jobs:
            table.add_row("-", "(none)", "-", "-", "-", "-", "-", "-", "-", key="__none__")
            table.cursor_coordinate = (0, 0)
            return

        selected = self._selected_job()
        if selected is None:
            table.cursor_coordinate = (0, 0)
            return
        selected_idx = 0
        for idx, job in enumerate(self.filtered_jobs):
            if job.job_id == selected.job_id:
                selected_idx = idx
                break
        table.cursor_coordinate = (selected_idx, 0)

    def _update_detail_tabs(self) -> None:
        job = self._selected_job()
        overview = self.query_one("#overview_pane", Static)
        stages = self.query_one("#stages_table", DataTable)
        batches = self.query_one("#batches_table", DataTable)
        workers = self.query_one("#workers_table", DataTable)
        events = self.query_one("#detail_events_table", DataTable)
        files = self.query_one("#files_table", DataTable)
        events_filter = self.query_one("#events_filter_line", Static)

        stages.clear(columns=False)
        batches.clear(columns=False)
        workers.clear(columns=False)
        events.clear(columns=False)
        files.clear(columns=False)

        if job is None or self._detail_snapshot is None:
            overview.update("No selected job.")
            return

        snapshot = self._detail_snapshot
        done, total = self._job_progress(job)
        pct = 0.0 if total <= 0 else (100.0 * done / total)
        last_event = self._latest_event_for_job(job.job_id)
        last_event_text = "-" if last_event is None else str(last_event.get("event", "event"))
        overview.update(
            "\n".join(
                [
                    f"Job: {job.job_id}",
                    f"Status: {job.status} lane={job.queue_lane} mode={job.mode}",
                    f"Input: {_clip(job.input_path or '-', 120)}",
                    f"Progress: {done}/{total} ({pct:.1f}%)",
                    f"Timestamps: submitted={job.submitted_at or '-'} started={job.started_at or '-'} "
                    f"updated={job.updated_at or '-'} finished={job.finished_at or '-'}",
                    f"Last activity: {last_event_text}",
                ]
            )
        )

        stage_rows = sorted(
            snapshot.stage_progress,
            key=lambda row: (0 if row.running_batches > 0 else 1 if row.failed_batches > 0 else 2, row.stage),
        )
        for row in stage_rows:
            if row.expected_batches > 0:
                done_count = row.succeeded_batches + row.failed_batches + row.skipped_batches
                done_text = f"{done_count}/{row.expected_batches}"
            else:
                done_text = "n/a"
            status = "PENDING"
            if row.failed_batches > 0:
                status = "FAILED"
            elif row.running_batches > 0:
                status = "RUNNING"
            elif row.expected_batches > 0 and (
                row.succeeded_batches + row.skipped_batches >= row.expected_batches
            ):
                status = "SUCCEEDED"
            stages.add_row(
                row.stage,
                done_text,
                str(row.succeeded_batches),
                str(row.running_batches),
                str(row.failed_batches),
                str(row.skipped_batches),
                status,
                key=f"stage:{row.stage}",
            )

        prioritized_batches = sorted(
            snapshot.batches,
            key=lambda batch: (
                0 if batch.status == "RUNNING" else 1 if batch.status == "FAILED" else 2,
                batch.start_idx,
            ),
        )
        for batch in prioritized_batches:
            batches.add_row(
                batch.range_label,
                batch.status,
                batch.active_stage or "-",
                f"{batch.completed_stages}/{batch.total_stages}",
                str(batch.worker_pid) if batch.worker_pid is not None else "-",
                _format_age(batch.updated_at),
                key=f"batch:{batch.range_label}",
            )

        for worker in snapshot.workers:
            workers.add_row(
                worker.role,
                str(worker.pid),
                str(worker.ppid) if worker.ppid is not None else "-",
                worker.cpu_percent,
                str(worker.rss_kib) if worker.rss_kib is not None else "-",
                worker.elapsed,
                worker.state,
                _clip(worker.command, 96),
                key=f"worker:{worker.pid}",
            )
        if not snapshot.workers:
            workers.add_row("-", "-", "-", "-", "-", "-", "-", "No gateway worker processes detected")

        events_filter.update(
            f"Mode={self.events_mode.upper()} | Follow={'ON' if self.follow_events else 'OFF'} "
            f"| Search={self.filters.search or '-'}"
        )
        for idx, event in enumerate(snapshot.events):
            severity = self._event_severity(event)
            details: list[str] = []
            if isinstance(event.get("status"), str):
                details.append(f"status={event['status']}")
            if isinstance(event.get("stage"), str):
                details.append(f"stage={event['stage']}")
            if isinstance(event.get("message"), str):
                details.append(event["message"])
            events.add_row(
                _format_ts_short(event.get("ts") if isinstance(event.get("ts"), str) else None),
                severity,
                str(event.get("job_id", "-"))[:10],
                str(event.get("event", "event")),
                _clip(" ".join(details), 96),
                key=f"event:{idx}",
            )

        files.add_row("job json", str(self.paths.jobs / f"{job.job_id}.json"))
        files.add_row("queue lane", str((self.paths.root / "queue" / job.queue_lane / f"{job.job_id}.json")))
        files.add_row("state root", str(self.paths.root))
        files.add_row("input", job.input_path or "-")

        if snapshot.events:
            ts = _parse_iso8601(snapshot.events[-1].get("ts"))
            if ts is not None:
                self._seen_event_ts_by_job[job.job_id] = max(
                    self._seen_event_ts_by_job.get(job.job_id, 0.0),
                    ts.timestamp(),
                )

    def _update_activity(self) -> None:
        filter_line = self.query_one("#activity_filters", Static)
        table = self.query_one("#activity_table", DataTable)
        table.clear(columns=False)
        self._activity_job_ids = []

        selected = self._selected_job()
        selected_job_id = selected.job_id if selected is not None else None
        job_scope = selected_job_id if self.events_mode == "job" else None
        rows = self._recent_events(limit=50, job_id=job_scope)

        for idx, event in enumerate(rows):
            severity = self._event_severity(event)
            if self.filters.warn_or_error_only and severity == "INFO":
                continue
            message = str(event.get("event", "event"))
            detail = str(event.get("message", ""))
            body = message if not detail else f"{message}: {detail}"
            table.add_row(
                _format_ts_short(event.get("ts") if isinstance(event.get("ts"), str) else None),
                severity,
                str(event.get("job_id", "-"))[:10],
                _clip(body, 120),
                key=f"activity:{idx}",
            )
            self._activity_job_ids.append(str(event.get("job_id", "")))
        filter_line.update(
            f"[All | WARN/ERR {'ON' if self.filters.warn_or_error_only else 'OFF'} | "
            f"Selected Job {'ON' if self.events_mode == 'job' else 'OFF'} | "
            f"Follow {'ON' if self.follow_events else 'OFF'}]"
        )
        if self.follow_events and table.row_count > 0:
            table.cursor_coordinate = (table.row_count - 1, 0)

    def _update_footer_hints(self) -> None:
        footer = self.query_one("#context_hints", Static)
        area = self.FOCUS_AREAS[self.focus_idx]
        if area == "filters":
            hint = "Filters: / search, a status, l lane, Enter apply saved view, Ctrl+S save, d delete"
        elif area == "jobs":
            hint = "Jobs: j/k move, 1..8 sort, Shift+1..8 reverse, Enter job actions, Space pin (todo)"
        elif area == "detail":
            hint = "Detail: [/] tabs, e job/global events, n/p error nav, Enter open selected detail item"
        else:
            hint = "Activity: Enter selects job + Events tab, f follow, F freeze updates"
        footer.update(hint)

    def _set_focus_area(self, area: str) -> None:
        if area not in self.FOCUS_AREAS:
            return
        self.focus_idx = self.FOCUS_AREAS.index(area)
        if area == "filters":
            self.query_one("#search_box", Input).focus()
        elif area == "jobs":
            self.query_one("#jobs_table", DataTable).focus()
        elif area == "detail":
            self.query_one("#detail_tabs", TabbedContent).focus()
        else:
            self.query_one("#activity_table", DataTable).focus()
        self._update_footer_hints()

    def _move_cursor(self, table: DataTable, delta: int) -> None:
        if table.row_count == 0:
            return
        row, col = table.cursor_coordinate
        row = max(0, min(table.row_count - 1, row + delta))
        table.cursor_coordinate = (row, col)

    def _go_edge(self, table: DataTable, *, top: bool) -> None:
        if table.row_count == 0:
            return
        row, col = table.cursor_coordinate
        table.cursor_coordinate = (0 if top else table.row_count - 1, col)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "search_box":
            return
        self.filters.search = event.value
        self._refresh(force=True)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id == "jobs_table":
            row = event.data_table.cursor_row
            if 0 <= row < len(self.filtered_jobs):
                self.selected_job_id = self.filtered_jobs[row].job_id
                self._refresh_detail_snapshot(force=True)
                self._update_detail_tabs()

    def action_focus_next_panel(self) -> None:
        area = self.FOCUS_AREAS[(self.focus_idx + 1) % len(self.FOCUS_AREAS)]
        self._set_focus_area(area)

    def action_focus_prev_panel(self) -> None:
        area = self.FOCUS_AREAS[(self.focus_idx - 1) % len(self.FOCUS_AREAS)]
        self._set_focus_area(area)

    def action_focus_search(self) -> None:
        self._set_focus_area("filters")

    def action_manual_refresh(self) -> None:
        self._refresh(force=True)
        self.notify("Refreshed")

    def action_cycle_status_filter(self) -> None:
        order = ["Active", "All", "Failed", "Completed", "Queued"]
        idx = order.index(self.filters.status) if self.filters.status in order else 0
        self.filters.status = order[(idx + 1) % len(order)]
        self._refresh(force=True)

    def action_cycle_lane_filter(self) -> None:
        lanes = sorted({job.queue_lane for job in self.jobs if job.queue_lane and job.queue_lane != "none"})
        choices = ["All"] + lanes
        idx = choices.index(self.filters.lane) if self.filters.lane in choices else 0
        self.filters.lane = choices[(idx + 1) % len(choices)] if choices else "All"
        self._refresh(force=True)

    def action_toggle_events_scope(self) -> None:
        self.events_mode = "global" if self.events_mode == "job" else "job"
        self._refresh(force=True)

    def action_toggle_follow_events(self) -> None:
        self.follow_events = not self.follow_events
        self._update_activity()
        self._update_detail_tabs()

    def action_toggle_freeze_updates(self) -> None:
        self.freeze_updates = not self.freeze_updates
        self._update_header()
        self.notify("Updates frozen" if self.freeze_updates else "Updates resumed")

    def action_select_prev(self) -> None:
        area = self.FOCUS_AREAS[self.focus_idx]
        if area == "filters":
            self.saved_view_idx = max(0, self.saved_view_idx - 1)
            self._update_sidebar()
            return
        if area == "jobs":
            table = self.query_one("#jobs_table", DataTable)
            self._move_cursor(table, -1)
            return
        if area == "activity":
            table = self.query_one("#activity_table", DataTable)
            self._move_cursor(table, -1)
            return

    def action_select_next(self) -> None:
        area = self.FOCUS_AREAS[self.focus_idx]
        if area == "filters":
            self.saved_view_idx = min(len(self.saved_view_names) - 1, self.saved_view_idx + 1)
            self._update_sidebar()
            return
        if area == "jobs":
            table = self.query_one("#jobs_table", DataTable)
            self._move_cursor(table, 1)
            return
        if area == "activity":
            table = self.query_one("#activity_table", DataTable)
            self._move_cursor(table, 1)
            return

    def action_go_top(self) -> None:
        area = self.FOCUS_AREAS[self.focus_idx]
        if area == "jobs":
            self._go_edge(self.query_one("#jobs_table", DataTable), top=True)
        elif area == "activity":
            self._go_edge(self.query_one("#activity_table", DataTable), top=True)

    def action_go_bottom(self) -> None:
        area = self.FOCUS_AREAS[self.focus_idx]
        if area == "jobs":
            self._go_edge(self.query_one("#jobs_table", DataTable), top=False)
        elif area == "activity":
            self._go_edge(self.query_one("#activity_table", DataTable), top=False)

    def _set_sort(self, col_num: int, *, reverse: bool) -> None:
        column = self.SORT_COLUMNS.get(col_num)
        if column is None:
            return
        self.sort_state.column = column
        self.sort_state.reverse = reverse
        self._refresh(force=True)

    def action_sort_col_1(self) -> None:
        self._set_sort(1, reverse=False)

    def action_sort_col_2(self) -> None:
        self._set_sort(2, reverse=False)

    def action_sort_col_3(self) -> None:
        self._set_sort(3, reverse=False)

    def action_sort_col_4(self) -> None:
        self._set_sort(4, reverse=False)

    def action_sort_col_5(self) -> None:
        self._set_sort(5, reverse=False)

    def action_sort_col_6(self) -> None:
        self._set_sort(6, reverse=False)

    def action_sort_col_7(self) -> None:
        self._set_sort(7, reverse=False)

    def action_sort_col_8(self) -> None:
        self._set_sort(8, reverse=False)

    def action_sort_col_1_rev(self) -> None:
        self._set_sort(1, reverse=True)

    def action_sort_col_2_rev(self) -> None:
        self._set_sort(2, reverse=True)

    def action_sort_col_3_rev(self) -> None:
        self._set_sort(3, reverse=True)

    def action_sort_col_4_rev(self) -> None:
        self._set_sort(4, reverse=True)

    def action_sort_col_5_rev(self) -> None:
        self._set_sort(5, reverse=True)

    def action_sort_col_6_rev(self) -> None:
        self._set_sort(6, reverse=True)

    def action_sort_col_7_rev(self) -> None:
        self._set_sort(7, reverse=True)

    def action_sort_col_8_rev(self) -> None:
        self._set_sort(8, reverse=True)

    def action_prev_tab(self) -> None:
        tabs = ["tab-overview", "tab-stages", "tab-batches", "tab-workers", "tab-events", "tab-files"]
        detail_tabs = self.query_one("#detail_tabs", TabbedContent)
        current = detail_tabs.active or tabs[0]
        idx = tabs.index(current) if current in tabs else 0
        detail_tabs.active = tabs[(idx - 1) % len(tabs)]

    def action_next_tab(self) -> None:
        tabs = ["tab-overview", "tab-stages", "tab-batches", "tab-workers", "tab-events", "tab-files"]
        detail_tabs = self.query_one("#detail_tabs", TabbedContent)
        current = detail_tabs.active or tabs[0]
        idx = tabs.index(current) if current in tabs else 0
        detail_tabs.active = tabs[(idx + 1) % len(tabs)]

    def action_next_error(self) -> None:
        events = self._detail_snapshot.events if self._detail_snapshot else []
        if not events:
            return
        start = self._event_error_idx + 1
        for idx in range(start, len(events)):
            if self._event_severity(events[idx]) == "ERROR":
                self._event_error_idx = idx
                table = self.query_one("#detail_events_table", DataTable)
                if idx < table.row_count:
                    table.cursor_coordinate = (idx, 0)
                return

    def action_prev_error(self) -> None:
        events = self._detail_snapshot.events if self._detail_snapshot else []
        if not events:
            return
        start = self._event_error_idx - 1
        for idx in range(start, -1, -1):
            if self._event_severity(events[idx]) == "ERROR":
                self._event_error_idx = idx
                table = self.query_one("#detail_events_table", DataTable)
                if idx < table.row_count:
                    table.cursor_coordinate = (idx, 0)
                return

    def action_save_view(self) -> None:
        def _save(name: str | None) -> None:
            if name is None:
                return
            self.saved_views[name] = self.filters.clone()
            self.saved_view_names = list(self.saved_views)
            self.saved_view_idx = self.saved_view_names.index(name)
            self._update_sidebar()
            self.notify(f"Saved view: {name}")

        self.push_screen(PromptScreen(title="Save current filters as a view", placeholder="View name"), _save)

    def action_delete_view(self) -> None:
        if not self.saved_view_names:
            return
        name = self.saved_view_names[self.saved_view_idx]
        if name in {"Active", "Failures", "Recently Updated"}:
            self.notify("Built-in views cannot be deleted", severity="warning")
            return

        def _delete(confirmed: bool) -> None:
            if not confirmed:
                return
            self.saved_views.pop(name, None)
            self.saved_view_names = list(self.saved_views)
            self.saved_view_idx = max(0, min(self.saved_view_idx, len(self.saved_view_names) - 1))
            self._update_sidebar()
            self.notify(f"Deleted view: {name}")

        self.push_screen(ConfirmScreen(message=f"Delete saved view '{name}'?", ok_label="Delete"), _delete)

    def action_default_action(self) -> None:
        area = self.FOCUS_AREAS[self.focus_idx]
        if area == "filters":
            if not self.saved_view_names:
                return
            name = self.saved_view_names[self.saved_view_idx]
            view = self.saved_views.get(name)
            if view is None:
                return
            self.filters = view.clone()
            self.query_one("#search_box", Input).value = self.filters.search
            self._refresh(force=True)
            self.notify(f"Applied view: {name}")
            return

        if area == "activity":
            table = self.query_one("#activity_table", DataTable)
            if table.row_count <= 0:
                return
            row = table.cursor_row
            if row < 0 or row >= len(self._activity_job_ids):
                return
            job_id = self._activity_job_ids[row].strip()
            if job_id and any(job.job_id.startswith(job_id) for job in self.filtered_jobs):
                target = next(job for job in self.filtered_jobs if job.job_id.startswith(job_id))
                self.selected_job_id = target.job_id
                self.query_one("#detail_tabs", TabbedContent).active = "tab-events"
                self._refresh(force=True)
                self._set_focus_area("detail")
            return

        selected = self._selected_job()
        if selected is None:
            return
        self.notify(
            f"Job actions: id={selected.job_id[:12]} | use tabs for events/workers/files",
            timeout=3.0,
        )

    def action_daemon_control(self) -> None:
        running = bool(self.gateway.get("alive"))

        def _apply(action: str | None) -> None:
            if action is None:
                return
            try:
                if action == "start":
                    pid = start_background(
                        state_root=self.paths.root,
                        poll_interval=self.daemon_poll_interval,
                        log_file=self.log_file,
                    )
                    self.notify(f"Gateway started pid={pid}")
                elif action == "stop":
                    stop_background(self.paths.root)
                    self.notify("Gateway stop signal sent")
                elif action == "restart":
                    stop_background(self.paths.root)
                    time.sleep(0.15)
                    pid = start_background(
                        state_root=self.paths.root,
                        poll_interval=self.daemon_poll_interval,
                        log_file=self.log_file,
                    )
                    self.notify(f"Gateway restarted pid={pid}")
            except Exception as exc:  # pragma: no cover
                self.notify(f"Gateway action failed: {type(exc).__name__}: {exc}", severity="error")
            self._refresh(force=True)

        self.push_screen(GatewayActionScreen(running=running), _apply)

    def action_show_help(self) -> None:
        self.notify("Tab panels | / search | a status | l lane | 1..8 sort | s daemon actions | ? help")

    def action_command_palette(self) -> None:
        self.notify("Command palette placeholder: use key hints/footer for now.")


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
