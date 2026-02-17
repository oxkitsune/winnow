"""Data models and constants used by the gateway TUI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_STAGE_NUMERIC_METRICS: dict[str, tuple[str, ...]] = {
    "blur": ("blur_score",),
    "darkness": ("mean_luma", "percentile_luma"),
}

_STAGE_BOOLEAN_METRICS: dict[str, tuple[str, ...]] = {
    "blur": ("is_blurry",),
    "darkness": ("is_dark",),
}

_METRIC_LABELS: dict[str, str] = {
    "blur_score": "mean blur score",
    "mean_luma": "mean darkness (luma)",
    "percentile_luma": "mean percentile luma",
    "is_blurry": "blurry",
    "is_dark": "dark",
}


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
class StageImageStats:
    """Best-effort input/output image counts for one stage."""

    stage: str
    input_images: int | None
    output_images: int | None


@dataclass(slots=True)
class StageMetricStats:
    """Best-effort aggregate metric stats for one stage."""

    stage: str
    record_count: int
    mean_values: dict[str, float]
    true_ratios: dict[str, float]


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
    stage_image_stats: dict[str, StageImageStats]
    stage_metric_stats: dict[str, StageMetricStats]
    batches: list[BatchStatus]
    workers: list[WorkerProcess]
    events: list[dict[str, Any]]
    generated_at: float


@dataclass(slots=True)
class _JsonCacheEntry:
    mtime_ns: int
    size: int
    payload: dict[str, Any] | None


@dataclass(slots=True)
class _ArtifactMetricCacheEntry:
    mtime_ns: int
    size: int
    numeric_fields: tuple[str, ...]
    boolean_fields: tuple[str, ...]
    record_count: int
    numeric_sums: dict[str, float]
    numeric_counts: dict[str, int]
    true_counts: dict[str, int]
    boolean_counts: dict[str, int]


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
