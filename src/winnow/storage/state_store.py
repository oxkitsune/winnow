"""State directory layout helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_STATE_ROOT = Path(".winnow/state/v1")
DEFAULT_ARTIFACT_ROOT = Path(".winnow/artifacts")


@dataclass(frozen=True, slots=True)
class StatePaths:
    """All important state paths rooted at `.winnow/state/v1`."""

    root: Path
    jobs: Path
    queue_pending: Path
    queue_running: Path
    queue_done: Path
    queue_failed: Path
    streams: Path
    stages: Path
    events: Path
    snapshots: Path
    runtime: Path

    @property
    def queue_all(self) -> tuple[Path, Path, Path, Path]:
        return (
            self.queue_pending,
            self.queue_running,
            self.queue_done,
            self.queue_failed,
        )


def build_state_paths(root: Path) -> StatePaths:
    """Build a typed state path object."""

    queue_root = root / "queue"
    return StatePaths(
        root=root,
        jobs=root / "jobs",
        queue_pending=queue_root / "pending",
        queue_running=queue_root / "running",
        queue_done=queue_root / "done",
        queue_failed=queue_root / "failed",
        streams=root / "streams",
        stages=root / "stages",
        events=root / "events",
        snapshots=root / "snapshots",
        runtime=root / "runtime",
    )


def ensure_state_layout(root: Path = DEFAULT_STATE_ROOT) -> StatePaths:
    """Create state directories if they do not already exist."""

    paths = build_state_paths(root)
    for directory in (
        paths.root,
        paths.jobs,
        paths.queue_pending,
        paths.queue_running,
        paths.queue_done,
        paths.queue_failed,
        paths.streams,
        paths.stages,
        paths.events,
        paths.snapshots,
        paths.runtime,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    return paths


def ensure_artifact_root(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> Path:
    """Create artifact directory if it does not already exist."""

    artifact_root.mkdir(parents=True, exist_ok=True)
    return artifact_root
