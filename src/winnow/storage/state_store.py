"""State directory layout helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any

from winnow.storage.atomic import atomic_write_json, atomic_write_text

DEFAULT_STATE_ROOT = Path(".winnow/state/v1")
DEFAULT_STATE_ROOT_V2 = Path(".winnow/state/v2")
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _clear_directory(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def migrate_state_v1_to_v2(
    source_root: Path = DEFAULT_STATE_ROOT,
    target_root: Path = DEFAULT_STATE_ROOT_V2,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Copy a v1 filesystem state tree into a v2 location.

    This keeps Winnow migration-free at runtime while still allowing explicit
    state upgrades through a deterministic copy step.
    """

    src = source_root.resolve()
    dst = target_root.resolve()

    if src == dst:
        raise ValueError("Source and target state roots must be different.")
    if not src.exists():
        raise FileNotFoundError(f"Source state root does not exist: {src}")

    dst.mkdir(parents=True, exist_ok=True)
    if any(dst.iterdir()):
        if not force:
            raise RuntimeError(
                f"Target state root is not empty: {dst}. Use force=True to overwrite."
            )
        _clear_directory(dst)

    copied_dirs: list[str] = []
    copied_files = 0
    expected_dirs = [
        "jobs",
        "queue",
        "streams",
        "stages",
        "events",
        "snapshots",
        "runtime",
    ]

    for directory_name in expected_dirs:
        src_dir = src / directory_name
        if not src_dir.exists():
            continue
        dst_dir = dst / directory_name
        shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
        copied_dirs.append(directory_name)
        copied_files += sum(1 for path in src_dir.rglob("*") if path.is_file())

    meta = {
        "schema": "state-migration/v1-to-v2",
        "migrated_at": _now(),
        "source_root": str(src),
        "target_root": str(dst),
        "copied_directories": copied_dirs,
        "copied_file_count": copied_files,
    }

    atomic_write_text(dst / "VERSION", "state/v2\n")
    atomic_write_json(dst / "migration.json", meta)
    return meta
