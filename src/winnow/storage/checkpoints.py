"""Stage checkpoint storage helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from winnow.storage.atomic import atomic_write_json, read_json
from winnow.storage.state_store import StatePaths


def checkpoint_path(
    paths: StatePaths,
    stage_name: str,
    stream_id: str,
    batch_start: int,
    batch_end: int,
    cache_key: str,
) -> Path:
    """Build checkpoint file path for one stage batch execution."""

    return (
        paths.stages
        / stage_name
        / stream_id
        / f"{batch_start}_{batch_end}"
        / f"{cache_key}.json"
    )


def read_checkpoint(path: Path) -> dict[str, Any] | None:
    """Read checkpoint payload if present."""

    if not path.exists():
        return None
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise TypeError(f"Checkpoint is not a JSON object: {path}")
    return payload


def write_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    """Write a checkpoint payload atomically."""

    atomic_write_json(path, payload)


def is_checkpoint_hit(payload: dict[str, Any] | None) -> bool:
    """Return true when checkpoint describes a successful stage run."""

    if payload is None:
        return False
    return payload.get("status") == "SUCCEEDED"
