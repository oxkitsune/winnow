"""Pipeline stage interfaces and input models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class FrameInput:
    """Frame-level input metadata used by stage runners."""

    frame_idx: int
    path: Path
    size_bytes: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class BatchInput:
    """A contiguous frame batch for one stream."""

    stream_id: str
    stream_path: Path
    start_idx: int
    end_idx: int
    frames: list[FrameInput]


class StageRunner(Protocol):
    """Call signature every stage runner must implement."""

    def __call__(self, batch: BatchInput, config: Any) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True, slots=True)
class StageDefinition:
    """Declarative stage metadata and execution hook."""

    name: str
    version: str
    config: Any
    runner: StageRunner
