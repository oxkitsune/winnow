"""Scan a stream directory and build an ordered frame manifest."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from winnow.ingest.sequence import find_missing_indices, parse_frame_index


@dataclass(slots=True)
class FrameRef:
    frame_idx: int
    path: Path


@dataclass(slots=True)
class StreamScanResult:
    stream_path: Path
    frames: list[FrameRef]
    missing_indices: list[int]

    @property
    def frame_count(self) -> int:
        return len(self.frames)


def scan_stream(stream_path: Path, strict_sequence: bool = True) -> StreamScanResult:
    """Return ordered frames and sequence diagnostics for a stream directory."""

    if not stream_path.exists():
        raise FileNotFoundError(f"Input stream path does not exist: {stream_path}")
    if not stream_path.is_dir():
        raise NotADirectoryError(f"Input stream path must be a directory: {stream_path}")

    parsed: list[FrameRef] = []
    for child in stream_path.iterdir():
        if not child.is_file():
            continue
        idx = parse_frame_index(child)
        if idx is None:
            continue
        parsed.append(FrameRef(frame_idx=idx, path=child))

    parsed.sort(key=lambda item: item.frame_idx)
    indices = [item.frame_idx for item in parsed]
    missing = find_missing_indices(indices)

    if strict_sequence and not parsed:
        raise ValueError(f"No files matching image_<idx> pattern found in {stream_path}")
    if strict_sequence and missing:
        joined = ", ".join(str(x) for x in missing[:20])
        suffix = "" if len(missing) <= 20 else ", ..."
        raise ValueError(f"Sequence gaps detected: {joined}{suffix}")

    return StreamScanResult(stream_path=stream_path, frames=parsed, missing_indices=missing)
