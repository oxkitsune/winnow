"""Idle period detection stage implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from winnow.config.schema import IdleMetricConfig
from winnow.pipeline.stage import BatchInput


def _load_grayscale(path: Path) -> np.ndarray:
    with Image.open(path) as image:
        gray = image.convert("L")
        arr = np.asarray(gray, dtype=np.float32)
    return arr / 255.0


def _motion_scores(frames: list[np.ndarray]) -> list[float]:
    if not frames:
        return []
    scores = [0.0]
    previous = frames[0]
    for current in frames[1:]:
        diff = np.abs(current - previous)
        scores.append(float(np.mean(diff)))
        previous = current
    return scores


def _smooth(scores: list[float], window: int) -> list[float]:
    window = max(1, window)
    out: list[float] = []
    for idx in range(len(scores)):
        start = max(0, idx - window + 1)
        sample = scores[start : idx + 1]
        out.append(sum(sample) / len(sample))
    return out


def _mark_idle_segments(
    frame_indices: list[int],
    smoothed: list[float],
    threshold: float,
    min_run: int,
) -> list[str | None]:
    min_run = max(1, min_run)
    flags = [score < threshold for score in smoothed]
    segment_ids: list[str | None] = [None] * len(flags)

    run_start: int | None = None
    for idx, is_idle in enumerate(flags + [False]):
        if is_idle and run_start is None:
            run_start = idx
            continue
        if is_idle:
            continue
        if run_start is None:
            continue

        run_end = idx - 1
        run_length = run_end - run_start + 1
        if run_length >= min_run:
            seg_start = frame_indices[run_start]
            seg_end = frame_indices[run_end]
            seg_id = f"{seg_start}_{seg_end}"
            for pos in range(run_start, run_end + 1):
                segment_ids[pos] = seg_id
        run_start = None

    return segment_ids


def run(batch: BatchInput, config: IdleMetricConfig) -> list[dict[str, Any]]:
    """Compute temporal idle activity features for one frame batch."""

    if not batch.frames:
        return []

    arrays = [_load_grayscale(frame.path) for frame in batch.frames]
    raw_motion = _motion_scores(arrays)
    smoothed_motion = _smooth(raw_motion, config.smoothing_window)
    frame_indices = [frame.frame_idx for frame in batch.frames]
    segment_ids = _mark_idle_segments(
        frame_indices=frame_indices,
        smoothed=smoothed_motion,
        threshold=config.motion_threshold,
        min_run=config.min_run,
    )

    records: list[dict[str, Any]] = []
    for idx, frame in enumerate(batch.frames):
        segment_id = segment_ids[idx]
        records.append(
            {
                "frame_idx": frame.frame_idx,
                "frame_path": str(frame.path),
                "motion_score": raw_motion[idx],
                "smoothed_motion_score": smoothed_motion[idx],
                "motion_threshold": config.motion_threshold,
                "is_idle": segment_id is not None,
                "idle_segment_id": segment_id,
                "smoothing_window": config.smoothing_window,
                "min_run": config.min_run,
            }
        )

    return records
