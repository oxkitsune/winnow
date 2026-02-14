"""Blur metric stage implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from winnow.config.schema import BlurMetricConfig
from winnow.pipeline.stage import BatchInput


def _load_grayscale(path: Path) -> np.ndarray:
    with Image.open(path) as image:
        return np.asarray(image.convert("L"), dtype=np.float32)


def _laplacian_variance(gray: np.ndarray) -> float:
    if gray.ndim != 2:
        raise ValueError(f"Expected grayscale image with 2 dims, got shape {gray.shape}")
    if gray.shape[0] < 3 or gray.shape[1] < 3:
        return 0.0

    center = gray[1:-1, 1:-1]
    lap = (
        -4.0 * center
        + gray[:-2, 1:-1]
        + gray[2:, 1:-1]
        + gray[1:-1, :-2]
        + gray[1:-1, 2:]
    )
    return float(np.var(lap))


def run(batch: BatchInput, config: BlurMetricConfig) -> list[dict[str, Any]]:
    """Compute blur score for each frame in the batch."""

    records: list[dict[str, Any]] = []
    for frame in batch.frames:
        gray = _load_grayscale(frame.path)
        score = _laplacian_variance(gray)
        records.append(
            {
                "frame_idx": frame.frame_idx,
                "frame_path": str(frame.path),
                "blur_score": score,
                "is_blurry": bool(score < config.threshold),
                "threshold": config.threshold,
                "method": config.method,
            }
        )
    return records
