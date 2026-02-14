"""Darkness metric stage implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from winnow.config.schema import DarknessMetricConfig
from winnow.pipeline.stage import BatchInput


def _load_grayscale(path: Path) -> np.ndarray:
    with Image.open(path) as image:
        return np.asarray(image.convert("L"), dtype=np.float32)


def run(batch: BatchInput, config: DarknessMetricConfig) -> list[dict[str, Any]]:
    """Compute luminance statistics for each frame in the batch."""

    percentile = min(max(config.percentile, 0.0), 1.0)
    percentile_label = int(percentile * 100)

    records: list[dict[str, Any]] = []
    for frame in batch.frames:
        gray = _load_grayscale(frame.path)
        mean_luma = float(np.mean(gray))
        pct_luma = float(np.percentile(gray, percentile * 100.0))

        records.append(
            {
                "frame_idx": frame.frame_idx,
                "frame_path": str(frame.path),
                "mean_luma": mean_luma,
                "percentile": percentile,
                "percentile_label": percentile_label,
                "percentile_luma": pct_luma,
                "is_dark": bool(pct_luma < config.threshold),
                "threshold": config.threshold,
            }
        )
    return records
