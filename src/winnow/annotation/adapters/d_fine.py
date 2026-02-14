"""Local heuristic detector adapter standing in for D-FINE."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PIL import Image

from winnow.annotation.schema import Detection


@dataclass(slots=True)
class DFineAdapter:
    """Simple detector that finds bright and dark regions."""

    bright_threshold: float = 0.75
    dark_threshold: float = 0.25

    def _load_rgb(self, frame_path: Path) -> np.ndarray:
        with Image.open(frame_path) as image:
            arr = np.asarray(image.convert("RGB"), dtype=np.float32)
        return arr / 255.0

    def _bbox_from_mask(self, mask: np.ndarray) -> list[float] | None:
        ys, xs = np.where(mask)
        if ys.size == 0 or xs.size == 0:
            return None

        x0 = float(xs.min())
        y0 = float(ys.min())
        x1 = float(xs.max())
        y1 = float(ys.max())
        return [x0, y0, (x1 - x0 + 1.0), (y1 - y0 + 1.0)]

    def _score_from_mask(self, mask: np.ndarray) -> float:
        coverage = float(np.mean(mask))
        return max(0.0, min(1.0, coverage * 3.0))

    def detect(self, frame_path: Path, min_score: float) -> tuple[int, int, list[Detection]]:
        """Return image size and detections for a frame."""

        rgb = self._load_rgb(frame_path)
        height, width = rgb.shape[0], rgb.shape[1]
        gray = np.mean(rgb, axis=2)

        detections: list[Detection] = []

        bright_mask = gray >= self.bright_threshold
        bright_bbox = self._bbox_from_mask(bright_mask)
        if bright_bbox is not None:
            score = self._score_from_mask(bright_mask)
            if score >= min_score:
                detections.append(Detection(label="bright_region", score=score, bbox=bright_bbox))

        dark_mask = gray <= self.dark_threshold
        dark_bbox = self._bbox_from_mask(dark_mask)
        if dark_bbox is not None:
            score = self._score_from_mask(dark_mask)
            if score >= min_score:
                detections.append(Detection(label="dark_region", score=score, bbox=dark_bbox))

        return width, height, detections
