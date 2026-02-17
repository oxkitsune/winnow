"""Lightweight segmentation adapter that emits rectangle polygons from boxes."""

from __future__ import annotations

from dataclasses import dataclass

from winnow.annotation.schema import Detection


@dataclass(slots=True)
class Sam2Adapter:
    """Convert bbox detections to simple polygon segmentation."""

    def segment(
        self, detection: Detection, image_width: int, image_height: int
    ) -> Detection:
        x, y, w, h = detection.bbox
        x0 = max(0.0, min(float(image_width), x))
        y0 = max(0.0, min(float(image_height), y))
        x1 = max(0.0, min(float(image_width), x + w))
        y1 = max(0.0, min(float(image_height), y + h))

        polygon = [x0, y0, x1, y0, x1, y1, x0, y1]
        return Detection(
            label=detection.label,
            score=detection.score,
            bbox=[x0, y0, max(0.0, x1 - x0), max(0.0, y1 - y0)],
            segmentation=polygon,
        )
