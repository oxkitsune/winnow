"""Annotation schema models and conversion helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Detection:
    """Single detection output for one frame."""

    label: str
    score: float
    bbox: list[float]
    segmentation: list[float] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "label": self.label,
            "score": float(self.score),
            "bbox": [float(x) for x in self.bbox],
        }
        if self.segmentation is not None:
            payload["segmentation"] = [float(x) for x in self.segmentation]
        return payload


@dataclass(slots=True)
class FrameAnnotation:
    """All detections for one frame."""

    frame_idx: int
    frame_path: str
    width: int
    height: int
    annotations: list[Detection] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "frame_idx": int(self.frame_idx),
            "frame_path": self.frame_path,
            "width": int(self.width),
            "height": int(self.height),
            "annotations": [item.to_dict() for item in self.annotations],
        }
