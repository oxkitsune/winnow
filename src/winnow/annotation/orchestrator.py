"""Annotation orchestration stage."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from winnow.annotation.adapters.d_fine import DFineAdapter
from winnow.annotation.adapters.sam2 import Sam2Adapter
from winnow.annotation.schema import Detection, FrameAnnotation
from winnow.config.schema import AnnotationConfig
from winnow.pipeline.stage import BatchInput


def _resolve_detector(name: str) -> DFineAdapter:
    normalized = name.strip().lower()
    if normalized in {"d-fine", "dfine", "d_fine"}:
        return DFineAdapter()
    raise ValueError(f"Unsupported detector adapter: {name}")


def _resolve_segmenter(name: str) -> Sam2Adapter | None:
    normalized = name.strip().lower()
    if normalized in {"", "none", "off", "disabled"}:
        return None
    if normalized in {"sam2", "sam-2", "sam_2"}:
        return Sam2Adapter()
    raise ValueError(f"Unsupported segmenter adapter: {name}")


def _annotate_frame(
    *,
    detector: DFineAdapter,
    segmenter: Sam2Adapter | None,
    frame_path: Path,
    frame_idx: int,
    min_score: float,
) -> FrameAnnotation:
    width, height, detections = detector.detect(frame_path=frame_path, min_score=min_score)

    final_detections: list[Detection] = []
    for item in detections:
        if segmenter is None:
            final_detections.append(item)
            continue
        final_detections.append(segmenter.segment(item, image_width=width, image_height=height))

    return FrameAnnotation(
        frame_idx=frame_idx,
        frame_path=str(frame_path),
        width=width,
        height=height,
        annotations=final_detections,
    )


def run(batch: BatchInput, config: AnnotationConfig) -> list[dict[str, Any]]:
    """Generate annotations for each frame in a batch."""

    detector = _resolve_detector(config.detector)
    segmenter = _resolve_segmenter(config.segmenter)

    records: list[dict[str, Any]] = []
    for frame in batch.frames:
        annotation = _annotate_frame(
            detector=detector,
            segmenter=segmenter,
            frame_path=frame.path,
            frame_idx=frame.frame_idx,
            min_score=config.min_score,
        )
        records.append(annotation.to_record())
    return records
