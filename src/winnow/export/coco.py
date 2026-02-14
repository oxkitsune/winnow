"""COCO export adapter."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from winnow.storage.artifacts import read_jsonl_artifact
from winnow.storage.atomic import atomic_write_json


def _annotation_records_from_job(job: dict[str, Any]) -> list[dict[str, Any]]:
    stage_outputs = job.get("stage_outputs", {})
    annotation_output = stage_outputs.get("annotation", {})
    artifact = annotation_output.get("artifact", {})
    uri = artifact.get("uri")
    if not isinstance(uri, str):
        raise RuntimeError("Job has no annotation global artifact to export.")
    artifact_path = Path(uri)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Annotation artifact not found: {artifact_path}")
    return read_jsonl_artifact(artifact_path)


def export_job_annotations(job: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    """Export one job's annotation outputs to COCO JSON."""

    records = _annotation_records_from_job(job)
    records.sort(key=lambda item: int(item.get("frame_idx", 0)))

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "annotations.coco.json"

    image_id_by_frame_idx: dict[int, int] = {}
    images: list[dict[str, Any]] = []
    categories: list[dict[str, Any]] = []
    annotations: list[dict[str, Any]] = []

    category_id_by_label: dict[str, int] = {}

    for image_id, record in enumerate(records, start=1):
        frame_idx = int(record.get("frame_idx", image_id - 1))
        frame_path = str(record.get("frame_path", ""))
        width = int(record.get("width", 0))
        height = int(record.get("height", 0))

        image_id_by_frame_idx[frame_idx] = image_id
        images.append(
            {
                "id": image_id,
                "frame_idx": frame_idx,
                "file_name": Path(frame_path).name if frame_path else f"frame_{frame_idx}",
                "path": frame_path,
                "width": width,
                "height": height,
            }
        )

    annotation_id = 1
    for record in records:
        frame_idx = int(record.get("frame_idx", 0))
        image_id = image_id_by_frame_idx[frame_idx]
        payloads = record.get("annotations", [])
        if not isinstance(payloads, list):
            continue

        for payload in payloads:
            if not isinstance(payload, dict):
                continue

            label = payload.get("label")
            if not isinstance(label, str) or not label:
                continue

            if label not in category_id_by_label:
                category_id_by_label[label] = len(category_id_by_label) + 1
                categories.append({"id": category_id_by_label[label], "name": label})

            bbox_raw = payload.get("bbox")
            if not (isinstance(bbox_raw, list) and len(bbox_raw) == 4):
                continue

            x, y, w, h = [float(value) for value in bbox_raw]
            area = max(0.0, w) * max(0.0, h)

            ann: dict[str, Any] = {
                "id": annotation_id,
                "image_id": image_id,
                "category_id": category_id_by_label[label],
                "bbox": [x, y, w, h],
                "area": area,
                "iscrowd": 0,
            }

            score = payload.get("score")
            if isinstance(score, (int, float)):
                ann["score"] = float(score)

            segmentation = payload.get("segmentation")
            if isinstance(segmentation, list) and len(segmentation) >= 6:
                ann["segmentation"] = [
                    [float(value) for value in segmentation]
                ]

            annotations.append(ann)
            annotation_id += 1

    coco_payload = {
        "info": {
            "description": "Winnow annotations export",
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "job_id": job.get("job_id"),
        },
        "images": images,
        "annotations": annotations,
        "categories": categories,
    }

    atomic_write_json(out_path, coco_payload)
    return {
        "path": str(out_path.resolve()),
        "image_count": len(images),
        "annotation_count": len(annotations),
        "category_count": len(categories),
    }
