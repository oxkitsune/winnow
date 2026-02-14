"""`winnow export` command."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from winnow.export.coco import export_job_annotations
from winnow.storage.atomic import read_json
from winnow.storage.state_store import DEFAULT_STATE_ROOT, ensure_state_layout


@dataclass(slots=True)
class ExportCommand:
    """Export job artifacts to a target format."""

    job_id: str
    out: Path
    format: str = "coco"
    state_root: Path = DEFAULT_STATE_ROOT


def execute(command: ExportCommand) -> None:
    paths = ensure_state_layout(command.state_root)
    job_path = paths.jobs / f"{command.job_id}.json"
    if not job_path.exists():
        raise FileNotFoundError(f"Unknown job id: {command.job_id}")

    job = read_json(job_path)
    fmt = command.format.strip().lower()
    if fmt != "coco":
        raise ValueError(f"Unsupported export format: {command.format}. Only 'coco' is available.")

    result = export_job_annotations(job=job, out_dir=command.out)
    print(
        f"exported job_id={command.job_id} format=coco path={result['path']} "
        f"images={result['image_count']} annotations={result['annotation_count']} "
        f"categories={result['category_count']}"
    )
