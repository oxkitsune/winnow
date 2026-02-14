"""`winnow export` command."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ExportCommand:
    """Export job artifacts to a target format (placeholder)."""

    job_id: str
    out: Path
    format: str = "coco"


def execute(command: ExportCommand) -> None:
    command.out.mkdir(parents=True, exist_ok=True)
    print(
        f"export placeholder job_id={command.job_id} format={command.format} out={command.out}"
    )
