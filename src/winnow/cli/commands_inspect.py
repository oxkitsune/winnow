"""`winnow inspect` command group."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Annotated

import tyro

from winnow.ingest.scanner import scan_stream
from winnow.storage.atomic import read_json
from winnow.storage.state_store import DEFAULT_STATE_ROOT, ensure_state_layout


@dataclass(slots=True)
class InspectJobCommand:
    """Inspect one job record from the state store."""

    id: Annotated[str, tyro.conf.arg(prefix_name=False)]
    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT


@dataclass(slots=True)
class InspectStreamCommand:
    """Inspect one stream folder and sequence health."""

    path: Annotated[Path, tyro.conf.arg(prefix_name=False)]
    strict_sequence: Annotated[bool, tyro.conf.arg(prefix_name=False)] = False


InspectSubcommand = Annotated[
    InspectJobCommand,
    tyro.conf.subcommand(name="job", prefix_name=False),
] | Annotated[
    InspectStreamCommand,
    tyro.conf.subcommand(name="stream", prefix_name=False),
]


@dataclass(slots=True)
class InspectCommand:
    """Inspect job or stream state."""

    command: InspectSubcommand


def execute(command: InspectCommand) -> None:
    sub = command.command
    if isinstance(sub, InspectJobCommand):
        paths = ensure_state_layout(sub.state_root)
        job_path = paths.jobs / f"{sub.id}.json"
        if not job_path.exists():
            raise FileNotFoundError(f"Unknown job id: {sub.id}")
        print(json.dumps(read_json(job_path), indent=2, sort_keys=True))
        return

    if isinstance(sub, InspectStreamCommand):
        scan = scan_stream(sub.path, strict_sequence=sub.strict_sequence)
        payload = {
            "path": str(sub.path.resolve()),
            "frame_count": scan.frame_count,
            "missing_indices": scan.missing_indices,
            "first_frame": scan.frames[0].frame_idx if scan.frames else None,
            "last_frame": scan.frames[-1].frame_idx if scan.frames else None,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    raise TypeError(f"Unsupported inspect command type: {type(sub).__name__}")
