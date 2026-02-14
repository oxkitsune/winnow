"""`winnow run` command."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from winnow.config.loader import load_pipeline_config
from winnow.ingest.scanner import scan_stream
from winnow.storage.atomic import atomic_write_json
from winnow.storage.state_store import DEFAULT_ARTIFACT_ROOT, DEFAULT_STATE_ROOT, ensure_artifact_root, ensure_state_layout


@dataclass(slots=True)
class RunCommand:
    """Execute a local direct pipeline run (scaffold version)."""

    input: Path
    config: str | None = None
    state_root: Path = DEFAULT_STATE_ROOT
    artifacts_root: Path = DEFAULT_ARTIFACT_ROOT
    strict_sequence: bool = True


def execute(command: RunCommand) -> None:
    cfg = load_pipeline_config(
        config_ref=command.config,
        input_path=command.input,
        strict_sequence=command.strict_sequence,
    )
    scan = scan_stream(command.input, strict_sequence=command.strict_sequence)
    paths = ensure_state_layout(command.state_root)
    ensure_artifact_root(command.artifacts_root)

    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid4().hex
    record = {
        "job_id": job_id,
        "status": "SUCCEEDED",
        "submitted_at": now,
        "started_at": now,
        "finished_at": now,
        "mode": "direct",
        "stream": {
            "path": str(command.input.resolve()),
            "frame_count": scan.frame_count,
            "missing_indices": scan.missing_indices,
        },
        "config": asdict(cfg),
        "message": "Scaffold run complete (pipeline execution not wired yet).",
    }
    atomic_write_json(paths.jobs / f"{job_id}.json", record)
    print(f"run job_id={job_id} frames={scan.frame_count}")
