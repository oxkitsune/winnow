"""`winnow submit` command."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from winnow.config.loader import load_pipeline_config
from winnow.ingest.scanner import scan_stream
from winnow.storage.queue import enqueue_job
from winnow.storage.state_store import DEFAULT_STATE_ROOT, ensure_state_layout


@dataclass(slots=True)
class SubmitCommand:
    """Submit a job to the local filesystem queue."""

    input: Path
    config: str | None = None
    state_root: Path = DEFAULT_STATE_ROOT
    strict_sequence: bool = True


def execute(command: SubmitCommand) -> None:
    cfg = load_pipeline_config(
        config_ref=command.config,
        input_path=command.input,
        strict_sequence=command.strict_sequence,
    )
    scan = scan_stream(command.input, strict_sequence=command.strict_sequence)
    paths = ensure_state_layout(command.state_root)
    payload = {
        "input": str(command.input.resolve()),
        "frame_count": scan.frame_count,
        "missing_indices": scan.missing_indices,
        "config": asdict(cfg),
    }
    job_id = enqueue_job(paths, payload)
    print(f"submitted job_id={job_id} pending={paths.queue_pending / f'{job_id}.json'}")
