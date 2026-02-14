"""Artifact storage helpers."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from winnow.storage.atomic import atomic_write_bytes, atomic_write_json


def _jsonl_blob(records: list[dict[str, Any]]) -> bytes:
    lines = [json.dumps(record, sort_keys=True) for record in records]
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")


def store_jsonl_artifact(
    artifact_root: Path,
    *,
    stage_name: str,
    stream_id: str,
    batch_start: int,
    batch_end: int,
    cache_key: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist stage output records as a content-addressed JSONL artifact."""

    blob = _jsonl_blob(records)
    digest = sha256(blob).hexdigest()

    artifact_dir = artifact_root / "sha256" / digest[:2] / digest
    artifact_path = artifact_dir / "artifact.jsonl"
    meta_path = artifact_dir / "artifact.meta.json"

    if not artifact_path.exists():
        atomic_write_bytes(artifact_path, blob)

    meta = {
        "artifact_id": digest,
        "artifact_type": "jsonl",
        "stage": stage_name,
        "stream_id": stream_id,
        "batch_start": batch_start,
        "batch_end": batch_end,
        "cache_key": cache_key,
        "record_count": len(records),
        "bytes": len(blob),
        "uri": str(artifact_path.resolve()),
    }
    atomic_write_json(meta_path, meta)
    return meta
