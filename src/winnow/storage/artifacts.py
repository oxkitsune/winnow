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


def _validate_jsonl_lines(blob: bytes, path: Path) -> int:
    record_count = 0
    for raw_line in blob.splitlines():
        text = raw_line.decode("utf-8").strip()
        if not text:
            continue
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise TypeError(f"Artifact line is not a JSON object: {path}")
        record_count += 1
    return record_count


def verify_jsonl_artifact(meta: dict[str, Any]) -> dict[str, Any]:
    """Verify integrity of a JSONL artifact metadata envelope.

    Raises if checks fail and returns a compact verification report otherwise.
    """

    uri = meta.get("uri")
    artifact_id = meta.get("artifact_id")
    expected_bytes = int(meta.get("bytes", 0))
    expected_records = int(meta.get("record_count", 0))

    if not isinstance(uri, str) or not uri:
        raise ValueError("Artifact metadata missing URI.")
    if not isinstance(artifact_id, str) or len(artifact_id) != 64:
        raise ValueError("Artifact metadata missing valid SHA256 artifact_id.")

    path = Path(uri)
    if not path.exists():
        raise FileNotFoundError(f"Artifact URI not found: {path}")

    blob = path.read_bytes()
    digest = sha256(blob).hexdigest()
    if digest != artifact_id:
        raise ValueError(
            f"Artifact digest mismatch for {path}: expected={artifact_id} got={digest}"
        )

    actual_bytes = len(blob)
    if actual_bytes != expected_bytes:
        raise ValueError(
            f"Artifact byte-size mismatch for {path}: expected={expected_bytes} got={actual_bytes}"
        )

    actual_records = _validate_jsonl_lines(blob, path)
    if actual_records != expected_records:
        raise ValueError(
            f"Artifact record-count mismatch for {path}: expected={expected_records} got={actual_records}"
        )

    return {
        "artifact_id": artifact_id,
        "bytes": actual_bytes,
        "record_count": actual_records,
    }


def read_jsonl_artifact(path: Path) -> list[dict[str, Any]]:
    """Read JSONL artifact records from disk."""

    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise TypeError(f"Artifact line is not a JSON object: {path}")
            records.append(payload)
    return records
