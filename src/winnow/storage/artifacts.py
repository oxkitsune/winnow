"""Artifact storage helpers."""

from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable, Iterator

from winnow.storage.atomic import atomic_move, atomic_write_json


def store_jsonl_artifact(
    artifact_root: Path,
    *,
    stage_name: str,
    stream_id: str,
    batch_start: int,
    batch_end: int,
    cache_key: str,
    records: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Persist stage output records as a content-addressed JSONL artifact."""

    tmp_dir = artifact_root / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".artifact.jsonl.", dir=str(tmp_dir))
    tmp_path = Path(tmp_name)

    hasher = sha256()
    record_count = 0
    total_bytes = 0
    try:
        with os.fdopen(fd, "wb") as handle:
            for record in records:
                line = json.dumps(record, sort_keys=True).encode("utf-8") + b"\n"
                handle.write(line)
                hasher.update(line)
                total_bytes += len(line)
                record_count += 1
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    digest = hasher.hexdigest()

    artifact_dir = artifact_root / "sha256" / digest[:2] / digest
    artifact_path = artifact_dir / "artifact.jsonl"
    meta_path = artifact_dir / "artifact.meta.json"

    if not artifact_path.exists():
        atomic_move(tmp_path, artifact_path)
    elif tmp_path.exists():
        tmp_path.unlink()

    meta = {
        "artifact_id": digest,
        "artifact_type": "jsonl",
        "stage": stage_name,
        "stream_id": stream_id,
        "batch_start": batch_start,
        "batch_end": batch_end,
        "cache_key": cache_key,
        "record_count": record_count,
        "bytes": total_bytes,
        "uri": str(artifact_path.resolve()),
    }
    atomic_write_json(meta_path, meta)
    return meta


def iter_jsonl_artifact(path: Path) -> Iterator[dict[str, Any]]:
    """Iterate JSONL artifact records from disk."""

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise TypeError(f"Artifact line is not a JSON object: {path}")
            yield payload


def _validate_jsonl_stream(path: Path) -> tuple[str, int, int]:
    digest = sha256()
    total_bytes = 0
    record_count = 0
    with path.open("rb") as handle:
        for raw_line in handle:
            digest.update(raw_line)
            total_bytes += len(raw_line)
            text = raw_line.decode("utf-8").strip()
            if not text:
                continue
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise TypeError(f"Artifact line is not a JSON object: {path}")
            record_count += 1
    return digest.hexdigest(), total_bytes, record_count


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

    digest, actual_bytes, actual_records = _validate_jsonl_stream(path)
    if digest != artifact_id:
        raise ValueError(
            f"Artifact digest mismatch for {path}: expected={artifact_id} got={digest}"
        )

    if actual_bytes != expected_bytes:
        raise ValueError(
            f"Artifact byte-size mismatch for {path}: expected={expected_bytes} got={actual_bytes}"
        )

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

    return list(iter_jsonl_artifact(path))
