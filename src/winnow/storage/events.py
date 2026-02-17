"""Append-only operational event journal and maintenance helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterator

from winnow.storage.state_store import StatePaths


_DAILY_PREFIX = "events-"
_COMPACT_PREFIX = "compact-"
_JSONL_SUFFIX = ".jsonl"


def _parse_daily_date(path: Path) -> date | None:
    stem = path.stem
    if not stem.startswith(_DAILY_PREFIX):
        return None
    value = stem[len(_DAILY_PREFIX) :]
    if len(value) != 8 or not value.isdigit():
        return None
    return datetime.strptime(value, "%Y%m%d").date()  # noqa: DTZ007


def _daily_files(paths: StatePaths) -> list[Path]:
    files: list[Path] = []
    for candidate in paths.events.glob(f"{_DAILY_PREFIX}*{_JSONL_SUFFIX}"):
        if _parse_daily_date(candidate) is not None:
            files.append(candidate)
    return sorted(files)


def _compact_dir(paths: StatePaths) -> Path:
    directory = paths.events / "compact"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def append_event(paths: StatePaths, payload: dict[str, Any]) -> Path:
    """Append a JSON event line to today's journal file."""

    now = datetime.now(timezone.utc)
    out_path = paths.events / f"{_DAILY_PREFIX}{now.strftime('%Y%m%d')}{_JSONL_SUFFIX}"

    envelope: dict[str, Any] = {
        "ts": now.isoformat(),
        **payload,
    }
    if "correlation_id" not in envelope:
        job_id = envelope.get("job_id")
        if isinstance(job_id, str):
            envelope["correlation_id"] = job_id

    with out_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(envelope, sort_keys=True) + "\n")
    return out_path


def iter_event_files(paths: StatePaths) -> Iterator[Path]:
    """Yield all event files in deterministic order (compacted first)."""

    compact_dir = paths.events / "compact"
    if compact_dir.exists():
        for compact_path in sorted(
            compact_dir.glob(f"{_COMPACT_PREFIX}*{_JSONL_SUFFIX}")
        ):
            yield compact_path

    for daily_path in _daily_files(paths):
        yield daily_path


def iter_events(paths: StatePaths) -> Iterator[dict[str, Any]]:
    """Iterate parsed events from all compacted and active journals."""

    for event_file in iter_event_files(paths):
        with event_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    raise TypeError(f"Event line is not a JSON object: {event_file}")
                yield payload


def compact_events(paths: StatePaths, *, keep_recent_days: int = 1) -> dict[str, Any]:
    """Compact older daily journals into one archived file.

    Args:
        keep_recent_days: Number of newest daily files to keep un-compacted.
    """

    if keep_recent_days < 0:
        raise ValueError(f"keep_recent_days must be >= 0, got {keep_recent_days}")

    daily = _daily_files(paths)
    if not daily:
        return {
            "compacted": False,
            "compacted_file": None,
            "compacted_files": [],
            "kept_files": [],
            "event_count": 0,
        }

    split_index = max(0, len(daily) - keep_recent_days)
    to_compact = daily[:split_index]
    kept = daily[split_index:]

    if not to_compact:
        return {
            "compacted": False,
            "compacted_file": None,
            "compacted_files": [],
            "kept_files": [str(path) for path in kept],
            "event_count": 0,
        }

    compact_dir = _compact_dir(paths)
    first_date = _parse_daily_date(to_compact[0])
    last_date = _parse_daily_date(to_compact[-1])
    if first_date is None or last_date is None:
        raise RuntimeError("Could not infer compact file date range.")

    compact_name = (
        f"{_COMPACT_PREFIX}{first_date.strftime('%Y%m%d')}"
        f"-{last_date.strftime('%Y%m%d')}{_JSONL_SUFFIX}"
    )
    compact_path = compact_dir / compact_name

    event_count = 0
    fd, tmp_name = tempfile.mkstemp(prefix=f".{compact_name}.", dir=str(compact_dir))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out_handle:
            for source in to_compact:
                with source.open("r", encoding="utf-8") as in_handle:
                    for line in in_handle:
                        text = line.strip()
                        if not text:
                            continue
                        out_handle.write(text + "\n")
                        event_count += 1

        os.replace(tmp_name, compact_path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)

    for source in to_compact:
        source.unlink()

    return {
        "compacted": True,
        "compacted_file": str(compact_path),
        "compacted_files": [str(path) for path in to_compact],
        "kept_files": [str(path) for path in kept],
        "event_count": event_count,
    }
