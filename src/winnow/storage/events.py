"""Append-only operational event journal."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any

from winnow.storage.state_store import StatePaths


def append_event(paths: StatePaths, payload: dict[str, Any]) -> Path:
    """Append a JSON event line to today's journal file."""

    now = datetime.now(timezone.utc)
    out_path = paths.events / f"events-{now.strftime('%Y%m%d')}.jsonl"
    envelope: dict[str, Any] = {
        "ts": now.isoformat(),
        **payload,
    }
    with out_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(envelope, sort_keys=True) + "\n")
    return out_path
