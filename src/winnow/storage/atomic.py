"""Atomic filesystem write helpers."""

from __future__ import annotations

from pathlib import Path
import json
import os
import tempfile
from typing import Any


def atomic_write_text(path: Path, content: str) -> None:
    """Write text atomically using a temp file + rename."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def atomic_write_json(path: Path, payload: Any, indent: int = 2) -> None:
    """Write JSON atomically."""

    atomic_write_text(path, json.dumps(payload, indent=indent, sort_keys=True) + "\n")


def atomic_move(src: Path, dst: Path) -> None:
    """Move a file atomically within a filesystem."""

    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)


def read_json(path: Path) -> Any:
    """Read JSON from disk."""

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
