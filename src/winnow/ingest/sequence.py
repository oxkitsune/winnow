"""Helpers for parsing and validating frame sequences."""

from __future__ import annotations

from pathlib import Path
import re

_FRAME_PATTERN = re.compile(r"^image_(?P<idx>\d+)(?:\.[A-Za-z0-9]+)?$")


def parse_frame_index(path: Path) -> int | None:
    """Extract frame index from a filename like image_123.jpg."""

    match = _FRAME_PATTERN.match(path.name)
    if match is None:
        return None
    return int(match.group("idx"))


def find_missing_indices(indices: list[int]) -> list[int]:
    """Find missing indices in a sorted integer sequence."""

    if not indices:
        return []
    missing: list[int] = []
    prev = indices[0]
    for idx in indices[1:]:
        if idx > prev + 1:
            missing.extend(range(prev + 1, idx))
        prev = idx
    return missing
