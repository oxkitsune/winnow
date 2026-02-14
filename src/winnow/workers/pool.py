"""Worker pool helper utilities."""

from __future__ import annotations

import os


def normalize_worker_count(requested: int | None) -> int:
    """Return a safe worker count for local multiprocessing."""

    if requested is None:
        return 1
    workers = max(1, int(requested))
    cpu = os.cpu_count() or 1
    return min(workers, cpu)
