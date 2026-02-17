"""Duplicate detection stage implementation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from PIL import Image

from winnow.config.schema import DuplicateMetricConfig
from winnow.pipeline.stage import BatchInput


@dataclass(slots=True)
class _DisjointSet:
    parent: list[int]
    rank: list[int]

    @classmethod
    def create(cls, size: int) -> "_DisjointSet":
        return cls(parent=list(range(size)), rank=[0] * size)

    def find(self, item: int) -> int:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if self.rank[root_left] < self.rank[root_right]:
            root_left, root_right = root_right, root_left
        self.parent[root_right] = root_left
        if self.rank[root_left] == self.rank[root_right]:
            self.rank[root_left] += 1


def _average_hash(path: Path) -> tuple[int, float]:
    with Image.open(path) as image:
        gray = image.convert("L").resize((8, 8), Image.Resampling.BILINEAR)
        arr = np.asarray(gray, dtype=np.float32)
    avg = float(np.mean(arr))
    bits = (arr > avg).ravel()

    value = 0
    for idx, bit in enumerate(bits):
        if bit:
            value |= 1 << idx
    return value, avg


def _hamming(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def run(batch: BatchInput, config: DuplicateMetricConfig) -> list[dict[str, Any]]:
    """Find duplicate groups within a batch using perceptual hash distance."""

    hash_and_luma = [_average_hash(frame.path) for frame in batch.frames]
    hashes = [item[0] for item in hash_and_luma]
    luma = [item[1] for item in hash_and_luma]
    total = len(batch.frames)
    dsu = _DisjointSet.create(total)
    min_distance = [64] * total

    for i in range(total):
        for j in range(i + 1, total):
            distance = _hamming(hashes[i], hashes[j])
            if distance < min_distance[i]:
                min_distance[i] = distance
            if distance < min_distance[j]:
                min_distance[j] = distance
            if (
                distance <= config.phash_hamming_threshold
                and abs(luma[i] - luma[j]) <= 16.0
            ):
                dsu.union(i, j)

    groups: dict[int, list[int]] = {}
    for idx in range(total):
        root = dsu.find(idx)
        groups.setdefault(root, []).append(idx)

    records: list[dict[str, Any]] = []
    for idx, frame in enumerate(batch.frames):
        group = groups[dsu.find(idx)]
        representative = min(group, key=lambda pos: batch.frames[pos].frame_idx)

        duplicate_group_id: str | None = None
        duplicate_rank = 0
        if len(group) > 1:
            rep_frame_idx = batch.frames[representative].frame_idx
            duplicate_group_id = (
                f"{batch.stream_id}:{batch.start_idx}-{batch.end_idx}:{rep_frame_idx}"
            )
            duplicate_rank = sorted(group).index(idx)

        confidence = 0.0
        if min_distance[idx] <= 64:
            confidence = max(0.0, 1.0 - (min_distance[idx] / 64.0))

        records.append(
            {
                "frame_idx": frame.frame_idx,
                "frame_path": str(frame.path),
                "duplicate_hash": f"{hashes[idx]:016x}",
                "mean_luma": luma[idx],
                "nearest_hamming": min_distance[idx]
                if min_distance[idx] != 64
                else None,
                "duplicate_group_id": duplicate_group_id,
                "duplicate_group_size": len(group),
                "duplicate_rank": duplicate_rank,
                "duplicate_confidence": confidence,
                "hamming_threshold": config.phash_hamming_threshold,
            }
        )

    return records


def globalize(
    records: Iterable[dict[str, Any]],
    config: DuplicateMetricConfig,
    stream_id: str,
) -> list[dict[str, Any]]:
    """Recompute duplicate grouping globally across all stream batches."""

    entries: list[tuple[int, str, str | None, int | None, float | None]] = []
    for record in records:
        frame_idx_raw = record.get("frame_idx")
        frame_idx = 0
        if isinstance(frame_idx_raw, (int, float, str)):
            try:
                frame_idx = int(frame_idx_raw)
            except ValueError:
                frame_idx = 0
        frame_path = str(record.get("frame_path", ""))

        hash_text = record.get("duplicate_hash")
        hash_value: int | None = None
        if isinstance(hash_text, str):
            try:
                hash_value = int(hash_text, 16)
            except ValueError:
                hash_value = None
        else:
            hash_text = None

        mean_luma = record.get("mean_luma")
        luma_value = float(mean_luma) if isinstance(mean_luma, (int, float)) else None
        entries.append((frame_idx, frame_path, hash_text, hash_value, luma_value))

    entries.sort(key=lambda item: item[0])
    total = len(entries)
    if total == 0:
        return []

    hashes: list[int | None] = []
    luma: list[float | None] = []
    for _, _, _, hash_value, luma_value in entries:
        hashes.append(hash_value)
        luma.append(luma_value)

    dsu = _DisjointSet.create(total)
    min_distance = [64] * total

    for i in range(total):
        hash_i = hashes[i]
        if hash_i is None:
            continue
        for j in range(i + 1, total):
            hash_j = hashes[j]
            if hash_j is None:
                continue
            distance = _hamming(hash_i, hash_j)
            if distance < min_distance[i]:
                min_distance[i] = distance
            if distance < min_distance[j]:
                min_distance[j] = distance
            luma_i = luma[i]
            luma_j = luma[j]
            luma_ok = True
            if luma_i is not None and luma_j is not None:
                luma_ok = abs(luma_i - luma_j) <= 16.0
            if distance <= config.phash_hamming_threshold and luma_ok:
                dsu.union(i, j)

    groups: dict[int, list[int]] = {}
    for idx, value in enumerate(hashes):
        if value is None:
            continue
        root = dsu.find(idx)
        groups.setdefault(root, []).append(idx)

    ordered_records: list[dict[str, Any]] = []
    for idx, (frame_idx, frame_path, hash_text, _, luma_value) in enumerate(entries):
        record: dict[str, Any] = {
            "frame_idx": frame_idx,
            "frame_path": frame_path,
            "duplicate_hash": hash_text,
            "mean_luma": luma_value,
        }
        if hashes[idx] is None:
            record["duplicate_group_id"] = None
            record["duplicate_group_size"] = 1
            record["duplicate_rank"] = 0
            record["nearest_hamming"] = None
            record["duplicate_confidence"] = 0.0
            ordered_records.append(record)
            continue

        group = groups[dsu.find(idx)]
        group_sorted = sorted(group, key=lambda pos: entries[pos][0])
        representative = group_sorted[0]
        rep_frame_idx = int(entries[representative][0])

        if len(group_sorted) > 1:
            record["duplicate_group_id"] = f"{stream_id}:{rep_frame_idx}"
            record["duplicate_group_size"] = len(group_sorted)
            record["duplicate_rank"] = group_sorted.index(idx)
        else:
            record["duplicate_group_id"] = None
            record["duplicate_group_size"] = 1
            record["duplicate_rank"] = 0

        record["nearest_hamming"] = (
            min_distance[idx] if min_distance[idx] != 64 else None
        )
        record["duplicate_confidence"] = max(0.0, 1.0 - (min_distance[idx] / 64.0))
        record["hamming_threshold"] = config.phash_hamming_threshold
        ordered_records.append(record)

    return ordered_records
