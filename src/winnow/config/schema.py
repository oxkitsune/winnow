"""Dataclass-based configuration schema for Winnow."""

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True)
class IngestConfig:
    """Input stream ingestion options."""

    root: str
    glob: str = "*"
    strict_sequence: bool = True


@dataclass(slots=True)
class BlurMetricConfig:
    """Blur metric options."""

    method: Literal["laplacian_var"] = "laplacian_var"
    threshold: float = 80.0


@dataclass(slots=True)
class DarknessMetricConfig:
    """Darkness metric options."""

    percentile: float = 0.1
    threshold: float = 25.0


@dataclass(slots=True)
class DuplicateMetricConfig:
    """Duplicate detection options."""

    enabled: bool = True
    phash_hamming_threshold: int = 6
    embedding_model: str = "clip-vit-large-patch14"
    ann_index: Literal["faiss_flat", "faiss_ivf"] = "faiss_ivf"


@dataclass(slots=True)
class IdleMetricConfig:
    """Idle period detection options."""

    enabled: bool = True
    motion_threshold: float = 0.02
    smoothing_window: int = 5
    min_run: int = 8


@dataclass(slots=True)
class AnnotationConfig:
    """Automatic annotation options."""

    enabled: bool = False
    detector: str = "d-fine"
    segmenter: str = "sam2"
    min_score: float = 0.25


@dataclass(slots=True)
class PipelineConfig:
    """Top-level pipeline configuration."""

    ingest: IngestConfig
    blur: BlurMetricConfig = field(default_factory=BlurMetricConfig)
    darkness: DarknessMetricConfig = field(default_factory=DarknessMetricConfig)
    duplicate: DuplicateMetricConfig = field(default_factory=DuplicateMetricConfig)
    idle: IdleMetricConfig = field(default_factory=IdleMetricConfig)
    annotation: AnnotationConfig = field(default_factory=AnnotationConfig)
    batch_size: int = 512
