"""Example Winnow pipeline config."""

from winnow.config.schema import (
    AnnotationConfig,
    BlurMetricConfig,
    DarknessMetricConfig,
    DuplicateMetricConfig,
    IdleMetricConfig,
    IngestConfig,
    PipelineConfig,
)


IMAGE_DIR = "/mnt/storage/gijs/data/htwk_beijing/extracted/G4/GP4_Hephaestus_Sydney_2025-08-15-16-54-11_out/temp/GP4_Hephaestus_Sydney_2025-08-15-16-54-11_out/"

PIPELINE = PipelineConfig(
    ingest=IngestConfig(
        root=IMAGE_DIR,
        glob="*",
        strict_sequence=False,
    ),
    blur=BlurMetricConfig(
        method="laplacian_var",
        threshold=400.0,
    ),
    darkness=DarknessMetricConfig(
        percentile=0.1,
        threshold=25.0,
    ),
    duplicate=DuplicateMetricConfig(
        enabled=True,
        phash_hamming_threshold=6,
        embedding_model="clip-vit-large-patch14",
        ann_index="faiss_ivf",
    ),
    idle=IdleMetricConfig(
        enabled=True,
        motion_threshold=0.02,
        smoothing_window=5,
        min_run=8,
    ),
    annotation=AnnotationConfig(
        enabled=False,
        detector="d-fine",
        segmenter="sam2",
        min_score=0.25,
    ),
    batch_size=512,
)
