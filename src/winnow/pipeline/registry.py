"""Stage registry helpers."""

from __future__ import annotations

from winnow.config.schema import PipelineConfig
from winnow.pipeline.dag import default_stage_sequence
from winnow.pipeline.stage import StageDefinition


def resolve_stages(config: PipelineConfig) -> list[StageDefinition]:
    """Resolve active stages for a pipeline config."""

    return default_stage_sequence(config)
