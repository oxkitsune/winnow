"""Pipeline stage ordering and registry selection."""

from __future__ import annotations

from winnow.config.schema import PipelineConfig
from winnow.metrics import blur, darkness
from winnow.pipeline.stage import StageDefinition


def default_stage_sequence(config: PipelineConfig) -> list[StageDefinition]:
    """Return the default v1 stage sequence."""

    return [
        StageDefinition(
            name="blur",
            version="v1",
            config=config.blur,
            runner=blur.run,
        ),
        StageDefinition(
            name="darkness",
            version="v1",
            config=config.darkness,
            runner=darkness.run,
        ),
    ]
