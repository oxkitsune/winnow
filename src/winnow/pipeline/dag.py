"""Pipeline stage ordering and registry selection."""

from __future__ import annotations

from winnow.config.schema import PipelineConfig
from winnow.metrics import blur, darkness, duplicates, idle
from winnow.pipeline.stage import StageDefinition


def default_stage_sequence(config: PipelineConfig) -> list[StageDefinition]:
    """Return the default v1 stage sequence."""

    stages = [
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
    if config.duplicate.enabled:
        stages.append(
            StageDefinition(
                name="duplicates",
                version="v1",
                config=config.duplicate,
                runner=duplicates.run,
            )
        )
    if config.idle.enabled:
        stages.append(
            StageDefinition(
                name="idle",
                version="v1",
                config=config.idle,
                runner=idle.run,
            )
        )
    return stages
