"""Load pipeline configs from Python references."""

from __future__ import annotations

from dataclasses import is_dataclass
import importlib
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

from winnow.config.schema import (
    AnnotationConfig,
    BlurMetricConfig,
    DarknessMetricConfig,
    DuplicateMetricConfig,
    IngestConfig,
    PipelineConfig,
)


def _load_module(module_ref: str) -> ModuleType:
    path_candidate = Path(module_ref).expanduser()
    if path_candidate.exists():
        module_name = f"_winnow_cfg_{path_candidate.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path_candidate)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load module from path: {path_candidate}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return importlib.import_module(module_ref)


def _resolve_attr(obj: Any, attr_path: str) -> Any:
    value = obj
    for part in attr_path.split("."):
        value = getattr(value, part)
    return value


def load_object(reference: str) -> Any:
    """Load object by `module_or_path:attribute` reference."""

    if ":" not in reference:
        raise ValueError("Config reference must be in form 'module_or_path:attribute'.")
    module_ref, attr = reference.split(":", maxsplit=1)
    module = _load_module(module_ref)
    return _resolve_attr(module, attr)


def default_pipeline_config(input_path: Path, strict_sequence: bool = True) -> PipelineConfig:
    """Build a default pipeline config bound to a specific input path."""

    return PipelineConfig(
        ingest=IngestConfig(
            root=str(input_path.resolve()),
            strict_sequence=strict_sequence,
        )
    )


def load_pipeline_config(
    config_ref: str | None,
    input_path: Path,
    strict_sequence: bool = True,
) -> PipelineConfig:
    """Load a PipelineConfig from reference or create a default."""

    if config_ref is None:
        return default_pipeline_config(input_path=input_path, strict_sequence=strict_sequence)

    loaded = load_object(config_ref)
    if not isinstance(loaded, PipelineConfig):
        type_name = type(loaded).__name__
        raise TypeError(
            f"Config reference must resolve to PipelineConfig, got {type_name}."
        )
    if not is_dataclass(loaded):
        raise TypeError("Loaded config is not a dataclass instance.")

    # Ensure CLI input remains the source of truth for stream root.
    loaded.ingest.root = str(input_path.resolve())
    loaded.ingest.strict_sequence = strict_sequence
    return loaded


def pipeline_config_from_dict(payload: dict[str, Any]) -> PipelineConfig:
    """Reconstruct a PipelineConfig from a plain dictionary."""

    ingest = payload.get("ingest", {})
    blur = payload.get("blur", {})
    darkness = payload.get("darkness", {})
    duplicate = payload.get("duplicate", {})
    annotation = payload.get("annotation", {})
    return PipelineConfig(
        ingest=IngestConfig(**ingest),
        blur=BlurMetricConfig(**blur),
        darkness=DarknessMetricConfig(**darkness),
        duplicate=DuplicateMetricConfig(**duplicate),
        annotation=AnnotationConfig(**annotation),
        batch_size=int(payload.get("batch_size", 512)),
    )
