"""Built-in configuration profiles for common execution environments."""

from __future__ import annotations

from dataclasses import dataclass

from winnow.config.schema import PipelineConfig


@dataclass(frozen=True, slots=True)
class ProfileSpec:
    """Declarative config defaults for a named runtime profile."""

    name: str
    description: str
    batch_size: int
    duplicate_enabled: bool
    idle_enabled: bool
    annotation_enabled: bool
    suggested_workers: int


_PROFILES: dict[str, ProfileSpec] = {
    "local-cpu": ProfileSpec(
        name="local-cpu",
        description="Balanced defaults for single-machine CPU execution.",
        batch_size=256,
        duplicate_enabled=True,
        idle_enabled=True,
        annotation_enabled=False,
        suggested_workers=2,
    ),
    "gpu-single": ProfileSpec(
        name="gpu-single",
        description="Larger batches tuned for one GPU-backed worker host.",
        batch_size=1024,
        duplicate_enabled=True,
        idle_enabled=True,
        annotation_enabled=True,
        suggested_workers=4,
    ),
    "high-throughput": ProfileSpec(
        name="high-throughput",
        description="Aggressive batching for larger multi-process runs.",
        batch_size=2048,
        duplicate_enabled=True,
        idle_enabled=True,
        annotation_enabled=False,
        suggested_workers=8,
    ),
}


def available_profiles() -> dict[str, ProfileSpec]:
    """Return built-in profiles by name."""

    return dict(_PROFILES)


def resolve_profile(name: str) -> ProfileSpec:
    """Resolve one profile by name."""

    key = name.strip().lower()
    profile = _PROFILES.get(key)
    if profile is None:
        known = ", ".join(sorted(_PROFILES))
        raise ValueError(f"Unknown profile '{name}'. Available profiles: {known}")
    return profile


def apply_profile(config: PipelineConfig, profile_name: str) -> ProfileSpec:
    """Apply a profile directly onto a PipelineConfig instance."""

    profile = resolve_profile(profile_name)
    config.batch_size = profile.batch_size
    config.duplicate.enabled = profile.duplicate_enabled
    config.idle.enabled = profile.idle_enabled
    config.annotation.enabled = profile.annotation_enabled
    return profile
