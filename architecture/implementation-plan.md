# Winnow Implementation Plan

## 1. Scope and Execution Model

This plan implements Winnow in incremental phases, with working software at the end of each phase.
There is **no separate testing phase**; validation is embedded in each phase as acceptance checks.

This version intentionally avoids SQL and HTTP service complexity in v1.

## 2. Implementation Principles

- Build vertical slices early: CLI -> pipeline -> state -> outputs.
- Keep all stages idempotent from day one.
- Prefer simple defaults first (single machine), then add daemon orchestration.
- Treat sequential frame order as a first-class contract in ingestion and temporal stages.
- Use file-backed state with atomic writes and append-only events.
- Avoid migration machinery; use state versioned directories (`state/v1`, `state/v2`, ...).

## 3. Phase Plan

## Phase 0: Project Bootstrap (Foundation)

### Goals

- Establish package structure and dependency baseline.
- Replace placeholder entrypoint with Tyro CLI scaffold.

### Tasks

- Create module layout from the architecture doc under `src/winnow/`.
- Add core dependencies in `pyproject.toml`:
  - `tyro`
  - `opencv-python-headless`, `numpy`, `pillow`, `blake3`
  - optional `msgpack`/`orjson` for state snapshots/events
- Implement `winnow.cli.app` with command groups:
  - `run`, `submit`, `inspect`, `gateway`, `export`.
- Add initial config dataclasses in `winnow.config.schema`.

### Deliverables

- `winnow --help` and subcommand help working.
- Dataclass config objects loadable from Python module path (e.g. `pipeline.py:cfg`).

### Acceptance Checks

- CLI starts and parses nested dataclass configs.
- Stub commands execute without runtime errors.

## Phase 1: Core Local Pipeline Engine (MVP)

### Goals

- Run a local end-to-end job on one stream folder.
- Persist stage state and outputs with idempotency using file-backed checkpoints.

### Tasks

- Implement ingestion:
  - frame scanner (`image_<index>` parser), ordering, gap detection.
  - persist stream and frame manifests (`stream.json`, `frames.jsonl`).
- Implement pipeline core:
  - `Stage` protocol/interface.
  - DAG/topological execution.
  - batch partitioning by contiguous frame ranges.
- Implement stage tracking:
  - per-stage checkpoint files keyed by `(stage, stream, batch, cache_key)`.
  - deterministic cache key and skip-on-hit behavior.
- Implement initial metrics stages:
  - blur (Laplacian variance).
  - darkness (luma statistics).
- Implement local executor for `winnow run`.
- Implement `winnow inspect job --id ...`.

### Deliverables

- Local run that ingests a folder and writes metrics artifacts.
- Re-running the same job skips completed stage partitions.

### Acceptance Checks

- Job status transitions visible (`PENDING` -> `RUNNING` -> `SUCCEEDED/FAILED`).
- Stage rerun shows cache hit/skip behavior.
- Output metrics contain per-frame records for full stream.

## Phase 2: Gateway Daemon and Parallel Scheduling

### Goals

- Introduce persistent orchestrator process.
- Add parallel task execution with resource-aware scheduling.

### Tasks

- Build local gateway daemon:
  - watch filesystem queue (`pending/running/done/failed`).
  - maintain heartbeat and pid files.
- Implement scheduler loop:
  - pick eligible stage partitions.
  - dispatch to worker pool with concurrency limits.
- Implement CPU worker pool with `ProcessPoolExecutor`.
- Add queue/backpressure controls and retry policy.
- Add CLI integration:
  - `winnow gateway start|stop|status`.
  - `winnow submit ...` writes job specs into queue.
- Add service installers:
  - Linux `systemd`.
  - macOS `launchd`.

### Deliverables

- Gateway can accept, execute, and report jobs independently of CLI process lifetime.
- Multi-process throughput improvement on CPU-bound metrics stages.

### Acceptance Checks

- Multiple submitted jobs progress concurrently.
- Failed tasks retry according to policy and surface final failure reasons.
- Service install command generates valid service definitions.

## Phase 3: Duplicate and Idle Analytics

### Goals

- Add sequence-aware and dataset-level curation intelligence.

### Tasks

- Implement duplicate detection stage (v1):
  - pHash computation per frame.
  - Hamming-distance candidate grouping.
- Implement optional duplicate verification stage (v1.1):
  - CLIP embeddings.
  - FAISS ANN verification.
- Implement idle period detection:
  - frame-diff motion score.
  - sliding window smoothing.
  - idle interval extraction.
- Extend inspect/export surfaces for duplicate clusters and idle intervals.

### Deliverables

- Duplicate cluster artifacts with representative frame references.
- Idle interval artifact per stream.

### Acceptance Checks

- Duplicate stage produces stable cluster IDs across reruns with same inputs/config.
- Idle detection identifies low-motion segments with configurable thresholds.

## Phase 4: Automatic Annotation

### Goals

- Generate machine annotations from pretrained models.

### Tasks

- Define internal annotation schema (`boxes`, `labels`, `scores`, optional `masks`).
- Build adapter layer:
  - Detector adapter (Pretrained D-FINE).
  - Optional segmenter adapter (SAM 2).
- Implement annotation orchestrator stage:
  - batch inference.
  - confidence filtering.
  - artifact persistence.
- Implement export path:
  - COCO first.
- Add CLI switches for annotation enablement and thresholds.

### Deliverables

- End-to-end annotation generation and COCO export.

### Acceptance Checks

- Annotation stage runs on selected stream and outputs valid schema records.
- COCO export produces category/image/annotation sections with consistent IDs.

## Phase 5: Hardening and Operational Readiness

### Goals

- Make Winnow reliable for longer-running production-like workloads.

### Tasks

- Add structured logging and correlation IDs.
- Add operational metrics (queue depth, stage latency, retries, failures).
- Add event journal compaction + snapshot rebuild.
- Add artifact integrity checks before marking stage success.
- Add state versioning tool (`state/v1` -> `state/v2` import utility).
- Add configuration profiles (local-cpu, gpu-single, high-throughput).
- Add docs for deployment and runbooks under `architecture/` and `README.md`.

### Deliverables

- Observable gateway with actionable status output.
- Reproducible state upgrade path without runtime migrations.

### Acceptance Checks

- Gateway status exposes worker health + backlog.
- Restart/recovery from interrupted jobs works without recomputing completed partitions.

## 4. Work Breakdown by Module

- `winnow/cli`: Tyro command surfaces and UX.
- `winnow/config`: dataclass schemas and loader.
- `winnow/storage`: queue, checkpoints, manifests, event journal, snapshots, atomic writes.
- `winnow/pipeline`: stage interfaces, DAG planner, executor, cache keys.
- `winnow/gateway`: daemon loop, scheduler, lifecycle management.
- `winnow/workers`: CPU/GPU task runners.
- `winnow/ingest`: stream scanning and sequence contracts.
- `winnow/metrics`: blur, darkness, duplicates, idle.
- `winnow/annotation`: model adapters and annotation pipeline.
- `winnow/export`: COCO (and later YOLO) exporters.
- `winnow/observability`: logging and metrics hooks.

## 5. Suggested Implementation Order (First 6 Weeks)

1. Week 1: Phase 0 completion + CLI/config skeleton.
2. Week 2: File-backed state layout + ingestion + stage interfaces.
3. Week 3: Local executor + blur/darkness + inspect.
4. Week 4: Gateway daemon + scheduler + submit/status.
5. Week 5: Worker pool tuning + retries + service install.
6. Week 6: Duplicate (pHash) + idle detection baseline.

Then continue with annotation and hardening based on model/runtime priorities.

## 6. Immediate Next Tasks

1. Implement Phase 0 scaffolding in codebase.
2. Land Phase 1 file-backed checkpoints + idempotent executor core.
3. Add first two metric stages (blur/darkness) and make `winnow run` functional.
