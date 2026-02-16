# Winnow

Winnow is an image-stream data preparation toolkit focused on composable, parallel pipelines with file-backed idempotency.

## What It Does

- Ingests one stream per folder (`image_0`, `image_1`, ...).
  - If no matching files are found at the top level, Winnow automatically falls
    back to recursive discovery under the input directory.
- Computes frame metrics (`blur`, `darkness`) and stream-aware analytics (`duplicates`, `idle`).
- Runs optional automatic annotation and exports COCO.
- Supports direct local runs and queue-based daemon orchestration.

## Quickstart

```bash
uv sync
uv run winnow run --input /path/to/stream
```

Example with a built-in execution profile:

```bash
uv run winnow run --input /path/to/stream --profile local-cpu
```

## CLI Overview

Direct execution:

```bash
uv run winnow run --input /path/to/stream --workers 4
```

Daemon mode:

```bash
uv run winnow gateway start
uv run winnow submit --input /path/to/stream --profile high-throughput
uv run winnow gateway status
uv run winnow gateway tui
uv run winnow gateway stop
```

Gateway Textual TUI controls:

- `q`: quit
- `Enter`: open selected job detail screen
- `Esc` or `Backspace`: return to job list
- `s`: start/stop gateway daemon
- `a`: toggle active jobs vs all jobs
- `e`: toggle events scoped to selected job vs all jobs
- `r`: refresh immediately
- `j`/`k` or arrow keys: change selected job

Inspection and export:

```bash
uv run winnow inspect job --id <job_id>
uv run winnow inspect duplicates --job-id <job_id>
uv run winnow inspect idle --job-id <job_id>
uv run winnow inspect annotations --job-id <job_id>
uv run winnow export --job-id <job_id> --out /tmp/coco --format coco
```

## Profiles

Built-in profiles:

- `local-cpu`
- `gpu-single`
- `high-throughput`

Profiles tune pipeline defaults (batch size, stage enablement) and suggested worker count.
CLI overrides still win, e.g. `--annotation-enabled True`, `--workers 1`.

## Phase-5 Operations

Snapshot rebuild:

```bash
uv run winnow gateway snapshot --state-root .winnow/state/v1
```

Event journal compaction:

```bash
uv run winnow gateway compact-events --state-root .winnow/state/v1 --keep-recent-days 1
```

Explicit state migration (no runtime DB migrations):

```bash
uv run winnow gateway migrate-state \
  --source-root .winnow/state/v1 \
  --target-root .winnow/state/v2
```

## Runtime Artifacts

- State root: `.winnow/state/v1`
- Artifacts root: `.winnow/artifacts`
- Runtime metrics: `.winnow/state/v1/runtime/metrics.json`
- State snapshot: `.winnow/state/v1/snapshots/state-latest.json`

## Logging and Integrity

- Structured JSON logs are emitted under the `winnow.*` logger namespace.
- Events include correlation IDs (`job_id`) for traceability.
- JSONL artifacts are checksum-verified before stage success checkpoints are written.
