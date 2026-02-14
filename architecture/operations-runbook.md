# Winnow Operations Runbook

## 1. Daemon Lifecycle

Start in foreground for debugging:

```bash
uv run winnow gateway start --foreground
```

Start background:

```bash
uv run winnow gateway start
```

Stop:

```bash
uv run winnow gateway stop
```

Status:

```bash
uv run winnow gateway status
```

Status output includes:

- queue depth (`pending`, `running`, `done`, `failed`)
- backlog (`pending + running`)
- worker health (`scheduler`, `active_jobs`)
- persisted runtime metrics (jobs/stages)
- latest snapshot timestamp

## 2. Queue and Job Recovery

Winnow is checkpoint-driven and idempotent.

- Stage batch results are checkpointed under `.winnow/state/v1/stages/...`.
- On rerun/restart, succeeded checkpoints with valid artifact URIs are skipped.
- Failed batches are retried with incremented attempt counts.

If a daemon exits unexpectedly:

1. Restart gateway: `uv run winnow gateway start`
2. Check status: `uv run winnow gateway status`
3. Inspect failed jobs if any:

```bash
uv run winnow inspect job --id <job_id>
```

## 3. Event Journal Maintenance

Compact old daily journals, keeping recent days un-compacted:

```bash
uv run winnow gateway compact-events --keep-recent-days 1
```

Compaction output includes compacted source files and resulting archive file.

## 4. Snapshot Rebuild

Rebuild a state snapshot from jobs/checkpoints/events:

```bash
uv run winnow gateway snapshot --name state-latest.json
```

Snapshot contains:

- queue depth
- job status counts
- checkpoint counts by stage/status
- event totals and type distribution

## 5. State Version Migration

Winnow avoids runtime schema migrations. Instead, state upgrades are explicit copy operations.

```bash
uv run winnow gateway migrate-state \
  --source-root .winnow/state/v1 \
  --target-root .winnow/state/v2
```

Use `--force` to overwrite a non-empty target root.
Migration writes:

- `<target>/VERSION` (`state/v2`)
- `<target>/migration.json` summary

## 6. Artifact Integrity Failures

Before marking stage success, artifact metadata and content are verified:

- SHA256 digest
- byte count
- JSONL record count + line validity

If integrity checks fail, checkpoint is written as `FAILED` and job fails fast.

## 7. Profile Selection Guide

Use `--profile` on `run`/`submit`:

- `local-cpu`: conservative defaults for laptops/workstations
- `gpu-single`: larger batches and annotation enabled
- `high-throughput`: aggressive batching for high parallel throughput

Override profile values with explicit CLI flags when needed.
