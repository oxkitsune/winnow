"""`winnow gateway` command group."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Annotated

import tyro

from winnow.gateway.daemon import GatewayDaemon, start_background, status, stop_background
from winnow.gateway.service_install import render_launchd_plist, render_systemd_unit
from winnow.gateway.tui import run_gateway_tui
from winnow.storage.events import compact_events
from winnow.storage.snapshots import rebuild_snapshot
from winnow.storage.state_store import (
    DEFAULT_STATE_ROOT,
    DEFAULT_STATE_ROOT_V2,
    ensure_state_layout,
    migrate_state_v1_to_v2,
)


@dataclass(slots=True)
class GatewayStartCommand:
    """Start the gateway daemon."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT
    poll_interval: Annotated[float, tyro.conf.arg(prefix_name=False)] = 1.0
    foreground: Annotated[bool, tyro.conf.arg(prefix_name=False)] = False
    log_file: Annotated[Path | None, tyro.conf.arg(prefix_name=False)] = None


@dataclass(slots=True)
class GatewayStopCommand:
    """Stop the gateway daemon."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT


@dataclass(slots=True)
class GatewayStatusCommand:
    """Show gateway daemon status."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT


@dataclass(slots=True)
class GatewayInstallServiceCommand:
    """Write service templates for systemd and launchd."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT
    out_dir: Annotated[Path, tyro.conf.arg(prefix_name=False)] = Path(".winnow/service")
    name: Annotated[str, tyro.conf.arg(prefix_name=False)] = "winnow-gateway"
    poll_interval: Annotated[float, tyro.conf.arg(prefix_name=False)] = 1.0


@dataclass(slots=True)
class GatewaySnapshotCommand:
    """Rebuild a state snapshot from jobs/checkpoints/events."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT
    name: Annotated[str, tyro.conf.arg(prefix_name=False)] = "state-latest.json"


@dataclass(slots=True)
class GatewayCompactEventsCommand:
    """Compact older event journals into archived files."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT
    keep_recent_days: Annotated[int, tyro.conf.arg(prefix_name=False)] = 1


@dataclass(slots=True)
class GatewayMigrateStateCommand:
    """Copy filesystem state from v1 into a v2 target root."""

    source_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT
    target_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT_V2
    force: Annotated[bool, tyro.conf.arg(prefix_name=False)] = False


@dataclass(slots=True)
class GatewayTuiCommand:
    """Run an interactive monitor for gateway and jobs."""

    state_root: Annotated[Path, tyro.conf.arg(prefix_name=False)] = DEFAULT_STATE_ROOT
    refresh_interval: Annotated[float, tyro.conf.arg(prefix_name=False)] = 1.0
    poll_interval: Annotated[float, tyro.conf.arg(prefix_name=False)] = 1.0
    log_file: Annotated[Path | None, tyro.conf.arg(prefix_name=False)] = None
    show_all_jobs: Annotated[bool, tyro.conf.arg(prefix_name=False)] = False
    events_limit: Annotated[int, tyro.conf.arg(prefix_name=False)] = 200


GatewaySubcommand = Annotated[
    GatewayStartCommand,
    tyro.conf.subcommand(name="start", prefix_name=False),
] | Annotated[
    GatewayStopCommand,
    tyro.conf.subcommand(name="stop", prefix_name=False),
] | Annotated[
    GatewayStatusCommand,
    tyro.conf.subcommand(name="status", prefix_name=False),
] | Annotated[
    GatewayInstallServiceCommand,
    tyro.conf.subcommand(name="install-service", prefix_name=False),
] | Annotated[
    GatewaySnapshotCommand,
    tyro.conf.subcommand(name="snapshot", prefix_name=False),
] | Annotated[
    GatewayCompactEventsCommand,
    tyro.conf.subcommand(name="compact-events", prefix_name=False),
] | Annotated[
    GatewayMigrateStateCommand,
    tyro.conf.subcommand(name="migrate-state", prefix_name=False),
] | Annotated[
    GatewayTuiCommand,
    tyro.conf.subcommand(name="tui", prefix_name=False),
]


@dataclass(slots=True)
class GatewayCommand:
    """Manage the local gateway daemon."""

    command: GatewaySubcommand


def execute(command: GatewayCommand) -> None:
    sub = command.command

    if isinstance(sub, GatewayStartCommand):
        ensure_state_layout(sub.state_root)
        current = status(sub.state_root)
        if current["alive"]:
            pid = current["pid"]
            print(f"gateway already running pid={pid}")
            return
        if sub.foreground:
            daemon = GatewayDaemon(state_root=sub.state_root, poll_interval=sub.poll_interval)
            daemon.run_forever()
            return
        pid = start_background(
            state_root=sub.state_root,
            poll_interval=sub.poll_interval,
            log_file=sub.log_file,
        )
        print(f"gateway started pid={pid}")
        return

    if isinstance(sub, GatewayStopCommand):
        stopped = stop_background(sub.state_root)
        print("gateway stopped" if stopped else "gateway not running")
        return

    if isinstance(sub, GatewayStatusCommand):
        print(json.dumps(status(sub.state_root), indent=2, sort_keys=True))
        return

    if isinstance(sub, GatewayInstallServiceCommand):
        out_dir = sub.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        systemd = render_systemd_unit(
            service_name=sub.name,
            cwd=Path.cwd(),
            state_root=sub.state_root,
            poll_interval=sub.poll_interval,
        )
        launchd = render_launchd_plist(
            label=sub.name,
            cwd=Path.cwd(),
            state_root=sub.state_root,
            poll_interval=sub.poll_interval,
        )
        systemd_path = out_dir / f"{sub.name}.service"
        launchd_path = out_dir / f"{sub.name}.plist"
        systemd_path.write_text(systemd, encoding="utf-8")
        launchd_path.write_text(launchd, encoding="utf-8")
        print(f"wrote {systemd_path}")
        print(f"wrote {launchd_path}")
        return

    if isinstance(sub, GatewaySnapshotCommand):
        paths = ensure_state_layout(sub.state_root)
        result = rebuild_snapshot(paths, name=sub.name)
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if isinstance(sub, GatewayCompactEventsCommand):
        paths = ensure_state_layout(sub.state_root)
        result = compact_events(paths, keep_recent_days=sub.keep_recent_days)
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if isinstance(sub, GatewayMigrateStateCommand):
        result = migrate_state_v1_to_v2(
            source_root=sub.source_root,
            target_root=sub.target_root,
            force=sub.force,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if isinstance(sub, GatewayTuiCommand):
        run_gateway_tui(
            state_root=sub.state_root,
            refresh_interval=sub.refresh_interval,
            poll_interval=sub.poll_interval,
            log_file=sub.log_file,
            show_all_jobs=sub.show_all_jobs,
            events_limit=sub.events_limit,
        )
        return

    raise TypeError(f"Unsupported gateway command type: {type(sub).__name__}")
