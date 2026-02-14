"""Service definition generators for systemd and launchd."""

from __future__ import annotations

from pathlib import Path
import sys


def render_systemd_unit(service_name: str, cwd: Path, state_root: Path, poll_interval: float) -> str:
    """Return a systemd unit template string."""

    exe = sys.executable
    return f"""[Unit]
Description=Winnow Gateway Daemon ({service_name})
After=network.target

[Service]
Type=simple
WorkingDirectory={cwd}
ExecStart={exe} -m winnow.gateway.daemon --state-root {state_root} --poll-interval {poll_interval}
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
"""


def render_launchd_plist(label: str, cwd: Path, state_root: Path, poll_interval: float) -> str:
    """Return a launchd plist template string."""

    exe = sys.executable
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
    <string>-m</string>
    <string>winnow.gateway.daemon</string>
    <string>--state-root</string>
    <string>{state_root}</string>
    <string>--poll-interval</string>
    <string>{poll_interval}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>{cwd}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
</dict>
</plist>
"""
