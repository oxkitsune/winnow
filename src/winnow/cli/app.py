"""Tyro CLI application entrypoint."""

from __future__ import annotations

from typing import Annotated

import tyro

from winnow.cli import (
    commands_export,
    commands_gateway,
    commands_inspect,
    commands_run,
    commands_submit,
)


TopLevelCommand = Annotated[
    commands_run.RunCommand,
    tyro.conf.subcommand(name="run"),
] | Annotated[
    commands_submit.SubmitCommand,
    tyro.conf.subcommand(name="submit"),
] | Annotated[
    commands_inspect.InspectCommand,
    tyro.conf.subcommand(name="inspect"),
] | Annotated[
    commands_gateway.GatewayCommand,
    tyro.conf.subcommand(name="gateway"),
] | Annotated[
    commands_export.ExportCommand,
    tyro.conf.subcommand(name="export"),
]


def dispatch(command: TopLevelCommand) -> None:
    """Dispatch parsed top-level command object."""

    if isinstance(command, commands_run.RunCommand):
        commands_run.execute(command)
        return
    if isinstance(command, commands_submit.SubmitCommand):
        commands_submit.execute(command)
        return
    if isinstance(command, commands_inspect.InspectCommand):
        commands_inspect.execute(command)
        return
    if isinstance(command, commands_gateway.GatewayCommand):
        commands_gateway.execute(command)
        return
    if isinstance(command, commands_export.ExportCommand):
        commands_export.execute(command)
        return
    raise TypeError(f"Unsupported command type: {type(command).__name__}")


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments and run requested command."""

    command = tyro.cli(TopLevelCommand, args=argv)
    dispatch(command)
