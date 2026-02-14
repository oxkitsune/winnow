"""Winnow package entrypoint."""

from winnow.cli.app import main as _cli_main


def main() -> None:
    """Run the Winnow CLI."""
    _cli_main()
