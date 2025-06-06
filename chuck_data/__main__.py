#!/usr/bin/env python3
"""Entry point for Chuck TUI."""

import argparse
import os

from chuck_data.logger import setup_logging
from chuck_data.ui.tui import ChuckTUI
from chuck_data.version import __version__


def setup_arg_parser() -> argparse.ArgumentParser:
    """Create and return the CLI argument parser."""
    parser = argparse.ArgumentParser(prog="chuck-data")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show program version and exit.",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable color output.")
    return parser


def main(argv: list[str] | None = None) -> None:
    """Main entry point for Chuck TUI."""
    setup_logging()

    parser = setup_arg_parser()
    args = parser.parse_args(argv)

    # Check for NO_COLOR environment variable (standard convention)
    no_color_env = os.environ.get("NO_COLOR", "").lower() in ("1", "true", "yes")
    no_color = args.no_color or no_color_env

    # Initialize and run the TUI
    tui = ChuckTUI(no_color=no_color)
    try:
        tui.run()
    except EOFError:
        print("Thank you for using chuck!")


if __name__ == "__main__":
    main()
