"""Command-line interface for the TRUST probe."""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from trust_probe.analyse import analyse
from trust_probe.capture import capture


def default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("captures") / f"trust-{timestamp}.ndjson"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Network Rail TRUST feed diagnostic probe"
    )

    subcommands = parser.add_subparsers(
        dest="command",
        required=True,
    )

    capture_parser = subcommands.add_parser(
        "capture",
        help="Capture STOMP messages to a local NDJSON file",
    )
    capture_parser.add_argument(
        "--minutes",
        type=float,
        default=1.0,
        help="Capture duration in minutes (default: 1)",
    )
    capture_parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output NDJSON file",
    )

    analyse_parser = subcommands.add_parser(
        "analyse",
        help="Analyse a previously captured NDJSON file",
    )
    analyse_parser.add_argument(
        "input",
        type=Path,
        help="Capture file to analyse",
    )
    analyse_parser.add_argument(
        "--examples-output",
        type=Path,
        default=None,
        help="Optional JSON file for example events by type",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.command == "capture":
        output = args.output or default_output_path()
        capture(args.minutes, output)

    elif args.command == "analyse":
        analyse(args.input, args.examples_output)


if __name__ == "__main__":
    main()
