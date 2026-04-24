"""
Airbyte CLI entrypoint for airbyte-source-garmin.

Airbyte invokes this file directly as a subprocess and communicates via
standard streams:
  - stdout: Airbyte protocol messages (one JSON object per line, flushed immediately)
  - stderr: human-readable logs (loguru output — never mixed into stdout)
  - exit code: 0 on success, 1 on fatal error

Supported commands:
  python main.py spec
  python main.py check   --config  /secrets/config.json
  python main.py discover --config  /secrets/config.json
  python main.py read    --config  /secrets/config.json
                         --catalog /secrets/catalog.json
                         [--state  /secrets/state.json]
"""

import argparse
import json
import sys

from loguru import logger

from source_garmin.config import build_spec
from source_garmin.source import SourceGarmin


def _configure_logging() -> None:
    """Route all loguru output to stderr, leaving stdout clean for Airbyte messages.

    Airbyte reads stdout line-by-line and expects every line to be a valid
    JSON protocol message. A stray log line on stdout would break the parse.
    Removing the default loguru handler (which also writes to stderr but with
    ANSI colour codes) and replacing it with a plain one keeps CI logs readable.
    """
    logger.remove()  # remove the default coloured stderr handler
    logger.add(sys.stderr, level="INFO", colorize=False, format="{time} | {level} | {message}")


def _emit(message: dict) -> None:
    """Serialise one Airbyte protocol message to stdout as a JSON line.

    flush=True is critical: Python buffers stdout by default, so without an
    explicit flush Airbyte could wait indefinitely for records that are already
    in memory but not yet written to the pipe.

    Args:
        message: An Airbyte protocol message dict (SPEC, CATALOG, RECORD, etc.)
    """
    print(json.dumps(message, default=str), flush=True)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser with one subcommand per Airbyte command.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="source-garmin",
        description="Airbyte source connector for Garmin Connect.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    # spec — no arguments needed
    subparsers.add_parser("spec", help="Output the connector specification.")

    # check --config
    check_p = subparsers.add_parser("check", help="Test the connection.")
    check_p.add_argument("--config", required=True, metavar="PATH",
                         help="Path to the connector config JSON file.")

    # discover --config
    discover_p = subparsers.add_parser("discover", help="Output the source catalog.")
    discover_p.add_argument("--config", required=True, metavar="PATH",
                            help="Path to the connector config JSON file.")

    # read --config --catalog [--state]
    read_p = subparsers.add_parser("read", help="Read records from the source.")
    read_p.add_argument("--config",  required=True,  metavar="PATH",
                        help="Path to the connector config JSON file.")
    read_p.add_argument("--catalog", required=True,  metavar="PATH",
                        help="Path to the configured catalog JSON file.")
    read_p.add_argument("--state",   required=False, metavar="PATH",
                        help="Path to the state JSON file from a previous run.")

    return parser


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate SourceGarmin method.

    This function is the console_scripts entry point registered in setup.py.
    It is intentionally thin: argument parsing + dispatch + serialisation only.
    All business logic lives in source_garmin/.
    """
    _configure_logging()

    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)

    source = SourceGarmin()

    # ── spec ────────────────────────────────────────────────────────────
    if args.command == "spec":
        _emit(build_spec())

    # ── check ───────────────────────────────────────────────────────────
    elif args.command == "check":
        _emit(source.check(args.config))

    # ── discover ────────────────────────────────────────────────────────
    elif args.command == "discover":
        _emit(source.discover(args.config))

    # ── read ─────────────────────────────────────────────────────────────
    elif args.command == "read":
        try:
            for message in source.read(args.config, args.catalog, args.state):
                _emit(message)
        except Exception as exc:
            # Emit a LOG message so Airbyte captures the error in its UI,
            # then exit with a non-zero code so the sync is marked as failed.
            logger.error("Fatal error during read: {}", exc)
            _emit({
                "type": "LOG",
                "log": {"level": "ERROR", "message": f"Fatal error: {exc}"},
            })
            sys.exit(1)


if __name__ == "__main__":
    main()
