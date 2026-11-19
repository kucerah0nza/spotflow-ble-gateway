"""Command line entry point: ``spotflow-gateway`` / ``python -m spotflow_gateway``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from . import cloud
from .gateway import Gateway


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="spotflow-gateway",
        description="Bridge Spotflow BLE devices to the Spotflow cloud over MQTT.",
    )
    parser.add_argument(
        "--ingest-key",
        default=os.environ.get("SPOTFLOW_INGEST_KEY"),
        help="Spotflow ingest key (default: $SPOTFLOW_INGEST_KEY)",
    )
    parser.add_argument(
        "--name",
        action="append",
        default=[],
        metavar="PATTERN",
        help="only connect to devices whose advertised name matches this glob (repeatable)",
    )
    parser.add_argument("--mqtt-host", default=cloud.DEFAULT_HOST, help=argparse.SUPPRESS)
    parser.add_argument("--mqtt-port", type=int, default=cloud.DEFAULT_PORT, help=argparse.SUPPRESS)
    parser.add_argument("--no-tls", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    args = parser.parse_args(argv)
    if not args.ingest_key:
        parser.error("an ingest key is required (--ingest-key or SPOTFLOW_INGEST_KEY)")
    return args


async def _run(gateway: Gateway) -> None:
    # Stop cleanly on SIGTERM (systemd, docker stop, kill) as well as Ctrl+C, so sessions
    # disconnect and the artifact cache directory is removed.
    task = asyncio.current_task()
    if sys.platform != "win32" and task is not None:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    await gateway.run()


# Syslog priorities, which journald reads from a "<N>" prefix (for journalctl -p).
_SYSLOG_PRIORITY = {logging.DEBUG: 7, logging.INFO: 6, logging.WARNING: 4, logging.ERROR: 3, logging.CRITICAL: 2}


class _Formatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.module_name = record.name.removeprefix("spotflow_gateway.")
        record.priority = _SYSLOG_PRIORITY.get(record.levelno, 6)
        return super().format(record)


def _stderr_is_journal() -> bool:
    """Whether stderr goes to the systemd journal, which timestamps every line itself."""
    try:
        device, inode = (int(n) for n in os.environ.get("JOURNAL_STREAM", "").split(":"))
        stat = os.fstat(sys.stderr.fileno())
    except (ValueError, OSError, AttributeError):  # not set, malformed, or no real stderr
        return False
    return (stat.st_dev, stat.st_ino) == (device, inode)


def log_format(verbose: bool, journal: bool) -> str:
    prefix = "<%(priority)d>" if journal else "%(asctime)s "
    # With -v, also which module logged it.
    return prefix + "%(levelname)-7s " + ("%(module_name)-9s " if verbose else "") + "%(message)s"


def configure_logging(verbose: bool) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_Formatter(log_format(verbose, _stderr_is_journal())))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    # Verbose means the gateway's own debug messages only. The libraries stay at their
    # normal levels: at DEBUG, bleak logs the bytes of every BLE read, write and
    # notification, and those include firmware download links and their secrets.
    logging.getLogger("spotflow_gateway").setLevel(logging.DEBUG if verbose else logging.INFO)
    # bleak is chatty at INFO; keep our own messages readable.
    logging.getLogger("bleak").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    settings = cloud.CloudSettings(
        ingest_key=args.ingest_key, host=args.mqtt_host, port=args.mqtt_port, tls=not args.no_tls
    )
    try:
        asyncio.run(_run(Gateway(settings, args.name)))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.getLogger("spotflow_gateway").info("Stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
