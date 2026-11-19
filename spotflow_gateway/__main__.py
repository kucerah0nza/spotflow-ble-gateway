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


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
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
        logging.info("Stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
