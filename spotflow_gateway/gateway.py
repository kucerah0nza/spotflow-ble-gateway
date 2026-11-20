"""Scans for Spotflow BLE devices and runs one session per connected device."""

from __future__ import annotations

import asyncio
import fnmatch
import importlib.metadata
import logging
import platform
import time

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from bleak.exc import BleakError

from . import cloud, protocol, status
from .artifacts import ArtifactCache, new_cache_directory
from .ble import BleLink
from .logs import DeviceLogger, debug_traceback, describe, printable
from .session import DeviceSession, SessionError

log = logging.getLogger(__name__)

SCAN_DURATION_S = 5.0
SCAN_PAUSE_S = 2.0
# Backoff after failures: of scanning as a whole, and of a single device (failed
# connection, unsupported or unpaired device, ...), so that a persistent problem
# neither floods the log nor keeps the adapter busy.
RETRY_MIN_S = 5.0
RETRY_MAX_S = 60.0
# While Bluetooth is off (or not allowed), check this often whether it is back. Checking
# costs nothing, and devices are found again soon after Bluetooth is turned on.
BLUETOOTH_CHECK_S = 5.0
# What the gateway logs about bleak's BleakBluetoothNotAvailableReason values (by name).
BLUETOOTH_UNAVAILABLE = {
    "POWERED_OFF": "Bluetooth is turned off",
    "NO_BLUETOOTH": "No Bluetooth adapter found",
    "NO_BLE_CENTRAL_ROLE": "The Bluetooth adapter does not support Bluetooth Low Energy",
    "DENIED_BY_USER": "The gateway is not allowed to use Bluetooth (on macOS, allow the terminal app in "
                      "System Settings > Privacy & Security > Bluetooth)",
    "DENIED_BY_SYSTEM": "The system does not allow the gateway to use Bluetooth",
    "DENIED_BY_UNKNOWN": "The gateway is not allowed to use Bluetooth",
}
# While no device is found, say so at most this often.
NOTHING_FOUND_REPORT_S = 600.0
# Devices whose name does not match --name are reported once per address. Devices with
# random addresses keep changing them, so the record of reported addresses is bounded.
MAX_REPORTED_ADDRESSES = 1000


def versions() -> str:
    """The gateway's version and what it runs on, for the first line of the log."""

    def version(distribution: str) -> str:
        try:
            return importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            return "unknown"

    return (
        f"{version('spotflow-ble-gateway')} (Python {platform.python_version()}, bleak {version('bleak')}, "
        f"paho-mqtt {version('paho-mqtt')}, {platform.platform(terse=True)})"
    )


def bluetooth_unavailable(error: Exception | None) -> str | None:
    """Why Bluetooth cannot be used, if that is what ``error`` says; else None.

    bleak (2.0 and later) raises BleakBluetoothNotAvailableError with a reason; the
    reason is matched by name so that older bleak versions need no special case.
    """
    if not isinstance(error, BleakError):
        return None
    reason = getattr(error, "reason", None)
    name = getattr(reason, "name", None)
    if not isinstance(name, str):
        return None
    message = error.args[0] if error.args and isinstance(error.args[0], str) else describe(error)
    return BLUETOOTH_UNAVAILABLE.get(name, f"Bluetooth is not available ({message})")


class Gateway:
    def __init__(
        self,
        settings: cloud.CloudSettings,
        name_patterns: list[str] | None = None,
        status_interval: float = status.DEFAULT_INTERVAL_S,
    ) -> None:
        self.settings = settings
        self.name_patterns = name_patterns or []
        self.status_interval = status_interval  # seconds between status lines; 0: none
        self._sessions: dict[str, asyncio.Task[None]] = {}
        # The running sessions' DeviceSession objects, and the counts of those that ended.
        self._devices: dict[str, DeviceSession] = {}
        self._ended = status.Counts()
        self._started = time.monotonic()
        self._artifacts: ArtifactCache | None = None
        # Device address -> (current retry delay, earliest time of the next attempt).
        self._retry: dict[str, tuple[float, float]] = {}
        self._scan_error: Exception | None = None
        # Why Bluetooth is unavailable, as last logged; None while it works.
        self._bluetooth_unavailable: str | None = None
        self._next_nothing_found_report = 0.0
        self._reported_unmatched: set[str] = set()

    async def run(self) -> None:
        log.info("Spotflow BLE gateway %s", versions())
        if not self.settings.tls:
            log.warning("TLS is disabled: the ingest key and all data go to %s unencrypted", self.settings.host)
        names = ", ".join(repr(p) for p in self.name_patterns)
        log.info("Scanning for Spotflow BLE devices (MQTT broker %s:%d%s)", self.settings.host, self.settings.port,
                 f", only devices named {names}" if names else "")
        self._started = time.monotonic()
        heartbeat = asyncio.create_task(self._heartbeat()) if self.status_interval > 0 else None
        # Firmware downloaded for OTA is cached here and deleted on exit.
        with new_cache_directory() as directory:
            self._artifacts = ArtifactCache(directory)
            try:
                await self._scan_loop()
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                await self._stop_sessions()

    def totals(self) -> status.Counts:
        """Counts since the gateway started, over ended and running sessions."""
        totals = self._ended
        for session in self._devices.values():
            totals += session.counts()
        return totals

    def status_line(self, recent: status.Counts) -> str:
        devices = [status.Device(s.label, s.cloud_online) for s in self._devices.values()]
        buffered = sum(s.buffered() for s in self._devices.values())
        return status.status_line(time.monotonic() - self._started, devices, self.status_interval, recent,
                                  buffered, self._bluetooth_unavailable)

    async def _heartbeat(self) -> None:
        """Log a status line every status_interval, so that a quiet gateway is seen to be alive."""
        previous = self.totals()
        while True:
            await asyncio.sleep(self.status_interval)
            previous = self._log_status(previous)

    def _log_status(self, previous: status.Counts) -> status.Counts:
        """Log the status line with the counts since ``previous``; return the current totals."""
        current = self.totals()
        log.info("%s", self.status_line(current - previous))
        return current

    async def _scan_loop(self) -> None:
        pause = SCAN_PAUSE_S
        while True:
            found = await self._scan()
            unavailable = bluetooth_unavailable(self._scan_error) if found is None else None
            if unavailable is not None:
                # Not a scanning problem: log it once, then wait for Bluetooth to come back.
                pause = BLUETOOTH_CHECK_S
                if unavailable != self._bluetooth_unavailable:
                    log.warning("%s, waiting for Bluetooth to become available", unavailable)
                self._bluetooth_unavailable = unavailable
            elif found is None:
                pause = min(max(pause * 2, RETRY_MIN_S), RETRY_MAX_S)
                error = self._scan_error
                log.error("BLE scan failed: %s (next scan in %.0f s)", describe(error) if error else "unknown error",
                          pause, exc_info=error if error and debug_traceback(log) else None)
            else:
                pause = SCAN_PAUSE_S
                for device, adv in found:
                    await self._connect(device, adv)
            await asyncio.sleep(pause)

    def _failed(self, address: str) -> float:
        """Note a failure of the device and return the delay before the next attempt."""
        previous, _ = self._retry.get(address, (0.0, 0.0))
        delay = min(max(previous * 2, RETRY_MIN_S), RETRY_MAX_S)
        self._retry[address] = (delay, asyncio.get_running_loop().time() + delay)
        return delay

    async def _stop_sessions(self) -> None:
        tasks = list(self._sessions.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _scan(self) -> list[tuple[BLEDevice, AdvertisementData]] | None:
        """Devices to connect to, or None if scanning failed."""
        try:
            found = await BleakScanner.discover(
                timeout=SCAN_DURATION_S, return_adv=True, service_uuids=[protocol.SERVICE_UUID]
            )
        except Exception as exc:
            # Mostly BleakError (adapter off, no permission), but the platform Bluetooth
            # stacks also raise their own errors; none of them may stop the gateway.
            self._scan_error = exc  # logged by the scan loop, with the time of the next scan
            return None
        if self._bluetooth_unavailable is not None:
            log.info("Bluetooth is available again, scanning")
            self._bluetooth_unavailable = None
        now = asyncio.get_running_loop().time()
        candidates = []
        matching = False
        for address, (device, adv) in found.items():
            if protocol.SERVICE_UUID not in (u.lower() for u in adv.service_uuids):
                continue
            name = adv.local_name or device.name or ""
            if self.name_patterns and not any(fnmatch.fnmatch(name, p) for p in self.name_patterns):
                self._report_unmatched(address, name)
                continue
            matching = True
            if address in self._sessions:
                continue
            if now < self._retry.get(address, (0.0, 0.0))[1]:
                continue
            candidates.append((device, adv))
        if not matching and not self._sessions and now >= self._next_nothing_found_report:
            # Otherwise a gateway that finds nothing would log nothing at all.
            self._next_nothing_found_report = now + NOTHING_FOUND_REPORT_S
            log.info("No Spotflow devices%s in range, still scanning",
                     " with a matching name" if self.name_patterns else "")
        return candidates

    def _report_unmatched(self, address: str, name: str) -> None:
        if address in self._reported_unmatched:
            return
        if len(self._reported_unmatched) >= MAX_REPORTED_ADDRESSES:
            self._reported_unmatched.clear()
        self._reported_unmatched.add(address)
        log.info("Ignoring Spotflow device %s (%s): its name does not match --name",
                 f"'{printable(name)}'" if name else "without a name", address)

    async def _connect(self, device: BLEDevice, adv: AdvertisementData) -> None:
        # Connect one device at a time; several BLE stacks dislike concurrent connects.
        link = BleLink(device)
        device_log = DeviceLogger(log, lambda: link.label)
        device_log.info("Connecting (RSSI %s)", adv.rssi)
        try:
            await link.connect()
        except Exception as exc:
            device_log.warning("Failed to connect: %s (next attempt in %.0f s)", describe(exc),
                               self._failed(device.address), exc_info=debug_traceback(log))
            await link.disconnect()
            return
        self._sessions[device.address] = asyncio.create_task(self._run_session(device.address, link))

    async def _run_session(self, address: str, link: BleLink) -> None:
        session = DeviceSession(link, self.settings, self._artifacts)
        self._devices[address] = session
        device_log = DeviceLogger(log, lambda: link.label)
        failed = False

        def ended(level: int, outcome: str, retry_in: float | None = None, exc_info: bool = False) -> None:
            """The one line logged when a session ends: outcome, summary, next attempt."""
            nonlocal failed
            failed = level > logging.INFO
            summary = session.summary()
            device_log.log(level, "Session %s%s: %s%s", "ended" if level <= logging.INFO else "failed",
                           f" {summary}" if summary else "", outcome,
                           "" if retry_in is None else f" (next attempt in {retry_in:.0f} s)", exc_info=exc_info)

        try:
            await session.run()
        except asyncio.CancelledError:
            ended(logging.INFO, "the gateway is stopping")
            raise
        except SessionError as exc:
            ended(logging.ERROR, str(exc), self._failed(address))
        except (BleakError, TimeoutError, OSError) as exc:
            ended(logging.WARNING, f"BLE error: {describe(exc)}", self._failed(address), debug_traceback(log))
        except Exception:
            ended(logging.ERROR, "unexpected error", self._failed(address), exc_info=True)
        else:
            self._retry.pop(address, None)  # ran until the device disconnected
            ended(logging.INFO, "the device disconnected")
        finally:
            self._ended += session.counts() + status.Counts(session_failures=int(failed))
            self._devices.pop(address, None)
            self._sessions.pop(address, None)
