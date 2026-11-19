"""Scans for Spotflow BLE devices and runs one session per connected device."""

from __future__ import annotations

import asyncio
import fnmatch
import logging

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from bleak.exc import BleakError

from . import cloud, protocol
from .artifacts import ArtifactCache, new_cache_directory
from .ble import BleLink
from .session import DeviceSession, SessionError

log = logging.getLogger(__name__)

SCAN_DURATION_S = 5.0
SCAN_PAUSE_S = 2.0
# Backoff after failures: of scanning as a whole, and of a single device (failed
# connection, unsupported or unpaired device, ...), so that a persistent problem
# neither floods the log nor keeps the adapter busy.
RETRY_MIN_S = 5.0
RETRY_MAX_S = 60.0


def _describe(exc: Exception) -> str:
    return str(exc) or type(exc).__name__  # e.g. TimeoutError has no message


class Gateway:
    def __init__(self, settings: cloud.CloudSettings, name_patterns: list[str] | None = None) -> None:
        self.settings = settings
        self.name_patterns = name_patterns or []
        self._sessions: dict[str, asyncio.Task[None]] = {}
        self._artifacts: ArtifactCache | None = None
        # Device address -> (current retry delay, earliest time of the next attempt).
        self._retry: dict[str, tuple[float, float]] = {}

    async def run(self) -> None:
        log.info("Scanning for Spotflow BLE devices (MQTT broker %s:%d)", self.settings.host, self.settings.port)
        # Firmware downloaded for OTA is cached here and deleted on exit.
        with new_cache_directory() as directory:
            self._artifacts = ArtifactCache(directory)
            try:
                await self._scan_loop()
            finally:
                await self._stop_sessions()

    async def _scan_loop(self) -> None:
        pause = SCAN_PAUSE_S
        while True:
            found = await self._scan()
            if found is None:
                pause = min(max(pause * 2, RETRY_MIN_S), RETRY_MAX_S)
                log.info("Next scan in %.0f s", pause)
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
            log.error("BLE scan failed: %s", _describe(exc))
            return None
        now = asyncio.get_running_loop().time()
        candidates = []
        for address, (device, adv) in found.items():
            if address in self._sessions:
                continue
            if now < self._retry.get(address, (0.0, 0.0))[1]:
                continue
            if protocol.SERVICE_UUID not in (u.lower() for u in adv.service_uuids):
                continue
            name = adv.local_name or device.name or ""
            if self.name_patterns and not any(fnmatch.fnmatch(name, p) for p in self.name_patterns):
                log.debug("Skipping %s (%s): name does not match filter", name, address)
                continue
            candidates.append((device, adv))
        return candidates

    async def _connect(self, device: BLEDevice, adv: AdvertisementData) -> None:
        # Connect one device at a time; several BLE stacks dislike concurrent connects.
        link = BleLink(device)
        log.info("Connecting to %s (%s, RSSI %s)", link.name, device.address, adv.rssi)
        try:
            await link.connect()
        except Exception as exc:
            log.warning("Failed to connect to %s: %s (next attempt in %.0f s)", link.name,
                        _describe(exc), self._failed(device.address))
            await link.disconnect()
            return
        self._sessions[device.address] = asyncio.create_task(self._run_session(device.address, link))

    async def _run_session(self, address: str, link: BleLink) -> None:
        try:
            await DeviceSession(link, self.settings, self._artifacts).run()
            self._retry.pop(address, None)  # ran until the device disconnected
        except SessionError as exc:
            log.error("[%s] %s (next attempt in %.0f s)", link.name, exc, self._failed(address))
        except (BleakError, TimeoutError, OSError) as exc:
            log.warning("[%s] BLE error: %s (next attempt in %.0f s)", link.name,
                        _describe(exc), self._failed(address))
        except Exception:
            log.exception("[%s] Unexpected error (next attempt in %.0f s)", link.name,
                          self._failed(address))
        finally:
            self._sessions.pop(address, None)
