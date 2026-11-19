"""Thin wrapper over bleak: one GATT connection to a Spotflow device."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from collections.abc import Callable
from typing import Any, Protocol

from bleak import BleakClient
from bleak.backends.device import BLEDevice

from . import protocol

log = logging.getLogger(__name__)


class DeviceLink(Protocol):
    """What a session needs from the BLE connection (implemented by BleLink and test fakes)."""

    name: str
    disconnected: asyncio.Event

    async def read(self, uuid: str) -> bytes: ...
    async def start_notify(self, callback: Callable[[bytes], None]) -> None: ...
    async def write(self, frame: bytes) -> None: ...
    async def disconnect(self) -> None: ...


class WriteGate:
    """Flow control for writes without response on macOS.

    Core Bluetooth silently drops such writes while its transmit queue is full, and
    bleak does not wait for room. A firmware block is hundreds of consecutive writes,
    so the gateway waits itself until the peripheral reports that it can take one.
    """

    POLL_S = 0.002
    TIMEOUT_S = 0.5

    def __init__(self, peripheral: Any) -> None:
        self._peripheral = peripheral  # CBPeripheral

    async def wait(self) -> None:
        if self._peripheral is None:
            return
        deadline = asyncio.get_running_loop().time() + self.TIMEOUT_S
        try:
            while not self._peripheral.canSendWriteWithoutResponse():
                if asyncio.get_running_loop().time() >= deadline:
                    # The flag never became true; do not slow every write down for it.
                    log.debug("Write flow control unavailable, writing unpaced")
                    self._peripheral = None
                    return
                await asyncio.sleep(self.POLL_S)
        except Exception as exc:
            log.debug("Write flow control unavailable (%s), writing unpaced", exc)
            self._peripheral = None


def _peripheral_of(device: BLEDevice) -> Any:
    """The CBPeripheral behind a device found by scanning on macOS, else None."""
    if sys.platform != "darwin":
        return None
    details = device.details  # (CBPeripheral, CentralManagerDelegate)
    return details[0] if isinstance(details, tuple) and details else None


class BleLink:
    def __init__(self, device: BLEDevice, connect_timeout: float = 20.0) -> None:
        self.name = device.name or device.address
        self.address = device.address
        self.disconnected = asyncio.Event()
        self._gate = WriteGate(_peripheral_of(device))
        self._client = BleakClient(
            device, disconnected_callback=self._on_disconnected, timeout=connect_timeout
        )

    def _on_disconnected(self, _client: BleakClient) -> None:
        log.info("[%s] BLE disconnected", self.name)
        self.disconnected.set()

    async def connect(self) -> None:
        await self._client.connect()

    async def read(self, uuid: str) -> bytes:
        return bytes(await self._client.read_gatt_char(uuid))

    async def start_notify(self, callback: Callable[[bytes], None]) -> None:
        await self._client.start_notify(
            protocol.TX_STREAM_UUID, lambda _char, data: callback(bytes(data))
        )

    async def write(self, frame: bytes) -> None:
        await self._gate.wait()
        await self._client.write_gatt_char(protocol.RX_STREAM_UUID, frame, response=False)

    async def disconnect(self) -> None:
        with contextlib.suppress(Exception):
            await self._client.disconnect()
        self.disconnected.set()
