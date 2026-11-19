"""Gateway -> device delivery of one message type over the RX stream.

Messages are sent one at a time, in order. With ``needs_ack`` every fragment carries
NEEDS_ACK; the device answers with ACK/NACK after reassembly and the whole message is
resent (same sequence, same bytes) on NACK or timeout, with backoff. A message that
still cannot be delivered is dropped and reported through ``on_undelivered``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable

from . import protocol
from .ble import DeviceLink
from .logs import DeviceLogger, debug_traceback, describe
from .protocol import Control

log = logging.getLogger(__name__)

QUEUE_SIZE = 32


class DownlinkChannel:
    def __init__(
        self,
        link: DeviceLink,
        message_type: protocol.MessageType,
        needs_ack: bool,
        label: str = "",
        frame_size: int = protocol.DEFAULT_FRAME_SIZE,
        ack_timeout: float = 3.0,
        max_attempts: int = 5,
        on_undelivered: Callable[[], None] | None = None,
        quiet: bool = False,
    ) -> None:
        self.link = link
        self.message_type = message_type
        self.needs_ack = needs_ack
        self.label = label or link.label
        self._log = DeviceLogger(log, lambda: self.label)
        # Log each delivered message at DEBUG instead of INFO (artifact blocks: hundreds
        # per firmware update).
        self._delivery_level = logging.DEBUG if quiet else logging.INFO
        self.frame_size = frame_size
        self.ack_timeout = ack_timeout
        self.max_attempts = max_attempts
        # Called when run() has to drop a message: the device did not acknowledge it
        # in max_attempts attempts, or writing it failed.
        self.on_undelivered = on_undelivered
        self.delivered = 0
        self.failed = 0
        self._sequence = 0
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(QUEUE_SIZE)
        self._waiting: tuple[int, asyncio.Future[bool]] | None = None
        self._lock = asyncio.Lock()

    def submit(self, payload: bytes) -> None:
        """Queue a complete message for delivery (never blocks)."""
        if self._queue.full():
            self._queue.get_nowait()
            self._log.warning("%s queue full, dropping oldest", self.message_type.name)
        self._queue.put_nowait(payload)

    def on_control(self, control: Control) -> None:
        """Feed an ACK/NACK received from the device."""
        if self._waiting is None or control.message_type != self.message_type:
            return
        sequence, future = self._waiting
        if control.sequence == sequence and not future.done():
            future.set_result(control.accepted)

    async def run(self) -> None:
        """Deliver messages queued with submit() until cancelled."""
        while True:
            payload = await self._queue.get()
            try:
                delivered = await self.send(payload)
            except Exception as exc:  # BLE write failures; the session handles disconnects
                self._log.warning("Failed to write %s: %s", self.message_type.name, describe(exc),
                                  exc_info=debug_traceback(self._log))
                self.failed += 1
                delivered = False
            if not delivered and self.on_undelivered is not None:
                self.on_undelivered()

    async def send(self, payload: bytes) -> bool:
        """Deliver one message now (after any message already being sent); True on success."""
        async with self._lock:
            sequence = self._sequence
            self._sequence = (sequence + 1) & 0xFF
            ok = await self._deliver(sequence, payload)
        if ok:
            self.delivered += 1
        else:
            self.failed += 1
        return ok

    async def _deliver(self, sequence: int, payload: bytes) -> bool:
        name = self.message_type.name
        frames = list(
            protocol.encode_frames(self.message_type, sequence, payload, self.frame_size, self.needs_ack)
        )
        if not self.needs_ack:
            await self._write(frames)
            self._log.log(self._delivery_level, "-> device %s (%d bytes), sent without acknowledgement",
                          name, len(payload))
            return True

        loop = asyncio.get_running_loop()
        outcome = "not sent"
        for attempt in range(1, self.max_attempts + 1):
            future: asyncio.Future[bool] = loop.create_future()
            self._waiting = (sequence, future)  # before writing, so a fast ACK is not missed
            try:
                await self._write(frames)
                with contextlib.suppress(TimeoutError):
                    if await asyncio.wait_for(future, self.ack_timeout):
                        self._log.log(self._delivery_level, "-> device %s (%d bytes), acknowledged",
                                      name, len(payload))
                        return True
            finally:
                self._waiting = None
            # On a timeout, wait_for() cancels the future: only a result means a NACK.
            outcome = "rejected (NACK)" if future.done() and not future.cancelled() else "not acknowledged"
            if attempt == self.max_attempts:
                break
            # Retries are routine over BLE; only giving up is an error.
            self._log.info("%s seq=%d %s, retrying (attempt %d/%d)",
                           name, sequence, outcome, attempt, self.max_attempts)
            await asyncio.sleep(min(0.5 * 2 ** (attempt - 1), 8.0))
        self._log.error("Giving up delivering %s seq=%d after %d attempts, the last one %s",
                        name, sequence, self.max_attempts, outcome)
        return False

    async def _write(self, frames: list[bytes]) -> None:
        for frame in frames:
            await self.link.write(frame)
