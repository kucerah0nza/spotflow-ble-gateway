"""Spotflow BLE wire protocol: GATT UUIDs, message types and frame (de)fragmentation.

Mirrors the device side in the Spotflow Device SDK
(zephyr/src/net/transport/ble/spotflow_ble_framing.c and spotflow_ble_rx.c) and
https://docs.spotflow.io/fundamentals/bluetooth-low-energy.
"""

from __future__ import annotations

import logging
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum, IntFlag

log = logging.getLogger(__name__)

_UUID_SUFFIX = "-81e5-4861-82ae-2c92e6887922"
SERVICE_UUID = "26530001" + _UUID_SUFFIX
CAPABILITIES_UUID = "26530002" + _UUID_SUFFIX
DEVICE_ID_UUID = "26530003" + _UUID_SUFFIX
SESSION_METADATA_UUID = "26530004" + _UUID_SUFFIX
TX_STREAM_UUID = "26530005" + _UUID_SUFFIX  # device -> gateway, NOTIFY
RX_STREAM_UUID = "26530006" + _UUID_SUFFIX  # gateway -> device, WRITE WITHOUT RESPONSE

SUPPORTED_MAJOR_VERSION = 1
# Minor versions only add to the protocol, so features are available from the minor
# version that introduced them onwards.
ACKS_MINOR_VERSION = 1
OTA_MINOR_VERSION = 1

# ATT notification / write-command header; frames must fit into ATT MTU - 3.
ATT_HEADER_LEN = 3
DEFAULT_ATT_MTU = 23
MAX_FRAME_SIZE = 512
# The device does not advertise how large a frame it accepts; the docs say to use
# 20-byte frames (default ATT MTU) for gateway -> device writes.
DEFAULT_FRAME_SIZE = DEFAULT_ATT_MTU - ATT_HEADER_LEN

FIRST_HEADER_LEN = 5  # type, flags, sequence, total length (uint16 LE)
CONT_HEADER_LEN = 3  # type, flags, sequence
MAX_MESSAGE_LEN = 0xFFFF


class MessageType(IntEnum):
    ACK = 0x00
    NACK = 0x01
    TELEMETRY = 0x02
    REPORTED_CONFIGURATION = 0x03
    DESIRED_CONFIGURATION = 0x04
    OTA_D2C = 0x05
    OTA_C2D = 0x06
    ARTIFACT_REQUEST = 0x07
    ARTIFACT_RESPONSE = 0x08


class FrameFlags(IntFlag):
    NONE = 0
    IS_FIRST = 0x01
    IS_LAST = 0x02
    NEEDS_ACK = 0x04


class ProtocolError(Exception):
    pass


@dataclass(frozen=True)
class Capabilities:
    major: int
    minor: int | None
    flags: int

    @classmethod
    def parse(cls, data: bytes) -> Capabilities:
        if len(data) < 1:
            raise ProtocolError("empty capabilities value")
        minor = data[1] if len(data) > 1 else None
        flags = data[2] if len(data) > 2 else 0
        return cls(major=data[0], minor=minor, flags=flags)

    @property
    def is_supported(self) -> bool:
        return self.major == SUPPORTED_MAJOR_VERSION

    @property
    def supports_acks(self) -> bool:
        # NEEDS_ACK / ACK / NACK arrived with protocol 1.1; legacy devices (major only)
        # get best-effort gateway -> device messages.
        return self.is_supported and (self.minor or 0) >= ACKS_MINOR_VERSION

    @property
    def supports_ota(self) -> bool:
        return self.is_supported and (self.minor or 0) >= OTA_MINOR_VERSION and bool(self.flags & 0x01)

    def __str__(self) -> str:
        minor = "?" if self.minor is None else str(self.minor)
        return f"v{self.major}.{minor} ota={'yes' if self.supports_ota else 'no'}"


def encode_frames(
    message_type: int, sequence: int, payload: bytes, frame_size: int, needs_ack: bool = False
) -> Iterator[bytes]:
    """Split a complete message into frames of at most ``frame_size`` bytes."""
    if not payload:
        raise ValueError("payload must not be empty")
    if len(payload) > MAX_MESSAGE_LEN:
        raise ValueError(f"payload too large for BLE framing: {len(payload)} bytes")
    if frame_size <= FIRST_HEADER_LEN:
        raise ValueError(f"frame size {frame_size} too small")

    offset = 0
    while offset < len(payload):
        first = offset == 0
        header_len = FIRST_HEADER_LEN if first else CONT_HEADER_LEN
        chunk = payload[offset : offset + frame_size - header_len]
        offset += len(chunk)

        flags = FrameFlags.NONE
        if first:
            flags |= FrameFlags.IS_FIRST
        if offset == len(payload):
            flags |= FrameFlags.IS_LAST
        if needs_ack:
            flags |= FrameFlags.NEEDS_ACK

        header = bytes((message_type, flags, sequence & 0xFF))
        if first:
            header += struct.pack("<H", len(payload))
        yield header + chunk


def encode_control(accepted: bool, message_type: int, sequence: int) -> bytes:
    """ACK / NACK frame acknowledging a message of ``message_type`` with ``sequence``."""
    return bytes((MessageType.ACK if accepted else MessageType.NACK, message_type, sequence & 0xFF))


@dataclass(frozen=True)
class Control:
    accepted: bool
    message_type: int
    sequence: int


@dataclass(frozen=True)
class Message:
    message_type: int
    sequence: int
    payload: bytes
    needs_ack: bool


@dataclass(frozen=True)
class Rejected:
    """A message that failed reassembly; if it requested an ACK, the peer expects a NACK."""

    message_type: int
    sequence: int
    needs_ack: bool


class _Channel:
    def __init__(self) -> None:
        self.active = False
        self.sequence = 0
        self.needs_ack = False
        self.total_len = 0
        self.buffer = bytearray()


class Reassembler:
    """Reassembles incoming frames into messages, independently per message type."""

    def __init__(self) -> None:
        self._channels: dict[int, _Channel] = {}

    def reset(self) -> None:
        self._channels.clear()

    def feed(self, frame: bytes) -> Message | Control | Rejected | None:
        """Process one frame.

        Returns a complete ``Message``, a ``Control`` (ACK/NACK), a ``Rejected`` marker
        for a broken message, or ``None`` if more fragments are needed or the frame
        was ignored.
        """
        if len(frame) < CONT_HEADER_LEN:
            log.debug("Ignoring too-short frame (%d bytes)", len(frame))
            return None

        message_type, flags, sequence = frame[0], frame[1], frame[2]

        if message_type in (MessageType.ACK, MessageType.NACK):
            return Control(message_type == MessageType.ACK, flags, sequence)

        needs_ack = bool(flags & FrameFlags.NEEDS_ACK)
        first = bool(flags & FrameFlags.IS_FIRST)
        last = bool(flags & FrameFlags.IS_LAST)
        ch = self._channels.setdefault(message_type, _Channel())

        def reject(reason: str) -> Rejected:
            log.warning("Dropping %s seq=%d: %s", _type_name(message_type), sequence, reason)
            rejected = Rejected(message_type, sequence, needs_ack or (ch.active and ch.needs_ack))
            ch.active = False
            return rejected

        if first:
            if len(frame) < FIRST_HEADER_LEN:
                return reject("first fragment too short")
            ch.active = True
            ch.sequence = sequence
            ch.needs_ack = needs_ack
            ch.total_len = struct.unpack_from("<H", frame, 3)[0]
            ch.buffer = bytearray()
            data = frame[FIRST_HEADER_LEN:]
        else:
            data = frame[CONT_HEADER_LEN:]

        if not ch.active:
            return reject("continuation without first fragment")
        if ch.sequence != sequence:
            return reject(f"sequence mismatch (expected {ch.sequence})")
        if ch.needs_ack != needs_ack:
            return reject("inconsistent NEEDS_ACK flag")
        if ch.total_len == 0:
            return reject("zero total length")
        if len(ch.buffer) + len(data) > ch.total_len:
            return reject("fragment exceeds declared length")

        ch.buffer += data
        if not last:
            return None

        ch.active = False
        if len(ch.buffer) != ch.total_len:
            return reject(f"length mismatch ({len(ch.buffer)} != {ch.total_len})")
        return Message(message_type, sequence, bytes(ch.buffer), needs_ack)


def _type_name(message_type: int) -> str:
    try:
        return MessageType(message_type).name
    except ValueError:
        return f"type 0x{message_type:02x}"
