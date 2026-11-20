"""Reading remote configuration messages, for the gateway's log only.

Configuration messages are still forwarded unchanged; the gateway decodes them to log
which log severity the cloud sets and which one the device applies. The format is the
Device SDK's (zephyr/src/config/spotflow_config_cbor.c): a CBOR map of unsigned integer
keys to unsigned integer values.
"""

from __future__ import annotations

from dataclasses import dataclass

KEY_MESSAGE_TYPE = 0x00
KEY_MINIMAL_SEVERITY = 0x10
KEY_COMPILED_MINIMAL_SEVERITY = 0x11
KEY_DESIRED_VERSION = 0x12
KEY_ACKED_DESIRED_VERSION = 0x13

DESIRED_MESSAGE_TYPE = 0x03
REPORTED_MESSAGE_TYPE = 0x04

# Log severities as the Device SDK sends them (spotflow_log_level.c).
SEVERITY_NAMES = {30: "DEBUG", 40: "INFO", 50: "WARNING", 60: "ERROR", 70: "CRITICAL"}

CBOR_UINT = 0
CBOR_MAP = 5


def severity_name(severity: int) -> str:
    return SEVERITY_NAMES.get(severity, f"unknown ({severity})")


@dataclass(frozen=True)
class Desired:
    """Configuration the cloud sends to the device."""

    version: int
    severity: int | None


@dataclass(frozen=True)
class Reported:
    """Configuration the device reports, at startup and after applying a desired one."""

    severity: int | None
    compiled_severity: int | None  # the lowest severity built into the firmware
    acked_version: int | None  # the desired configuration this report answers


def parse_desired(payload: bytes) -> Desired | None:
    fields = _decode(payload)
    if fields is None or fields.get(KEY_MESSAGE_TYPE) != DESIRED_MESSAGE_TYPE:
        return None
    version = fields.get(KEY_DESIRED_VERSION)
    if version is None:
        return None
    return Desired(version=version, severity=fields.get(KEY_MINIMAL_SEVERITY))


def parse_reported(payload: bytes) -> Reported | None:
    fields = _decode(payload)
    if fields is None or fields.get(KEY_MESSAGE_TYPE) != REPORTED_MESSAGE_TYPE:
        return None
    return Reported(
        severity=fields.get(KEY_MINIMAL_SEVERITY),
        compiled_severity=fields.get(KEY_COMPILED_MINIMAL_SEVERITY),
        acked_version=fields.get(KEY_ACKED_DESIRED_VERSION),
    )


def _decode(payload: bytes) -> dict[int, int] | None:
    """The CBOR map in ``payload``, or None if it is not a map of unsigned integers."""
    try:
        major, count, pos = _head(payload, 0)
        if major != CBOR_MAP:
            return None
        fields: dict[int, int] = {}
        for _ in range(count):
            key_major, key, pos = _head(payload, pos)
            value_major, value, pos = _head(payload, pos)
            if key_major != CBOR_UINT or value_major != CBOR_UINT:
                return None
            fields[key] = value
    except ValueError:
        return None
    return fields if pos == len(payload) else None


def _head(data: bytes, pos: int) -> tuple[int, int, int]:
    """Decode a CBOR item head at ``pos``: (major type, argument, position after it)."""
    if pos >= len(data):
        raise ValueError("truncated")
    major, info = data[pos] >> 5, data[pos] & 0x1F
    pos += 1
    if info < 24:
        return major, info, pos
    if info > 27:  # indefinite length or reserved
        raise ValueError("unsupported")
    size = 1 << (info - 24)
    if pos + size > len(data):
        raise ValueError("truncated")
    return major, int.from_bytes(data[pos : pos + size], "big"), pos + size
