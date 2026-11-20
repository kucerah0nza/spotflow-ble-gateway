"""The gateway's periodic status line: a heartbeat with what it forwarded lately.

Without it, a gateway that works logs nothing for hours, which looks the same as one
that has hung.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

from .logs import count, duration

DEFAULT_INTERVAL_S = 300.0
# Device IDs named in the line; beyond this, only how many more there are.
MAX_LISTED_DEVICES = 5


@dataclass(frozen=True)
class Counts:
    """Message counts, since a session or the gateway started."""

    to_cloud: int = 0
    to_devices: int = 0  # configuration and OTA commands, not firmware blocks
    dropped: int = 0  # buffer for the cloud full
    discarded: int = 0  # still buffered when the device disconnected
    undelivered: int = 0  # commands the device did not acknowledge
    malformed: int = 0  # messages from the device that could not be reassembled
    session_failures: int = 0  # sessions that ended with an error

    def __add__(self, other: Counts) -> Counts:
        return Counts(*(getattr(self, f.name) + getattr(other, f.name) for f in fields(self)))

    def __sub__(self, other: Counts) -> Counts:
        return Counts(*(getattr(self, f.name) - getattr(other, f.name) for f in fields(self)))


@dataclass(frozen=True)
class Device:
    label: str  # the device ID, or its BLE name and address until it has reported its ID
    online: bool  # connected to the cloud


def status_line(
    uptime: float,
    devices: list[Device],
    interval: float,
    recent: Counts,
    buffered: int,
    bluetooth_unavailable: str | None = None,
) -> str:
    """E.g. "Status: up 2h 15m, 1 device connected (sensor-042), all online; last 5 min:
    68 messages to the cloud, 1 to devices; 0 buffered"."""
    state = [f"up {duration(uptime)}"]
    if bluetooth_unavailable:
        state.append(bluetooth_unavailable)
    if devices:
        labels = [d.label for d in devices[:MAX_LISTED_DEVICES]]
        if len(devices) > MAX_LISTED_DEVICES:
            labels.append(f"+{len(devices) - MAX_LISTED_DEVICES} more")
        state.append(f"{count(len(devices), 'device')} connected ({', '.join(labels)})")
        offline = sum(not d.online for d in devices)
        state.append(f"{offline} offline from the cloud" if offline else "all online")
    else:
        state.append("no devices connected")

    activity = [count(recent.to_cloud, "message") + " to the cloud", f"{recent.to_devices} to devices"]
    for value, text in (
        (recent.dropped, "dropped"),
        (recent.discarded, "discarded"),
        (recent.undelivered, "undelivered to devices"),
        (recent.malformed, "malformed from devices"),
    ):
        if value:
            activity.append(f"{value} {text}")
    if recent.session_failures:
        activity.append(count(recent.session_failures, "session failure"))

    return f"Status: {', '.join(state)}; last {_period(interval)}: {', '.join(activity)}; {buffered} buffered"


def _period(seconds: float) -> str:
    """ "5 min" for whole minutes, else as duration() writes it."""
    if seconds >= 60 and seconds % 60 == 0:
        return f"{int(seconds // 60)} min"
    return duration(seconds)
