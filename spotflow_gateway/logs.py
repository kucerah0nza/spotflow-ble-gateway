"""Logging helpers shared by the gateway's modules."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, MutableMapping
from typing import Any

MAX_LABEL_LEN = 64


def printable(text: str, limit: int = MAX_LABEL_LEN) -> str:
    """``text`` made safe to log: control and other non-printable characters escaped, and
    cut to ``limit`` characters.

    For strings a BLE device controls (its advertised name, its device ID): unescaped, a
    newline in them could forge log lines and an escape sequence could control the terminal.
    """
    safe = "".join(c if c.isprintable() else c.encode("unicode_escape").decode("ascii") for c in text)
    return safe if len(safe) <= limit else safe[: limit - 3] + "..."


def describe(exc: BaseException) -> str:
    """The exception's message, or its type if it has none (e.g. TimeoutError)."""
    return str(exc) or type(exc).__name__


def debug_traceback(logger: logging.Logger | logging.LoggerAdapter[Any]) -> bool:
    """Value for ``exc_info=``: include the traceback only with debug logging (-v).

    Catch-all handlers log the error message as a warning; the traceback is for finding
    out where an unexpected error came from.
    """
    return logger.isEnabledFor(logging.DEBUG)


def count(n: int, noun: str) -> str:
    """``n`` and the noun in singular or plural: "1 message", "3 messages"."""
    return f"{n} {noun}" if n == 1 else f"{n} {noun}s"


def duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


class DeviceLogger(logging.LoggerAdapter[logging.Logger]):
    """Prefixes every message with the device's current label: ``[sensor-042] ...``.

    The label is looked up on every call, as it changes from the BLE name and address to
    the device ID once the device has reported it.
    """

    def __init__(self, logger: logging.Logger, label: Callable[[], str]) -> None:
        super().__init__(logger, {})
        self._label = label

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        # The label becomes part of the format string, so a "%" in it must be escaped.
        return f"[{self._label().replace('%', '%%')}] {msg}", kwargs


class Throttle:
    """Limits a warning that a peer can trigger at will to ``burst`` per ``interval`` seconds.

    Use: ``if throttle.allow(): log.warning("..." + throttle.suppressed_note())``.
    """

    def __init__(self, interval: float = 60.0, burst: int = 5, clock: Callable[[], float] = time.monotonic) -> None:
        self.interval = interval
        self.burst = burst
        self._clock = clock
        self._window_end = float("-inf")
        self._allowed = 0
        self._suppressed = 0

    def allow(self) -> bool:
        now = self._clock()
        if now >= self._window_end:
            self._window_end = now + self.interval
            self._allowed = 0
        if self._allowed < self.burst:
            self._allowed += 1
            return True
        self._suppressed += 1
        return False

    def suppressed_note(self) -> str:
        """Suffix for an allowed message that says how many were left out before it."""
        count, self._suppressed = self._suppressed, 0
        return f" ({count} similar messages suppressed)" if count else ""
