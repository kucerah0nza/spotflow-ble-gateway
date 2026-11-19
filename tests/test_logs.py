import logging

import pytest

from spotflow_gateway.logs import DeviceLogger, Throttle, describe, duration, printable


def test_printable_escapes_what_could_forge_log_lines_or_control_the_terminal():
    assert printable("sensor-042") == "sensor-042"
    assert printable("Čidlo 1") == "Čidlo 1"  # printable non-ASCII stays readable
    assert printable("a\nb\r\x1b[31mc\x00​") == "a\\nb\\r\\x1b[31mc\\x00\\u200b"


def test_printable_cuts_long_strings():
    assert printable("x" * 64) == "x" * 64
    assert printable("x" * 500) == "x" * 61 + "..."
    assert printable("\n" * 100, limit=10) == "\\n\\n\\n\\..."


def test_describe_never_returns_an_empty_string():
    assert describe(OSError("write failed")) == "write failed"
    assert describe(TimeoutError()) == "TimeoutError"


@pytest.mark.parametrize(
    "seconds, text", [(0.4, "0s"), (59, "59s"), (60, "1m 00s"), (250, "4m 10s"), (7380, "2h 03m")]
)
def test_duration(seconds, text):
    assert duration(seconds) == text


def test_device_logger_prefixes_the_current_label(caplog):
    caplog.set_level(logging.INFO)
    label = "Spotflow AA:BB"
    device_log = DeviceLogger(logging.getLogger("spotflow_gateway.test"), lambda: label)
    device_log.info("Connecting (RSSI %s)", -58)
    label = "100%s-device"  # a "%" in the label must not be taken for a format field
    device_log.info("MQTT connected to %s", "broker")
    assert [r.getMessage() for r in caplog.records] == [
        "[Spotflow AA:BB] Connecting (RSSI -58)",
        "[100%s-device] MQTT connected to broker",
    ]


def test_throttle_allows_a_burst_per_interval_and_counts_the_rest():
    now = 0.0
    throttle = Throttle(interval=60, burst=2, clock=lambda: now)
    assert [throttle.allow() for _ in range(5)] == [True, True, False, False, False]
    now = 59.9
    assert not throttle.allow()
    now = 60.0
    assert throttle.allow()
    assert throttle.suppressed_note() == " (4 similar messages suppressed)"
    assert throttle.suppressed_note() == ""
