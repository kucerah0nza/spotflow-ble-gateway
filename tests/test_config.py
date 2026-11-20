from spotflow_gateway import config
from spotflow_gateway.config import Desired, Reported


def test_parses_desired_configuration():
    # {0: 3, 0x10: 60, 0x12: 1}, the desired-config vector from the SDK's ble_config test.
    assert config.parse_desired(bytes([0xA3, 0x00, 0x03, 0x10, 0x18, 0x3C, 0x12, 0x01])) == Desired(1, 60)
    # Without a severity, and with a version that needs 8 bytes.
    big = bytes([0xA2, 0x00, 0x03, 0x12, 0x1B]) + (2**40).to_bytes(8, "big")
    assert config.parse_desired(big) == Desired(2**40, None)


def test_parses_reported_configuration():
    payload = bytes([0xA4, 0x00, 0x04, 0x10, 0x18, 0x28, 0x11, 0x18, 0x1E, 0x13, 0x01])
    assert config.parse_reported(payload) == Reported(severity=40, compiled_severity=30, acked_version=1)


def test_rejects_what_it_cannot_read():
    desired = bytes([0xA3, 0x00, 0x03, 0x10, 0x18, 0x3C, 0x12, 0x01])
    for payload in (
        b"",
        desired[:-1],  # truncated
        desired + b"\x00",  # trailing data
        bytes([0xBF, 0x00, 0x03, 0xFF]),  # indefinite-length map
        bytes([0xA1, 0x00, 0x61, 0x78]),  # text value
        bytes([0xA2, 0x00, 0x03, 0x10, 0x18]),  # missing argument byte
        bytes([0xA1, 0x00, 0x03]),  # no version
        bytes([0x83, 0x00, 0x03, 0x01]),  # an array
    ):
        assert config.parse_desired(payload) is None, payload
    assert config.parse_reported(desired) is None  # wrong message type
    assert config.parse_desired(bytes([0xA2, 0x00, 0x04, 0x12, 0x01])) is None


def test_severity_names():
    assert [config.severity_name(s) for s in (30, 40, 50, 60, 70)] == ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    assert config.severity_name(0) == "unknown (0)"
