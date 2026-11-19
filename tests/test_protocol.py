import pytest

from spotflow_gateway import protocol
from spotflow_gateway.protocol import (
    Capabilities,
    Control,
    FrameFlags,
    Message,
    MessageType,
    Reassembler,
    Rejected,
    encode_frames,
)


def test_single_frame_layout():
    frames = list(encode_frames(MessageType.TELEMETRY, 7, b"\xaa\xbb", 20))
    assert frames == [bytes([0x02, FrameFlags.IS_FIRST | FrameFlags.IS_LAST, 7, 0x02, 0x00, 0xAA, 0xBB])]


def test_fragment_layout_matches_device():
    payload = bytes(range(40))
    frames = list(encode_frames(MessageType.TELEMETRY, 1, payload, 20))
    # 15 data bytes in the first 20-byte frame, 17 in each continuation.
    assert [len(f) for f in frames] == [20, 20, 3 + 8]
    assert frames[0][:5] == bytes([0x02, FrameFlags.IS_FIRST, 1, 40, 0])
    assert frames[1][:3] == bytes([0x02, 0, 1])
    assert frames[2][:3] == bytes([0x02, FrameFlags.IS_LAST, 1])


def test_needs_ack_set_on_every_fragment():
    frames = list(encode_frames(MessageType.OTA_C2D, 0, bytes(50), 20, needs_ack=True))
    assert all(f[1] & FrameFlags.NEEDS_ACK for f in frames)


@pytest.mark.parametrize("size", [1, 14, 15, 16, 17, 32, 33, 500, 4096, 65535])
@pytest.mark.parametrize("frame_size", [20, 64, 244, 512])
def test_roundtrip(size, frame_size):
    payload = bytes(i % 251 for i in range(size))
    r = Reassembler()
    results = [r.feed(f) for f in encode_frames(MessageType.TELEMETRY, 255, payload, frame_size)]
    assert all(x is None for x in results[:-1])
    assert results[-1] == Message(MessageType.TELEMETRY, 255, payload, False)


def test_types_reassemble_independently():
    r = Reassembler()
    a = list(encode_frames(MessageType.TELEMETRY, 1, b"A" * 30, 20))
    b = list(encode_frames(MessageType.REPORTED_CONFIGURATION, 9, b"B" * 30, 20))
    out = [r.feed(f) for f in (a[0], b[0], a[1], b[1])]
    assert out[2] == Message(MessageType.TELEMETRY, 1, b"A" * 30, False)
    assert out[3] == Message(MessageType.REPORTED_CONFIGURATION, 9, b"B" * 30, False)


def test_new_first_fragment_restarts_message():
    r = Reassembler()
    stale = list(encode_frames(MessageType.TELEMETRY, 1, b"X" * 30, 20))
    fresh = list(encode_frames(MessageType.TELEMETRY, 2, b"Y" * 10, 20))
    assert r.feed(stale[0]) is None
    assert r.feed(fresh[0]) == Message(MessageType.TELEMETRY, 2, b"Y" * 10, False)


def test_orphan_continuation_rejected():
    frames = list(encode_frames(MessageType.TELEMETRY, 3, bytes(40), 20))
    assert isinstance(Reassembler().feed(frames[1]), Rejected)


def test_sequence_mismatch_rejected():
    r = Reassembler()
    frames = list(encode_frames(MessageType.TELEMETRY, 3, bytes(40), 20))
    r.feed(frames[0])
    bad = bytes([frames[1][0], frames[1][1], 4]) + frames[1][3:]
    assert isinstance(r.feed(bad), Rejected)


def test_length_overflow_rejected():
    r = Reassembler()
    first = bytes([0x02, FrameFlags.IS_FIRST, 0, 4, 0]) + b"12345"
    assert isinstance(r.feed(first), Rejected)


def test_short_last_rejected():
    r = Reassembler()
    first = bytes([0x02, FrameFlags.IS_FIRST | FrameFlags.IS_LAST, 0, 10, 0]) + b"123"
    assert isinstance(r.feed(first), Rejected)


def test_rejected_message_requests_nack_when_ack_needed():
    r = Reassembler()
    frames = list(encode_frames(MessageType.OTA_D2C, 5, bytes(40), 20, needs_ack=True))
    r.feed(frames[0])
    out = r.feed(bytes([0x05, FrameFlags.NEEDS_ACK, 6]) + bytes(5))
    assert out == Rejected(MessageType.OTA_D2C, 6, True)


def test_control_frames():
    assert Reassembler().feed(bytes([0x00, 0x06, 9])) == Control(True, MessageType.OTA_C2D, 9)
    assert Reassembler().feed(bytes([0x01, 0x04, 2])) == Control(False, MessageType.DESIRED_CONFIGURATION, 2)
    assert protocol.encode_control(False, MessageType.OTA_D2C, 300) == bytes([0x01, 0x05, 44])


def test_too_short_frame_ignored():
    assert Reassembler().feed(b"\x02\x03") is None


@pytest.mark.parametrize(
    "raw, supported, ota",
    [
        (b"\x01\x01\x01", True, True),
        (b"\x01\x01\x00", True, False),
        (b"\x01\x00\x01", True, False),
        (b"\x01\x01", True, False),
        (b"\x01\x02\x01", True, True),  # later minor versions keep OTA
        (b"\x01\x02\x00", True, False),
        (b"\x01", True, False),
        (b"\x02\x01\x01", False, False),
    ],
)
def test_capabilities(raw, supported, ota):
    caps = Capabilities.parse(raw)
    assert caps.supports_acks is (supported and len(raw) > 1 and raw[1] >= 1)
    assert caps.is_supported is supported
    assert caps.supports_ota is ota

