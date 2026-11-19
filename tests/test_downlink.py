import asyncio
import logging

import pytest

from spotflow_gateway import protocol
from spotflow_gateway.downlink import DownlinkChannel
from spotflow_gateway.protocol import FrameFlags, MessageType

from .conftest import FakeLink


@pytest.fixture
async def setup():
    link = FakeLink()
    tasks = []

    def make(needs_ack=True, **kwargs):
        channel = DownlinkChannel(link, MessageType.DESIRED_CONFIGURATION, needs_ack, **kwargs)
        reassembler = protocol.Reassembler()
        link._callback = lambda frame: channel.on_control(reassembler.feed(frame))
        tasks.append(asyncio.create_task(channel.run()))
        return channel

    yield link, make
    for task in tasks:
        task.cancel()


async def wait_until(predicate, timeout=5.0):
    async def _wait():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_wait(), timeout)


async def test_acknowledged_delivery_in_order(setup):
    link, make = setup
    channel = make()
    payload = bytes(range(60))  # several 20-byte frames
    channel.submit(b"first")
    channel.submit(payload)
    await wait_until(lambda: channel.delivered == 2)

    assert [m.payload for m in link.received] == [b"first", payload]
    assert [m.sequence for m in link.received] == [0, 1]
    assert all(m.message_type == MessageType.DESIRED_CONFIGURATION for m in link.received)
    assert all(f[1] & FrameFlags.NEEDS_ACK for f in link.written)
    assert all(len(f) <= protocol.DEFAULT_FRAME_SIZE for f in link.written)


async def test_nack_triggers_identical_resend(setup, caplog):
    caplog.set_level(logging.INFO)
    link, make = setup
    link.ack_replies = [False]
    channel = make()
    channel.submit(b"cfg")
    await wait_until(lambda: channel.delivered == 1)
    assert [(m.sequence, m.payload) for m in link.received] == [(0, b"cfg"), (0, b"cfg")]
    assert "seq=0 rejected (NACK), retrying (attempt 1/5)" in caplog.text


async def test_missing_ack_resends_then_gives_up(setup, caplog):
    caplog.set_level(logging.INFO)
    link, make = setup
    link.ack_replies = [None, None]
    channel = make(ack_timeout=0.05, max_attempts=2)
    channel.submit(b"cfg")
    await wait_until(lambda: channel.failed == 1)
    assert len(link.received) == 2
    # A retry is routine over BLE; only giving up is an error.
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "[fake-device AA:BB] DESIRED_CONFIGURATION seq=0 not acknowledged, retrying (attempt 1/2)"),
        (logging.ERROR, "[fake-device AA:BB] Giving up delivering DESIRED_CONFIGURATION seq=0 after 2 attempts, "
                        "the last one not acknowledged"),
    ]

    channel.submit(b"next")  # the channel keeps working afterwards
    await wait_until(lambda: channel.delivered == 1)
    assert link.received[-1].sequence == 1


async def test_late_ack_for_old_sequence_is_ignored(setup):
    link, make = setup
    channel = make(ack_timeout=0.2)
    link.ack_replies = [None]
    channel.submit(b"cfg")
    await link.wait_received(1)
    channel.on_control(protocol.Control(True, MessageType.DESIRED_CONFIGURATION, 99))
    channel.on_control(protocol.Control(True, MessageType.OTA_C2D, 0))
    await wait_until(lambda: channel.delivered == 1)
    assert len(link.received) == 2  # neither stray ACK counted; the resend was ACKed


async def test_without_acks_sends_once(setup, caplog):
    caplog.set_level(logging.INFO)
    link, make = setup
    channel = make(needs_ack=False)
    channel.submit(b"cfg")
    await wait_until(lambda: channel.delivered == 1)
    assert len(link.received) == 1
    assert not any(f[1] & FrameFlags.NEEDS_ACK for f in link.written)
    # Logged like an acknowledged delivery, so that older devices are not left out.
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "[fake-device AA:BB] -> device DESIRED_CONFIGURATION (3 bytes), sent without acknowledgement")
    ]


async def test_quiet_channel_logs_deliveries_at_debug(setup, caplog):
    caplog.set_level(logging.DEBUG)
    link, make = setup
    channel = make(needs_ack=False, quiet=True)
    channel.submit(b"block")
    await wait_until(lambda: channel.delivered == 1)
    assert [r.levelno for r in caplog.records] == [logging.DEBUG]


async def test_undelivered_message_is_reported(setup):
    link, make = setup
    undelivered = []
    channel = make(ack_timeout=0.05, max_attempts=1, on_undelivered=lambda: undelivered.append(True))

    channel.submit(b"delivered")
    await wait_until(lambda: channel.delivered == 1)
    assert undelivered == []

    link.ack_replies = [None]  # the device never acknowledges
    channel.submit(b"ignored")
    await wait_until(lambda: channel.failed == 1)
    await wait_until(lambda: len(undelivered) == 1)

    async def failing_write(frame):
        raise OSError("write failed")

    link.write = failing_write
    channel.submit(b"unwritable")
    await wait_until(lambda: len(undelivered) == 2)
    assert channel.failed == 2
