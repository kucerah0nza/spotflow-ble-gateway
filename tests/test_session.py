import asyncio
import contextlib
import gc
from types import SimpleNamespace

import pytest

from spotflow_gateway import cloud, protocol
from spotflow_gateway import session as session_module
from spotflow_gateway.protocol import MessageType
from spotflow_gateway.session import C2D_ROUTES, DeviceSession, SessionError

from .conftest import FakeLink, publish


def settings_for(broker):
    return cloud.CloudSettings(ingest_key="secret", host="127.0.0.1", port=broker.port, tls=False)


async def test_forwards_session_metadata_then_telemetry(broker, recorder):
    link = FakeLink(metadata=b"\xa1\x00\x01META")
    session = DeviceSession(link, settings_for(broker))
    task = asyncio.create_task(session.run())

    await asyncio.wait_for(link.notifying.wait(), 5)
    big = bytes(range(256)) * 4  # spans many 20-byte frames
    link.send(MessageType.TELEMETRY, b"log-1")
    link.send(MessageType.TELEMETRY, big)
    link.send(MessageType.ARTIFACT_RESPONSE, b"never sent by a device, not routed")

    await recorder.wait_for(3)
    assert recorder.messages == [
        ("ingest-cbor", b"\xa1\x00\x01META"),
        ("ingest-cbor", b"log-1"),
        ("ingest-cbor", big),
    ]
    assert session.device_id == "test-device"

    link.disconnected.set()
    await asyncio.wait_for(task, 5)
    assert session.forwarded == 2


async def test_mqtt_reconnect_republishes_metadata_and_keeps_messages(broker, recorder):
    link = FakeLink(metadata=b"META")
    task = asyncio.create_task(DeviceSession(link, settings_for(broker)).run())
    await asyncio.wait_for(link.notifying.wait(), 5)
    link.send(MessageType.TELEMETRY, b"before")
    await recorder.wait_for(2)

    link.metadata_gate = asyncio.Event()  # hold the gateway's reconnect...
    broker.stop()
    link.send(MessageType.TELEMETRY, b"during-1")
    await asyncio.sleep(0.5)
    link.send(MessageType.TELEMETRY, b"during-2")
    broker.start()
    await recorder.wait_subscribed(2)  # ...until the recorder listens again
    link.metadata_gate.set()

    await recorder.wait_for(5, timeout=20)
    assert recorder.messages[:2] == [("ingest-cbor", b"META"), ("ingest-cbor", b"before")]
    # After reconnecting, session metadata goes first, then the buffered telemetry.
    assert recorder.messages[2] == ("ingest-cbor", b"META")
    assert recorder.messages[-2:] == [("ingest-cbor", b"during-1"), ("ingest-cbor", b"during-2")]

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_unsupported_protocol_version(broker):
    link = FakeLink(capabilities=b"\x02")
    with pytest.raises(SessionError):
        await DeviceSession(link, settings_for(broker)).run()
    assert link.disconnected.is_set()


async def test_ends_when_device_disconnects_while_mqtt_down(tmp_path):
    link = FakeLink()
    unreachable = cloud.CloudSettings(ingest_key="k", host="127.0.0.1", port=1, tls=False)
    task = asyncio.create_task(DeviceSession(link, unreachable).run())
    await asyncio.sleep(0.3)
    link.disconnected.set()
    await asyncio.wait_for(task, 5)


# CBOR {0: 3, 0x10: 60 (ERROR), 0x12: 1}, the desired-config vector from the SDK's ble_config test.
DESIRED_ERROR_V1 = bytes([0xA3, 0x00, 0x03, 0x10, 0x18, 0x3C, 0x12, 0x01])
# CBOR {0: 4, 0x10: 40, 0x11: 30, 0x13: 1}: reported configuration acknowledging version 1.
REPORTED_V1 = bytes([0xA4, 0x00, 0x04, 0x10, 0x18, 0x28, 0x11, 0x18, 0x1E, 0x13, 0x01])


async def test_remote_log_severity_roundtrip(broker, recorder):
    link = FakeLink(metadata=b"META")
    task = asyncio.create_task(DeviceSession(link, settings_for(broker)).run())
    await asyncio.wait_for(link.notifying.wait(), 5)

    await publish(broker, "config-cbor-c2d", DESIRED_ERROR_V1)
    await link.wait_received(1)
    desired = link.received[0]
    assert desired.message_type == MessageType.DESIRED_CONFIGURATION
    assert desired.payload == DESIRED_ERROR_V1
    assert desired.needs_ack  # protocol 1.1 device

    # The device applies it and reports back; the gateway forwards the report.
    link.send(MessageType.REPORTED_CONFIGURATION, REPORTED_V1)
    await recorder.wait_for(3)  # META, the desired config itself (recorder sees all), the report
    assert ("config-cbor-d2c", REPORTED_V1) in recorder.messages

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_config_resubscribed_after_mqtt_reconnect(broker, recorder):
    link = FakeLink(metadata=b"META")
    task = asyncio.create_task(DeviceSession(link, settings_for(broker)).run())
    await asyncio.wait_for(link.notifying.wait(), 5)
    await recorder.wait_for(1)

    link.metadata_gate = asyncio.Event()
    broker.stop()
    broker.start()
    await recorder.wait_subscribed(2)
    link.metadata_gate.set()
    await recorder.wait_for(2, timeout=20)  # metadata republished => gateway reconnected

    # The subscription follows the metadata publish, so retry until it is in place.
    for _ in range(20):
        await publish(broker, "config-cbor-c2d", DESIRED_ERROR_V1)
        with contextlib.suppress(TimeoutError):
            await link.wait_received(1, timeout=0.5)
            break
    assert link.received[0].payload == DESIRED_ERROR_V1

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_legacy_device_gets_config_without_ack_flag(broker, recorder):
    link = FakeLink(capabilities=b"\x01")
    task = asyncio.create_task(DeviceSession(link, settings_for(broker)).run())
    await asyncio.wait_for(link.notifying.wait(), 5)

    await publish(broker, "config-cbor-c2d", DESIRED_ERROR_V1)
    await link.wait_received(1)
    assert not link.received[0].needs_ack

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


def test_device_specific_c2d_topic_is_routed():
    session = DeviceSession(FakeLink(), cloud.CloudSettings(ingest_key="k"))
    session._routes = {route.topic: route for route in C2D_ROUTES}
    channel = SimpleNamespace(items=[])
    channel.submit = channel.items.append
    session._downlinks[MessageType.DESIRED_CONFIGURATION] = channel
    session._on_cloud_message("config-cbor-c2d/workspace/device", b"\x01")
    session._on_cloud_message("ota-cbor-c2d/workspace/device", b"\x02")
    session._on_cloud_message("config-cbor-c2d", b"")
    assert channel.items == [b"\x01"]


async def test_messages_buffered_during_outage_are_reported_when_device_disconnects(caplog):
    link = FakeLink()
    unreachable = cloud.CloudSettings(ingest_key="k", host="127.0.0.1", port=1, tls=False)
    session = DeviceSession(link, unreachable)
    session._enqueue(("ingest-cbor", b"a", 0))
    session._enqueue(("ingest-cbor", b"b", 0))
    task = asyncio.create_task(session.run())
    await asyncio.sleep(0)
    link.disconnected.set()
    await asyncio.wait_for(task, 10)
    assert "2 buffered messages discarded" in caplog.text
    assert session.forwarded == 0


async def test_refused_ingest_key_is_logged_and_retried(mini_broker, caplog):
    broker = await mini_broker(return_code=4)
    link = FakeLink()
    settings = cloud.CloudSettings(ingest_key="wrong", host="127.0.0.1", port=broker.port, tls=False)
    task = asyncio.create_task(DeviceSession(link, settings).run())

    async with asyncio.timeout(10):
        while len(broker.credentials) < 2:  # the second attempt follows the first backoff
            await asyncio.sleep(0.05)
    # The message the README's troubleshooting table refers to.
    assert "[test-device] MQTT error: connection refused: Bad user name or password" in caplog.text
    assert broker.credentials[0] == ("test-device", "wrong")
    assert not link.notifying.is_set()  # nothing is requested from the device meanwhile

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_failed_acknowledgement_write_is_logged_not_raised(caplog):
    unhandled = []
    asyncio.get_running_loop().set_exception_handler(lambda _loop, context: unhandled.append(context))
    link = FakeLink()

    async def failing_write(frame):
        raise OSError("write failed")

    link.write = failing_write
    session = DeviceSession(link, cloud.CloudSettings(ingest_key="k"))
    for frame in protocol.encode_frames(MessageType.OTA_D2C, 7, b"result", 20, needs_ack=True):
        session._on_frame(frame)
    while session._tasks:
        await asyncio.sleep(0)
    gc.collect()
    await asyncio.sleep(0)
    assert "Failed to write ACK for type 0x05 seq=7" in caplog.text
    assert unhandled == []


def retry_delays(caplog):
    """The delays announced by the session's "MQTT error ... (retrying in ...)" warnings."""
    return [record.args[-1] for record in caplog.records if "MQTT error" in record.getMessage()]


async def run_against_dropping_broker(mini_broker, caplog, failures):
    """Run a session against a broker that accepts connections and then drops them."""
    broker = await mini_broker(drop_subscribers=True)
    link = FakeLink()
    settings = cloud.CloudSettings(ingest_key="k", host="127.0.0.1", port=broker.port, tls=False)
    task = asyncio.create_task(DeviceSession(link, settings).run())
    async with asyncio.timeout(10):
        while len(retry_delays(caplog)) < failures:
            await asyncio.sleep(0.01)
    link.disconnected.set()
    await asyncio.wait_for(task, 5)
    return retry_delays(caplog)[:failures]


async def test_connection_dropped_right_after_connecting_backs_off(mini_broker, caplog, monkeypatch):
    monkeypatch.setattr(session_module, "MIN_BACKOFF_S", 0.01)
    delays = await run_against_dropping_broker(mini_broker, caplog, failures=4)
    assert delays == [0.01, 0.02, 0.04, 0.08]  # not a reconnect every MIN_BACKOFF_S


async def test_backoff_starts_over_after_a_stable_connection(mini_broker, caplog, monkeypatch):
    monkeypatch.setattr(session_module, "MIN_BACKOFF_S", 0.01)
    monkeypatch.setattr(session_module, "STABLE_CONNECTION_S", 0.0)  # every connection counts as stable
    delays = await run_against_dropping_broker(mini_broker, caplog, failures=3)
    assert delays == [0.01, 0.01, 0.01]


async def test_full_buffer_is_reported_once_per_interval(caplog, monkeypatch):
    monkeypatch.setattr(session_module, "QUEUE_SIZE", 3)
    session = DeviceSession(FakeLink(), cloud.CloudSettings(ingest_key="k"))

    def warnings():
        return [r.getMessage() for r in caplog.records if "Buffer for the cloud is full" in r.getMessage()]

    for i in range(10):
        session._enqueue(("ingest-cbor", bytes([i]), 0))
    assert session.dropped == 7
    assert len(warnings()) == 1 and "(1 dropped so far)" in warnings()[0]
    assert [session._queue.get_nowait()[1] for _ in range(3)] == [b"\x07", b"\x08", b"\x09"]  # newest kept

    for i in range(4):
        session._enqueue(("ingest-cbor", bytes([i]), 0))
    session._next_drop_report = 0.0  # the reporting interval has passed
    session._enqueue(("ingest-cbor", b"x", 0))
    assert len(warnings()) == 2 and "(9 dropped so far)" in warnings()[1]


async def test_only_supported_message_types_are_acknowledged():
    link = FakeLink()
    session = DeviceSession(link, cloud.CloudSettings(ingest_key="k"))

    async def controls():
        while session._tasks:
            await asyncio.sleep(0)
        return link.written

    # Unsupported types get neither ACK nor NACK: a type the gateway only sends, an
    # unknown type, and artifact requests of a device that has no OTA session.
    for message_type in (MessageType.DESIRED_CONFIGURATION, 0x7F, MessageType.ARTIFACT_REQUEST):
        for frame in protocol.encode_frames(message_type, 1, bytes(40), 20, needs_ack=True):
            session._on_frame(frame)
        session._on_frame(bytes([message_type, protocol.FrameFlags.NEEDS_ACK, 2]) + b"orphan")
    assert await controls() == []
    assert session._reassembler._channels == {}  # and they take no reassembly memory
    assert session._queue.empty()

    # A supported type is acknowledged, or refused if it cannot be reassembled.
    for frame in protocol.encode_frames(MessageType.TELEMETRY, 3, bytes(40), 20, needs_ack=True):
        session._on_frame(frame)
    session._on_frame(bytes([MessageType.TELEMETRY, protocol.FrameFlags.NEEDS_ACK, 4]) + b"orphan")
    assert await controls() == [
        protocol.encode_control(True, MessageType.TELEMETRY, 3),
        protocol.encode_control(False, MessageType.TELEMETRY, 4),
    ]
    assert session._queue.qsize() == 1
