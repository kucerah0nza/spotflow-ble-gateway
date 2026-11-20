"""End-to-end OTA through a gateway session: a fake device running the Device SDK's
BLE downloader sequence, a real local MQTT broker, and a fake artifact host."""

import asyncio
import struct

import pytest

from spotflow_gateway import cloud
from spotflow_gateway.artifacts import Status
from spotflow_gateway.protocol import FrameFlags, MessageType
from spotflow_gateway.session import DeviceSession, SessionError

from .conftest import FakeLink, publish, wait_online
from .test_artifacts import FIRMWARE, SECRET, Origin, close_req, make_cache, open_req, read_req

MANIFEST = b"\xa4\x00\x06\x18\x20\x01..."  # opaque to the gateway
RESULTS = b"\xa3\x00\x09\x18\x20\x01\x18\x2a\x81\x00"  # opaque to the gateway


async def start(broker, tmp_path, capabilities=b"\x01\x01\x01", tx_frame_size=20):
    origin = Origin()
    cache = make_cache(tmp_path, origin)
    link = FakeLink(capabilities=capabilities)
    link.tx_frame_size = tx_frame_size
    settings = cloud.CloudSettings(ingest_key="k", host="127.0.0.1", port=broker.port, tls=False)
    session = DeviceSession(link, settings, cache)
    task = asyncio.create_task(session.run())
    await wait_online(session)
    return link, session, task, origin


async def exchange(link, request):
    """Send an ARTIFACT_REQUEST like the device and wait for the gateway's response."""
    n = len(link.received)
    link.send(MessageType.ARTIFACT_REQUEST, request)
    await link.wait_received(n + 1)
    response = link.received[n]
    assert response.message_type == MessageType.ARTIFACT_RESPONSE
    assert not response.needs_ack
    return response.payload


async def download(link, block=512):
    tid, rid = 0x1234567, 1
    rid += 1
    opened = await exchange(link, open_req(tid, rid))
    assert opened[9] == Status.OK
    size = struct.unpack_from("<I", opened, 10)[0]
    data = b""
    while len(data) < size:
        rid += 1
        count = min(block, size - len(data))
        response = await exchange(link, read_req(tid, rid, len(data), count))
        assert response[9] == Status.OK
        data += response[14:]
    rid += 1
    assert (await exchange(link, close_req(tid, rid, size)))[9] == Status.OK
    return data


async def test_ota_update_end_to_end(broker, recorder, tmp_path):
    link, session, task, origin = await start(broker, tmp_path)
    assert session.ota_enabled

    # Cloud -> device: the update command is forwarded unchanged and acknowledged.
    await publish(broker, "ota-cbor-c2d", MANIFEST)
    await link.wait_received(1)
    command = link.received[0]
    assert (command.message_type, command.payload, command.needs_ack) == (MessageType.OTA_C2D, MANIFEST, True)

    # Device downloads the firmware through the gateway.
    assert await download(link) == FIRMWARE
    assert len(origin.requests) == 1
    assert origin.requests[0].headers["Authorization"] == f"OtaSecret {SECRET}"
    assert all(len(f) <= 20 for f in link.written)

    # Device -> cloud: update results go to ota-cbor-d2c.
    link.send(MessageType.OTA_D2C, RESULTS)
    await recorder.wait_for(3)  # session metadata, the command (recorder sees all), results
    assert ("ota-cbor-d2c", RESULTS) in recorder.messages

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_responses_use_largest_frame_the_device_sent(broker, recorder, tmp_path):
    link, session, task, _ = await start(broker, tmp_path, tx_frame_size=244)
    sent = []
    original = link._callback
    link._callback = lambda frame: (sent.append(len(frame)), original(frame))

    # Only small single-frame requests so far: frames grow to their size, not the MTU.
    assert await download(link, block=4096) == FIRMWARE
    assert 20 < max(len(f) for f in link.written) <= max(sent)

    # A large fragmented message (e.g. a core dump chunk) proves the full frame size.
    link.send(MessageType.TELEMETRY, bytes(1000))
    assert session.frame_size == 244
    link.written.clear()
    assert await download(link, block=4096) == FIRMWARE
    assert max(len(f) for f in link.written) == 244

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_device_without_ota_gets_no_ota_traffic(broker, recorder, tmp_path):
    link, session, task, origin = await start(broker, tmp_path, capabilities=b"\x01\x01\x00")
    assert not session.ota_enabled

    await publish(broker, "ota-cbor-c2d", MANIFEST)
    link.send(MessageType.ARTIFACT_REQUEST, open_req(1, 2))
    await publish(broker, "config-cbor-c2d", b"\xa1\x00\x03")  # still gets configuration
    await link.wait_received(1)
    assert [m.message_type for m in link.received] == [MessageType.DESIRED_CONFIGURATION]
    assert origin.requests == []

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_ota_command_retried_until_acknowledged(broker, recorder, tmp_path):
    link, session, task, _ = await start(broker, tmp_path)
    link.ack_replies = [False, None]  # NACK, then no answer, then ACK
    session._downlinks[MessageType.OTA_C2D].ack_timeout = 0.2

    await publish(broker, "ota-cbor-c2d", MANIFEST)
    await link.wait_received(3)
    assert [(m.sequence, m.payload) for m in link.received] == [(0, MANIFEST)] * 3
    assert all(f[1] & FrameFlags.NEEDS_ACK for f in link.written)

    link.disconnected.set()
    await asyncio.wait_for(task, 5)


async def test_undeliverable_ota_command_restarts_the_session(broker, recorder, tmp_path):
    """The cloud repeats a command only on a new subscription, so a command the device
    never acknowledges must not be dropped silently: the session ends with an error and
    the gateway connects to the device again."""
    link, session, task, _ = await start(broker, tmp_path)
    link.ack_replies = [None, None]
    channel = session._downlinks[MessageType.OTA_C2D]
    channel.ack_timeout, channel.max_attempts = 0.05, 2

    await publish(broker, "ota-cbor-c2d", MANIFEST)
    with pytest.raises(SessionError, match="OTA command could not be delivered"):
        await asyncio.wait_for(task, 10)
    assert link.disconnected.is_set()
    assert len(link.received) == 2


async def test_undeliverable_configuration_keeps_the_session(broker, recorder, tmp_path):
    link, session, task, _ = await start(broker, tmp_path)
    link.ack_replies = [None]
    channel = session._downlinks[MessageType.DESIRED_CONFIGURATION]
    channel.ack_timeout, channel.max_attempts = 0.05, 1

    await publish(broker, "config-cbor-c2d", b"\xa1\x00\x03")
    async with asyncio.timeout(5):
        while channel.failed == 0:
            await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)
    assert not link.disconnected.is_set() and not task.done()

    link.disconnected.set()
    await asyncio.wait_for(task, 5)
