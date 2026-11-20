import asyncio
import os
import shutil
import socket
import subprocess
import time

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.enums import CallbackAPIVersion

from spotflow_gateway import protocol

_MOSQUITTO_DIRS = ["/opt/homebrew/sbin", "/usr/local/sbin", "/usr/sbin", r"C:\Program Files\mosquitto"]
MOSQUITTO = shutil.which("mosquitto") or shutil.which("mosquitto", path=os.pathsep.join(_MOSQUITTO_DIRS))
# CI sets this so the broker-backed tests fail instead of being skipped.
REQUIRE_MOSQUITTO = os.environ.get("REQUIRE_MOSQUITTO") == "1"

class Broker:
    """A local Mosquitto broker that can be stopped and restarted on the same port."""

    def __init__(self, tmp_path):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.log_path = tmp_path / "mosquitto.log"
        self.proc = None

    def start(self):
        log = open(self.log_path, "ab")
        try:
            # No config file: with only -p, Mosquitto 2 runs in "local only mode"
            # (loopback, anonymous allowed). Ubuntu's packaged broker also cannot read
            # config files from pytest's temp directory.
            self.proc = subprocess.Popen([MOSQUITTO, "-v", "-p", str(self.port)],
                                         stdout=log, stderr=subprocess.STDOUT)
        finally:
            log.close()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and self.proc.poll() is None:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        code = self.proc.poll()
        self.stop()
        output = self.log_path.read_text(errors="replace")[-2000:]
        raise RuntimeError(f"mosquitto did not start (exit code {code}):\n{output}")

    def stop(self):
        if self.proc:
            if self.proc.poll() is None:
                self.proc.terminate()
                self.proc.wait(5)
            self.proc = None


@pytest.fixture
def broker(tmp_path):
    if not MOSQUITTO:
        if REQUIRE_MOSQUITTO:
            pytest.fail("mosquitto not installed but REQUIRE_MOSQUITTO=1")
        pytest.skip("mosquitto not installed")
    b = Broker(tmp_path)
    b.start()
    yield b
    b.stop()


class Recorder:
    """Subscribes to all topics and records (topic, payload) pairs.

    Uses paho's own network thread and reconnects (and resubscribes) on its own,
    so it keeps recording across broker restarts.
    """

    def __init__(self, port):
        self.port = port
        self.messages: list[tuple[str, bytes]] = []
        self._loop = asyncio.get_running_loop()
        self._changed = asyncio.Event()
        self._subscribed = asyncio.Event()
        self.subscriptions = 0
        self._client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self._client.reconnect_delay_set(min_delay=1, max_delay=1)
        self._client.on_connect = lambda c, *_: c.subscribe("#")
        self._client.on_subscribe = lambda *_: self._loop.call_soon_threadsafe(self._on_subscribed)
        self._client.on_message = lambda _c, _u, m: self._loop.call_soon_threadsafe(
            self._add, m.topic, bytes(m.payload)
        )

    def _on_subscribed(self):
        self.subscriptions += 1
        self._subscribed.set()

    async def wait_subscribed(self, count, timeout=20.0):
        """Wait until the recorder has subscribed ``count`` times (e.g. after a broker restart)."""
        async def _wait():
            while self.subscriptions < count:
                self._subscribed.clear()
                await self._subscribed.wait()
        await asyncio.wait_for(_wait(), timeout)

    def _add(self, topic, payload):
        self.messages.append((topic, payload))
        self._changed.set()

    async def start(self):
        await asyncio.to_thread(self._client.connect, "127.0.0.1", self.port)
        self._client.loop_start()
        await asyncio.wait_for(self._subscribed.wait(), 5)

    async def stop(self):
        self._client.disconnect()
        await asyncio.to_thread(self._client.loop_stop)

    async def wait_for(self, count, timeout=10.0):
        async def _wait():
            while len(self.messages) < count:
                self._changed.clear()
                await self._changed.wait()
        await asyncio.wait_for(_wait(), timeout)


@pytest.fixture
async def recorder(broker):
    rec = Recorder(broker.port)
    await rec.start()
    yield rec
    await rec.stop()


class MiniBroker:
    """A minimal in-process MQTT 3.1.1 broker for what the Mosquitto setup above cannot do
    without a config file: TLS and refused connections.

    Answers every CONNECT with ``return_code`` (0 accepts) and records the credentials
    it was given and the messages published to it. With ``acknowledge=False`` it keeps
    connections open but never acknowledges a publish or a subscription; with
    ``drop_subscribers=True`` it closes the connection of a client that subscribes.
    """

    def __init__(self, return_code=0, ssl_context=None, acknowledge=True, drop_subscribers=False):
        self.return_code = return_code
        self.ssl_context = ssl_context
        self.acknowledge = acknowledge
        self.drop_subscribers = drop_subscribers
        self.port = 0
        self.credentials: list[tuple[str, str]] = []
        self.subscriptions: list[str] = []
        self.messages: list[tuple[str, bytes]] = []
        self._server = None
        self._writers = set()

    async def start(self):
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0, ssl=self.ssl_context)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        self._server.close()
        for writer in list(self._writers):
            writer.close()
        await self._server.wait_closed()

    async def _serve(self, reader, writer):
        self._writers.add(writer)
        try:
            while True:
                reply, keep_open = self._handle(*await _read_packet(reader))
                writer.write(reply)
                await writer.drain()
                if not keep_open:
                    break
        except (asyncio.IncompleteReadError, OSError, ValueError):
            pass  # the client went away, or did not speak MQTT (e.g. TLS to a plain broker)
        finally:
            self._writers.discard(writer)
            writer.close()

    def _handle(self, first, body):
        """Process one packet; returns (reply, whether to keep the connection open)."""
        kind, qos = first >> 4, (first >> 1) & 3
        if kind == 1:  # CONNECT: 10-byte variable header, then client ID, user name, password
            _client_id, username, password = _fields(body[10:])
            self.credentials.append((username.decode(), password.decode()))
            return bytes([0x20, 2, 0, self.return_code]), self.return_code == 0
        if kind == 3:  # PUBLISH: topic, message ID (QoS 1 only), payload
            topic = _fields(body)[0]
            payload = body[2 + len(topic):]
            message_id, payload = (payload[:2], payload[2:]) if qos else (b"", payload)
            self.messages.append((topic.decode(), payload))
            return (b"\x40\x02" + message_id if qos and self.acknowledge else b""), True
        if kind == 8:  # SUBSCRIBE: message ID, then one topic filter with its requested QoS
            topic = _fields(body[2:-1])[0]
            self.subscriptions.append(topic.decode())
            if self.drop_subscribers:
                return b"", False
            return (b"\x90\x03" + body[:2] + body[-1:] if self.acknowledge else b""), True
        return b"", kind != 14  # DISCONNECT closes; anything else is ignored


async def _read_packet(reader):
    """Read one MQTT packet; returns (first header byte, everything after the length)."""
    first = (await reader.readexactly(1))[0]
    length = shift = 0
    while True:
        byte = (await reader.readexactly(1))[0]
        length |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return first, await reader.readexactly(length)


def _fields(data):
    """Split consecutive MQTT strings (2-byte length prefix each)."""
    fields = []
    while len(data) >= 2:
        size = int.from_bytes(data[:2], "big")
        fields.append(data[2:2 + size])
        data = data[2 + size:]
    return fields


@pytest.fixture
async def mini_broker():
    """Factory for MiniBroker instances that are stopped after the test."""
    brokers = []

    async def start(**kwargs):
        broker = MiniBroker(**kwargs)
        await broker.start()
        brokers.append(broker)
        return broker

    yield start
    for broker in brokers:
        await broker.stop()


def _publish_blocking(port, topic, payload, qos, retain):
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.connect("127.0.0.1", port)
    client.loop_start()
    try:
        client.publish(topic, payload, qos=qos, retain=retain).wait_for_publish(5)
    finally:
        client.disconnect()
        client.loop_stop()


async def publish(broker, topic, payload, qos=1, retain=False):
    """Publish one message to the test broker, as the cloud would.

    A retained message reaches the gateway as soon as it subscribes, like the desired
    configuration that the cloud sends to every new subscription.
    """
    await asyncio.to_thread(_publish_blocking, broker.port, topic, payload, qos, retain)


class FakeLink:
    """Simulates the GATT side of a device running the Spotflow Device SDK."""

    def __init__(self, device_id=b"test-device", capabilities=b"\x01\x01\x00", metadata=b"\xa1\x00\x01"):
        self.name = "fake-device"
        self.label = "fake-device AA:BB"
        self.disconnected = asyncio.Event()
        self.values = {
            protocol.CAPABILITIES_UUID: capabilities,
            protocol.DEVICE_ID_UUID: device_id,
            protocol.SESSION_METADATA_UUID: metadata,
        }
        self.written: list[bytes] = []
        # Messages the gateway wrote, reassembled like the device does.
        self.received: list[protocol.Message] = []
        self.received_changed = asyncio.Event()
        # Reply to each NEEDS_ACK message in turn: True=ACK, False=NACK, None=silent.
        # Once exhausted, the device ACKs.
        self.ack_replies: list[bool | None] = []
        self._rx = protocol.Reassembler()
        self.notifying = asyncio.Event()
        self._callback = None
        self._sequence = {}
        # When set, reading session metadata (done right after every MQTT connect,
        # before anything is published) waits for it. Lets tests hold a reconnect.
        self.metadata_gate: asyncio.Event | None = None
        self.tx_frame_size = 20  # frames the device sends (ATT MTU - 3)
        self.notify_delay = 0.0

    async def read(self, uuid):
        if uuid == protocol.SESSION_METADATA_UUID and self.metadata_gate is not None:
            await self.metadata_gate.wait()
        return self.values[uuid]

    async def start_notify(self, callback):
        # Enabling notifications is a GATT write: it takes a while over a real link.
        await asyncio.sleep(self.notify_delay)
        self._callback = callback
        self.notifying.set()

    async def write(self, frame):
        self.written.append(frame)
        result = self._rx.feed(frame)
        if not isinstance(result, protocol.Message):
            return
        self.received.append(result)
        self.received_changed.set()
        if result.needs_ack:
            reply = self.ack_replies.pop(0) if self.ack_replies else True
            # Like the Device SDK, the device cannot send an ACK or NACK before the
            # gateway has enabled notifications: it is dropped.
            if reply is not None and self.notifying.is_set():
                control = protocol.encode_control(reply, result.message_type, result.sequence)
                asyncio.get_running_loop().call_soon(self._callback, control)

    async def wait_received(self, count, timeout=10.0):
        async def _wait():
            while len(self.received) < count:
                self.received_changed.clear()
                await self.received_changed.wait()
        await asyncio.wait_for(_wait(), timeout)

    async def disconnect(self):
        self.disconnected.set()

    def send(self, message_type, payload):
        seq = self._sequence.get(message_type, 0)
        self._sequence[message_type] = (seq + 1) & 0xFF
        for frame in protocol.encode_frames(message_type, seq, payload, self.tx_frame_size):
            self._callback(frame)


async def wait_online(session, timeout=5.0):
    """Wait until the session is connected to the broker and subscribed to its topics.

    Messages the cloud publishes before that would not reach the gateway (the broker
    keeps only retained ones).
    """
    async def _wait():
        while not session.cloud_online:
            await asyncio.sleep(0.01)
    await asyncio.wait_for(_wait(), timeout)
