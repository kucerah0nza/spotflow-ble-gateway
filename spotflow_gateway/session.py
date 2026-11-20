"""Bridge between one BLE device and its MQTT connection to the Spotflow cloud.

Payloads are forwarded unchanged; the gateway never decodes telemetry, configuration
or OTA messages. The only protocol it terminates is the artifact (firmware) transfer.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Coroutine
from dataclasses import dataclass

from . import cloud, config, protocol
from .artifacts import ArtifactCache, ArtifactServer
from .ble import DeviceLink
from .downlink import DownlinkChannel
from .logs import DeviceLogger, Throttle, count, debug_traceback, describe, duration, printable
from .protocol import Control, Message, MessageType, Rejected
from .status import Counts

log = logging.getLogger(__name__)

# Device -> cloud routing of reassembled BLE messages: type -> (topic, publish QoS).
# Telemetry and configuration use QoS 0, like the Device SDK. OTA results use QoS 1
# (the cloud accepts 0 or 1 there), so that a result is not lost with a connection that
# is just breaking: the cloud repeats an update command until it has the results.
D2C_TOPICS: dict[int, tuple[str, int]] = {
    MessageType.TELEMETRY: (cloud.TOPIC_INGEST_CBOR, 0),
    MessageType.REPORTED_CONFIGURATION: (cloud.TOPIC_CONFIG_D2C, 0),
    MessageType.OTA_D2C: (cloud.TOPIC_OTA_D2C, 1),
}


@dataclass(frozen=True)
class C2DRoute:
    topic: str
    message_type: MessageType
    qos: int
    ota: bool  # only for devices that advertise OTA support
    max_attempts: int


# Cloud -> device routing. The broker may deliver on a device-specific subtopic,
# e.g. ota-cbor-c2d/<workspace>/<device>, so routing uses the first topic level.
C2D_ROUTES = [
    C2DRoute(cloud.TOPIC_CONFIG_C2D, MessageType.DESIRED_CONFIGURATION, qos=0, ota=False, max_attempts=5),
    C2DRoute(cloud.TOPIC_OTA_C2D, MessageType.OTA_C2D, qos=1, ota=True, max_attempts=10),
]

QUEUE_SIZE = 1000
# While the buffer overflows, say so at most this often (not once per dropped message).
DROP_REPORT_INTERVAL_S = 60.0
# Delay before reconnecting to the cloud: doubles after every failure and starts over
# only once a connection has lasted STABLE_CONNECTION_S. A connection that is accepted
# and then dropped right away (a refused subscription, another session using the same
# device ID) therefore backs off like one that cannot be established at all.
MIN_BACKOFF_S = 1.0
MAX_BACKOFF_S = 60.0
STABLE_CONNECTION_S = 30.0

# A message waiting to be published: (topic, payload, QoS).
Outgoing = tuple[str, bytes, int]


class SessionError(Exception):
    pass


class DeviceSession:
    def __init__(
        self,
        link: DeviceLink,
        settings: cloud.CloudSettings,
        artifacts: ArtifactCache | None = None,
    ) -> None:
        self.link = link
        self.settings = settings
        self.device_id: str | None = None
        self.capabilities: protocol.Capabilities | None = None
        self.forwarded = 0
        self.dropped = 0  # messages lost to a full buffer
        self.rejected = 0  # messages from the device that could not be reassembled
        self.discarded = 0  # buffered messages lost because the session ended
        self.cloud_online = False  # whether the MQTT connection is up and subscribed
        self.started: float | None = None  # when the device was set up (time.monotonic())
        # The device's minimal log severity: as it last reported it, or as the cloud last
        # set it. Only for the log; None until known.
        self.log_severity: int | None = None
        self._log = DeviceLogger(log, lambda: self.label)
        self._reject_throttle = Throttle()
        self.frame_size = protocol.DEFAULT_FRAME_SIZE
        self._artifact_cache = artifacts
        self._reassembler = protocol.Reassembler()
        # None marks the end of the queue: the BLE device has disconnected.
        self._queue: asyncio.Queue[Outgoing | None] = asyncio.Queue(QUEUE_SIZE)
        self._pending: Outgoing | None = None
        # Messages buffered while the cloud was unreachable that are still to be sent
        # after reconnecting, and how many there were (for the log).
        self._backlog = 0
        self._backlog_total = 0
        self._next_drop_report = 0.0
        self._end_queued = False  # whether the end-of-queue marker (None) is in the queue
        self._notifying = False
        # Why the session has to be restarted, if it does (see _on_undelivered).
        self._failure: str | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._routes: dict[str, C2DRoute] = {}
        self._downlinks: dict[int, DownlinkChannel] = {}
        self._artifact_server: ArtifactServer | None = None

    @property
    def label(self) -> str:
        """How log messages name the device: its BLE name and address, then its device ID."""
        return self.link.label

    @property
    def ota_enabled(self) -> bool:
        return self._artifact_server is not None

    async def run(self) -> None:
        """Run until the BLE device disconnects (or the task is cancelled).

        Raises SessionError if the device cannot be served; the caller is expected to
        connect to it again later.
        """
        watcher = asyncio.create_task(self._watch_disconnect())
        try:
            await self._cloud_loop(await self._discover())
            if self._failure is not None:
                raise SessionError(self._failure)
        finally:
            watcher.cancel()
            for task in self._tasks:
                task.cancel()
            await self.link.disconnect()
            discarded = self._discard_buffered()
            self.discarded += discarded
            if discarded:
                self._log.warning("%d buffered messages discarded: the session ended before "
                                  "they could be sent to the cloud", discarded)
            # The gateway logs the end of the session, with its outcome and summary().

    def counts(self) -> Counts:
        """What the session has forwarded so far (for the gateway's status line)."""
        # Artifact responses are firmware blocks; their transfers are logged on their own.
        downlinks = [c for t, c in self._downlinks.items() if t != MessageType.ARTIFACT_RESPONSE]
        return Counts(
            to_cloud=self.forwarded,
            to_devices=sum(c.delivered for c in downlinks),
            dropped=self.dropped,
            discarded=self.discarded,
            undelivered=sum(c.failed for c in downlinks),
            malformed=self.rejected,
        )

    def summary(self) -> str:
        """How long the session ran and what it forwarded, e.g. "after 2h 03m (1520 messages
        to the cloud, 3 to the device)"; empty if the device was never set up."""
        if self.started is None:
            return ""
        counts = self.counts()
        parts = [f"{counts.to_cloud} messages to the cloud"]
        if counts.dropped:
            parts.append(f"{counts.dropped} dropped")
        parts.append(f"{counts.to_devices} to the device")
        if counts.undelivered:
            parts.append(f"{counts.undelivered} undelivered")
        if counts.malformed:
            parts.append(f"{counts.malformed} malformed from the device")
        return f"after {duration(time.monotonic() - self.started)} ({', '.join(parts)})"

    def buffered(self) -> int:
        """Messages waiting to be sent to the cloud."""
        # The end-of-queue marker is not a message (and may already have been taken).
        return max(0, (self._pending is not None) + self._queue.qsize() - self._end_queued)

    def _discard_buffered(self) -> int:
        """Drop messages not yet published; the buffer does not outlive the BLE session."""
        discarded = 0 if self._pending is None else 1
        self._pending = None
        while not self._queue.empty():
            if self._queue.get_nowait() is not None:
                discarded += 1
        return discarded

    async def _discover(self) -> str:
        """Read the device's identity, set up its downlink channels, return its device ID."""
        caps = protocol.Capabilities.parse(await self.link.read(protocol.CAPABILITIES_UUID))
        self.capabilities = caps
        if not caps.is_supported:
            raise SessionError(f"unsupported protocol version {caps}")

        raw_id = await self.link.read(protocol.DEVICE_ID_UUID)
        device_id = raw_id.decode("utf-8", errors="replace").rstrip("\x00").strip()
        if not device_id:
            raise SessionError("device reported an empty device ID")
        self.device_id = device_id  # used as is towards the cloud, but logged escaped
        ble_label, self.link.label = self.link.label, printable(device_id)
        self.started = time.monotonic()

        cache = self._artifact_cache if caps.supports_ota else None
        self._log.info("Connected to %s, protocol %s%s", ble_label, caps,
                       "" if cache or not caps.supports_ota else " (OTA disabled: no artifact cache)")

        for route in C2D_ROUTES:
            if route.ota and cache is None:
                continue
            # OTA commands always require an ACK; configuration does from protocol 1.1.
            channel = DownlinkChannel(
                self.link, route.message_type, needs_ack=route.ota or caps.supports_acks,
                label=self.label, max_attempts=route.max_attempts,
            )
            if route.ota:
                channel.on_undelivered = self._on_undelivered
            self._routes[route.topic] = route
            self._downlinks[route.message_type] = channel
            self._start_task(channel.run())

        if cache is not None:
            responses = DownlinkChannel(
                self.link, MessageType.ARTIFACT_RESPONSE, needs_ack=False, label=self.label, quiet=True
            )
            self._downlinks[MessageType.ARTIFACT_RESPONSE] = responses
            self._artifact_server = ArtifactServer(cache, responses.send, self.label)
            self._start_task(self._artifact_server.run())
        return device_id

    async def _cloud_loop(self, device_id: str) -> None:
        backoff = MIN_BACKOFF_S
        offline_since: float | None = None  # when the cloud became unreachable
        while not self.link.disconnected.is_set():
            connection = cloud.CloudConnection(self.settings, device_id, self._on_cloud_message)
            connected_at: float | None = None
            try:
                await connection.connect()
                connected_at = time.monotonic()
                self._log_connected(offline_since)
                offline_since = None
                # Session metadata must precede telemetry on every MQTT session.
                metadata = await self.link.read(protocol.SESSION_METADATA_UUID)
                await connection.publish(cloud.TOPIC_INGEST_CBOR, metadata, qos=0)
                # Enable notifications before subscribing. The cloud sends the desired
                # configuration and pending OTA commands as soon as the subscription
                # exists, and the device can acknowledge them only once notifications
                # are on: before that, its ACKs are dropped and every command would be
                # resent after a timeout. What the device sends meanwhile (it reports
                # its configuration right away) is buffered, and published only in
                # _serve(), once the subscriptions are in place to receive any answer.
                if not self._notifying:
                    await self.link.start_notify(self._on_frame)
                    self._notifying = True
                for route in self._routes.values():
                    await connection.subscribe(route.topic, qos=route.qos)
                self.cloud_online = True  # connected and subscribed
                await self._serve(connection)
                return
            except cloud.CloudError as exc:
                if connected_at is not None and time.monotonic() - connected_at >= STABLE_CONNECTION_S:
                    backoff = MIN_BACKOFF_S
                # Refused credentials are a configuration error, not a passing network problem.
                level = logging.ERROR if isinstance(exc, cloud.CloudAuthError) else logging.WARNING
                # Device messages are buffered from the first connection on.
                buffered = f", {count(self.buffered(), 'message')} buffered" if self._notifying else ""
                self._log.log(level, "MQTT error: %s (retrying in %.0fs%s)", exc, backoff, buffered)
                if offline_since is None:
                    offline_since = time.monotonic()
            finally:
                self.cloud_online = False
                await connection.close()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.link.disconnected.wait(), backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)

    def _log_connected(self, offline_since: float | None) -> None:
        if offline_since is None or not self._notifying:  # not reconnecting: nothing buffered
            self._log.info("MQTT connected to %s", self.settings.host)
            return
        # Messages that arrive from now on are sent right away; count only the backlog.
        self._backlog = self._backlog_total = self.buffered()
        offline = duration(time.monotonic() - offline_since)
        if self._backlog:
            self._log.info("MQTT connected to %s after %s offline, sending %s buffered meanwhile",
                           self.settings.host, offline, count(self._backlog, "message"))
        else:
            self._log.info("MQTT connected to %s after %s offline, no messages were buffered",
                           self.settings.host, offline)

    async def _serve(self, connection: cloud.CloudConnection) -> None:
        """Forward device messages until BLE disconnects; raises CloudError if MQTT drops."""
        forward = asyncio.create_task(self._forward(connection))
        dropped = asyncio.create_task(connection.wait_disconnected())
        try:
            done, _ = await asyncio.wait({forward, dropped}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (forward, dropped):
                task.cancel()
            await asyncio.gather(forward, dropped, return_exceptions=True)
        if forward in done:
            forward.result()  # BLE disconnected (returns) or a publish failed (raises)
            return
        raise cloud.CloudError(dropped.result())

    async def _forward(self, connection: cloud.CloudConnection) -> None:
        while True:
            item = self._pending or await self._queue.get()
            if item is None:
                return
            self._pending = item  # keep until published, so an MQTT drop does not lose it
            topic, payload, qos = item
            await connection.publish(topic, payload, qos)
            self._pending = None
            self.forwarded += 1
            self._log.debug("-> %s (%d bytes)", topic, len(payload))
            if self._backlog:
                self._backlog -= 1
                if not self._backlog:
                    self._log.info("All %s buffered while offline sent to the cloud",
                                   count(self._backlog_total, "message"))

    async def _watch_disconnect(self) -> None:
        await self.link.disconnected.wait()
        self._enqueue(None)
        self._end_queued = True

    def _enqueue(self, item: Outgoing | None) -> None:
        if self._queue.full():
            self._queue.get_nowait()
            self.dropped += 1
            now = time.monotonic()
            if now >= self._next_drop_report:
                self._next_drop_report = now + DROP_REPORT_INTERVAL_S
                self._log.warning("Buffer for the cloud is full, dropping the oldest messages "
                                  "(%d dropped so far)", self.dropped)
        self._queue.put_nowait(item)

    def _on_frame(self, frame: bytes) -> None:
        self._adapt_frame_size(len(frame))
        if len(frame) < protocol.CONT_HEADER_LEN:
            self._log.debug("Ignoring too-short frame (%d bytes)", len(frame))
            return
        if not self._accepts(frame[0]):
            # Per the protocol, unsupported types get no ACK/NACK; they are not
            # reassembled either.
            self._log.debug("Ignoring frame of unsupported type 0x%02x", frame[0])
            return
        result = self._reassembler.feed(frame)
        if isinstance(result, Message):
            self._on_device_message(result)
            if result.needs_ack:
                self._send_control(True, result.message_type, result.sequence)
        elif isinstance(result, Rejected):
            self._on_rejected(result)
        elif isinstance(result, Control):
            self._log.debug("Received %s for type 0x%02x seq=%d",
                            "ACK" if result.accepted else "NACK", result.message_type, result.sequence)
            channel = self._downlinks.get(result.message_type)
            if channel is not None:
                channel.on_control(result)

    def _on_rejected(self, rejected: Rejected) -> None:
        self.rejected += 1
        # The device decides how often this happens, so the warning is rate-limited.
        if self._reject_throttle.allow():
            self._log.warning("Dropping %s seq=%d: %s%s", protocol.type_name(rejected.message_type),
                              rejected.sequence, rejected.reason, self._reject_throttle.suppressed_note())
        if rejected.needs_ack:
            self._send_control(False, rejected.message_type, rejected.sequence)

    def _accepts(self, message_type: int) -> bool:
        """Whether the gateway handles frames of this type coming from the device."""
        if message_type == MessageType.ARTIFACT_REQUEST:
            return self._artifact_server is not None
        return message_type in D2C_TOPICS or message_type in (MessageType.ACK, MessageType.NACK)

    def _on_device_message(self, message: Message) -> None:
        if message.message_type == MessageType.ARTIFACT_REQUEST:
            if self._artifact_server is not None:
                self._artifact_server.submit(message.payload)
            return
        if message.message_type == MessageType.REPORTED_CONFIGURATION:
            self._log_reported_config(message.payload)
        topic, qos = D2C_TOPICS[message.message_type]
        self._enqueue((topic, message.payload, qos))

    def _log_reported_config(self, payload: bytes) -> None:
        reported = config.parse_reported(payload)
        if reported is None:
            self._log.debug("Reported configuration not understood, forwarding it as is")
            return
        details = []
        if reported.acked_version is not None:
            details.append(f"configuration version {reported.acked_version} applied")
        if reported.compiled_severity is not None:
            details.append(f"compiled minimum {config.severity_name(reported.compiled_severity)}")
        if reported.severity is None:
            text = "Device reports its configuration"
        else:
            text = f"Device reports log severity {config.severity_name(reported.severity)}"
            self.log_severity = reported.severity
        self._log.info("%s%s", text, f" ({', '.join(details)})" if details else "")

    def _describe_desired_config(self, payload: bytes) -> str:
        """What a desired configuration changes, e.g. "log severity INFO -> WARNING
        (configuration version 7)"; empty if the payload cannot be read."""
        desired = config.parse_desired(payload)
        if desired is None:
            return ""
        version = f"configuration version {desired.version}"
        if desired.severity is None:
            return f"{version}, log severity not set"
        new = config.severity_name(desired.severity)
        old, self.log_severity = self.log_severity, desired.severity
        if old is None:
            return f"log severity {new} ({version}; current severity not reported yet)"
        if old == desired.severity:
            return f"log severity {new}, unchanged ({version})"
        return f"log severity {config.severity_name(old)} -> {new} ({version})"

    def _adapt_frame_size(self, received: int) -> None:
        # The device sizes its frames to the negotiated ATT MTU and can receive frames
        # at least as large as it sends, so frames it sent are safe to send back.
        size = min(received, protocol.MAX_FRAME_SIZE)
        if size > self.frame_size:
            self._log.debug("Using %d-byte frames towards the device", size)
            self.frame_size = size
            for channel in self._downlinks.values():
                channel.frame_size = size

    def _on_cloud_message(self, topic: str, payload: bytes) -> None:
        route = self._routes.get(topic.split("/", 1)[0])
        channel = self._downlinks.get(route.message_type) if route else None
        if channel is None:
            self._log.debug("Ignoring message on %s", topic)
            return
        if not payload:
            self._log.warning("Ignoring empty message on %s", topic)
            return
        self._log.debug("<- %s (%d bytes)", topic, len(payload))
        note = ""
        if route is not None and route.message_type == MessageType.DESIRED_CONFIGURATION:
            note = self._describe_desired_config(payload)
        channel.submit(payload, note)

    def _on_undelivered(self) -> None:
        """An OTA command could not be delivered to the device.

        The broker has already been told that the gateway received the command, and the
        cloud sends it again only when the gateway subscribes anew. So start over:
        disconnecting makes run() end with an error, and the next session with the
        device subscribes again.
        """
        if self.link.disconnected.is_set():
            return  # the session is ending anyway
        self._failure = "an OTA command could not be delivered to the device"
        self._start_task(self.link.disconnect())

    def _send_control(self, accepted: bool, message_type: int, sequence: int) -> None:
        self._start_task(self._write_control(accepted, message_type, sequence))

    async def _write_control(self, accepted: bool, message_type: int, sequence: int) -> None:
        try:
            await self.link.write(protocol.encode_control(accepted, message_type, sequence))
        except Exception as exc:  # BLE write failure; the device resends what was not ACKed
            self._log.warning("Failed to write %s for type 0x%02x seq=%d: %s",
                              "ACK" if accepted else "NACK", message_type, sequence, describe(exc),
                              exc_info=debug_traceback(self._log))

    def _start_task(self, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
