"""MQTT connection to the Spotflow cloud (one connection per BLE device).

paho-mqtt runs the network I/O of each connection in its own background thread and
its callbacks are handed to the asyncio loop with call_soon_threadsafe(). The asyncio
loop never touches the socket, so a dropped connection cannot leave a closed socket
registered with the event loop (which breaks select() on Windows).
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

import certifi
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode
from paho.mqtt.reasoncodes import ReasonCode

log = logging.getLogger(__name__)
T = TypeVar("T")

DEFAULT_HOST = "mqtt.spotflow.io"
DEFAULT_PORT = 8883
KEEPALIVE_S = 60
CONNECT_TIMEOUT_S = 30.0
# How long the broker may take to acknowledge a publish or a subscription. Without a
# limit, a connection that stays open but stops answering would stall its device forever.
ACK_TIMEOUT_S = 30.0

TOPIC_INGEST_CBOR = "ingest-cbor"
TOPIC_CONFIG_D2C = "config-cbor-d2c"
TOPIC_CONFIG_C2D = "config-cbor-c2d"
TOPIC_OTA_D2C = "ota-cbor-d2c"
TOPIC_OTA_C2D = "ota-cbor-c2d"


@dataclass(frozen=True)
class CloudSettings:
    ingest_key: str
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    tls: bool = True

    def __repr__(self) -> str:  # never leak the ingest key into logs
        return f"CloudSettings(host={self.host!r}, port={self.port}, tls={self.tls})"


class CloudError(Exception):
    """The MQTT connection failed, was refused, or was lost."""


class CloudConnection:
    """A single MQTT session authenticated as ``device_id`` with the gateway's ingest key.

    It does not reconnect by itself: once lost, the session creates a new connection
    (and republishes session metadata and subscriptions).
    """

    def __init__(
        self,
        settings: CloudSettings,
        device_id: str,
        on_message: Callable[[str, bytes], None],
    ) -> None:
        self.settings = settings
        self._on_message = on_message
        self._loop = asyncio.get_running_loop()
        self._connected: asyncio.Future[ReasonCode] = self._loop.create_future()
        self._disconnected = asyncio.Event()
        self._disconnect_reason = "connection lost"
        # Publish and subscribe acknowledgements by message ID; a subscription's result
        # is the list of granted (or refused) reason codes, a publish has none.
        self._acks: dict[int, asyncio.Future[list[ReasonCode] | None]] = {}

        self._client = mqtt.Client(CallbackAPIVersion.VERSION2, reconnect_on_failure=False)
        self._client.username_pw_set(device_id, settings.ingest_key)
        if settings.tls:
            self._client.tls_set_context(ssl.create_default_context(cafile=certifi.where()))
        self._client.on_connect = self._cb_connect
        self._client.on_disconnect = self._cb_disconnect
        self._client.on_publish = self._cb_publish
        self._client.on_subscribe = self._cb_subscribe
        self._client.on_message = self._cb_message

    # --- asyncio API -----------------------------------------------------------------

    async def connect(self) -> None:
        """Open the connection and wait for the broker to accept it."""
        try:
            # DNS, TCP and the TLS handshake block, so run them off the event loop.
            await asyncio.to_thread(
                self._client.connect, self.settings.host, self.settings.port, KEEPALIVE_S
            )
        except (OSError, ValueError) as exc:
            raise CloudError(f"cannot connect to {self.settings.host}: {exc}") from None
        self._client.loop_start()
        accepted = _first(self._connected, self._disconnected.wait())
        try:
            reason = await asyncio.wait_for(accepted, CONNECT_TIMEOUT_S)
        except TimeoutError:
            raise CloudError("timed out waiting for the broker to accept the connection") from None
        if reason is None:
            raise CloudError(self._disconnect_reason)
        if reason.is_failure:
            raise CloudError(f"connection refused: {reason}")

    async def publish(self, topic: str, payload: bytes, qos: int) -> None:
        """Publish and wait until it is sent (QoS 0) or acknowledged (QoS 1).

        Raises CloudError if that does not happen within ACK_TIMEOUT_S.
        """
        info = self._client.publish(topic, payload, qos=qos)
        await self._wait_ack(info.rc, info.mid, f"publish to {topic}")

    async def subscribe(self, topic: str, qos: int) -> None:
        rc, mid = self._client.subscribe(topic, qos=qos)
        granted = await self._wait_ack(rc, mid, f"subscribe to {topic}")
        if not granted or any(code.is_failure for code in granted):
            raise CloudError(f"subscription to {topic} refused")

    async def wait_disconnected(self) -> str:
        await self._disconnected.wait()
        return self._disconnect_reason

    async def close(self) -> None:
        self._client.disconnect()
        await asyncio.to_thread(self._client.loop_stop)
        self._fail_pending()

    async def _wait_ack(
        self, rc: MQTTErrorCode, mid: int | None, what: str
    ) -> list[ReasonCode] | None:
        if rc != MQTTErrorCode.MQTT_ERR_SUCCESS or mid is None:
            raise CloudError(f"{what} failed: {mqtt.error_string(rc)}")
        if self._disconnected.is_set():  # its ack would never arrive
            raise CloudError(self._disconnect_reason)
        # Callbacks reach the loop via call_soon_threadsafe, so the ack cannot be
        # processed before this future is registered (no await in between).
        future: asyncio.Future[list[ReasonCode] | None] = self._loop.create_future()
        self._acks[mid] = future
        try:
            return await asyncio.wait_for(future, ACK_TIMEOUT_S)
        except TimeoutError:
            raise CloudError(f"{what} was not acknowledged by the broker") from None
        finally:
            self._acks.pop(mid, None)

    def _fail_pending(self) -> None:
        for future in self._acks.values():
            if not future.done():
                future.set_exception(CloudError(self._disconnect_reason))
        self._acks.clear()

    # --- paho callbacks (network thread) ------------------------------------------------

    def _call(self, callback: Callable[..., None], *args: object) -> None:
        try:
            self._loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            pass  # the event loop is already closed (shutdown)

    def _cb_connect(self, _client, _userdata, _flags, reason_code, _properties) -> None:
        self._call(_set_result, self._connected, reason_code)

    def _cb_disconnect(self, _client, _userdata, _flags, reason_code, _properties) -> None:
        self._call(self._on_disconnected, str(reason_code))

    def _cb_publish(self, _client, _userdata, mid, _reason_code, _properties) -> None:
        self._call(self._on_ack, mid, None)

    def _cb_subscribe(self, _client, _userdata, mid, reason_codes, _properties) -> None:
        self._call(self._on_ack, mid, list(reason_codes))

    def _cb_message(self, _client, _userdata, message: mqtt.MQTTMessage) -> None:
        self._call(self._on_message, message.topic, bytes(message.payload))

    # --- loop-side handlers ---------------------------------------------------------------

    def _on_disconnected(self, reason: str) -> None:
        if not self._disconnected.is_set():
            self._disconnect_reason = f"connection lost: {reason}"
            self._disconnected.set()
        self._fail_pending()

    def _on_ack(self, mid: int, value: list[ReasonCode] | None) -> None:
        future = self._acks.pop(mid, None)
        if future is not None and not future.done():
            future.set_result(value)


def _set_result(future: asyncio.Future[T], value: T) -> None:
    if not future.done():
        future.set_result(value)


async def _first(future: asyncio.Future[T], other: Awaitable[object]) -> T | None:
    """Result of ``future``, or None if ``other`` completes first."""
    other_task = asyncio.ensure_future(other)
    waiting: set[asyncio.Future[Any]] = {future, other_task}
    try:
        done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
    finally:
        other_task.cancel()
    return future.result() if future in done else None
