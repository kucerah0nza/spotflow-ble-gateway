"""OTA artifact transfer: the gateway downloads firmware over HTTPS and serves it to the
device in blocks (ARTIFACT_REQUEST / ARTIFACT_RESPONSE: OPEN, READ, CLOSE).

Wire format (all integers unsigned little-endian), see the "Artifact transfer" section of
https://docs.spotflow.io/fundamentals/bluetooth-low-energy:

    request header:  op u8 | transfer_id u32 | request_id u32
    OPEN  request:   header | url_len u16 | secret_len u8 | url | secret
    READ  request:   header | offset u32 | count u16
    CLOSE request:   header | consumed u32 | reason u8
    response:        op|0x80 | transfer_id | request_id | status u8 [| size or offset u32 | data]

URLs and secrets are credentials: they are never logged.

The URL comes from the BLE peer, so the gateway must not become a way to reach hosts the
peer could not reach itself: only HTTPS, no redirects, and only public addresses.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import logging
import os
import socket
import ssl
import struct
import tempfile
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum

import certifi
import httpx

from .logs import DeviceLogger, Throttle, debug_traceback, describe

log = logging.getLogger(__name__)
# httpx logs every request URL at INFO (and httpcore internals at DEBUG). Artifact URLs
# can carry signatures, so keep those loggers quiet regardless of the gateway's log level.
for _name in ("httpx", "httpcore"):
    logging.getLogger(_name).setLevel(logging.WARNING)

OP_OPEN, OP_READ, OP_CLOSE = 1, 2, 3
RESPONSE_FLAG = 0x80
HEADER = struct.Struct("<BII")  # op, transfer_id, request_id
MAX_URL_LEN = 128
MAX_SECRET_LEN = 24
MAX_READ = 4096
MAX_ARTIFACT_SIZE = 256 * 1024 * 1024
CACHE_ENTRIES = 4


class Status(IntEnum):
    OK = 0
    INVALID_REQUEST = 1
    ACCESS_DENIED = 2
    TRANSIENT_FAILURE = 3
    TOO_LARGE = 4


class ArtifactError(Exception):
    def __init__(self, status: Status, reason: str) -> None:
        super().__init__(reason)
        self.status = status


@dataclass(frozen=True)
class Artifact:
    path: str
    size: int


Resolver = Callable[[str, int], Awaitable[list[str]]]


async def _resolve(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


def _is_public(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address.split("%", 1)[0])  # drop an IPv6 zone ID
    except ValueError:
        return False
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_global


class ArtifactCache:
    """Downloads artifacts once and keeps the last few on disk, shared by all devices."""

    def __init__(
        self,
        directory: str,
        transport: httpx.AsyncBaseTransport | None = None,
        allow_http: bool = False,
        max_size: int = MAX_ARTIFACT_SIZE,
        allow_private: bool = False,
        resolve: Resolver = _resolve,
    ) -> None:
        self.directory = directory
        self.allow_http = allow_http
        self.allow_private = allow_private
        self.max_size = max_size
        self._transport = transport
        self._resolve = resolve
        self._entries: OrderedDict[tuple[str, str], Artifact] = OrderedDict()
        # Per-artifact download lock and the number of tasks using it, so that an entry
        # (which holds the URL and secret) goes away with its last user.
        self._locks: dict[tuple[str, str], tuple[asyncio.Lock, int]] = {}
        self._counter = 0

    @contextlib.asynccontextmanager
    async def _locked(self, key: tuple[str, str]) -> AsyncIterator[None]:
        lock, users = self._locks.get(key) or (asyncio.Lock(), 0)
        self._locks[key] = (lock, users + 1)
        try:
            async with lock:
                yield
        finally:
            lock, users = self._locks[key]
            if users > 1:
                self._locks[key] = (lock, users - 1)
            else:
                del self._locks[key]

    async def get(self, url: str, secret: str) -> Artifact:
        key = (url, secret)
        async with self._locked(key):
            if key in self._entries:
                self._entries.move_to_end(key)
                return self._entries[key]
            artifact = await self._download(url, secret)
            self._entries[key] = artifact
            while len(self._entries) > CACHE_ENTRIES:
                _, old = self._entries.popitem(last=False)
                with contextlib.suppress(OSError):
                    os.remove(old.path)
            return artifact

    async def _download(self, url: str, secret: str) -> Artifact:
        try:
            target = httpx.URL(url)
        except httpx.InvalidURL:
            raise ArtifactError(Status.INVALID_REQUEST, "malformed artifact URL") from None
        allowed = ("https", "http") if self.allow_http else ("https",)
        if target.scheme not in allowed or not target.host:
            raise ArtifactError(Status.INVALID_REQUEST, "unsupported artifact URL")
        host = target.host  # safe to log; path and query may carry credentials
        await self._check_host(host, target.port or (443 if target.scheme == "https" else 80))

        self._counter += 1
        path = os.path.join(self.directory, f"artifact-{self._counter}.bin")
        log.info("Downloading artifact from %s", host)
        size = 0
        complete = False
        try:
            async with httpx.AsyncClient(
                transport=self._transport,
                verify=ssl.create_default_context(cafile=certifi.where()),
                timeout=httpx.Timeout(30.0, connect=10.0),
                # A redirect could lead to plain HTTP or to an internal host. The Device
                # SDK's own downloader does not follow redirects either.
                follow_redirects=False,
                # Connect directly (no proxy from the environment), so that the peer
                # address checked below is the artifact host itself.
                trust_env=False,
            ) as client:
                headers = {"Authorization": f"OtaSecret {secret}"}
                async with client.stream("GET", target, headers=headers) as response:
                    self._check_peer(response)
                    self._check_status(response.status_code)
                    declared = response.headers.get("content-length", "")
                    if declared.isdigit() and int(declared) > self.max_size:
                        raise ArtifactError(Status.TOO_LARGE, f"artifact is {declared} bytes")
                    with open(path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > self.max_size:
                                raise ArtifactError(Status.TOO_LARGE, "artifact exceeds size limit")
                            f.write(chunk)
            if size == 0:
                raise ArtifactError(Status.TRANSIENT_FAILURE, "artifact is empty")
            complete = True
        except (httpx.HTTPError, OSError) as exc:
            raise ArtifactError(Status.TRANSIENT_FAILURE, f"download failed: {type(exc).__name__}") from None
        finally:
            # Also on cancellation (the device disconnected mid-download): never leave a
            # partial file behind.
            if not complete:
                _remove(path)
        log.info("Downloaded artifact from %s (%d bytes)", host, size)
        return Artifact(path, size)

    async def _check_host(self, host: str, port: int) -> None:
        if self.allow_private:
            return
        try:
            addresses = await self._resolve(host, port)
        except OSError:
            raise ArtifactError(Status.TRANSIENT_FAILURE, f"cannot resolve {host}") from None
        if not addresses or not all(_is_public(address) for address in addresses):
            raise ArtifactError(Status.ACCESS_DENIED, f"{host} is not a public address")

    def _check_peer(self, response: httpx.Response) -> None:
        """Reject a connection that ended up at a non-public address after all (the name
        may resolve differently for the connection than it did for the check)."""
        if self.allow_private:
            return
        stream = response.extensions.get("network_stream")
        if stream is None and self._transport is not None:
            return  # a transport supplied by the caller (tests) need not use a connection
        # Without a connection to look at, the address cannot be verified: refuse.
        peer = stream.get_extra_info("server_addr") if stream is not None else None
        if not peer or not _is_public(str(peer[0])):
            raise ArtifactError(Status.ACCESS_DENIED, "artifact host is not a public address")

    @staticmethod
    def _check_status(code: int) -> None:
        if 200 <= code < 300:
            return
        if code in (408, 429) or code >= 500:
            raise ArtifactError(Status.TRANSIENT_FAILURE, f"HTTP {code}")
        raise ArtifactError(Status.ACCESS_DENIED, f"HTTP {code}")


def _remove(path: str) -> None:
    with contextlib.suppress(OSError):
        os.remove(path)


def new_cache_directory() -> tempfile.TemporaryDirectory[str]:
    return tempfile.TemporaryDirectory(prefix="spotflow-gateway-")


class ArtifactServer:
    """Answers one device's artifact requests, one at a time, in arrival order."""

    def __init__(
        self, cache: ArtifactCache, send: Callable[[bytes], Awaitable[object]], label: str = ""
    ) -> None:
        self.cache = cache
        self.label = label
        self._log = DeviceLogger(log, lambda: self.label)
        # The warnings below are triggered by what the device sends.
        self._throttle = Throttle()
        self._send = send
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        # Requests queued or being answered. The device retransmits a pending request
        # every 2 s; answering each copy would only queue the same block repeatedly.
        self._pending: set[bytes] = set()
        self._transfers: dict[int, Artifact] = {}

    def submit(self, request: bytes) -> None:
        if request in self._pending:
            self._log.debug("Ignoring retransmitted artifact request")
            return
        self._pending.add(request)
        self._queue.put_nowait(request)

    async def run(self) -> None:
        while True:
            request = await self._queue.get()
            try:
                await self._answer(request)
            finally:
                self._pending.discard(request)

    async def _answer(self, request: bytes) -> None:
        try:
            response = await self.handle(request)
        except Exception as exc:
            # A bug, as handle() turns expected failures into a status for the device.
            # Only the type is logged: the message (or a traceback with it) could quote
            # the download link. The device retransmits the request.
            self._log.error("Unexpected %s while handling an artifact request", type(exc).__name__)
            return
        if response is None:
            return
        try:
            await self._send(response)
        except Exception as exc:  # e.g. BLE write failure; the device will retransmit
            self._log.warning("Failed to answer artifact request: %s", describe(exc),
                              exc_info=debug_traceback(self._log))

    def _warn(self, msg: str, *args: object) -> None:
        if self._throttle.allow():
            self._log.warning(msg + self._throttle.suppressed_note(), *args)

    async def handle(self, request: bytes) -> bytes | None:
        if len(request) < HEADER.size:
            self._warn("Ignoring too-short artifact request")
            return None
        op, transfer_id, request_id = HEADER.unpack_from(request)
        header = HEADER.pack(op | RESPONSE_FLAG, transfer_id, request_id)
        handler = {OP_OPEN: self._open, OP_READ: self._read, OP_CLOSE: self._close}.get(op)
        if handler is None:
            self._warn("Ignoring artifact request with unknown operation %d", op)
            return None
        try:
            if transfer_id == 0:
                raise ArtifactError(Status.INVALID_REQUEST, "transfer ID must be non-zero")
            return header + bytes([Status.OK]) + await handler(transfer_id, request[HEADER.size :])
        except ArtifactError as exc:
            self._warn("Artifact %s failed: %s (status %d)",
                       {OP_OPEN: "OPEN", OP_READ: "READ", OP_CLOSE: "CLOSE"}[op], exc, exc.status)
            return header + bytes([exc.status])

    async def _open(self, transfer_id: int, body: bytes) -> bytes:
        if len(body) < 3:
            raise ArtifactError(Status.INVALID_REQUEST, "malformed OPEN")
        url_len, secret_len = struct.unpack_from("<HB", body)
        if len(body) != 3 + url_len + secret_len:
            raise ArtifactError(Status.INVALID_REQUEST, "OPEN length mismatch")
        if not 1 <= url_len <= MAX_URL_LEN or not 1 <= secret_len <= MAX_SECRET_LEN:
            raise ArtifactError(Status.INVALID_REQUEST, "URL or secret length out of range")
        url_bytes, secret_bytes = body[3 : 3 + url_len], body[3 + url_len :]
        if not all(0x21 <= b <= 0x7E for b in url_bytes + secret_bytes):
            raise ArtifactError(Status.INVALID_REQUEST, "URL and secret must be printable ASCII")

        artifact = await self.cache.get(url_bytes.decode("ascii"), secret_bytes.decode("ascii"))
        self._transfers[transfer_id] = artifact
        self._log.info("Artifact transfer %08x opened (%d bytes)", transfer_id, artifact.size)
        return struct.pack("<I", artifact.size)

    async def _read(self, transfer_id: int, body: bytes) -> bytes:
        if len(body) != 6:
            raise ArtifactError(Status.INVALID_REQUEST, "malformed READ")
        offset, count = struct.unpack("<IH", body)
        artifact = self._transfers.get(transfer_id)
        if artifact is None:
            raise ArtifactError(Status.INVALID_REQUEST, "READ for unknown transfer")
        if not 1 <= count <= MAX_READ or offset + count > artifact.size:
            raise ArtifactError(Status.INVALID_REQUEST, f"READ {offset}+{count} out of range")
        try:
            with open(artifact.path, "rb") as f:
                f.seek(offset)
                data = f.read(count)
        except OSError:
            # E.g. evicted from the cache by other downloads; the device reopens and we re-download.
            self._transfers.pop(transfer_id, None)
            raise ArtifactError(Status.TRANSIENT_FAILURE, "cached artifact unavailable") from None
        if len(data) != count:
            raise ArtifactError(Status.TRANSIENT_FAILURE, "cached artifact is truncated")
        if offset + count == artifact.size or offset % (64 * 1024) < count:
            self._log.info("Artifact transfer %08x: %d/%d bytes", transfer_id, offset + count, artifact.size)
        return struct.pack("<I", offset) + data

    async def _close(self, transfer_id: int, body: bytes) -> bytes:
        if len(body) != 5:
            raise ArtifactError(Status.INVALID_REQUEST, "malformed CLOSE")
        consumed, reason = struct.unpack("<IB", body)
        # Repeated CLOSE must succeed, so an unknown transfer is not an error.
        if self._transfers.pop(transfer_id, None) is not None:
            self._log.info("Artifact transfer %08x closed (%s, %d bytes consumed)",
                           transfer_id, "complete" if reason == 0 else "interrupted", consumed)
        return b""
