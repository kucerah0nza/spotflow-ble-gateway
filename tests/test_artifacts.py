import asyncio
import logging
import struct

import httpx
import pytest

from spotflow_gateway.artifacts import ArtifactCache, ArtifactError, ArtifactServer, Status, _is_public

URL = "https://artifacts.example.com/fw/42?sig=topsecretsignature"
SECRET = "s3cr3t-OTA"
FIRMWARE = bytes(512) + bytes.fromhex("DEADBEEF") + bytes(i % 256 for i in range(10_000))


# Request encoders, mirroring zephyr/src/ota/protocol/spotflow_ota_ble_protocol.c
def open_req(tid, rid, url=URL, secret=SECRET):
    u, s = url.encode(), secret.encode()
    return struct.pack("<BIIHB", 1, tid, rid, len(u), len(s)) + u + s


def read_req(tid, rid, offset, count):
    return struct.pack("<BIIIH", 2, tid, rid, offset, count)


def close_req(tid, rid, consumed, reason=0):
    return struct.pack("<BIIIB", 3, tid, rid, consumed, reason)


class Origin:
    """Fake artifact host: records requests, serves FIRMWARE or a configured failure."""

    def __init__(self, content=FIRMWARE, status=200, error=None):
        self.content, self.status, self.error = content, status, error
        self.requests: list[httpx.Request] = []

    def __call__(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        return httpx.Response(self.status, content=self.content if self.status == 200 else b"")


async def public_address(host, port):
    """Resolver for tests: every host name is a public address, without real DNS."""
    return ["93.184.216.34"]


def make_cache(tmp_path, origin, **kwargs):
    kwargs.setdefault("resolve", public_address)
    return ArtifactCache(str(tmp_path), transport=httpx.MockTransport(origin), **kwargs)


@pytest.fixture
def origin():
    return Origin()


@pytest.fixture
def server(tmp_path, origin):
    return ArtifactServer(make_cache(tmp_path, origin), send=None, label="dev")


async def test_open_downloads_with_ota_secret(server, origin):
    response = await server.handle(open_req(7, 2))
    assert response == struct.pack("<BIIBI", 0x81, 7, 2, Status.OK, len(FIRMWARE))
    assert origin.requests[0].headers["Authorization"] == f"OtaSecret {SECRET}"
    assert str(origin.requests[0].url) == URL


async def test_read_response_matches_documented_example(server):
    await server.handle(open_req(1, 2))
    response = await server.handle(read_req(1, 3, 512, 4))
    assert response == bytes.fromhex("82 01000000 03000000 00 00020000 DEADBEEF")


async def test_full_transfer(server):
    await server.handle(open_req(9, 1))
    received, offset, rid = b"", 0, 2
    while offset < len(FIRMWARE):
        count = min(4096, len(FIRMWARE) - offset)
        response = await server.handle(read_req(9, rid, offset, count))
        assert response[9] == Status.OK
        assert struct.unpack_from("<I", response, 10)[0] == offset
        received += response[14:]
        offset, rid = offset + count, rid + 1
    assert received == FIRMWARE
    assert await server.handle(close_req(9, rid, offset)) == struct.pack("<BIIB", 0x83, 9, rid, 0)
    # Repeated CLOSE (device retransmission) must still succeed.
    assert (await server.handle(close_req(9, rid, offset)))[9] == Status.OK
    # The transfer is gone after CLOSE.
    assert (await server.handle(read_req(9, rid + 1, 0, 1)))[9] == Status.INVALID_REQUEST


async def test_reopen_uses_cache(server, origin):
    await server.handle(open_req(1, 2))
    await server.handle(open_req(2, 2))
    assert len(origin.requests) == 1


@pytest.mark.parametrize(
    "origin_kwargs, status",
    [
        ({"status": 403}, Status.ACCESS_DENIED),
        ({"status": 404}, Status.ACCESS_DENIED),
        ({"status": 302}, Status.ACCESS_DENIED),
        ({"status": 503}, Status.TRANSIENT_FAILURE),
        ({"status": 429}, Status.TRANSIENT_FAILURE),
        ({"error": httpx.ConnectError("boom")}, Status.TRANSIENT_FAILURE),
        ({"content": b""}, Status.TRANSIENT_FAILURE),
    ],
)
async def test_download_failures(tmp_path, origin_kwargs, status):
    cache = make_cache(tmp_path, Origin(**origin_kwargs))
    response = await ArtifactServer(cache, send=None).handle(open_req(1, 2))
    assert response == struct.pack("<BIIB", 0x81, 1, 2, status)  # error responses are 10 bytes
    assert list(tmp_path.iterdir()) == []  # no partial files left behind


async def test_failed_download_is_retried_on_next_open(tmp_path):
    origin = Origin(status=503)
    server = ArtifactServer(make_cache(tmp_path, origin), send=None)
    assert (await server.handle(open_req(1, 2)))[9] == Status.TRANSIENT_FAILURE
    origin.status = 200
    assert (await server.handle(open_req(1, 3)))[9] == Status.OK


async def test_too_large(tmp_path, origin):
    cache = make_cache(tmp_path, origin, max_size=100)
    assert (await ArtifactServer(cache, send=None).handle(open_req(1, 2)))[9] == Status.TOO_LARGE


async def test_redirects_are_not_followed(tmp_path):
    requests = []

    def redirecting(request):
        requests.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/meta-data/"})

    server = ArtifactServer(make_cache(tmp_path, redirecting), send=None)
    assert (await server.handle(open_req(1, 2)))[9] == Status.ACCESS_DENIED
    assert requests == [URL]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "addresses",
    [
        ["127.0.0.1"],
        ["10.1.2.3"],
        ["192.168.1.10"],
        ["169.254.169.254"],  # link-local (cloud metadata services)
        ["::1"],
        ["fe80::1%en0"],
        ["::ffff:10.1.2.3"],  # IPv4-mapped private address
        ["93.184.216.34", "10.1.2.3"],  # any non-public address disqualifies the host
        [],
    ],
)
async def test_non_public_hosts_are_refused(tmp_path, origin, addresses):
    async def resolve(host, port):
        return addresses

    server = ArtifactServer(make_cache(tmp_path, origin, resolve=resolve), send=None)
    assert (await server.handle(open_req(1, 2)))[9] == Status.ACCESS_DENIED
    assert origin.requests == []


async def test_unresolvable_host_is_a_transient_failure(tmp_path, origin):
    async def resolve(host, port):
        raise OSError("no such host")

    server = ArtifactServer(make_cache(tmp_path, origin, resolve=resolve), send=None)
    assert (await server.handle(open_req(1, 2)))[9] == Status.TRANSIENT_FAILURE
    assert origin.requests == []


def test_public_address_classification():
    assert _is_public("93.184.216.34") and _is_public("2606:4700::1111")
    assert not any(map(_is_public, ["10.0.0.1", "100.64.0.1", "0.0.0.0", "not-an-address"]))


async def test_connection_to_non_public_address_is_refused(tmp_path):
    """The name passes the check but the connection lands on a local address (as with
    DNS rebinding): nothing of the response may reach the device."""
    served = []

    async def serve(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        served.append(True)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nSECRET")
        await writer.drain()
        writer.close()

    http_server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = http_server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}/fw"
    try:
        cache = ArtifactCache(str(tmp_path), allow_http=True, resolve=public_address)
        server = ArtifactServer(cache, send=None)
        assert (await server.handle(open_req(1, 2, url=url)))[9] == Status.ACCESS_DENIED
        assert served and list(tmp_path.iterdir()) == []

        # The same server is reachable when private addresses are allowed explicitly.
        cache = ArtifactCache(str(tmp_path), allow_http=True, allow_private=True)
        response = await ArtifactServer(cache, send=None).handle(open_req(1, 2, url=url))
        assert response[9] == Status.OK and struct.unpack_from("<I", response, 10)[0] == 6
    finally:
        http_server.close()
        await http_server.wait_closed()


def test_unverifiable_connection_is_refused(tmp_path, origin):
    """If the HTTP client does not say where a real connection went, the download is
    refused rather than assumed to be fine."""
    response = httpx.Response(200)  # carries no connection information
    with pytest.raises(ArtifactError) as error:
        ArtifactCache(str(tmp_path))._check_peer(response)
    assert error.value.status == Status.ACCESS_DENIED
    make_cache(tmp_path, origin)._check_peer(response)  # a test transport has no connection
    ArtifactCache(str(tmp_path), allow_private=True)._check_peer(response)


async def test_cancelled_download_leaves_no_partial_file(tmp_path):
    started = asyncio.Event()

    class Stalling(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield bytes(1000)
            started.set()
            await asyncio.Event().wait()

    cache = make_cache(tmp_path, lambda request: httpx.Response(200, stream=Stalling()))
    task = asyncio.create_task(ArtifactServer(cache, send=None).handle(open_req(1, 2)))
    await asyncio.wait_for(started.wait(), 5)
    assert len(list(tmp_path.iterdir())) == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert list(tmp_path.iterdir()) == []
    assert cache._locks == {}  # and nothing keeps the URL and secret in memory


@pytest.mark.parametrize(
    "request_bytes",
    [
        open_req(0, 2),  # transfer ID 0
        open_req(1, 2, secret="has space"),
        open_req(1, 2, secret="x" * 25),
        open_req(1, 2, url="https://e.com/" + "a" * 120),  # URL > 128 bytes
        open_req(1, 2, url="http://insecure.example.com/fw"),
        open_req(1, 2, url="ftp://example.com/fw"),
        open_req(1, 2, url="https://example.com:port/fw"),  # malformed
        open_req(1, 2, url="https:///fw"),  # no host
        open_req(1, 2, url="https://example.com/fw 1"),  # not printable ASCII
        open_req(1, 2, url="https://example.com/\x7f"),
        open_req(1, 2)[:-1],  # length mismatch
        read_req(5, 3, 0, 16),  # unknown transfer
    ],
)
async def test_invalid_requests(server, origin, request_bytes):
    response = await server.handle(request_bytes)
    assert len(response) == 10 and response[9] == Status.INVALID_REQUEST
    assert origin.requests == []


@pytest.mark.parametrize("offset, count", [(0, 0), (0, 4097), (len(FIRMWARE) - 1, 2)])
async def test_read_out_of_range(server, offset, count):
    await server.handle(open_req(1, 2))
    assert (await server.handle(read_req(1, 3, offset, count)))[9] == Status.INVALID_REQUEST


async def test_unknown_or_short_requests_are_ignored(server):
    assert await server.handle(b"\x02\x01") is None
    assert await server.handle(struct.pack("<BII", 9, 1, 1)) is None


async def test_retransmitted_request_answered_once_while_pending(server):
    sent, sending, release = [], asyncio.Event(), asyncio.Event()

    async def slow_send(payload):
        sending.set()
        await release.wait()
        sent.append(payload)

    async def idle():
        while server._pending:
            await asyncio.sleep(0)

    server._send = slow_send
    task = asyncio.create_task(server.run())
    await server.handle(open_req(1, 2))
    server.submit(read_req(1, 3, 0, 4))
    await asyncio.wait_for(sending.wait(), 5)
    server.submit(read_req(1, 3, 0, 4))  # device retry while the response is still being sent
    release.set()
    await asyncio.wait_for(idle(), 5)
    assert len(sent) == 1
    server.submit(read_req(1, 3, 0, 4))  # a retry after completion means the response was lost
    await asyncio.wait_for(idle(), 5)
    assert len(sent) == 2
    task.cancel()


async def test_credentials_never_logged(server, caplog):
    caplog.set_level(logging.DEBUG)
    await server.handle(open_req(1, 2))
    await server.handle(open_req(1, 3, secret="bad secret"))
    text = caplog.text
    assert "artifacts.example.com" in text  # the host is fine to log
    assert SECRET not in text and "topsecretsignature" not in text and "/fw/42" not in text


async def test_evicted_artifact_reports_transient_failure(server, tmp_path):
    await server.handle(open_req(1, 2))
    for f in tmp_path.iterdir():
        f.unlink()
    assert (await server.handle(read_req(1, 3, 0, 4)))[9] == Status.TRANSIENT_FAILURE
