import contextlib
import ssl

import certifi
import pytest
import trustme

from spotflow_gateway import cloud


@contextlib.asynccontextmanager
async def connection_to(broker, device_id="device-1", ingest_key="ingest-key", tls=False):
    settings = cloud.CloudSettings(ingest_key=ingest_key, host="127.0.0.1", port=broker.port, tls=tls)
    connection = cloud.CloudConnection(settings, device_id, lambda _topic, _payload: None)
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
def ca():
    return trustme.CA()


@pytest.fixture
def trusted(ca, tmp_path, monkeypatch):
    """Make the gateway trust the test CA instead of the public CA bundle."""
    path = tmp_path / "ca.pem"
    ca.cert_pem.write_to_path(path)
    monkeypatch.setattr(certifi, "where", lambda: str(path))


def server_tls(ca, *hosts):
    """Server-side TLS context with a certificate issued by ``ca`` for ``hosts``."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert(*hosts).configure_cert(context)
    return context


# --- authentication ----------------------------------------------------------------------


async def test_authenticates_as_the_device_with_the_ingest_key(mini_broker):
    broker = await mini_broker()
    async with connection_to(broker, device_id="sensor-042", ingest_key="secret-key") as connection:
        await connection.connect()
    assert broker.credentials == [("sensor-042", "secret-key")]


@pytest.mark.parametrize(
    "return_code, reason",
    [(4, "Bad user name or password"), (5, "Not authorized")],
)
async def test_refused_connection_reports_the_reason(mini_broker, return_code, reason):
    broker = await mini_broker(return_code=return_code)
    async with connection_to(broker) as connection:
        with pytest.raises(cloud.CloudError, match=f"^connection refused: {reason}$"):
            await connection.connect()


async def test_refused_connection_does_not_leak_the_ingest_key(mini_broker):
    broker = await mini_broker(return_code=4)
    async with connection_to(broker, ingest_key="secret-key") as connection:
        with pytest.raises(cloud.CloudError) as error:
            await connection.connect()
    assert "secret-key" not in str(error.value)
    assert "secret-key" not in repr(connection.settings)


# --- TLS ---------------------------------------------------------------------------------


async def test_tls_connection_to_a_trusted_broker(mini_broker, ca, trusted):
    broker = await mini_broker(ssl_context=server_tls(ca, "127.0.0.1"))
    async with connection_to(broker, tls=True) as connection:
        await connection.connect()
        await connection.subscribe(cloud.TOPIC_CONFIG_C2D, qos=0)
        await connection.publish(cloud.TOPIC_OTA_D2C, b"result", qos=1)
    assert broker.credentials == [("device-1", "ingest-key")]
    assert broker.subscriptions == [cloud.TOPIC_CONFIG_C2D]
    assert broker.messages == [(cloud.TOPIC_OTA_D2C, b"result")]


async def test_tls_is_the_default():
    assert cloud.CloudSettings(ingest_key="k").tls


async def test_tls_refuses_a_certificate_from_an_unknown_authority(mini_broker, ca):
    # Without the `trusted` fixture the gateway uses the public CA bundle.
    broker = await mini_broker(ssl_context=server_tls(ca, "127.0.0.1"))
    async with connection_to(broker, tls=True) as connection:
        with pytest.raises(cloud.CloudError, match="cannot connect to 127.0.0.1.*certificate verify failed"):
            await connection.connect()
    assert broker.credentials == []  # the ingest key was never sent


async def test_tls_refuses_a_certificate_for_another_host(mini_broker, ca, trusted):
    broker = await mini_broker(ssl_context=server_tls(ca, "other.example"))
    async with connection_to(broker, tls=True) as connection:
        with pytest.raises(cloud.CloudError, match="cannot connect to 127.0.0.1.*certificate verify failed"):
            await connection.connect()
    assert broker.credentials == []


async def test_tls_refuses_a_broker_without_tls(mini_broker):
    broker = await mini_broker()
    async with connection_to(broker, tls=True) as connection:
        with pytest.raises(cloud.CloudError, match="cannot connect to 127.0.0.1"):
            await connection.connect()
    assert broker.credentials == []


# --- acknowledgements --------------------------------------------------------------------


async def test_unanswered_publish_and_subscription_fail_instead_of_waiting_forever(mini_broker, monkeypatch):
    monkeypatch.setattr(cloud, "ACK_TIMEOUT_S", 0.2)
    broker = await mini_broker(acknowledge=False)
    async with connection_to(broker) as connection:
        await connection.connect()
        await connection.publish(cloud.TOPIC_INGEST_CBOR, b"telemetry", qos=0)  # needs no answer
        with pytest.raises(cloud.CloudError, match="^publish to ota-cbor-d2c was not acknowledged"):
            await connection.publish(cloud.TOPIC_OTA_D2C, b"result", qos=1)
        with pytest.raises(cloud.CloudError, match="^subscribe to config-cbor-c2d was not acknowledged"):
            await connection.subscribe(cloud.TOPIC_CONFIG_C2D, qos=0)
        assert connection._acks == {}
    assert broker.messages == [(cloud.TOPIC_INGEST_CBOR, b"telemetry"), (cloud.TOPIC_OTA_D2C, b"result")]
