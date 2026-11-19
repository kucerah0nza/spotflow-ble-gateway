"""Scanning, connecting and per-device retry handling, with bleak and the session faked."""

import asyncio
from types import SimpleNamespace

import pytest
from bleak.exc import BleakError

from spotflow_gateway import cloud, gateway, protocol
from spotflow_gateway.gateway import Gateway
from spotflow_gateway.session import SessionError


def advertisement(address, name="Spotflow device", uuids=(protocol.SERVICE_UUID,)):
    device = SimpleNamespace(address=address, name=name)
    adv = SimpleNamespace(local_name=name, service_uuids=list(uuids), rssi=-50)
    return address, (device, adv)


class Radio:
    """Fakes for BleakScanner.discover, BleLink and DeviceSession."""

    def __init__(self, monkeypatch):
        self.found = {}
        self.scan_error = None
        self.connect_error = None
        self.session_error = None
        self.connected = []
        self.session_end = asyncio.Event()
        radio = self

        async def discover(**kwargs):
            assert kwargs["service_uuids"] == [protocol.SERVICE_UUID]
            if radio.scan_error:
                raise radio.scan_error
            return dict(radio.found)

        class Link:
            def __init__(self, device):
                self.name = device.name
                self.closed = False

            async def connect(self):
                if radio.connect_error:
                    raise radio.connect_error
                radio.connected.append(self.name)

            async def disconnect(self):
                self.closed = True

        class Session:
            def __init__(self, link, settings, artifacts):
                pass

            async def run(self):
                if radio.session_error:
                    raise radio.session_error
                await radio.session_end.wait()

        monkeypatch.setattr(gateway.BleakScanner, "discover", discover)
        monkeypatch.setattr(gateway, "BleLink", Link)
        monkeypatch.setattr(gateway, "DeviceSession", Session)


@pytest.fixture
def radio(monkeypatch):
    return Radio(monkeypatch)


@pytest.fixture
def gw():
    return Gateway(cloud.CloudSettings(ingest_key="k"))


async def scan_and_connect(gw):
    found = await gw._scan()
    for device, adv in found or []:
        await gw._connect(device, adv)
    await asyncio.sleep(0)  # let new session tasks start
    return found


async def test_connects_to_spotflow_devices_once(radio, gw):
    radio.found = dict([
        advertisement("AA", "one"),
        advertisement("BB", "other service", uuids=["0000180f-0000-1000-8000-00805f9b34fb"]),
        advertisement("CC", "upper", uuids=[protocol.SERVICE_UUID.upper()]),
    ])
    await scan_and_connect(gw)
    assert radio.connected == ["one", "upper"]
    assert set(gw._sessions) == {"AA", "CC"}

    await scan_and_connect(gw)  # devices with a running session are left alone
    assert radio.connected == ["one", "upper"]

    radio.session_end.set()  # devices disconnect
    await asyncio.gather(*gw._sessions.values())
    assert gw._sessions == {} and gw._retry == {}
    radio.session_end.clear()
    await scan_and_connect(gw)  # ...and are picked up again right away
    assert radio.connected == ["one", "upper", "one", "upper"]
    await gw._stop_sessions()


async def test_name_filter(radio):
    gw = Gateway(cloud.CloudSettings(ingest_key="k"), ["Sensor-*", "exact"])
    radio.found = dict([
        advertisement("AA", "Sensor-1"), advertisement("BB", "exact"), advertisement("CC", "other"),
    ])
    await scan_and_connect(gw)
    assert radio.connected == ["Sensor-1", "exact"]
    await gw._stop_sessions()


@pytest.mark.parametrize("error", [BleakError("adapter off"), OSError("radio not ready"), EOFError()])
async def test_scan_failure_does_not_stop_the_gateway(radio, gw, monkeypatch, error):
    radio.scan_error = error
    assert await gw._scan() is None

    pauses = []

    async def sleep(seconds):
        pauses.append(seconds)
        if len(pauses) == 6:
            raise asyncio.CancelledError

    monkeypatch.setattr(gateway.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await gw._scan_loop()
    assert pauses == [5, 10, 20, 40, 60, 60]  # backs off instead of failing every 2 s


@pytest.mark.parametrize("error", [BleakError("no"), TimeoutError(), OSError("no"), RuntimeError("no")])
async def test_failed_connection_is_retried_with_backoff(radio, gw, monkeypatch, error):
    now = 1000.0
    monkeypatch.setattr(asyncio.get_running_loop(), "time", lambda: now)
    radio.found = dict([advertisement("AA", "one")])
    radio.connect_error = error

    assert len(await scan_and_connect(gw)) == 1
    assert gw._sessions == {} and gw._retry["AA"] == (5.0, 1005.0)
    now += 4
    assert await scan_and_connect(gw) == []  # too early to try again
    now += 2
    assert len(await scan_and_connect(gw)) == 1
    assert gw._retry["AA"] == (10.0, 1016.0)

    for _ in range(5):
        now += 100
        await scan_and_connect(gw)
    assert gw._retry["AA"][0] == 60.0  # capped

    radio.connect_error = None  # e.g. the device has been paired meanwhile
    now += 100
    await scan_and_connect(gw)
    assert radio.connected == ["one"]
    radio.session_end.set()
    await asyncio.gather(*gw._sessions.values())
    assert gw._retry == {}


@pytest.mark.parametrize(
    "error", [SessionError("unsupported protocol version"), BleakError("auth"), ValueError("bug")]
)
async def test_failed_session_is_retried_with_backoff(radio, gw, error):
    radio.found = dict([advertisement("AA", "one")])
    radio.session_error = error
    await scan_and_connect(gw)
    await asyncio.sleep(0)
    assert gw._sessions == {}  # the failure is contained in the session task
    assert gw._retry["AA"][0] == 5.0
    assert await scan_and_connect(gw) == []
