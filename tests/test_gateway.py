"""Scanning, connecting and per-device retry handling, with bleak and the session faked."""

import asyncio
import logging
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
        self.summary = ""
        radio = self

        async def discover(**kwargs):
            assert kwargs["service_uuids"] == [protocol.SERVICE_UUID]
            if radio.scan_error:
                raise radio.scan_error
            return dict(radio.found)

        class Link:
            def __init__(self, device):
                self.name = device.name
                self.label = f"{device.name} {device.address}"
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

            def summary(self):
                return radio.summary

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


def messages(caplog, level=logging.INFO):
    return [r.getMessage() for r in caplog.records if r.levelno >= level]


@pytest.mark.parametrize(
    "error, text", [(BleakError("adapter off"), "adapter off"), (OSError("radio not ready"), "radio not ready"),
                    (EOFError(), "EOFError")]
)
async def test_scan_failure_does_not_stop_the_gateway(radio, gw, monkeypatch, caplog, error, text):
    caplog.set_level(logging.INFO)
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
    # One line per failure, saying when the next scan is.
    assert messages(caplog)[:2] == [f"BLE scan failed: {text} (next scan in 5 s)",
                                    f"BLE scan failed: {text} (next scan in 10 s)"]


@pytest.mark.parametrize(
    "error, text", [(BleakError("no"), "no"), (TimeoutError(), "TimeoutError"), (OSError("no"), "no"),
                    (RuntimeError("no"), "no")]
)
async def test_failed_connection_is_retried_with_backoff(radio, gw, monkeypatch, caplog, error, text):
    now = 1000.0
    monkeypatch.setattr(asyncio.get_running_loop(), "time", lambda: now)
    radio.found = dict([advertisement("AA", "one")])
    radio.connect_error = error

    assert len(await scan_and_connect(gw)) == 1
    assert gw._sessions == {} and gw._retry["AA"] == (5.0, 1005.0)
    assert messages(caplog, logging.WARNING) == [f"[one AA] Failed to connect: {text} (next attempt in 5 s)"]
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
    "error, level, outcome",
    [
        (SessionError("unsupported protocol version"), logging.ERROR, "unsupported protocol version"),
        (BleakError("auth"), logging.WARNING, "BLE error: auth"),
        (TimeoutError(), logging.WARNING, "BLE error: TimeoutError"),
        (ValueError("bug"), logging.ERROR, "unexpected error"),
    ],
)
async def test_failed_session_is_retried_with_backoff(radio, gw, caplog, error, level, outcome):
    radio.found = dict([advertisement("AA", "one")])
    radio.session_error = error
    await scan_and_connect(gw)
    await asyncio.sleep(0)
    assert gw._sessions == {}  # the failure is contained in the session task
    assert gw._retry["AA"][0] == 5.0
    assert await scan_and_connect(gw) == []
    [record] = [r for r in caplog.records if "Session" in r.getMessage()]
    assert (record.levelno, record.getMessage()) == (level, f"[one AA] Session failed: {outcome} (next attempt in 5 s)")
    assert bool(record.exc_info) == isinstance(error, ValueError)  # a bug: show where


async def test_end_of_session_is_one_line_with_its_summary(radio, gw, caplog):
    caplog.set_level(logging.INFO)
    radio.found = dict([advertisement("AA", "one"), advertisement("BB", "two")])
    await scan_and_connect(gw)
    radio.summary = "after 2h 03m (1520 messages to the cloud, 3 to the device)"
    radio.session_end.set()
    await asyncio.gather(*gw._sessions.values())
    radio.session_end.clear()
    await scan_and_connect(gw)
    await gw._stop_sessions()
    ends = [m for m in messages(caplog) if "Session" in m]
    assert ends == [
        "[one AA] Session ended after 2h 03m (1520 messages to the cloud, 3 to the device): the device disconnected",
        "[two BB] Session ended after 2h 03m (1520 messages to the cloud, 3 to the device): the device disconnected",
        "[one AA] Session ended after 2h 03m (1520 messages to the cloud, 3 to the device): the gateway is stopping",
        "[two BB] Session ended after 2h 03m (1520 messages to the cloud, 3 to the device): the gateway is stopping",
    ]


async def test_startup_says_what_runs_and_how_it_is_configured(gw, caplog, monkeypatch):
    caplog.set_level(logging.INFO)

    async def no_scanning():
        pass

    gw.name_patterns = ["Sensor-*", "exact"]
    monkeypatch.setattr(gw, "_scan_loop", no_scanning)
    await gw.run()
    first, scanning = messages(caplog)
    assert first.startswith("Spotflow BLE gateway ") and "Python 3." in first and "bleak " in first
    assert scanning == "Scanning for Spotflow BLE devices (MQTT broker mqtt.spotflow.io:8883, " \
                       "only devices named 'Sensor-*', 'exact')"

    caplog.clear()
    plain = Gateway(cloud.CloudSettings(ingest_key="k", host="localhost", port=1883, tls=False))
    monkeypatch.setattr(plain, "_scan_loop", no_scanning)
    await plain.run()
    assert messages(caplog, logging.WARNING) == [
        "TLS is disabled: the ingest key and all data go to localhost unencrypted"
    ]


async def test_finding_nothing_is_reported_now_and_then(radio, gw, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    now = 1000.0
    monkeypatch.setattr(asyncio.get_running_loop(), "time", lambda: now)
    nothing = "No Spotflow devices in range, still scanning"

    await scan_and_connect(gw)
    await scan_and_connect(gw)
    assert messages(caplog) == [nothing]  # not after every scan
    now += gateway.NOTHING_FOUND_REPORT_S
    await scan_and_connect(gw)
    assert messages(caplog) == [nothing, nothing]

    caplog.clear()
    radio.found = dict([advertisement("AA", "one")])
    await scan_and_connect(gw)
    radio.found = {}  # connected devices stop advertising
    now += gateway.NOTHING_FOUND_REPORT_S
    await scan_and_connect(gw)
    assert nothing not in messages(caplog)
    await gw._stop_sessions()


async def test_devices_left_out_by_the_name_filter_are_reported_once(radio, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(asyncio.get_running_loop(), "time", lambda: 1000.0)
    gw = Gateway(cloud.CloudSettings(ingest_key="k"), ["Sensor-*"])
    radio.found = dict([advertisement("AA", "other"), advertisement("BB", "x\n FORGED"), advertisement("CC", "")])
    await scan_and_connect(gw)
    await scan_and_connect(gw)
    assert messages(caplog) == [
        "Ignoring Spotflow device 'other' (AA): its name does not match --name",
        "Ignoring Spotflow device 'x\\n FORGED' (BB): its name does not match --name",
        "Ignoring Spotflow device without a name (CC): its name does not match --name",
        "No Spotflow devices with a matching name in range, still scanning",
    ]
    assert radio.connected == []
