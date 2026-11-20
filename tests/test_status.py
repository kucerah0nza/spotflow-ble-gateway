from spotflow_gateway.status import Counts, Device, status_line


def test_quiet_line_shows_the_essentials():
    line = status_line(8100, [Device("asset-tracker-02", True)], 300, Counts(to_cloud=68, to_devices=1), 0)
    assert line == ("Status: up 2h 15m, 1 device connected (asset-tracker-02), all online; "
                    "last 5 min: 68 messages to the cloud, 1 to devices; 0 buffered")


def test_problems_are_shown_only_when_they_happened():
    recent = Counts(to_cloud=1, dropped=12, discarded=3, undelivered=2, malformed=4, session_failures=1)
    devices = [Device("a", True), Device("b", False)]
    assert status_line(60, devices, 300, recent, 230) == (
        "Status: up 1m 00s, 2 devices connected (a, b), 1 offline from the cloud; last 5 min: 1 message to the cloud, "
        "0 to devices, 12 dropped, 3 discarded, 2 undelivered to devices, 4 malformed from devices, "
        "1 session failure; 230 buffered"
    )


def test_bluetooth_state_and_no_devices():
    assert status_line(30, [], 90, Counts(), 0, "Bluetooth is turned off") == (
        "Status: up 30s, Bluetooth is turned off, no devices connected; "
        "last 1m 30s: 0 messages to the cloud, 0 to devices; 0 buffered"
    )


def test_long_device_lists_are_cut():
    devices = [Device(f"d{i}", True) for i in range(7)]
    assert "7 devices connected (d0, d1, d2, d3, d4, +2 more)" in status_line(0, devices, 300, Counts(), 0)


def test_counts_add_and_subtract():
    assert Counts(to_cloud=5, dropped=1) + Counts(to_cloud=2) - Counts(dropped=1) == Counts(to_cloud=7)
