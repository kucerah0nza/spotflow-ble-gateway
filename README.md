# Spotflow BLE Gateway

Connects Bluetooth Low Energy devices running the
[Spotflow Device SDK](https://github.com/spotflow-io/device-sdk) to the
[Spotflow](https://spotflow.io) cloud. The gateway runs on Linux, macOS and Windows.

> [!WARNING]
> **For testing and development purposes only.** This gateway is not intended for production
> use and is not actively maintained. It is provided as is, without warranty, and fixes or
> updates are not guaranteed (see [License](#license)).

- **Telemetry:** forwards logs, metrics and crash reports (core dumps) from devices to Spotflow.
- **Remote configuration:** delivers configuration changes from Spotflow to devices, such as the
  minimum log severity, and reports the applied configuration back.
- **Firmware updates (OTA):** delivers update commands to devices, downloads the firmware and
  transfers it to the device over BLE, and reports the update results to Spotflow.
- **Several devices at once,** with automatic reconnection on both the BLE and the cloud side.

## How it works

```
 BLE device                     Spotflow BLE Gateway                    Spotflow cloud
 (Device SDK)                   (this application)
┌──────────────┐   BLE GATT   ┌────────────────────────┐  MQTT over TLS  ┌──────────────────┐
│ logs,        │ ───────────▶ │ one session per device │ ──────────────▶ │ mqtt.spotflow.io │
│ metrics,     │              │                        │                 │                  │
│ crash reports│ ◀─────────── │ firmware download and  │ ◀────────────── │ configuration,   │
│              │  config, OTA │ block transfer (OTA)   │   config, OTA   │ OTA updates      │
└──────────────┘              └────────────────────────┘                 └──────────────────┘
```

The gateway scans for devices that advertise the Spotflow BLE service and connects to each one.
For every device it opens a separate MQTT connection to Spotflow, authenticated with the
device's ID and the gateway's ingest key. Devices therefore hold no cloud credentials.

The gateway forwards messages without decoding them, so it works with any data the Device SDK
sends. The only exception is OTA: the gateway downloads the firmware and serves it to the device
block by block.

The protocol is described in [Bluetooth Low Energy](https://docs.spotflow.io/fundamentals/bluetooth-low-energy)
in the Spotflow documentation.

## Requirements

**Gateway host**

- Linux (with BlueZ), macOS, or Windows 10 or newer
- A Bluetooth Low Energy adapter (built-in or USB)
- Python 3.11 or newer
- Outbound network access to `mqtt.spotflow.io` on TCP port 8883 (MQTT over TLS). For OTA, the
  host also needs direct HTTPS access (TCP port 443) to the firmware download location; HTTP
  proxies are not used.

**Devices**

Firmware built with the Spotflow Device SDK for Zephyr with the BLE transport
(`CONFIG_SPOTFLOW_TRANSPORT_BLE=y`). The SDK's
[BLE sample](https://github.com/spotflow-io/device-sdk/tree/main/zephyr/samples/ble) is a good
starting point. Optional features need these options:

| Feature | Device configuration |
|---|---|
| Logs | `CONFIG_SPOTFLOW_LOG_BACKEND=y` |
| Metrics | `CONFIG_SPOTFLOW_METRICS=y` |
| Crash reports | `CONFIG_SPOTFLOW_COREDUMPS=y` |
| Remote log severity | `CONFIG_SPOTFLOW_LOG_BACKEND=y` |
| OTA updates | `CONFIG_SPOTFLOW_OTA=y` (see [OTA updates](#ota-updates)) |

**Spotflow account**

An [ingest key](https://app.spotflow.io/ingest-keys) for your workspace. Devices appear in
Spotflow under the device ID their firmware reports, typically set with
`CONFIG_SPOTFLOW_DEVICE_ID`. Give each device a unique ID.

## Installation

Clone this repository, then install the gateway into a virtual environment:

**Linux and macOS**

```sh
cd spotflow-ble-gateway
python3 -m venv .venv
.venv/bin/pip install .
```

**Windows (PowerShell)**

```powershell
cd spotflow-ble-gateway
py -m venv .venv
.venv\Scripts\pip install .
```

## Usage

Provide the ingest key in the `SPOTFLOW_INGEST_KEY` environment variable and start the gateway:

**Linux and macOS**

```sh
export SPOTFLOW_INGEST_KEY='<your ingest key>'
.venv/bin/spotflow-gateway
```

**Windows (PowerShell)**

```powershell
$env:SPOTFLOW_INGEST_KEY = '<your ingest key>'
.venv\Scripts\spotflow-gateway
```

The gateway keeps scanning for devices and connects to every Spotflow device it finds. If a
device cannot be connected or its session fails, the gateway tries again with an increasing
delay of up to one minute. Stop the gateway with Ctrl+C (or `SIGTERM` on Linux and macOS); it
disconnects all devices before exiting. Example output:

```
2026-11-18 10:15:02,101 INFO    Spotflow BLE gateway 0.1.0 (Python 3.13.7, bleak 1.1.1, paho-mqtt 2.1.0, Linux-6.8.0-aarch64)
2026-11-18 10:15:02,114 INFO    Scanning for Spotflow BLE devices (MQTT broker mqtt.spotflow.io:8883)
2026-11-18 10:15:07,530 INFO    [Spotflow BLE device C4:3F:2A:91:0B:7E] Connecting (RSSI -58)
2026-11-18 10:15:08,912 INFO    [sensor-042] Connected to Spotflow BLE device C4:3F:2A:91:0B:7E, protocol v1.1 ota=yes
2026-11-18 10:15:09,377 INFO    [sensor-042] MQTT connected to mqtt.spotflow.io
2026-11-18 12:18:41,006 INFO    [sensor-042] Session ended after 2h 03m (1520 messages to the cloud, 3 to the device): the device disconnected
```

Each line about a device starts with the device ID in brackets or, until the device has reported
its ID, its advertised name and address. If the gateway finds no device, it logs
`No Spotflow devices in range, still scanning` (at most every 10 minutes).

### Options

| Option | Description |
|---|---|
| `--ingest-key KEY` | Spotflow ingest key. Defaults to the `SPOTFLOW_INGEST_KEY` environment variable. Prefer the environment variable: command-line arguments are visible to other users of the host. |
| `--name PATTERN` | Connect only to devices whose advertised BLE name matches this pattern (`*` and `?` wildcards). Can be given several times. Example: `--name 'Spotflow*'`. |
| `-v`, `--verbose` | Debug logging, including every forwarded message (its topic and size, never its content), which module logged each line, and tracebacks of unexpected errors. |

## Running as a service (Linux)

To keep the gateway running in the background, for example on a test bench, you can run it with
systemd:

1. Install the gateway to a fixed location, for example `/opt/spotflow-gateway`:

   ```sh
   sudo python3 -m venv /opt/spotflow-gateway
   sudo /opt/spotflow-gateway/bin/pip install /path/to/spotflow-ble-gateway
   ```

2. Create a system user. On distributions that restrict BlueZ access to the `bluetooth` group,
   add the user to it:

   ```sh
   sudo useradd --system --no-create-home --groups bluetooth spotflow-gateway
   ```

3. Store the ingest key in a file that only root can read:

   ```sh
   sudo install -d -m 755 /etc/spotflow-gateway
   sudo install -m 600 /dev/null /etc/spotflow-gateway/env
   echo "SPOTFLOW_INGEST_KEY=<your ingest key>" | sudo tee /etc/spotflow-gateway/env > /dev/null
   ```

4. Create `/etc/systemd/system/spotflow-gateway.service`:

   ```ini
   [Unit]
   Description=Spotflow BLE Gateway
   Wants=network-online.target bluetooth.target
   After=network-online.target bluetooth.target

   [Service]
   User=spotflow-gateway
   EnvironmentFile=/etc/spotflow-gateway/env
   ExecStart=/opt/spotflow-gateway/bin/spotflow-gateway
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

5. Start the service and follow its logs:

   ```sh
   sudo systemctl daemon-reload
   sudo systemctl enable --now spotflow-gateway
   journalctl -u spotflow-gateway -f
   ```

   Under systemd, the gateway leaves timestamps to the journal and tags each line with its
   severity, so `journalctl -u spotflow-gateway -p warning` shows only warnings and errors.

On macOS and Windows, run the gateway under the service manager you already use (for example
launchd or the Windows Task Scheduler) with the same environment variable.

## OTA updates

The gateway enables OTA for a device when the device reports OTA support, which requires firmware
built with `CONFIG_SPOTFLOW_OTA=y`. Create the deployment in Spotflow as for any other device and
keep the gateway running until the update completes.

During an update:

1. The gateway delivers the update command from Spotflow to the device. If the device does not
   acknowledge the command after several attempts, the gateway disconnects the device and
   connects to it again, and Spotflow then repeats the command.
2. When the device requests the firmware, the gateway downloads it over HTTPS and transfers it to
   the device in blocks. If the BLE connection drops, the device retries the transfer after it
   reconnects.
3. The device installs the update and reports the result, which the gateway forwards to Spotflow.

Downloaded firmware is kept in a temporary directory, shared by all devices, so several devices
updating to the same firmware download it once. The directory holds the four most recently used
files (each up to 256 MiB) and is deleted when the gateway stops.

The directory is created in the system's temporary location. To use another location, set the
`TMPDIR` environment variable (`TEMP` on Windows), for example when `/tmp` is kept in memory on
a host with little RAM.

Transfer speed depends on the BLE connection. The gateway starts with 20-byte BLE writes, which
every device supports, and switches to larger writes once the device has shown it supports them.

## Security

- **Ingest key:** the key stays on the gateway host and is never logged. Anyone with the key can
  send data to your workspace, so restrict access to it (see
  [Running as a service](#running-as-a-service-linux)). Revoke and replace it in Spotflow if it
  leaks.
- **Encrypted BLE:** devices are identified by the device ID they report, so unless every
  device in range is trusted, use firmware built with `CONFIG_SPOTFLOW_BLE_REQUIRE_ENCRYPTION=y`
  (the default when OTA is enabled; the SDK's BLE sample turns it off for development), with
  each device paired only to authorized gateways. On Linux, pair devices with
  `bluetoothctl` before starting the gateway. On macOS and Windows, the operating system
  typically starts pairing when the gateway first reads a protected characteristic; accept the
  prompt if one appears. You can also limit which devices the gateway connects to with `--name`.
- **Cloud connections** use TLS with certificate verification.
- **Firmware download links and their access secrets** are used only for the download and are
  never logged, not even with `-v`.
- **Firmware downloads** are requested by the device, so the gateway restricts where they can
  go: HTTPS only, with certificate verification, to public internet addresses only, and without
  following redirects. A device therefore cannot use the gateway to reach hosts in the
  gateway's local network.

## Limitations

- A device can be connected to one gateway at a time. While connected, it stops advertising, so
  other gateways and apps do not see it.
- If the cloud connection is lost, the gateway buffers up to 1,000 messages per device in
  memory and sends them after reconnecting. Beyond that, the oldest messages are dropped and
  the gateway logs how many.
  Buffered messages are lost if the device disconnects or the gateway stops before the cloud
  connection is back; the gateway logs how many were discarded.
- The gateway itself does not appear as a device in Spotflow; each BLE device appears under its
  own device ID.

## Troubleshooting

| Symptom | What to check |
|---|---|
| No devices found (`No Spotflow devices in range`) | If the log says `Ignoring Spotflow device ...: its name does not match --name`, adjust `--name`. Otherwise, check that the device is powered, advertising and in range, and not connected to another gateway or app. On macOS, allow Bluetooth access for the terminal or app running the gateway (System Settings → Privacy & Security → Bluetooth). On Linux, check that `bluetoothd` is running and the adapter is on (`bluetoothctl show`). |
| `MQTT error: connection refused: Bad user name or password` | The ingest key is wrong or has been revoked. |
| `MQTT error: cannot connect to mqtt.spotflow.io` | Network access to `mqtt.spotflow.io` on TCP port 8883 (firewall, proxy, DNS). |
| `unsupported protocol version` | The device firmware uses a newer protocol version than this gateway. Update the gateway. |
| BLE errors right after connecting | The device requires encryption. Pair it with the gateway host (see [Security](#security)). The gateway retries with an increasing delay, so it can take up to a minute to connect after pairing. |
| `an OTA command could not be delivered to the device` | The device did not acknowledge an update command. The gateway disconnects it and tries again after reconnecting. If it keeps happening, check that the device is in range and look at the device's own log. |
| `Buffer for the cloud is full, dropping the oldest messages` | The gateway cannot reach Spotflow, or the device sends data faster than the connection can forward it. Check the preceding `MQTT error` messages. |
| `Artifact OPEN failed: ... is not a public address` | The firmware download location resolves to a private or local address, which the gateway refuses (see [Security](#security)). |
| A device is missing from Spotflow | Run with `-v` to see whether the gateway connects to it and forwards messages, and check the device ID it reports. |

Run with `-v` for detailed logs. If you report a problem, include the gateway log (its first
line gives the gateway version and the platform) and the Device SDK version used by the device.

## Development

Install the gateway with its development dependencies and run the tests:

```sh
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/mypy
```

The integration tests use a simulated BLE device and a local
[Mosquitto](https://mosquitto.org) MQTT broker. Without Mosquitto installed, they are skipped.
CI runs the full test suite on Linux, macOS and Windows.

| Module | Responsibility |
|---|---|
| `gateway.py` | Scanning for devices and running one session per device |
| `session.py` | Bridging one BLE device and its cloud connection |
| `protocol.py` | BLE framing: fragmentation, reassembly, acknowledgements |
| `ble.py` | BLE connection (based on [bleak](https://github.com/hbldh/bleak)) |
| `cloud.py` | MQTT connection to Spotflow (based on [paho-mqtt](https://github.com/eclipse-paho/paho.mqtt.python)) |
| `downlink.py` | Ordered, acknowledged delivery of messages to the device |
| `artifacts.py` | Firmware download and block transfer for OTA |

## License

The gateway is released under the [MIT License](LICENSE): you are free to use, modify and
distribute it. It is provided as is, without warranty of any kind.
