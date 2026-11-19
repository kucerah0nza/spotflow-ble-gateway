import asyncio
import logging

from spotflow_gateway.ble import WriteGate


class Peripheral:
    """Stands in for a CBPeripheral whose transmit queue has room after a few polls."""

    def __init__(self, busy_polls=0, error=None):
        self.busy_polls = busy_polls
        self.error = error
        self.polls = 0

    def canSendWriteWithoutResponse(self):  # noqa: N802 (Core Bluetooth name)
        self.polls += 1
        if self.error:
            raise self.error
        return self.polls > self.busy_polls


async def test_waits_until_peripheral_can_take_a_write():
    peripheral = Peripheral(busy_polls=3)
    await asyncio.wait_for(WriteGate(peripheral).wait(), 5)
    assert peripheral.polls == 4


async def test_no_peripheral_means_no_waiting():
    await asyncio.wait_for(WriteGate(None).wait(), 1)


async def test_gives_up_pacing_when_flag_never_clears():
    peripheral = Peripheral(busy_polls=10**9)
    gate = WriteGate(peripheral)
    gate.TIMEOUT_S = 0.05
    await asyncio.wait_for(gate.wait(), 5)
    polls = peripheral.polls
    await asyncio.wait_for(gate.wait(), 5)  # later writes are not delayed again
    assert peripheral.polls == polls


async def test_gives_up_pacing_when_flag_is_unavailable(caplog):
    caplog.set_level(logging.DEBUG)
    peripheral = Peripheral(error=AttributeError("canSendWriteWithoutResponse"))
    gate = WriteGate(peripheral, lambda: "sensor-042")
    await gate.wait()
    await gate.wait()
    assert peripheral.polls == 1
    assert caplog.messages == [
        "[sensor-042] Write flow control unavailable (canSendWriteWithoutResponse), writing unpaced"
    ]
