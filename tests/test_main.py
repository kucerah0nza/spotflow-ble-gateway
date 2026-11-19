import logging
import os
import sys

import pytest

from spotflow_gateway import __main__ as main_module
from spotflow_gateway.__main__ import configure_logging, log_format, parse_args


def test_ingest_key_from_environment(monkeypatch):
    monkeypatch.setenv("SPOTFLOW_INGEST_KEY", "from-env")
    args = parse_args([])
    assert args.ingest_key == "from-env"
    assert (args.mqtt_host, args.mqtt_port, args.no_tls) == ("mqtt.spotflow.io", 8883, False)
    assert args.name == [] and not args.verbose


def test_options_override_defaults(monkeypatch):
    monkeypatch.setenv("SPOTFLOW_INGEST_KEY", "from-env")
    args = parse_args(["--ingest-key", "from-arg", "--name", "A*", "--name", "B?", "-v"])
    assert args.ingest_key == "from-arg"
    assert args.name == ["A*", "B?"] and args.verbose


def test_ingest_key_is_required(monkeypatch, capsys):
    monkeypatch.delenv("SPOTFLOW_INGEST_KEY", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        parse_args([])
    assert exit_info.value.code == 2
    assert "ingest key is required" in capsys.readouterr().err


@pytest.fixture
def restored_log_levels():
    loggers = [logging.getLogger(name) for name in (None, "spotflow_gateway", "bleak")]
    levels = [logger.level for logger in loggers]
    yield
    for logger, level in zip(loggers, levels, strict=True):
        logger.setLevel(level)


@pytest.mark.parametrize("verbose", [False, True])
def test_verbose_logging_covers_only_the_gateway(restored_log_levels, verbose):
    configure_logging(verbose)
    assert logging.getLogger("spotflow_gateway.session").isEnabledFor(logging.DEBUG) == verbose
    # bleak logs the bytes of BLE reads, writes and notifications at DEBUG, and those
    # carry firmware download links and secrets; httpx logs request URLs at INFO.
    for name in ("bleak.backends.bluezdbus.client", "bleak.backends.bluezdbus.manager", "dbus_fast"):
        assert not logging.getLogger(name).isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("httpx").isEnabledFor(logging.INFO)


def format_record(fmt, name="spotflow_gateway.session", level=logging.WARNING):
    record = logging.LogRecord(name, level, __file__, 1, "[%s] %s", ("sensor-042", "message"), None)
    return main_module._Formatter(fmt).format(record)


def test_log_format():
    assert log_format(verbose=False, journal=False) == "%(asctime)s %(levelname)-7s %(message)s"
    # -v adds the module, for developers; the journal timestamps lines itself and reads
    # the syslog priority from the prefix (journalctl -p).
    assert format_record(log_format(verbose=True, journal=True)) == "<4>WARNING session   [sensor-042] message"
    assert format_record(log_format(verbose=True, journal=True), "bleak", logging.ERROR) == \
        "<3>ERROR   bleak     [sensor-042] message"


def test_journal_is_detected_only_for_the_stream_it_names(monkeypatch, tmp_path):
    stderr = open(tmp_path / "stderr", "w")  # noqa: SIM115
    try:
        monkeypatch.setattr(sys, "stderr", stderr)
        stat = os.fstat(stderr.fileno())
        monkeypatch.delenv("JOURNAL_STREAM", raising=False)
        assert not main_module._stderr_is_journal()
        monkeypatch.setenv("JOURNAL_STREAM", f"{stat.st_dev}:{stat.st_ino}")
        assert main_module._stderr_is_journal()
        # Inherited by a process whose stderr goes elsewhere (e.g. run from a shell in a unit).
        monkeypatch.setenv("JOURNAL_STREAM", f"{stat.st_dev}:{stat.st_ino + 1}")
        assert not main_module._stderr_is_journal()
        monkeypatch.setenv("JOURNAL_STREAM", "garbage")
        assert not main_module._stderr_is_journal()
    finally:
        stderr.close()
