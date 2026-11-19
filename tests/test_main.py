import logging

import pytest

from spotflow_gateway.__main__ import configure_logging, parse_args


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
