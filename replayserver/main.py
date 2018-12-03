import asyncio
import logging
import os
import signal

from replayserver import Server
from replayserver.receive.mergestrategy import MergeStrategies
from replayserver.logging import logger

__all__ = ["main"]


def eget(*args, **kwargs):
    return os.environ.get(*args, **kwargs)


def get_config_from_env():
    MISSING = object()
    env_config = {
        "merger_grace_period_time": ("REPLAY_GRACE_PERIOD", 30, int),
        "replay_merge_strategy":
            ("REPLAY_MERGE_STRATEGY", MergeStrategies.FOLLOW_STREAM,
             MergeStrategies),
        "mergestrategy_stall_check_period":
            ("MERGESTRATEGY_STALL_CHECK_PERIOD", 60, int),
        "connection_header_read_timeout":
            ("CONNECTION_HEADER_READ_TIMEOUT", 6 * 60 * 60, int),
        "sent_replay_delay": ("REPLAY_DELAY", 5 * 60, int),
        "replay_forced_end_time": ("REPLAY_FORCE_END_TIME", 5 * 60 * 60, int),
        "server_port": ("PORT", 15000, int),
        "db_host": ("MYSQL_HOST", MISSING, str),
        "db_port": ("MYSQL_PORT", MISSING, int),
        "db_user": ("MYSQL_USER", MISSING, str),
        "db_password": ("MYSQL_PASSWORD", MISSING, str),
        "db_name": ("MYSQL_DB", MISSING, str),
        "replay_store_path": ("REPLAY_DIR", MISSING, str),
        "sent_replay_position_update_interval":
            ("SENT_REPLAY_UPDATE_INTERVAL", 1, int),
        "prometheus_port": ("PROMETHEUS_PORT", None, int),
    }

    config = {}
    for k, v in env_config.items():
        env_name, env_default, env_type = v
        env_value = eget(env_name, None)
        if env_value is None:
            if env_default is MISSING:
                raise ValueError((f"Missing config key: {k}. "
                                  f"Set it using env var {env_name}."))
            config[k] = env_default
        else:
            try:
                config[k] = v[2](env_value)
            except ValueError as e:
                raise ValueError(f"Bad value for {k} ({env_name}) - {str(e)}")
    config = {"config_" + k: v for k, v in config.items()}
    return config


def setup_signal_handler(server, loop):
    shutting_down = False

    def shutdown_gracefully():
        nonlocal shutting_down
        if not shutting_down:
            shutting_down = True
            asyncio.ensure_future(server.stop(), loop=loop)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, shutdown_gracefully)


def main():
    # FIXME - report errors regarding this as well
    logger.setLevel(int(eget("LOG_LEVEL", logging.INFO)))
    try:
        config = get_config_from_env()
    except ValueError as e:
        logger.critical(e)
        return 1

    server = Server.build(**config)
    loop = asyncio.get_event_loop()
    setup_signal_handler(server, loop)
    try:
        loop.run_until_complete(server.run())
        return 0
    except Exception as e:
        logger.critical(f"Critical server error!")
        logger.exception(e)
        return 1
