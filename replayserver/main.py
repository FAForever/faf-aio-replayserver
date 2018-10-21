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
    env_config = {
        "merger_grace_period_time": ("REPLAY_GRACE_PERIOD", 30, int),
        "replay_merge_strategy":
            ("REPLAY_MERGE_STRATEGY", "FOLLOW_STREAM", MergeStrategies),
        "sent_replay_delay": ("REPLAY_DELAY", 5 * 60, int),
        "replay_forced_end_time": ("REPLAY_FORCE_END_TIME", 5 * 60 * 60, int),
        "server_port": ("PORT", 15000, int),
        "db_host": ("MYSQL_HOST", None, str),
        "db_port": ("MYSQL_PORT", None, int),
        "db_user": ("MYSQL_USER", None, str),
        "db_password": ("MYSQL_PASSWORD", None, str),
        "db_name": ("MYSQL_DB", None, str),
        "replay_store_path": ("REPLAY_DIR", None, str),
        "sent_replay_position_update_interval":
            ("SENT_REPLAY_UPDATE_INTERVAL", 1, int),
    }

    config = {}
    for k, v in env_config.items():
        env_name, env_default, env_type = v
        config[k] = eget(env_name, env_default)
        if config[k] is None:
            raise ValueError((f"Missing config key: {k}. "
                              f"Set it using env var {env_name}."))
        try:
            config[k] = v[2](config[k])
        except ValueError as e:
            raise ValueError(f"Invalid value for {k} ({env_name}) - {str(e)}")
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
        logger.critical(f"Critical server error! {e.__class__}: {str(e)}")
        logger.exception(e)
        return 1
