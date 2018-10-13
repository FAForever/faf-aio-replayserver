import asyncio
import logging
import os

from replayserver import Server
from replayserver.receive.mergestrategy import MergeStrategies
from replayserver.logging import logger

__all__ = ["main"]


def eget(*args, **kwargs):
    return os.environ.get(*args, **kwargs)


def main():
    logger.setLevel(eget("LOG_LEVEL", logging.INFO))

    env_config_source = {
        "config_merger_grace_period_time": ("REPLAY_GRACE_PERIOD", 30, int),
        "config_replay_merge_strategy": ("REPLAY_MERGE_STRATEGY", "FOLLOW_STREAM", MergeStrategies),
        "config_sent_replay_delay": ("REPLAY_DELAY", 5 * 60, int),
        "config_replay_forced_end_time": ("REPLAY_FORCE_END_TIME", 5 * 60 * 60, int),
        "config_server_port": ("PORT", 15000, int),
        "config_db_host": ("MYSQL_HOST", None, str),
        "config_db_port": ("MYSQL_PORT", None, int),
        "config_db_user": ("MYSQL_USER", None, str),
        "config_db_password": ("MYSQL_PASSWORD", None, str),
        "config_db_name": ("MYSQL_DB", None, str),
        "config_replay_store_path": ("REPLAY_DIR", None, str)
    }

    env_config = {}
    for k, v in env_config_source.items():
        env_name, env_default, env_type = v
        env_config[k] = eget(env_name, env_default)
        if env_config[k] is None:
            logger.critical((f"Missing config key: {k}. "
                             f"Set it using env var {env_name}."))
            return 1
        try:
            env_config[k] = v[2](env_config[k])
        except ValueError as e:
            logger.critical(f"Invalid value for {k} ({env_name}) - "
                            f"{str(e)}")
            return 1

    config = {
        "config_sent_replay_position_update_interval": 1,
    }
    config.update(env_config)

    server = Server.build(**config)
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(server.start())
    try:
        loop.run_forever()
        return 0
    except Exception as e:
        logger.critical(f"Critical server error! {e.__class__}: {str(e)}")
        loop.run_until_complete(server.stop())
        return 1
    finally:
        loop.close()
