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
    env_config = {
        "config_merger_grace_period_time": ("REPLAY_GRACE_PERIOD", 30),
        "config_replay_merge_strategy": ("REPLAY_MERGE_STRATEGY", "FOLLOW_STREAM"),
        "config_sent_replay_delay": ("REPLAY_DELAY", 5 * 60),
        "config_replay_forced_end_time": ("REPLAY_FORCE_END_TIME", 5 * 60 * 60),
        "config_server_port": ("PORT", 15000),
        "config_db_host": ("MYSQL_HOST", None),
        "config_db_port": ("MYSQL_PORT", None),
        "config_db_user": ("MYSQL_USER", None),
        "config_db_password": ("MYSQL_PASSWORD", None),
        "config_db_name": ("MYSQL_DB", None),
        "config_replay_store_path": ("REPLAY_DIR", None)
    }

    config = {
        "config_sent_replay_position_update_interval": 1,
    }
    config.update({k: eget(*v) for k, v in env_config.items()})

    logger.setLevel(eget("LOG_LEVEL", logging.INFO))
    for key in config:
        if config[key] is None:
            logger.critical((f"Missing config key: {key}. "
                             f"Set it using env var {env_config[key][0]}."))
            return 1

    strat_str = config["config_replay_merge_strategy"]
    try:
        config["config_replay_merge_strategy"] = MergeStrategies(strat_str)
    except ValueError:
        logger.critical(f"{strat_str} is not a valid replay merge strategy")
        return 1

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
