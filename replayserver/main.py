import asyncio
from os.environ import get as eget
from replayserver import Server
from replayserver.receive.mergestrategy import MergeStrategies

__all__ = ["main"]

config = {
    "merger_grace_period_time": eget("REPLAY_GRACE_PERIOD", 30),
    "replay_merge_strategy": MergeStrategies.FOLLOW_STREAM,
    "sent_replay_delay": eget("REPLAY_DELAY", 5 * 60),
    "sent_replay_position_update_interval": 1,
    "replay_forced_end_time": eget("REPLAY_FORCE_END_TIME", 5 * 60 * 60),
    "server_port": eget("PORT", 15000),
    "db_host": eget("MYSQL_HOST", None),
    "db_port": eget("MYSQL_PORT", None),
    "db_user": eget("MYSQL_USER", None),
    "db_password": eget("MYSQL_PASSWORD", None),
    "db_name": eget("MYSQL_DB", None),
    "replay_store_path": eget("REPLAY_DIR", None)
}
config = {"config_" + k: v for k, v in config.items()}


def main():
    for key in config:
        if config[key] is None:
            return 1

    server = Server.build(**config)
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(server.start())
    try:
        loop.run_forever()
        return 0
    except Exception:
        loop.run_until_complete(server.stop())
        return 1
    finally:
        loop.close()
