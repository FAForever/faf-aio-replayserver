import asyncio
from replayserver import Server
from replayserver.receive.mergestrategy import MergeStrategies


config = {
    "merger_grace_period_time": 30,
    "replay_merge_strategy": MergeStrategies.GREEDY,
    "sent_replay_delay": 5 * 60,
    "sent_replay_position_update_interval": 1,
    "replay_forced_end_time": 5 * 60 * 60,
    "server_port": 15000,
}
config = {"config_" + k: v for k, v in config.items()}


def main():
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
