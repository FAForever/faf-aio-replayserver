import pytest
import asyncio
from tests import timeout
from tests.replays import example_replay

from replayserver import Server
from replayserver.receive.mergestrategy import MergeStrategies


config = {
    "merger_grace_period_time": 1,
    "replay_merge_strategy": MergeStrategies.GREEDY,
    "sent_replay_delay": 5,
    "sent_replay_position_update_interval": 0.1,
    "replay_forced_end_time": 60,
    "server_port": 15000,
    "db_host": "117.0.0.1",
    "db_port": 3306,
    "db_user":     "root",
    "db_password": "banana",
    "db_name":     "faf",
    "replay_store_path": "/tmp/replaceme"
}
config = {"config_" + k: v for k, v in config.items()}


def test_server_init():
    Server.build(**config)


@pytest.mark.asyncio
@timeout(10)
async def test_server_single_connection(event_loop, mock_database, tmpdir):
    conf = dict(config)
    conf["config_server_port"] = 15001
    conf["config_replay_store_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda **kwargs: mock_database,
                          **conf)
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', 15001)

    w.write(b"P/1/foo\0")
    w.write(example_replay.data)
    await w.drain()
    w.close()
    rep = await server._replays.wait_for_replay(1)
    await rep.wait_for_ended()

    rfile = list(tmpdir.visit('1.fafreplay'))
    assert len(rfile) == 1
