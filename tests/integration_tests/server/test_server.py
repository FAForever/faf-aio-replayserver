import pytest
import asynctest

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


@pytest.fixture
def mock_database():
    return lambda *args, **kwargs: asynctest.Mock()     # TODO


@pytest.fixture
def mock_connection_producer():
    return lambda *args, **kwargs: asynctest.Mock()     # TODO


def test_server_init(mock_connection_producer, mock_database, tmpdir):
    conf = dict(config)
    conf["config_replay_store_path"] = str(tmpdir)
    Server.build(dep_connection_producer=mock_connection_producer,
                 dep_database=mock_database,
                 **conf)
