from replayserver.server.replays import Replays
from replayserver.server.replay import ReplayConfig
from tests import config_from_dict


config_dict = {
    "forced_end_time": 5 * 60 * 60,
    "grace_period": 30,
    "send": {
        "replay_delay": 5 * 60,
        "update_interval": 1,
    },
    "merge": {
        "stall_check_period": 60
    }
}


def replay_config(d):
    return ReplayConfig(config_from_dict(d))


def test_replays_init(mock_bookkeeper):
    Replays.build(mock_bookkeeper, replay_config(config_dict))
