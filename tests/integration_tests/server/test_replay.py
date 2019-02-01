from replayserver.server.replay import Replay, ReplayConfig
from tests import config_from_dict


config_dict = {
    "forced_end_time": 5 * 60 * 60,
    "send": {
        "replay_delay": 5 * 60,
        "update_interval": 1,
    },
    "merge": {
        "strategy": "FOLLOW_STREAM",
        "grace_period": 30,
        "strategy_config": {
            "follow_stream": {
                "stall_check_period": 60
            }
        }
    }
}


def replay_config(d):
    return ReplayConfig(config_from_dict(d))


def test_replay_init(mock_bookkeeper):
    Replay.build(1, mock_bookkeeper, replay_config(config_dict))
