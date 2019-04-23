import pytest
import asyncio
from tests import config_from_dict, fast_forward_time, timeout
from tests.replays import example_replay

from replayserver.server.replay import Replay, ReplayConfig
from replayserver.server.connection import ConnectionHeader

# Suitable for short tests.
config_dict = {
    "forced_end_time": 1000,
    "grace_period": 5,
    "send": {
        "replay_delay": 0.1,
        "update_interval": 0.1,
    },
    "merge": {
        "desired_quorum": 2
    }
}


def replay_config(d):
    return ReplayConfig(config_from_dict(d))


def test_replay_init(mock_bookkeeper):
    Replay.build(1, mock_bookkeeper, replay_config(config_dict))
