from replayserver.receive.mergestrategy import MergeStrategies
from replayserver.receive.merger import Merger


config = {
    "config_merger_grace_period_time": 30,
    "config_replay_merge_strategy": MergeStrategies.GREEDY,
}


def test_merger_init():
    Merger.build(**config)
