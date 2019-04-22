import pytest

from replayserver.receive.mergestrategy import QuorumMergeStrategy
from replayserver.streams import ReplayStream, ConcreteDataMixin


# Technically we shouldn't rely on abstract classes being good, and should use
# mocks, yadda yadda. This way is way less bothersome.
class MockStream(ConcreteDataMixin, ReplayStream):
    def __init__(self):
        ConcreteDataMixin.__init__(self)
        ReplayStream.__init__(self)
        self._ended = False

    def ended(self):
        return self._ended


# We shouldn't keep all config values in one object since they're separate in
# reality, but for sake of simplicity...
class MockStrategyConfig:
    def __init__(self):
        self.stall_check_period = 60


general_test_strats = [QuorumMergeStrategy]


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_ends_stream_when_finalized(strategy, outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    strat.stream_added(stream1)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.ended()


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_picks_at_least_one_header(strategy, outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream2)
    strat.new_header(stream2)
    strat.stream_added(stream1)
    strat.stream_removed(stream2)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.header == "Header"


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_gets_all_data_of_one(strategy, outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    stream1._header = "Header"

    strat.stream_added(stream1)
    strat.new_header(stream1)
    stream1._data += b"Best f"
    strat.new_data(stream1)
    stream1._data += b"r"
    strat.new_data(stream1)
    stream1._data += b"iends"
    strat.new_data(stream1)
    strat.stream_removed(stream1)
    strat.finalize()

    assert outside_source_stream.header == "Header"
    assert outside_source_stream.data.bytes() == b"Best friends"


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_gets_common_prefix_of_all(strategy, outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.new_header(stream2)

    stream1._data += b"Best f"
    strat.new_data(stream1)
    stream2._data += b"Best"
    strat.new_data(stream2)
    stream1._data += b"r"
    strat.new_data(stream1)
    stream2._data += b" pals"
    strat.new_data(stream2)
    stream1._data += b"iends"
    strat.new_data(stream1)

    strat.stream_removed(stream2)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.data.bytes().startswith(b"Best ")


@pytest.mark.parametrize("strategy", [QuorumMergeStrategy])
def test_strategy_follow_stream_later_has_more_data(strategy,
                                                    outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.new_header(stream2)

    stream1._data += b"Data"
    print("S1 new data")
    strat.new_data(stream1)
    stream2._data += b"Data and stuff"
    print("S2 new data")
    strat.new_data(stream2)

    print("S2 removed")
    strat.stream_removed(stream2)
    print("S1 removed")
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.data.bytes() == b"Data and stuff"


# TODO - extend this one
@pytest.mark.parametrize("strategy", [QuorumMergeStrategy])
def test_strategy_tracked_stream_diverges(
        strategy, outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.new_header(stream2)

    stream1._data += b"Data and stuff"
    strat.new_data(stream1)
    strat.stream_removed(stream1)
    stream2._data += b"Data and smeg and blahblah"
    strat.new_data(stream2)
    strat.stream_removed(stream2)
    strat.finalize()
    assert outside_source_stream.data.bytes() in [
        b"Data and stuff",
        b"Data and smeg and blahblah"
    ]


# TODO - add tests with stalling connections

@pytest.mark.parametrize("strategy", [QuorumMergeStrategy])
def test_strategy_quorum_newly_arrived_quorum(
        strategy, outside_source_stream):
    strat = strategy.build(outside_source_stream, MockStrategyConfig())
    stream1 = MockStream()
    stream2 = MockStream()
    stream3 = MockStream()
    stream1._header = "Header"
    stream2._header = "Header"
    stream3._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.stream_added(stream3)

    strat.new_header(stream1)
    stream1._data += b"Data and stuff"
    strat.new_data(stream1)
    strat.stream_removed(stream1)

    stream2._data += b"Data and smeg and blahblah"
    strat.new_data(stream2)
    stream3._data += b"Data and smeg and blahblah"
    strat.new_data(stream3)
    strat.stream_removed(stream2)
    strat.stream_removed(stream3)
    strat.finalize()
    assert outside_source_stream.data.bytes() == b"Data and smeg and blahblah"
