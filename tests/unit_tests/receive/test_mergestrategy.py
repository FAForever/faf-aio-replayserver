import pytest
import asyncio

from tests import fast_forward_time
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
    strat.new_data(stream1)
    stream2._data += b"Data and stuff"
    strat.new_data(stream2)

    strat.stream_removed(stream2)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.data.bytes() == b"Data and stuff"


@pytest.mark.parametrize("strategy", [QuorumMergeStrategy])
def test_strategy_follow_stream_new_tracked_stream_diverges(
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
    assert outside_source_stream.data.bytes() == b"Data and stuff"


@fast_forward_time(10, 0.1)
@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", [QuorumMergeStrategy])
async def test_strategy_follow_stream_deals_with_stalled_connections(
        event_loop, strategy, outside_source_stream):

    config = MockStrategyConfig()
    config.stall_check_period = 3
    strat = strategy.build(outside_source_stream, config)
    stalled_stream = MockStream()
    ahead_stream = MockStream()

    stalled_stream._header = "Header"
    stalled_stream._data += b"a" * 10
    strat.stream_added(stalled_stream)
    strat.new_header(stalled_stream)
    strat.new_data(stalled_stream)

    # Check that we're tracking the stalled stream
    assert outside_source_stream.data.bytes() == b"a" * 10

    ahead_stream._header = "Header"
    strat.stream_added(ahead_stream)
    strat.new_header(ahead_stream)

    for i in range(20):
        await asyncio.sleep(0.1)
        ahead_stream._data += b"a"
        strat.new_data(ahead_stream)

    # We should still be tracking the stalled stream
    assert outside_source_stream.data.bytes() == b"a" * 10

    for i in range(40):
        await asyncio.sleep(0.1)
        ahead_stream._data += b"a"
        strat.new_data(ahead_stream)

    # By now we should've switched
    assert outside_source_stream.data.bytes() == b"a" * 60

    strat.stream_removed(stalled_stream)
    strat.stream_removed(ahead_stream)
    strat.finalize()


@pytest.mark.parametrize("strategy", [QuorumMergeStrategy])
def test_strategy_follow_stream_new_data_of_diverged_stream(
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
    stream2._data += b"this should not cause an exception"
    strat.new_data(stream2)
    strat.stream_removed(stream2)
    strat.finalize()
    assert outside_source_stream.data.bytes() == b"Data and stuff"
