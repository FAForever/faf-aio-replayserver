import pytest

from replayserver.receive.mergestrategy import QuorumMergeStrategy
from replayserver.streams import ReplayStream, ConcreteDataMixin


# Technically we shouldn't rely on abstract classes being good, and should use
# mocks, yadda yadda. This way is way less bothersome.
class MockStream(ConcreteDataMixin, ReplayStream):
    def __init__(self, set_future=False):
        ConcreteDataMixin.__init__(self)
        ReplayStream.__init__(self)
        self._ended = False
        self._future_data = bytearray() if set_future else None

    def ended(self):
        return self._ended

    def _maybe_future_data(self):
        return (self._future_data if self._future_data is not None
                else self._data)

    def _future_data_length(self):
        return len(self._maybe_future_data())

    def _future_data_slice(self, s):
        return self._maybe_future_data()[s]

    def _future_data_bytes(self):
        return self._maybe_future_data()

    def _future_data_view(self):
        return memoryview(self._maybe_future_data())


class MockStrategyConfig:
    def __init__(self):
        self.desired_quorum = 2


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
def test_strategy_later_has_more_data(strategy,
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


def test_quorum_strategy_uses_future_data(outside_source_stream):
    strat = QuorumMergeStrategy.build(outside_source_stream,
                                      MockStrategyConfig())
    stream1 = MockStream(True)
    stream2 = MockStream(True)
    stream3 = MockStream(True)
    stream1._header = "Header"
    stream2._header = "Header"
    stream3._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.stream_added(stream3)

    stream1._future_data += b"foo"
    stream2._future_data += b"foo"
    stream1._data += b"f"
    stream2._data += b"fo"

    # First check calculating quorum point.
    # We expect to send f, since 2 future data items confirm it.
    # Note that we only react to streams when new_data is called, future_data
    # is strictly for comparison purposes.
    strat.new_data(stream1)
    strat.new_data(stream2)
    assert outside_source_stream.data.bytes() == b"fo"
    stream1._data += b"oo"
    strat.new_data(stream1)
    assert outside_source_stream.data.bytes() == b"foo"

    # And now stalemate resolution.
    # After resolution, we should have confirmed "foo aaaaa", so sending some
    # "a"s should come through.
    stream3._future_data += b"foo aaaaa"
    stream1._future_data += b" bbbbb"
    stream2._future_data += b" aaaaa"
    stream1._data += b" bbbbb"
    stream2._data += b"o aaa"
    stream3._data += b"foo"
    strat.new_data(stream1)
    strat.new_data(stream2)
    strat.new_data(stream3)
    assert outside_source_stream.data.bytes() == b"foo aaa"


# This one kind of relies on internals? Maybe we should allow querying what
# role a stream has?
def test_quorum_strategy_immediate_stalemate_resolve(outside_source_stream):
    strat = QuorumMergeStrategy.build(outside_source_stream,
                                      MockStrategyConfig())
    stream1 = MockStream(True)
    stream2 = MockStream(True)
    stream3 = MockStream(True)
    stream1._header = "Header"
    stream2._header = "Header"
    stream3._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.stream_added(stream3)

    stream1._future_data += b"foo"
    stream2._future_data += b"foo"
    stream1._data += b"f"
    stream2._data += b"f"

    strat.new_data(stream1)
    strat.new_data(stream2)

    # Streams 1 and 2 should make a quorum now.
    assert outside_source_stream.data.bytes() == b"f"

    stream1._future_data += b"aaa"
    stream2._future_data += b"bbb"
    stream3._future_data += b"fooaaa"
    stream1._data += b"ooaaa"
    stream2._data += b"oobbb"
    stream3._data += b"fooaaa"

    strat.new_data(stream3)     # This one's not tracked
    assert outside_source_stream.data.bytes() == b"f"
    strat.new_data(stream2)
    # Now we should've entered a stalemate, then resolved it with stream 3.
    assert outside_source_stream.data.bytes() == b"fooaaa"
