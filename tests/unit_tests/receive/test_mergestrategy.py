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

    def _future_data_length(self):
        if self._future_data is None:
            return self._data_length()
        else:
            return len(self._future_data)

    def _future_data_slice(self, s):
        if self._future_data is None:
            return self._data_slice(s)
        else:
            return self._future_data[s]

    def _future_data_bytes(self):
        if self._future_data is None:
            return self._data_bytes()
        else:
            return self._future_data

    def _future_data_view(self, start, end):
        if self._future_data is None:
            return self._data_view(start, end)
        else:
            return memoryview(self._future_data)[start:end]


class MockStrategyConfig:
    def __init__(self):
        self.desired_quorum = 2
        self.stream_comparison_cutoff = None


general_test_strats = [QuorumMergeStrategy]


def test_strategy_ends_stream_when_finalized(outside_source_stream):
    strat = QuorumMergeStrategy.build(outside_source_stream,
                                      MockStrategyConfig())
    stream1 = MockStream()
    strat.stream_added(stream1)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.ended()


def test_strategy_picks_at_least_one_header(outside_source_stream):
    strat = QuorumMergeStrategy.build(outside_source_stream,
                                      MockStrategyConfig())
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


@pytest.mark.parametrize("trimming", [None, 2])
def test_strategy_gets_all_data_of_one(trimming, outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
    stream1 = MockStream()
    stream1._header = "Header"

    strat.stream_added(stream1)
    strat.new_header(stream1)
    stream1._add_data(b"Best f")
    strat.new_data(stream1)
    stream1._add_data(b"r")
    strat.new_data(stream1)
    stream1._add_data(b"iends")
    strat.new_data(stream1)
    strat.stream_removed(stream1)
    strat.finalize()

    assert outside_source_stream.header == "Header"
    assert outside_source_stream.data.bytes() == b"Best friends"


@pytest.mark.parametrize("trimming", [None, 3])
def test_strategy_gets_common_prefix_of_all(trimming, outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.new_header(stream2)

    stream1._add_data(b"Best f")
    strat.new_data(stream1)
    stream2._add_data(b"Best")
    strat.new_data(stream2)
    stream1._add_data(b"r")
    strat.new_data(stream1)
    stream2._add_data(b" pals")
    strat.new_data(stream2)
    stream1._add_data(b"iends")
    strat.new_data(stream1)

    strat.stream_removed(stream2)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.data.bytes().startswith(b"Best ")


@pytest.mark.parametrize("trimming", [None, 3])
def test_strategy_later_has_more_data(trimming,
                                      outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.new_header(stream2)

    stream1._add_data(b"Data")
    strat.new_data(stream1)
    stream2._add_data(b"Data and stuff")
    strat.new_data(stream2)

    strat.stream_removed(stream2)
    strat.stream_removed(stream1)
    strat.finalize()
    assert outside_source_stream.data.bytes() == b"Data and stuff"


# TODO - extend this one
@pytest.mark.parametrize("trimming", [None, 3])
def test_strategy_tracked_stream_diverges(
        trimming, outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream1)
    strat.stream_added(stream2)
    strat.new_header(stream2)

    stream1._add_data(b"Data and stuff")
    strat.new_data(stream1)
    strat.stream_removed(stream1)
    stream2._add_data(b"Data and smeg and blahblah")
    strat.new_data(stream2)
    strat.stream_removed(stream2)
    strat.finalize()
    assert outside_source_stream.data.bytes() in [
        b"Data and stuff",
        b"Data and smeg and blahblah"
    ]


# TODO - add tests with stalling connections

@pytest.mark.parametrize("trimming", [None, 3])
def test_strategy_quorum_newly_arrived_quorum(
        trimming, outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
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
    stream1._add_data(b"Data and stuff")
    strat.new_data(stream1)
    strat.stream_removed(stream1)

    stream2._add_data(b"Data and smeg and blahblah")
    strat.new_data(stream2)
    stream3._add_data(b"Data and smeg and blahblah")
    strat.new_data(stream3)
    strat.stream_removed(stream2)
    strat.stream_removed(stream3)
    strat.finalize()
    assert outside_source_stream.data.bytes() == b"Data and smeg and blahblah"


@pytest.mark.parametrize("trimming", [None, 2])
def test_quorum_strategy_uses_future_data(trimming, outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
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
    stream1._add_data(b"f")
    stream2._add_data(b"fo")

    # First check calculating quorum point.
    # We expect to send f, since 2 future data items confirm it.
    # Note that we only react to streams when new_data is called, future_data
    # is strictly for comparison purposes.
    strat.new_data(stream1)
    strat.new_data(stream2)
    assert outside_source_stream.data.bytes() == b"fo"
    stream1._add_data(b"oo")
    strat.new_data(stream1)
    assert outside_source_stream.data.bytes() == b"foo"

    # And now stalemate resolution.
    # After resolution, we should have confirmed "foo aaaaa", so sending some
    # "a"s should come through.
    stream3._future_data += b"foo aaaaa"
    stream1._future_data += b" bbbbb"
    stream2._future_data += b" aaaaa"
    stream1._add_data(b" bbbbb")
    stream2._add_data(b"o aaa")
    stream3._add_data(b"foo")
    strat.new_data(stream1)
    strat.new_data(stream2)
    strat.new_data(stream3)
    assert outside_source_stream.data.bytes() == b"foo aaa"


# This one kind of relies on internals? Maybe we should allow querying what
# role a stream has?
@pytest.mark.parametrize("trimming", [None, 3])
def test_quorum_strategy_immediate_stalemate_resolve(trimming,
                                                     outside_source_stream):
    conf = MockStrategyConfig()
    conf.stream_comparison_cutoff = trimming
    strat = QuorumMergeStrategy.build(outside_source_stream, conf)
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
    stream1._add_data(b"f")
    stream2._add_data(b"f")

    strat.new_data(stream1)
    strat.new_data(stream2)

    # Streams 1 and 2 should make a quorum now.
    assert outside_source_stream.data.bytes() == b"f"

    stream1._future_data += b"aaa"
    stream2._future_data += b"bbb"
    stream3._future_data += b"fooaaa"
    stream1._add_data(b"ooaaa")
    stream2._add_data(b"oobbb")
    stream3._add_data(b"fooaaa")

    strat.new_data(stream3)     # This one's not tracked
    assert outside_source_stream.data.bytes() == b"f"
    strat.new_data(stream2)
    # Now we should've entered a stalemate, then resolved it with stream 3.
    assert outside_source_stream.data.bytes() == b"fooaaa"


def test_quorum_strategy_accepts_cutoff(outside_source_stream):
    config = MockStrategyConfig()
    config.stream_comparison_cutoff = 4
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

    # No verification, just check if nothing breaks

    stream1._future_data += b"abcdefg"
    stream1._add_data(b"abcdefg")
    strat.new_data(stream1)
    stream2._future_data += b"abcdefg"
    stream2._add_data(b"abcdefg")
    strat.new_data(stream2)
    assert outside_source_stream.data.bytes() == b"abcdefg"

    stream1._future_data += b"h"
    stream1._add_data(b"h")
    stream2._future_data += b"i"
    stream2._add_data(b"i")
    strat.new_data(stream1)
    strat.new_data(stream2)

    # Check cutoff side effects, just to be sure it's enabled
    stream3._future_data += b"aaadefgh"
    stream3._add_data(b"aaadefgh")
    strat.new_data(stream3)
    assert outside_source_stream.data.bytes() == b"abcdefgh"
