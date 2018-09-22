import pytest

from replayserver.receive.mergestrategy import GreedyMergeStrategy
from replayserver.stream import ReplayStream, ConcreteDataMixin


# Technically we shouldn't rely on abstract classes being good, and should use
# mocks, yadda yadda. This way is way less bothersome.
class MockStream(ConcreteDataMixin, ReplayStream):
    def __init__(self):
        ConcreteDataMixin.__init__(self)
        ReplayStream.__init__(self)
        self._ended = False

    def ended(self):
        return self._ended


class MockOutsideSourceStream(ConcreteDataMixin, ReplayStream):
    def __init__(self):
        ConcreteDataMixin.__init__(self)
        ReplayStream.__init__(self)
        self._ended = False

    def set_header(self, header):
        self._header = header

    def feed_data(self, data):
        self._data += data

    def finish(self):
        self._ended = True

    def ended(self):
        return self._ended


@pytest.fixture
def mock_sink_stream():
    return MockOutsideSourceStream()


general_test_strats = [GreedyMergeStrategy]


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_ends_stream_when_finalized(strategy, mock_sink_stream):
    strat = strategy(mock_sink_stream)
    stream1 = MockStream()
    strat.stream_added(stream1)
    strat.stream_removed(stream1)
    strat.finalize()
    assert mock_sink_stream.ended()


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_picks_at_least_one_header(strategy, mock_sink_stream):
    strat = strategy(mock_sink_stream)
    stream1 = MockStream()
    stream2 = MockStream()
    stream2._header = "Header"

    strat.stream_added(stream2)
    strat.new_header(stream2)
    strat.stream_added(stream1)
    strat.stream_removed(stream2)
    strat.stream_removed(stream1)
    strat.finalize()
    assert mock_sink_stream._header == "Header"


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_gets_all_data_of_one(strategy, mock_sink_stream):
    strat = strategy(mock_sink_stream)
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

    assert mock_sink_stream._header == "Header"
    assert mock_sink_stream._data == b"Best friends"


@pytest.mark.parametrize("strategy", general_test_strats)
def test_strategy_gets_common_prefix_of_all(strategy, mock_sink_stream):
    strat = strategy(mock_sink_stream)
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
    assert mock_sink_stream._data.startswith(b"Best ")
