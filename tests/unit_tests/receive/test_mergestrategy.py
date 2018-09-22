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


@pytest.mark.parametrize("strategy", [GreedyMergeStrategy])
def test_strategy_ends_stream_when_finalized(strategy, mock_sink_stream):
    strat = strategy(mock_sink_stream)
    strat.finalize()
    assert mock_sink_stream.ended()
