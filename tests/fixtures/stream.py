import pytest
import asynctest

from replayserver.stream import ReplayStream, OutsideSourceReplayStream


@pytest.fixture
def mock_replay_streams():
    def build():
        stream = ReplayStream()
        return asynctest.create_autospec(stream)
    return build


# Used as a mock stream we can control. Technically we're breaking the unittest
# rule of not involving other units. In practice for testing purposes we'd
# basically have to reimplement OutsideSourceReplayStream, so let's just use it
# - at least it's unit-tested itself.
@pytest.fixture
def outside_source_stream(event_loop):
    # We use event_loop fixture so that stream uses the same loop as all tests
    s = OutsideSourceReplayStream()
    m = asynctest.MagicMock(wraps=s)
    # Wrapping does not include magic methods
    m.data.__getitem__.side_effect = lambda v: m._data_slice(v)
    m.data.__len__.side_effect = lambda: m._data_length()
    # Or properties
    type(m).header = asynctest.PropertyMock(
        side_effect=lambda: OutsideSourceReplayStream.header.__get__(s))
    return m
