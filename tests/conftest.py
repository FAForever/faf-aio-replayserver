import pytest
import asynctest
from asyncio.locks import Event
from replayserver.stream import ReplayStream
from replayserver.receive.stream import OutsideSourceReplayStream
from tests import TimeSkipper


def mock_connection(reader, writer):
    class C:
        type = None
        uid = None

        async def read_header():
            pass

        async def read():
            pass

        async def write():
            pass

        def close():
            pass

    return asynctest.Mock(spec=C, _reader=reader, _writer=writer)


@pytest.fixture
def mock_unhandled_connections():
    def build(reader, writer):
        return mock_connection(reader, writer)
    return build


@pytest.fixture
def mock_connections(mock_unhandled_connections):
    def build(type_, uid):
        conn = mock_unhandled_connections(None, None)
        conn.type = type_
        conn.uid = uid
        return conn
    return build


@pytest.fixture
def locked_mock_coroutines(event_loop):
    def get():
        manual_end = Event(loop=event_loop)

        async def manual_wait(*args, **kwargs):
            await manual_end.wait()

        ended_wait_mock = asynctest.CoroutineMock(side_effect=manual_wait)
        return (manual_end, ended_wait_mock)

    return get


@pytest.fixture
def mock_replay_streams():
    def build():
        stream = ReplayStream()
        return asynctest.create_autospec(stream)
    return build


@pytest.fixture
def time_skipper(event_loop):
    return TimeSkipper(event_loop)


# Used as a mock stream we can control. Technically we're breaking the unittest
# rule of not involving other units. In practice for testing purposes we'd
# basically have to reimplement OutsideSourceReplayStream, so let's just use it
# - at least it's unit-tested itself.
#
# Yes, OutsideSourceReplayStream can change in the future so it's not useful
# for unit testing anymore. We'll just haul the current implementation here
# when that happens.
@pytest.fixture
def outside_source_stream(event_loop):
    s = OutsideSourceReplayStream()
    m = asynctest.MagicMock(wraps=s)
    # Wrapping does not include magic methods
    m.data.__getitem__.side_effect = lambda v: m._data_slice(v)
    m.data.__len__.side_effect = lambda: m._data_length()
    # Or properties
    type(m).header = asynctest.PropertyMock(
        side_effect=lambda: OutsideSourceReplayStream.header.__get__(s))
    return m
