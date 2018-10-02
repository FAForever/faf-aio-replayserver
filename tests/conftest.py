import pytest
import asynctest
from asyncio.locks import Event
from tests import TimeSkipper

from replayserver.stream import ReplayStream
from replayserver.receive.stream import OutsideSourceReplayStream
from replayserver.errors import MalformedDataError


class MockConnection():
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self._mock_read_data = b""
        self._position = 0
        self._mock_write_data = b""

    def _get_mock_data(self, to):
        to = min(to, self._position)
        data = self._mock_read_data[self._position:to]
        self._position = to
        return data

    async def read(self, size):
        newpos = self._position + min(size, 100)
        return self._get_mock_data(newpos)

    async def readuntil(self, delim):
        newpos = self._mock_read_data.find(delim, self._position)
        if newpos == -1:
            raise MalformedDataError
        return self._get_mock_data(newpos)

    async def readexactly(self, amount):
        newpos = self._position + amount
        if newpos > len(self._mock_read_data):
            raise MalformedDataError
        return self._get_mock_data(newpos)

    async def write(self, data):
        self._mock_write_data += data

    def close(self):
        pass


def mock_connection(reader, writer):
    conn = MockConnection(reader, writer)
    return asynctest.Mock(wraps=conn)


@pytest.fixture
def mock_connections():
    def build(type_, uid):
        conn = mock_connection(None, None)
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
