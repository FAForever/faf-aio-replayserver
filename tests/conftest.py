import pytest
import asynctest
from asyncio.locks import Event
from asyncio.streams import StreamReader
from tests import TimeSkipper

from replayserver.stream import ReplayStream
from replayserver.receive.stream import OutsideSourceReplayStream
from replayserver.errors import MalformedDataError


class ControlledConnection():
    def __init__(self, data, limit):
        self._reader = StreamReader(limit)
        self._reader.feed_data(data)
        self._reader.feed_eof()
        self._mock_write_data = b""

    def get_mock_write_data(self):
        return self._mock_write_data

    async def read(self, size):
        try:
            return await self._reader.read(min(size, 100))
        except Exception:
            raise MalformedDataError

    async def readuntil(self, delim):
        try:
            return await self._reader.readuntil(delim)
        except Exception:
            raise MalformedDataError

    async def readexactly(self, amount):
        try:
            return await self._reader.readexactly(amount)
        except Exception:
            raise MalformedDataError

    async def write(self, data):
        self._mock_write_data += data

    def close(self):
        pass


@pytest.fixture
def mock_connections():
    def build():
        conn = ControlledConnection(b"", 1000000000)
        return asynctest.Mock(spec=conn)

    return build


@pytest.fixture
def controlled_connections():
    def build(data, limit=100000000):
        conn = ControlledConnection(data, limit)
        return asynctest.Mock(wraps=conn, spec=conn)

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
def mock_bookkeeper():
    class C:
        async def save_replay():
            pass

    return asynctest.Mock(spec=C)


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
