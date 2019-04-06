import asyncio
import pytest
import asynctest
from asyncio.streams import StreamReader
from replayserver.errors import MalformedDataError


__all__ = ["mock_connections", "controlled_connections"]


class ControlledConnection:
    def __init__(self, limit=100000000):
        self._reader = StreamReader(limit)
        self._mock_write_data = b""
        self._closed = False

    def _feed_data(self, data):
        self._reader.feed_data(data)

    def _feed_eof(self):
        self._reader.feed_eof()

    def _get_mock_write_data(self):
        return self._mock_write_data

    def _check_eof(self):
        if self._reader.at_eof():
            self._closed = True

    async def read(self, size):
        self._check_eof()
        try:
            return await self._reader.read(min(size, 100))
        except Exception:
            self._closed = True
            raise MalformedDataError

    async def readuntil(self, delim):
        self._check_eof()
        try:
            return await self._reader.readuntil(delim)
        except Exception:
            self._closed = True
            raise MalformedDataError

    async def readexactly(self, amount):
        self._check_eof()
        try:
            return await self._reader.readexactly(amount)
        except Exception:
            self._closed = True
            raise MalformedDataError

    async def write(self, data):
        self._mock_write_data += data
        return not self._closed

    async def add_header(self, header):
        pass

    def close(self):
        self._closed = True

    async def wait_closed(self):
        while not self._closed:
            await asyncio.sleep(1)


@pytest.fixture
def mock_connections():
    def build():
        conn = ControlledConnection(1000000000)
        return asynctest.Mock(spec=conn)

    return build


@pytest.fixture
def controlled_connections():
    def build(limit=100000000):
        conn = ControlledConnection(limit)
        return asynctest.Mock(wraps=conn, spec=conn)

    return build


@pytest.fixture
def mock_connection_headers():
    def build(type_, game_id):
        return asynctest.Mock(type=type_, game_id=game_id, name="")

    return build
