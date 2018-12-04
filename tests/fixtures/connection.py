import pytest
import asynctest
from asyncio.streams import StreamReader
from replayserver.errors import MalformedDataError


__all__ = ["mock_connections", "controlled_connections"]


class ControlledConnection:
    def __init__(self, data, limit, leave_open=False):
        self._reader = StreamReader(limit)
        self._reader.feed_data(data)
        if not leave_open:
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

    async def add_header(self, header):
        pass

    async def close(self):
        pass


@pytest.fixture
def mock_connections():
    def build():
        conn = ControlledConnection(b"", 1000000000)
        return asynctest.Mock(spec=conn)

    return build


@pytest.fixture
def controlled_connections():
    def build(data, limit=100000000, leave_open=False):
        conn = ControlledConnection(data, limit, leave_open)
        return asynctest.Mock(wraps=conn, spec=conn)

    return build
