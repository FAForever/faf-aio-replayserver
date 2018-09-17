import pytest
import asynctest
from asyncio.streams import StreamReader, StreamWriter

from tests import timeout
from replayserver.server.connection import Connection


@pytest.fixture
def mock_stream_writers():
    return lambda: asynctest.Mock(spec=StreamWriter)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_init(event_loop, mock_stream_writers):
    r = StreamReader(loop=event_loop)
    w = mock_stream_writers()
    connection = Connection(r, w)
    assert connection.reader is r
    assert connection.writer is w
