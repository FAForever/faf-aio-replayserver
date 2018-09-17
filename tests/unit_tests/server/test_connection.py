import pytest
import asynctest
from asyncio.streams import StreamReader, StreamWriter

from tests import timeout
from replayserver.server.connection import Connection
from replayserver.errors import MalformedDataError


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


@pytest.mark.asyncio
@timeout(1)
async def test_connection_type(event_loop, mock_stream_writers):
    r = StreamReader(loop=event_loop)
    w = mock_stream_writers()
    connection = Connection(r, w)
    r.feed_data(b"P/1/foo\0")
    await connection.read_header()
    assert connection.type == Connection.Type.WRITER

    r = StreamReader(loop=event_loop)
    w = mock_stream_writers()
    connection = Connection(r, w)
    r.feed_data(b"G/1/foo\0")
    await connection.read_header()
    assert connection.type == Connection.Type.READER


@pytest.mark.asyncio
@timeout(1)
async def test_connection_invalid_type(event_loop, mock_stream_writers):
    r = StreamReader(loop=event_loop)
    w = mock_stream_writers()
    connection = Connection(r, w)
    r.feed_data(b"garbage/1/foo\0")
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_type_short_data(event_loop, mock_stream_writers):
    r = StreamReader(loop=event_loop)
    w = mock_stream_writers()
    connection = Connection(r, w)
    r.feed_data(b"g")
    r.feed_eof()
    with pytest.raises(MalformedDataError):
        await connection.read_header()
