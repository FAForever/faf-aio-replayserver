import pytest
import asynctest
from asyncio.streams import StreamReader, StreamWriter

from tests import timeout
from replayserver.server.connection import Connection
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_stream_writers():
    return lambda: asynctest.Mock(spec=StreamWriter)


@pytest.fixture
def rw_pairs_with_data(event_loop, mock_stream_writers):
    def make(data, *, limit=None):
        if limit is not None:
            r = StreamReader(loop=event_loop, limit=limit)
        else:
            r = StreamReader(loop=event_loop)
        w = mock_stream_writers()
        r.feed_data(data)
        r.feed_eof()
        return r, w
    return make


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
async def test_connection_type(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"P/1/foo\0")
    connection = Connection(r, w)
    await connection.read_header()
    assert connection.type == Connection.Type.WRITER

    r, w = rw_pairs_with_data(b"G/1/foo\0")
    connection = Connection(r, w)
    await connection.read_header()
    assert connection.type == Connection.Type.READER


@pytest.mark.asyncio
@timeout(1)
async def test_connection_invalid_type(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"garbage/1/foo\0")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_type_short_data(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"g")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/1/foo\0")
    connection = Connection(r, w)
    await connection.read_header()
    assert connection.uid == 1
    assert connection.name == "foo"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_uid_not_int(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/bar/foo\0")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_no_null_end(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/bar/foo")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_no_delimiter(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/1\0")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_more_slashes(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/1/name/with/slash\0")
    connection = Connection(r, w)
    await connection.read_header()
    assert connection.uid == 1
    assert connection.name == "name/with/slash"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_invalid_unicode(rw_pairs_with_data):
    # Lonely start character is invalid unicode
    r, w = rw_pairs_with_data(b"G/1/\xc0 blahblah\0")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_limit_overrun(rw_pairs_with_data):
    # Lonely start character is invalid unicode
    r, w = rw_pairs_with_data(b"G/1/All Welcome 115k+\0", limit=8)
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_closes(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/1/foo\0")
    connection = Connection(r, w)
    connection.close()
    w.transport.abort.assert_called()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_rw(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"G/1/foo\0some_data")
    connection = Connection(r, w)
    await connection.read_header()

    data = await connection.read(4096)
    assert data == b"some_data"

    await connection.write(b"other_data")
    w.write.assert_called_with(b"other_data")
