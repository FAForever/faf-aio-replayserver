import pytest
import asynctest
from asyncio.streams import StreamReader, StreamWriter

from tests import timeout, fast_forward_time
from replayserver.server.connection import Connection, ConnectionHeader
from replayserver.errors import MalformedDataError, EmptyConnectionError


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
async def test_connection_read(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"foo")
    connection = Connection(r, w)
    data = await connection.read(10)
    assert data == b"foo"
    data = await connection.read(10)
    assert data == b""


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readuntil(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some string\0and some other string")
    connection = Connection(r, w)
    data = await connection.readuntil(b"\0")
    assert data == b"some string\0"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readuntil_no_delim(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some unterminated string")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.readuntil(b"\0")


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readuntil_over_limit(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some terminated string\0", limit=10)
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.readuntil(b"\0")


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readexactly(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"something long")
    connection = Connection(r, w)
    data = await connection.readexactly(9)
    assert data == b"something"
    data = await connection.readexactly(3)
    assert data == b" lo"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readexactly_too_big(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"something")
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.readexactly(32)


# TODO - test connection closing with real readers / writers


@pytest.mark.asyncio
@timeout(1)
async def test_connection_rw(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some_data")
    connection = Connection(r, w)

    data = await connection.read(4096)
    assert data == b"some_data"

    await connection.write(b"other_data")
    w.write.assert_called_with(b"other_data")


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_type(controlled_connections):
    mock_conn = controlled_connections(b"P/1/foo\0")
    ch = await ConnectionHeader.read(mock_conn)
    assert ch.type == ConnectionHeader.Type.WRITER

    mock_conn = controlled_connections(b"G/1/foo\0")
    ch = await ConnectionHeader.read(mock_conn)
    assert ch.type == ConnectionHeader.Type.READER


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_invalid_type(controlled_connections):
    mock_conn = controlled_connections(b"garbage/1/foo\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_type_short_data(controlled_connections):
    # FIXME - should be MalformedDataError, but we hacked it for convenience
    mock_conn = controlled_connections(b"g")
    with pytest.raises(EmptyConnectionError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_no_data(controlled_connections):
    mock_conn = controlled_connections(b"g")
    with pytest.raises(EmptyConnectionError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info(controlled_connections):
    mock_conn = controlled_connections(b"G/1/foo\0")
    ch = await ConnectionHeader.read(mock_conn)
    assert ch.game_id == 1
    assert ch.game_name == "foo"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_uid_not_int(controlled_connections):
    mock_conn = controlled_connections(b"G/bar/foo\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info_no_null_end(
        controlled_connections):
    mock_conn = controlled_connections(b"G/1/foo")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info_no_delimiter(
        controlled_connections):
    mock_conn = controlled_connections(b"G/1\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info_more_slashes(
        controlled_connections):
    mock_conn = controlled_connections(b"G/1/name/with/slash\0")
    ch = await ConnectionHeader.read(mock_conn)
    assert ch.game_id == 1
    assert ch.game_name == "name/with/slash"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_invalid_unicode(controlled_connections):
    # Lonely start character is invalid unicode
    mock_conn = controlled_connections(b"G/1/\xc0 blahblah\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_limit_overrun(controlled_connections):
    mock_conn = controlled_connections(b"G/1/All Welcome 115k+\0", 8)
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_negative_id(controlled_connections):
    mock_conn = controlled_connections(b"G/-1/foo\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn)


@fast_forward_time(1, 20)
@pytest.mark.asyncio
@timeout(10)
async def test_connection_header_read_times_out(event_loop,
                                                controlled_connections):
    mock_conn = controlled_connections(b"G/-1/fo", leave_open=True)
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, timeout=5)
