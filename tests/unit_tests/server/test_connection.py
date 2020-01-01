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
def conn_with_data(controlled_connections):
    def build(data):
        conn = controlled_connections()
        conn._feed_data(data)
        conn._feed_eof()
        return conn

    return build


@pytest.fixture
def rw_pairs_with_data(event_loop, mock_stream_writers):
    def make(data, *, limit=None, exc=None):
        if limit is not None:
            r = StreamReader(loop=event_loop, limit=limit)
        else:
            r = StreamReader(loop=event_loop)
        w = mock_stream_writers()
        r.feed_data(data)
        if exc is not None:
            r.set_exception(exc)
        else:
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
    # Did we screw up __str__?
    str(connection)
    connection.add_header("foo")
    str(connection)


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
async def test_connection_read_exception(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"", exc=ConnectionError)
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.read(10)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readuntil(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some string\0and some other string")
    connection = Connection(r, w)
    data = await connection.readuntil(b"\0")
    assert data == b"some string\0"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_readuntil_exception(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"bcd", exc=ConnectionError)
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.readuntil(b"a")


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
async def test_connection_readexactly_exception(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"bcd", exc=ConnectionError)
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.readexactly(10)


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
async def test_connection_write_exception(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some_data")
    w.write.side_effect = ConnectionError
    connection = Connection(r, w)
    with pytest.raises(MalformedDataError):
        await connection.write(b"other_data")


@pytest.mark.asyncio
async def test_connection_wait_closed_exceptions(rw_pairs_with_data):
    r, w = rw_pairs_with_data(b"some_data")
    w.wait_closed.side_effect = ConnectionError
    connection = Connection(r, w)
    connection.close()
    await connection.wait_closed()

    r, w = rw_pairs_with_data(b"some_data")
    w.wait_closed.side_effect = TimeoutError
    connection = Connection(r, w)
    connection.close()
    await connection.wait_closed()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_type(conn_with_data):
    mock_conn = conn_with_data(b"P/1/foo\0")
    ch = await ConnectionHeader.read(mock_conn, 60)
    assert ch.type == ConnectionHeader.Type.WRITER

    mock_conn = conn_with_data(b"G/1/foo\0")
    ch = await ConnectionHeader.read(mock_conn, 60)
    assert ch.type == ConnectionHeader.Type.READER


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_invalid_type(conn_with_data):
    mock_conn = conn_with_data(b"garbage/1/foo\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_type_short_data(conn_with_data):
    # FIXME - should be MalformedDataError, but we hacked it for convenience
    mock_conn = conn_with_data(b"G")
    with pytest.raises(EmptyConnectionError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_no_data(conn_with_data):
    mock_conn = conn_with_data(b"")
    with pytest.raises(EmptyConnectionError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info(conn_with_data):
    mock_conn = conn_with_data(b"G/1/foo\0")
    ch = await ConnectionHeader.read(mock_conn, 60)
    assert ch.game_id == 1
    assert ch.game_name == "foo"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_uid_not_int(conn_with_data):
    mock_conn = conn_with_data(b"G/bar/foo\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info_no_null_end(
        conn_with_data):
    mock_conn = conn_with_data(b"G/1/foo")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info_no_delimiter(
        conn_with_data):
    mock_conn = conn_with_data(b"G/1\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_header_replay_info_more_slashes(
        conn_with_data):
    mock_conn = conn_with_data(b"G/1/name/with/slash\0")
    ch = await ConnectionHeader.read(mock_conn, 60)
    assert ch.game_id == 1
    assert ch.game_name == "name/with/slash"


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_invalid_unicode(conn_with_data):
    # Lonely start character is invalid unicode
    mock_conn = conn_with_data(b"G/1/\xc0 blahblah\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_limit_overrun(controlled_connections):
    mock_conn = controlled_connections(8)
    mock_conn._feed_data(b"G/1/All Welcome 115k+\0")
    mock_conn._feed_eof()
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_replay_info_negative_id(conn_with_data):
    mock_conn = conn_with_data(b"G/-1/foo\0")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, 60)


@fast_forward_time(20)
@pytest.mark.asyncio
@timeout(10)
async def test_connection_header_read_times_out(controlled_connections):
    mock_conn = controlled_connections()
    mock_conn._feed_data(b"G/-1/fo")
    with pytest.raises(MalformedDataError):
        await ConnectionHeader.read(mock_conn, timeout=5)
