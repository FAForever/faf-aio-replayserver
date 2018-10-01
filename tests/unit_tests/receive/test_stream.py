import pytest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.server.connection import ConnectionHeader
from replayserver.receive.stream import ConnectionReplayStream, \
    OutsideSourceReplayStream
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_header_reader(mocker):
    class R:
        def done():
            pass

        def send():
            pass

        def result():
            pass

    return mocker.Mock(spec=R)


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_read_header(mock_header_reader, mock_connections):
    mock_conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ipsum ", b"dolor"]
    read_data = b""

    def mock_send(data):
        nonlocal read_data
        read_data += data

    def enough_data():
        return read_data.startswith(b"Lorem ips")

    mock_header_reader.send.side_effect = mock_send
    mock_header_reader.done.side_effect = enough_data
    mock_header_reader.result.return_value = (b"Lorem ips", b"um ")

    await stream.read_header()
    assert mock_conn.read.await_count == 2
    assert stream.header == b"Lorem ips"

    # Replay stream should withhold data until we call read()
    assert stream.data.bytes() == b""
    await stream.read()
    assert stream.data.bytes().startswith(b"um ")


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_invalid_header(
        mock_header_reader, mock_connections):
    mock_conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ipsum ", b"dolor"]
    read_data = b""

    def mock_send(data):
        nonlocal read_data
        read_data += data
        if read_data.startswith(b"Lorem ips"):
            raise ValueError

    mock_header_reader.send.side_effect = mock_send
    mock_header_reader.done.return_value = False

    with pytest.raises(MalformedDataError):
        await stream.read_header()
    assert stream.ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_too_short_header(
        mock_header_reader, mock_connections):
    mock_conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ip", b""]
    read_data = b""

    def mock_send(data):
        nonlocal read_data
        read_data += data

    def enough_data():
        return read_data.startswith(b"Lorem ips")

    mock_header_reader.send.side_effect = mock_send
    mock_header_reader.done.side_effect = enough_data

    with pytest.raises(MalformedDataError):
        await stream.read_header()
    assert stream.ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_read(
        mock_header_reader, mock_connections):
    mock_conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ipsum", b""]
    await stream.read()
    assert stream.data.bytes() == b"Lorem "
    assert not stream.ended()
    await stream.read()
    assert stream.data.bytes() == b"Lorem ipsum"
    assert not stream.ended()
    await stream.read()
    assert stream.data.bytes() == b"Lorem ipsum"
    assert stream.ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_read_header():
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_header())
    header = "header"
    stream.set_header(header)
    got_header = await f
    assert got_header is header


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_read(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data())
    await exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"Lorem")
    await f
    assert stream.data.bytes() == b"Lorem"


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_immediate_feed():
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data())
    stream.feed_data(b"Lorem")
    await f
    assert stream.data.bytes() == b"Lorem"


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_finish():
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data())
    stream.finish()
    await f
    assert stream.ended()
