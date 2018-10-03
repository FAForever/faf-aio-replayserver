import pytest
import asynctest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.receive.stream import ConnectionReplayStream, \
    OutsideSourceReplayStream
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_header_read():
    return asynctest.CoroutineMock(spec=[])


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_read_header(mock_header_read,
                                         controlled_connections):
    mock_conn = controlled_connections(b"Lorem ipsum")
    stream = ConnectionReplayStream(mock_header_read, mock_conn)

    mock_header_read.return_value = "Header", b"Leftover"

    await stream.read_header()
    assert stream.header == "Header"

    # Replay stream should withhold data until we call read()
    assert stream.data.bytes() == b""
    await stream.read()
    assert stream.data.bytes().startswith(b"Leftover")


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_invalid_header(
        mock_header_read, mock_connections):
    mock_conn = mock_connections()
    stream = ConnectionReplayStream(mock_header_read, mock_conn)
    mock_header_read.side_effect = MalformedDataError
    with pytest.raises(MalformedDataError):
        await stream.read_header()
    assert stream.ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_read(
        mock_header_read, mock_connections):
    mock_conn = mock_connections()
    stream = ConnectionReplayStream(mock_header_read, mock_conn)

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
