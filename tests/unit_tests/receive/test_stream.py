import pytest
import asynctest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.receive.stream import ReplayStreamReader, \
    OutsideSourceReplayStream
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_header_read():
    return asynctest.CoroutineMock(spec=[])


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


# We're using OutsideSourceStream here, but who cares, its mock would look
# exactly the same
@pytest.mark.asyncio
@timeout(1)
async def test_reader_normal_read(mock_header_read,
                                  mock_connections,
                                  outside_source_stream):
    mock_conn = mock_connections()
    reader = ReplayStreamReader(mock_header_read, outside_source_stream,
                                mock_conn)

    mock_header_read.return_value = "Header", b"Leftover"
    mock_conn.read.side_effect = [b"Lorem ", b"ipsum", b""]

    f = asyncio.ensure_future(reader.read())
    assert (await outside_source_stream.wait_for_header()) == "Header"
    await outside_source_stream.wait_for_ended()
    assert outside_source_stream.data.bytes() == b"LeftoverLorem ipsum"
    await f


@pytest.mark.asyncio
@timeout(1)
async def test_reader_invalid_header(
        mock_header_read, outside_source_stream, mock_connections):
    mock_conn = mock_connections()
    reader = ReplayStreamReader(mock_header_read, outside_source_stream,
                                mock_conn)
    mock_header_read.side_effect = MalformedDataError
    with pytest.raises(MalformedDataError):
        await reader.read()
    assert outside_source_stream.ended()
    assert outside_source_stream.header is None


@pytest.mark.asyncio
@timeout(1)
async def test_reader_recovers_from_connection_error(
        mock_header_read, outside_source_stream, mock_connections):
    mock_conn = mock_connections()
    reader = ReplayStreamReader(mock_header_read, outside_source_stream,
                                mock_conn)
    mock_conn.read.side_effect = [b"Lorem ", MalformedDataError, b"ipsum"]
    mock_header_read.return_value = "Header", b""
    await reader.read()
    assert outside_source_stream.data.bytes() == b"Lorem "
    assert outside_source_stream.ended()
