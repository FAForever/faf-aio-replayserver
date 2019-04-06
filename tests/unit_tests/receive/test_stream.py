import pytest
import asynctest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.receive.stream import ReplayStreamReader
from replayserver.stream import OutsideSourceReplayStream
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_header_read():
    return asynctest.CoroutineMock(spec=[])


class TestDemangler:
    def demangle(self, data):
        return data

    def drain(self):
        return b""


@pytest.fixture
def id_demangler():
    return TestDemangler()


@pytest.fixture
def mock_demangler(mocker):
    return mocker.Mock(spec=TestDemangler)


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_immediate_end(event_loop):
    stream = OutsideSourceReplayStream()
    f1 = asyncio.ensure_future(stream.wait_for_header())
    f2 = asyncio.ensure_future(stream.wait_for_data(0))
    f3 = asyncio.ensure_future(stream.wait_for_ended())
    exhaust_callbacks(event_loop)
    assert not any(x.done() for x in [f1, f2, f3])
    stream.finish()

    assert await f1 is None
    assert await f2 == b""
    assert stream.header is None
    assert stream.data.bytes() == b""
    await f3


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_read_header(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_header())
    exhaust_callbacks(event_loop)
    assert not f.done()
    stream.set_header("header")
    assert await f == "header"
    assert stream.header == "header"

    stream.finish()
    await stream.wait_for_ended()
    assert stream.data.bytes() == b""


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_read(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data(0))
    stream.set_header("header")
    await exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"Lorem")
    assert await f == b"Lorem"
    assert stream.data.bytes() == b"Lorem"

    stream.finish()
    await stream.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_immediate_data():
    stream = OutsideSourceReplayStream()
    f1 = asyncio.ensure_future(stream.wait_for_header())
    f2 = asyncio.ensure_future(stream.wait_for_data(0))
    stream.set_header("header")
    stream.feed_data(b"Lorem")
    assert await f1 == "header"
    assert await f2 == b"Lorem"
    assert stream.header == "header"
    assert stream.data.bytes() == b"Lorem"


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_finish():
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data(0))
    stream.finish()
    await f
    assert stream.ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_wait_until_position(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data(3))
    stream.set_header("header")
    stream.feed_data(b"a")
    exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"aa")
    exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"ccc")
    assert await f == b"ccc"


# We're using OutsideSourceStream here, but who cares, its mock would look
# exactly the same
@pytest.mark.asyncio
@timeout(1)
async def test_reader_normal_read(mock_header_read,
                                  id_demangler,
                                  mock_connections,
                                  outside_source_stream):
    mock_conn = mock_connections()
    reader = ReplayStreamReader(mock_header_read, id_demangler,
                                outside_source_stream, mock_conn)

    mock_header_read.return_value = "Header", b"Leftover"
    mock_conn.read.side_effect = [b"Lorem ", b"ipsum", b""]

    f = asyncio.ensure_future(reader.read())
    assert (await outside_source_stream.wait_for_header()) == "Header"
    await outside_source_stream.wait_for_ended()
    assert outside_source_stream.data.bytes() == b"LeftoverLorem ipsum"
    await f


@pytest.mark.asyncio
@timeout(1)
async def test_reader_uses_demangler(mock_header_read, mock_demangler,
                                     mock_connections, outside_source_stream):
    mock_conn = mock_connections()
    mock_demangler.demangle.side_effect = [b"aaa", b"bbb"]
    mock_demangler.drain.return_value = b"ccc"
    mock_header_read.return_value = "Header", b""
    mock_conn.read.side_effect = [b"Lorem ", b"ipsum", b""]
    reader = ReplayStreamReader(mock_header_read, mock_demangler,
                                outside_source_stream, mock_conn)
    await reader.read()
    assert outside_source_stream.data.bytes() == b"aaabbbccc"


@pytest.mark.asyncio
@timeout(1)
async def test_reader_invalid_header(mock_header_read, outside_source_stream,
                                     id_demangler, mock_connections):
    mock_conn = mock_connections()
    reader = ReplayStreamReader(mock_header_read, id_demangler,
                                outside_source_stream, mock_conn)
    mock_header_read.side_effect = MalformedDataError
    with pytest.raises(MalformedDataError):
        await reader.read()
    assert outside_source_stream.ended()
    assert outside_source_stream.header is None


@pytest.mark.asyncio
@timeout(1)
async def test_reader_recovers_from_connection_error(
        mock_header_read, outside_source_stream,
        id_demangler, mock_connections):
    mock_conn = mock_connections()
    reader = ReplayStreamReader(mock_header_read, id_demangler,
                                outside_source_stream, mock_conn)
    mock_conn.read.side_effect = [b"Lorem ", MalformedDataError, b"ipsum"]
    mock_header_read.return_value = "Header", b""
    await reader.read()
    assert outside_source_stream.data.bytes() == b"Lorem "
    assert outside_source_stream.ended()
