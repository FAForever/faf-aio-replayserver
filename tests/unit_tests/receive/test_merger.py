import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.receive.merger import Merger, ReplayStreamReader
from replayserver.errors import CannotAcceptConnectionError, \
    BadConnectionError, MalformedDataError


@pytest.fixture
def mock_header_read():
    return asynctest.CoroutineMock(spec=[])


@pytest.fixture
def mock_merge_strategy(mocker):
    # A little unspoken assumption that merger only uses these methods.
    class S:
        async def track_stream():
            pass

        def finalize():
            pass

    return asynctest.Mock(spec=S)


@pytest.fixture
def mock_readers():
    class R:
        stream = None

        async def read():
            pass

    def build():
        return asynctest.Mock(spec=R)

    return build


@pytest.fixture
def mock_reader_builder(mocker):
    return mocker.Mock(spec=[])


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
async def test_reader_invalid_header(mock_header_read, outside_source_stream,
                                     mock_connections):
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


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_ends_when_refusing_conns_and_no_connections(
        outside_source_stream, mock_merge_strategy, mock_reader_builder):
    canonical_stream = outside_source_stream
    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    merger.stop_accepting_connections()
    await merger.wait_for_ended()
    mock_merge_strategy.finalize.assert_called()
    canonical_stream.finish.assert_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_rejects_writers_when_asked(outside_source_stream,
                                                 mock_merge_strategy,
                                                 mock_reader_builder,
                                                 mock_connections):
    connection = mock_connections()
    canonical_stream = outside_source_stream

    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    merger.stop_accepting_connections()
    with pytest.raises(CannotAcceptConnectionError):
        await merger.handle_connection(connection)
    mock_reader_builder.assert_not_called()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_one_connection_lifetime(outside_source_stream,
                                              mock_merge_strategy,
                                              mock_reader_builder,
                                              mock_readers,
                                              mock_connections):
    connection = mock_connections()
    reader = mock_readers()
    mock_reader_builder.side_effect = [reader]
    canonical_stream = outside_source_stream

    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    await merger.handle_connection(connection)

    reader.read.assert_awaited()
    mock_merge_strategy.track_stream.assert_awaited_with(reader.stream)

    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_read_exception(outside_source_stream,
                                     mock_merge_strategy,
                                     mock_reader_builder,
                                     mock_readers,
                                     mock_connections):
    connection = mock_connections()
    canonical_stream = outside_source_stream
    reader = mock_readers()
    mock_reader_builder.side_effect = [reader]
    reader.read.side_effect = BadConnectionError

    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    with pytest.raises(BadConnectionError):
        await merger.handle_connection(connection)

    mock_merge_strategy.track_stream.assert_awaited_with(reader.stream)
    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_no_connections_wait_empty(
        outside_source_stream, mock_merge_strategy, mock_reader_builder,
        event_loop):
    canonical_stream = outside_source_stream
    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    f = asyncio.ensure_future(merger.no_connections_for(0.01))
    exhaust_callbacks(event_loop)
    assert not f.done()
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert f.done()
    await f

    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_no_connection_wait_extends(
        outside_source_stream, mock_merge_strategy, mock_reader_builder,
        mock_readers, mock_connections, event_loop):
    connection = mock_connections()
    reader = mock_readers()
    mock_reader_builder.side_effect = [reader]
    canonical_stream = outside_source_stream

    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    f = asyncio.ensure_future(merger.no_connections_for(0.03))
    await asyncio.sleep(0.02)
    assert not f.done()
    await merger.handle_connection(connection)
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert not f.done()
    await f

    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_no_connection_wait_active_connection(
        outside_source_stream, mock_merge_strategy, mock_reader_builder,
        mock_readers, mock_connections, event_loop):
    connection = mock_connections()
    reader = mock_readers()
    mock_reader_builder.side_effect = [reader]
    canonical_stream = outside_source_stream

    async def long_read():
        await asyncio.sleep(0.05)
        return

    reader.read.side_effect = long_read

    merger = Merger(mock_reader_builder, mock_merge_strategy, canonical_stream)
    f = asyncio.ensure_future(merger.no_connections_for(0.01))
    h = asyncio.ensure_future(merger.handle_connection(connection))

    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert not f.done()
    await h
    exhaust_callbacks(event_loop)
    assert not f.done()
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert f.done()
    await f

    merger.stop_accepting_connections()
    await merger.wait_for_ended()
