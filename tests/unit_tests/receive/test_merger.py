import pytest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.receive.merger import Merger
from replayserver.server.connection import Connection
from replayserver.errors import CannotAcceptConnectionError, \
    BadConnectionError


@pytest.fixture
def mock_merge_strategy(mocker):
    class S:
        def stream_added():
            pass

        def stream_removed():
            pass

        def new_header():
            pass

        def new_data():
            pass

        def finalize():
            pass

    return mocker.Mock(spec=S)


@pytest.fixture
def mock_connection_streams(mock_replay_streams):
    class CS:
        async def read_header():
            pass

        async def read():
            pass

    def build():
        stream = mock_replay_streams()
        stream.mock_add_spec(CS)
        return stream

    return build


@pytest.fixture
def mock_stream_builder(mocker):
    return mocker.Mock(spec=[])


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_times_out_after_creation(mock_outside_source_stream,
                                               mock_merge_strategy,
                                               mock_stream_builder):
    canonical_stream = mock_outside_source_stream
    merger = Merger(mock_stream_builder, 0.01, mock_merge_strategy,
                    canonical_stream)
    await merger.wait_for_ended()
    canonical_stream.finish.assert_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_rejects_writers_after_ending(mock_outside_source_stream,
                                                   mock_merge_strategy,
                                                   mock_stream_builder,
                                                   mock_connections):
    connection = mock_connections(None, None)
    connection.type = Connection.Type.WRITER
    connection.uid = 1
    canonical_stream = mock_outside_source_stream

    merger = Merger(mock_stream_builder, 0.01, mock_merge_strategy,
                    canonical_stream)
    await merger.wait_for_ended()
    with pytest.raises(CannotAcceptConnectionError):
        await merger.handle_connection(connection)
    mock_stream_builder.assert_not_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_one_connection_lifetime(mock_outside_source_stream,
                                              mock_merge_strategy,
                                              mock_stream_builder,
                                              mock_connection_streams,
                                              mock_connections):
    connection = mock_connections(None, None)
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = mock_outside_source_stream

    conn_data = b""

    async def mock_read():
        nonlocal conn_data
        conn_data = b"foo"

    def mock_ended():
        return conn_data == b"foo"

    connection_stream.read_header.return_value = "Header"
    connection_stream.read.side_effect = mock_read
    connection_stream.ended.side_effect = mock_ended

    merger = Merger(mock_stream_builder, 0.01, mock_merge_strategy,
                    canonical_stream)
    await merger.handle_connection(connection)

    connection_stream.read_header.assert_called()
    connection_stream.read.assert_called()

    mock_merge_strategy.stream_added.assert_called_with(connection_stream)
    mock_merge_strategy.stream_removed.assert_called_with(connection_stream)
    mock_merge_strategy.new_header.assert_called_with(connection_stream)
    mock_merge_strategy.new_data.assert_called_with(connection_stream)

    await asyncio.sleep(0.02)
    mock_merge_strategy.finalize.assert_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_read_header_exception(mock_outside_source_stream,
                                            mock_merge_strategy,
                                            mock_stream_builder,
                                            mock_connection_streams,
                                            mock_connections):
    connection = mock_connections(None, None)
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = mock_outside_source_stream

    connection_stream.read_header.side_effect = BadConnectionError

    merger = Merger(mock_stream_builder, 0.01, mock_merge_strategy,
                    canonical_stream)
    with pytest.raises(BadConnectionError):
        await merger.handle_connection(connection)

    mock_merge_strategy.stream_added.assert_called_with(connection_stream)
    mock_merge_strategy.stream_removed.assert_called_with(connection_stream)
    mock_merge_strategy.new_header.assert_not_called()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_read_data_exception(mock_outside_source_stream,
                                          mock_merge_strategy,
                                          mock_stream_builder,
                                          mock_connection_streams,
                                          mock_connections):
    connection = mock_connections(None, None)
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = mock_outside_source_stream

    connection_stream.read_header.return_value = "Header"
    connection_stream.read.side_effect = BadConnectionError
    connection_stream.ended.return_value = False

    merger = Merger(mock_stream_builder, 0.01, mock_merge_strategy,
                    canonical_stream)
    with pytest.raises(BadConnectionError):
        await merger.handle_connection(connection)

    mock_merge_strategy.stream_added.assert_called_with(connection_stream)
    mock_merge_strategy.stream_removed.assert_called_with(connection_stream)
    mock_merge_strategy.new_header.assert_called()
    mock_merge_strategy.new_data.assert_not_called()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_connection_extends_grace_period(
        mock_outside_source_stream, mock_merge_strategy, mock_stream_builder,
        mock_connection_streams, mock_connections, event_loop):
    connection = mock_connections(None, None)
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = mock_outside_source_stream

    connection_stream.ended.return_value = True

    merger = Merger(mock_stream_builder, 0.03, mock_merge_strategy,
                    canonical_stream)
    f = asyncio.ensure_future(merger.wait_for_ended())
    await asyncio.sleep(0.02)
    await merger.handle_connection(connection)
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert not f.done()
    await f


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_active_connection_prevents_ending(
        mock_outside_source_stream, mock_merge_strategy, mock_stream_builder,
        mock_connection_streams, mock_connections, event_loop):
    connection = mock_connections(None, None)
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = mock_outside_source_stream

    async def long_read():
        await asyncio.sleep(0.05)
        return b""

    connection_stream.read.side_effect = long_read
    connection_stream.ended.side_effect = lambda: connection_stream.read.called

    merger = Merger(mock_stream_builder, 0.01, mock_merge_strategy,
                    canonical_stream)
    f = asyncio.ensure_future(merger.wait_for_ended())
    h = asyncio.ensure_future(merger.handle_connection(connection))
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert not f.done()
    await h
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert f.done()
