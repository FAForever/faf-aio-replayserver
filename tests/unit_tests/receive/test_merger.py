import pytest
import asyncio
from tests import timeout, skip_if_needs_asynctest_107
from asynctest.helpers import exhaust_callbacks

from replayserver.receive.merger import Merger
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
async def test_merger_ends_when_refusing_conns_and_no_connections(
        outside_source_stream, mock_merge_strategy, mock_stream_builder):
    canonical_stream = outside_source_stream
    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
    merger.stop_accepting_connections()
    await merger.wait_for_ended()
    mock_merge_strategy.finalize.assert_called()
    canonical_stream.finish.assert_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_rejects_writers_when_asked(outside_source_stream,
                                                 mock_merge_strategy,
                                                 mock_stream_builder,
                                                 mock_connections):
    connection = mock_connections()
    canonical_stream = outside_source_stream

    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
    merger.stop_accepting_connections()
    with pytest.raises(CannotAcceptConnectionError):
        await merger.handle_connection(connection)
    mock_stream_builder.assert_not_called()
    await merger.wait_for_ended()


@skip_if_needs_asynctest_107
@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_one_connection_lifetime(outside_source_stream,
                                              mock_merge_strategy,
                                              mock_stream_builder,
                                              mock_connection_streams,
                                              mock_connections):
    connection = mock_connections()
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = outside_source_stream

    conn_data = b""

    async def mock_read():
        nonlocal conn_data
        conn_data = b"foo"

    def mock_ended():
        return conn_data == b"foo"

    connection_stream.read_header.return_value = "Header"
    connection_stream.read.side_effect = mock_read
    connection_stream.ended.side_effect = mock_ended

    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
    await merger.handle_connection(connection)

    connection_stream.read_header.assert_called()
    connection_stream.read.assert_called()

    mock_merge_strategy.stream_added.assert_called_with(connection_stream)
    mock_merge_strategy.stream_removed.assert_called_with(connection_stream)
    mock_merge_strategy.new_header.assert_called_with(connection_stream)
    mock_merge_strategy.new_data.assert_called_with(connection_stream)

    merger.stop_accepting_connections()
    await merger.wait_for_ended()
    mock_merge_strategy.finalize.assert_called()


@skip_if_needs_asynctest_107
@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_read_header_exception(outside_source_stream,
                                            mock_merge_strategy,
                                            mock_stream_builder,
                                            mock_connection_streams,
                                            mock_connections):
    connection = mock_connections()
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = outside_source_stream

    connection_stream.read_header.side_effect = BadConnectionError

    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
    with pytest.raises(BadConnectionError):
        await merger.handle_connection(connection)

    mock_merge_strategy.stream_added.assert_called_with(connection_stream)
    mock_merge_strategy.stream_removed.assert_called_with(connection_stream)
    mock_merge_strategy.new_header.assert_not_called()
    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@skip_if_needs_asynctest_107
@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_read_data_exception(outside_source_stream,
                                          mock_merge_strategy,
                                          mock_stream_builder,
                                          mock_connection_streams,
                                          mock_connections):
    connection = mock_connections()
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = outside_source_stream

    connection_stream.read_header.return_value = "Header"
    connection_stream.read.side_effect = BadConnectionError
    connection_stream.ended.return_value = False

    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
    with pytest.raises(BadConnectionError):
        await merger.handle_connection(connection)

    mock_merge_strategy.stream_added.assert_called_with(connection_stream)
    mock_merge_strategy.stream_removed.assert_called_with(connection_stream)
    mock_merge_strategy.new_header.assert_called()
    mock_merge_strategy.new_data.assert_not_called()
    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_no_connections_wait_empty(
        outside_source_stream, mock_merge_strategy, mock_stream_builder,
        event_loop):
    canonical_stream = outside_source_stream
    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
    f = asyncio.ensure_future(merger.no_connections_for(0.01))
    exhaust_callbacks(event_loop)
    assert not f.done()
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    assert f.done()
    await f

    merger.stop_accepting_connections()
    await merger.wait_for_ended()


@skip_if_needs_asynctest_107
@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_no_connection_wait_extends(
        outside_source_stream, mock_merge_strategy, mock_stream_builder,
        mock_connection_streams, mock_connections, event_loop):
    connection = mock_connections()
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = outside_source_stream

    connection_stream.ended.return_value = True

    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
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


@skip_if_needs_asynctest_107
@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_no_connection_wait_active_connection(
        outside_source_stream, mock_merge_strategy, mock_stream_builder,
        mock_connection_streams, mock_connections, event_loop):
    connection = mock_connections()
    connection_stream = mock_connection_streams()
    mock_stream_builder.side_effect = [connection_stream]
    canonical_stream = outside_source_stream

    async def long_read():
        await asyncio.sleep(0.05)
        return b""

    connection_stream.read.side_effect = long_read
    connection_stream.ended.side_effect = lambda: connection_stream.read.called

    merger = Merger(mock_stream_builder, mock_merge_strategy, canonical_stream)
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
