import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.send.sender import Sender, ReplayStreamWriter
from replayserver.errors import CannotAcceptConnectionError


@pytest.fixture
def mock_writer(blockable_coroutines):
    class S:
        async def send_to():
            pass

    blockable = blockable_coroutines()
    return asynctest.Mock(spec=S,
                          send_to=blockable)


@pytest.fixture
def mock_stream(mock_replay_streams):
    return mock_replay_streams()


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_doesnt_end_while_connection_runs(
        mock_connections, mock_writer, mock_stream, event_loop):
    connection = mock_connections()

    sender = Sender(mock_stream, mock_writer)
    h = asyncio.ensure_future(sender.handle_connection(connection))
    w = asyncio.ensure_future(sender.wait_for_ended())
    await exhaust_callbacks(event_loop)
    sender.stop_accepting_connections()
    await exhaust_callbacks(event_loop)
    assert not w.done()

    mock_writer.send_to._lock.set()
    await h
    await w


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_ends_when_refusing_conns_and_no_connections(
        mock_writer, mock_stream):
    sender = Sender(mock_stream, mock_writer)
    sender.stop_accepting_connections()
    await sender.wait_for_ended()
    mock_stream.wait_for_ended.assert_awaited()


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_refuses_connection_when_told_to(
        mock_connections, mock_writer, mock_stream, event_loop):
    connection = mock_connections()
    sender = Sender(mock_stream, mock_writer)
    sender.stop_accepting_connections()
    with pytest.raises(CannotAcceptConnectionError):
        await sender.handle_connection(connection)
    await sender.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_send_doesnt_end_until_stream_ends(
        mock_connections, outside_source_stream, mock_replay_headers,
        event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    outside_source_stream.feed_data(b"aaaaa")
    outside_source_stream.set_header(mock_header)

    sender = ReplayStreamWriter(outside_source_stream)
    h = asyncio.ensure_future(sender.send_to(connection))
    await exhaust_callbacks(event_loop)
    assert not h.done()

    outside_source_stream.finish()
    await h


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_no_header(mock_connections, outside_source_stream,
                                       event_loop):
    connection = mock_connections()
    sender = ReplayStreamWriter(outside_source_stream)
    f = asyncio.ensure_future(sender.send_to(connection))
    await exhaust_callbacks(event_loop)
    outside_source_stream.finish()
    await f     # We expect no errors
    connection.write.assert_not_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_connection_calls(mock_connections,
                                              outside_source_stream,
                                              mock_replay_headers,
                                              event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    mock_header.data = b"Header"
    sender = ReplayStreamWriter(outside_source_stream)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.feed_data(b"Data")
    outside_source_stream.finish()
    await sender.send_to(connection)
    connection.write.assert_has_awaits([asynctest.call(b"Header"),
                                        asynctest.call(b"Data")])


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_empty_data(mock_connections,
                                        outside_source_stream,
                                        mock_replay_headers,
                                        event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    mock_header.data = b"Header"
    sender = ReplayStreamWriter(outside_source_stream)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.finish()
    await sender.send_to(connection)
    connection.write.assert_has_awaits([asynctest.call(b"Header")])
