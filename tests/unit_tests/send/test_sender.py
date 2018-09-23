import pytest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.send.sender import Sender
from replayserver.errors import CannotAcceptConnectionError
from replayserver.server.connection import Connection


@pytest.fixture
def mock_header(mocker):
    return mocker.Mock(spec=["header", "data"])


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_not_accepting_after_stream_ends(mock_connections,
                                                      outside_source_stream):
    connection = mock_connections(Connection.Type.READER, 1)
    sender = Sender(outside_source_stream)
    outside_source_stream.finish()
    with pytest.raises(CannotAcceptConnectionError):
        await sender.handle_connection(connection)
    await sender.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_not_accepting_after_explicit_end(mock_connections,
                                                       outside_source_stream):
    connection = mock_connections(Connection.Type.READER, 1)
    sender = Sender(outside_source_stream)
    sender.close()
    with pytest.raises(CannotAcceptConnectionError):
        await sender.handle_connection(connection)
    outside_source_stream.finish()
    await sender.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_doesnt_end_while_connection_runs(
        mock_connections, outside_source_stream, locked_mock_coroutines,
        mock_header, event_loop):
    connection = mock_connections(Connection.Type.READER, 1)
    outside_source_stream.feed_data(b"aaaaa")
    outside_source_stream.set_header(mock_header)
    end, coro = locked_mock_coroutines()
    connection.write.side_effect = coro

    sender = Sender(outside_source_stream)
    h = asyncio.ensure_future(sender.handle_connection(connection))
    w = asyncio.ensure_future(sender.wait_for_ended())
    await exhaust_callbacks(event_loop)
    outside_source_stream.finish()
    await exhaust_callbacks(event_loop)
    assert not w.done()
    end.set()
    await h
    await w


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_ends_with_ended_stream_and_no_connections(
        outside_source_stream):
    sender = Sender(outside_source_stream)
    outside_source_stream.finish()
    await sender.wait_for_ended()
