import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.send.sender import Sender
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
