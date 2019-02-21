import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.send.sender import Sender
from replayserver.errors import CannotAcceptConnectionError


@pytest.fixture
def mock_send_strategy(locked_mock_coroutines):
    class S:
        async def send_to():
            pass

    sent_wait, sent_to = locked_mock_coroutines()
    return asynctest.Mock(spec=S,
                          _end_send=sent_wait,
                          send_to=sent_to)


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_doesnt_end_while_connection_runs(
        mock_connections, mock_send_strategy, event_loop):
    connection = mock_connections()

    sender = Sender(mock_send_strategy)
    h = asyncio.ensure_future(sender.handle_connection(connection))
    w = asyncio.ensure_future(sender.wait_for_ended())
    await exhaust_callbacks(event_loop)
    sender.stop_accepting_connections()
    await exhaust_callbacks(event_loop)
    assert not w.done()

    mock_send_strategy._end_send.set()
    await h
    await w


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_ends_when_refusing_conns_and_no_connections(
        mock_send_strategy):
    sender = Sender(mock_send_strategy)
    sender.stop_accepting_connections()
    await sender.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_refuses_connection_when_told_to(
        mock_connections, mock_send_strategy, event_loop):
    connection = mock_connections()
    sender = Sender(mock_send_strategy)
    sender.stop_accepting_connections()
    with pytest.raises(CannotAcceptConnectionError):
        await sender.handle_connection(connection)
    await sender.wait_for_ended()
