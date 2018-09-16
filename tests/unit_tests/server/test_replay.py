import pytest
import asynctest
import asyncio
from asynctest.helpers import exhaust_callbacks

from tests import timeout
from replayserver.server.replay import Replay
from replayserver.server.connection import Connection
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_merger(locked_mock_coroutines):
    class M:
        canonical_stream = None

        async def handle_connection():
            pass

        def close():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=M, _manual_end=replay_end,
                          wait_for_ended=ended_wait)


@pytest.fixture
def mock_sender(locked_mock_coroutines):
    class S:
        async def handle_connection():
            pass

        def close():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=S, _manual_end=replay_end,
                          wait_for_ended=ended_wait)


@pytest.fixture
def mock_bookkeeper():
    class B:
        async def save_replay():
            pass

    return asynctest.Mock(spec=B)


@pytest.mark.asyncio
@timeout(1)
async def test_replay_closes_after_timeout(
        mock_merger, mock_sender, mock_bookkeeper, event_loop):
    timeout = 0.01
    replay = Replay(mock_merger, mock_sender, mock_bookkeeper, timeout)
    mock_merger.close.assert_not_called()
    mock_sender.close.assert_not_called()
    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    mock_merger.close.assert_called()
    mock_sender.close.assert_called()

    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await replay.wait_for_ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_close_cancels_timeout(
        mock_merger, mock_sender, mock_bookkeeper, event_loop):
    timeout = 0.01
    replay = Replay(mock_merger, mock_sender, mock_bookkeeper, timeout)
    exhaust_callbacks(event_loop)
    replay.close()
    mock_merger.close.assert_called()
    mock_sender.close.assert_called()
    mock_merger.close.reset_mock()
    mock_sender.close.reset_mock()

    await asyncio.sleep(0.02)
    exhaust_callbacks(event_loop)
    mock_merger.close.assert_not_called()
    mock_sender.close.assert_not_called()

    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await replay.wait_for_ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_forwarding_connections(
        mock_merger, mock_sender, mock_bookkeeper, mock_connections):
    reader = mock_connections(None, None)
    reader.uid = 1
    reader.type = Connection.Type.READER
    writer = mock_connections(None, None)
    writer.uid = 1
    writer.type = Connection.Type.WRITER
    invalid = mock_connections(None, None)
    invalid.uid = 1
    invalid.type = 17
    timeout = 0.1
    replay = Replay(mock_merger, mock_sender, mock_bookkeeper, timeout)

    await replay.handle_connection(reader)
    mock_merger.handle_connection.assert_not_awaited()
    mock_sender.handle_connection.assert_awaited_with(reader)
    mock_sender.handle_connection.reset_mock()

    await replay.handle_connection(writer)
    mock_sender.handle_connection.assert_not_awaited()
    mock_merger.handle_connection.assert_awaited_with(writer)
    mock_merger.handle_connection.reset_mock()

    with pytest.raises(MalformedDataError):
        await replay.handle_connection(invalid)
    mock_sender.handle_connection.assert_not_awaited()
    mock_merger.handle_connection.assert_not_awaited()

    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await replay.wait_for_ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_keeps_proper_event_order(
        mock_merger, mock_sender, mock_bookkeeper, event_loop):

    async def bookkeeper_check(*args, **kwargs):
        # Merging has to end before bookkeeping starts
        mock_merger.wait_for_ended.assert_awaited()
        # We shall not wait for stream sending to end before bookkeeping
        mock_sender.wait_for_ended.assert_not_awaited()
        return

    mock_bookkeeper.save_replay.side_effect = bookkeeper_check

    timeout = 0.2
    replay = Replay(mock_merger, mock_sender, mock_bookkeeper, timeout)
    await exhaust_callbacks(event_loop)
    mock_merger._manual_end.set()
    await exhaust_callbacks(event_loop)
    mock_sender._manual_end.set()
    await replay.wait_for_ended()
