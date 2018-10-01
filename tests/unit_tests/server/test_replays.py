import asyncio
import pytest
import asynctest
from asynctest.helpers import exhaust_callbacks

from tests import timeout
from replayserver.server.connection import ConnectionHeader
from replayserver.server.replays import Replays
from replayserver.errors import CannotAcceptConnectionError


def mock_replay(locked_mock_coroutines):
    class R:
        async def handle_connection():
            pass

        def close():
            pass

        def do_not_wait_for_more_connections():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=R, _manual_end=replay_end,
                          wait_for_ended=ended_wait)


@pytest.fixture
def mock_replays(locked_mock_coroutines):
    return lambda: mock_replay(locked_mock_coroutines)


@pytest.fixture
def mock_replay_builder(mocker):
    return mocker.Mock(spec=[])


@pytest.mark.asyncio
@timeout(1)
async def test_reader_with_no_existing_replay_is_not_processed(
        mock_connections, mock_replay_builder, mock_bookkeeper):
    connection = mock_connections(ConnectionHeader.Type.READER, 1)

    replays = Replays(mock_replay_builder, mock_bookkeeper)
    with pytest.raises(CannotAcceptConnectionError):
        await replays.handle_connection(connection)
    mock_replay_builder.assert_not_called()


@pytest.mark.asyncio
@timeout(1)
async def test_readers_successful_connection(
        mock_replays, mock_replay_builder, mock_connections, mock_bookkeeper):
    writer = mock_connections(ConnectionHeader.Type.WRITER, 1)
    reader = mock_connections(ConnectionHeader.Type.READER, 1)
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder, mock_bookkeeper)

    await replays.handle_connection(writer)
    mock_replay_builder.assert_called_once()
    mock_replay.handle_connection.assert_called_with(writer)
    mock_replay.handle_connection.reset_mock()
    mock_replay_builder.reset_mock()

    await replays.handle_connection(reader)
    mock_replay_builder.assert_not_called()
    mock_replay.handle_connection.assert_called_with(reader)
    mock_replay.handle_connection.reset_mock()
    mock_replay_builder.reset_mock()

    mock_replay._manual_end.set()
    await replays.stop()


@pytest.mark.asyncio
@timeout(1)
async def test_replays_closing(
        mock_replays, mock_replay_builder, mock_connections, mock_bookkeeper):
    writer = mock_connections(ConnectionHeader.Type.WRITER, 1)
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder, mock_bookkeeper)

    await replays.handle_connection(writer)
    assert 1 in replays
    mock_replay._manual_end.set()
    await replays.stop()
    mock_replay.close.assert_called()
    mock_replay.wait_for_ended.assert_awaited()     # Only true if await ended
    assert 1 not in replays


@pytest.mark.asyncio
@timeout(1)
async def test_replays_closing_waits_for_replay(
        mock_replays, mock_replay_builder, mock_connections, event_loop,
        mock_bookkeeper):
    writer = mock_connections(ConnectionHeader.Type.WRITER, 1)
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder, mock_bookkeeper)

    await replays.handle_connection(writer)
    f = asyncio.ensure_future(replays.stop())
    await exhaust_callbacks(event_loop)
    assert not f.done()
    mock_replay._manual_end.set()
    await f


@pytest.mark.asyncio
@timeout(1)
async def test_replay_ending_is_tracked(
        mock_replays, mock_replay_builder, mock_connections, event_loop,
        mock_bookkeeper):
    writer = mock_connections(ConnectionHeader.Type.WRITER, 1)

    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay, mock_replay]
    replays = Replays(mock_replay_builder, mock_bookkeeper)

    await replays.handle_connection(writer)
    mock_replay._manual_end.set()
    await exhaust_callbacks(event_loop)
    assert 1 not in replays


@pytest.mark.asyncio
@timeout(1)
async def test_connections_are_not_accepted_when_closing(
        mock_replays, mock_replay_builder, mock_connections, event_loop,
        mock_bookkeeper):
    writer = mock_connections(ConnectionHeader.Type.WRITER, 1)
    writer_2 = mock_connections(ConnectionHeader.Type.WRITER, 1)

    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay, mock_replay]
    replays = Replays(mock_replay_builder, mock_bookkeeper)

    await replays.handle_connection(writer)
    f = asyncio.ensure_future(replays.stop())
    await exhaust_callbacks(event_loop)

    with pytest.raises(CannotAcceptConnectionError):
        await replays.handle_connection(writer_2)

    mock_replay._manual_end.set()
    await f
