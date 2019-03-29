import asyncio
import pytest
import asynctest
from asynctest.helpers import exhaust_callbacks

from tests import timeout
from replayserver.server.connection import ConnectionHeader
from replayserver.server.replays import Replays
from replayserver.errors import CannotAcceptConnectionError


@pytest.fixture
def mock_replays(blockable_coroutines):
    def build():
        class R:
            async def handle_connection():
                pass

            def close():
                pass

            async def wait_for_ended():
                pass

        ended_wait = blockable_coroutines()
        return asynctest.Mock(spec=R, wait_for_ended=ended_wait)
    return build


@pytest.fixture
def mock_replay_builder(mocker):
    return mocker.Mock(spec=[])


@pytest.mark.asyncio
@timeout(1)
async def test_reader_with_no_existing_replay_is_not_processed(
        mock_conn_plus_head, mock_replay_builder):
    conn = mock_conn_plus_head(ConnectionHeader.Type.READER, 1)

    replays = Replays(mock_replay_builder)
    with pytest.raises(CannotAcceptConnectionError):
        await replays.handle_connection(*conn)
    mock_replay_builder.assert_not_called()


@pytest.mark.asyncio
@timeout(1)
async def test_readers_successful_connection(
        mock_replays, mock_replay_builder, mock_conn_plus_head):
    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)
    reader = mock_conn_plus_head(ConnectionHeader.Type.READER, 1)
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(*writer)
    mock_replay_builder.assert_called_once()
    mock_replay.handle_connection.assert_called_with(*writer)
    mock_replay.handle_connection.reset_mock()
    mock_replay_builder.reset_mock()

    await replays.handle_connection(*reader)
    mock_replay_builder.assert_not_called()
    mock_replay.handle_connection.assert_called_with(*reader)
    mock_replay.handle_connection.reset_mock()
    mock_replay_builder.reset_mock()

    mock_replay.wait_for_ended._lock.set()
    await replays.stop_all()


@pytest.mark.asyncio
@timeout(1)
async def test_replays_closing(
        mock_replays, mock_replay_builder, mock_conn_plus_head):
    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(*writer)
    assert 1 in replays
    mock_replay.wait_for_ended._lock.set()
    await replays.stop_all()
    mock_replay.close.assert_called()
    mock_replay.wait_for_ended.assert_awaited()     # Only true if await ended
    assert 1 not in replays


@pytest.mark.asyncio
@timeout(1)
async def test_replays_closing_waits_for_replay(
        mock_replays, mock_replay_builder, mock_conn_plus_head, event_loop):
    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(*writer)
    f = asyncio.ensure_future(replays.stop_all())
    await exhaust_callbacks(event_loop)
    assert not f.done()
    mock_replay.wait_for_ended._lock.set()
    await f


@pytest.mark.asyncio
@timeout(1)
async def test_replay_ending_is_tracked(
        mock_replays, mock_replay_builder, mock_conn_plus_head, event_loop):
    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)

    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay, mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(*writer)
    mock_replay.wait_for_ended._lock.set()
    await exhaust_callbacks(event_loop)
    assert 1 not in replays


@pytest.mark.asyncio
@timeout(1)
async def test_connections_are_not_accepted_when_closing(
        mock_replays, mock_replay_builder, mock_conn_plus_head, event_loop):
    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)
    writer_2 = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)

    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay, mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(*writer)
    f = asyncio.ensure_future(replays.stop_all())
    await exhaust_callbacks(event_loop)

    with pytest.raises(CannotAcceptConnectionError):
        await replays.handle_connection(*writer_2)

    mock_replay.wait_for_ended._lock.set()
    await f
