import asyncio
import pytest
import asynctest
from tests import timeout
from asyncio.locks import Event

from replayserver.server.connection import Connection
from replayserver.server.replays import Replays
from replayserver.errors import CannotAcceptConnectionError


def mock_replay():
    class R:
        async def handle_connection():
            pass

        def close():
            pass

        def do_not_wait_for_more_connections():
            pass

        async def wait_for_ended():
            pass

    manual_replay_end = Event()
    mr = asynctest.Mock(spec=R)

    async def manual_ended_wait():
        await manual_replay_end.wait()

    ended_wait_mock = asynctest.CoroutineMock(wraps=manual_ended_wait)

    mr.configure_mock(_manual_replay_end=manual_replay_end,
                      wait_for_ended=ended_wait_mock)
    return mr


@pytest.fixture
def mock_replays():
    return mock_replay


@pytest.fixture
def mock_replay_builder(mocker):
    return mocker.Mock(spec=[])


@pytest.mark.asyncio
@timeout(1)
async def test_reader_with_no_existing_replay_is_not_processed(
        mock_connections, mock_replay_builder):
    connection = mock_connections(None, None)
    connection.type = Connection.Type.READER
    connection.uid = 1

    replays = Replays(mock_replay_builder)
    with pytest.raises(CannotAcceptConnectionError):
        await replays.handle_connection(connection)
    mock_replay_builder.assert_not_called()


@pytest.mark.asyncio
@timeout(1)
async def test_readers_successful_connection(
        mock_replays, mock_replay_builder, mock_connections):
    writer = mock_connections(None, None)
    writer.type = Connection.Type.WRITER
    writer.uid = 1
    reader = mock_connections(None, None)
    reader.type = Connection.Type.READER
    reader.uid = 1
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder)

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

    mock_replay._manual_replay_end.set()
    await replays.stop()


@pytest.mark.asyncio
@timeout(1)
async def test_replays_closing(
        mock_replays, mock_replay_builder, mock_connections):
    writer = mock_connections(None, None)
    writer.type = Connection.Type.WRITER
    writer.uid = 1
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(writer)
    assert 1 in replays
    mock_replay._manual_replay_end.set()
    await replays.stop()
    mock_replay.close.assert_called()
    mock_replay.wait_for_ended.assert_awaited()     # Only true if await ended
    assert 1 not in replays


@pytest.mark.asyncio
@timeout(1)
async def test_replays_closing_waits_for_replay(
        mock_replays, mock_replay_builder, mock_connections):
    writer = mock_connections(None, None)
    writer.type = Connection.Type.WRITER
    writer.uid = 1
    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(writer)
    f = asyncio.ensure_future(replays.stop())
    await asyncio.sleep(0.1)    # FIXME - sensitive to races
    assert not f.done()
    mock_replay._manual_replay_end.set()
    await f


@pytest.mark.asyncio
@timeout(1)
async def test_replay_ending_is_tracked(
        mock_replays, mock_replay_builder, mock_connections):
    writer = mock_connections(None, None)
    writer.type = Connection.Type.WRITER
    writer.uid = 1

    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay, mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(writer)
    mock_replay._manual_replay_end.set()
    while 1 in replays:
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
@timeout(1)
async def test_connections_are_not_accepted_when_closing(
        mock_replays, mock_replay_builder, mock_connections):
    writer = mock_connections(None, None)
    writer.type = Connection.Type.WRITER
    writer.uid = 1

    writer_2 = mock_connections(None, None)
    writer_2.type = Connection.Type.WRITER
    writer_2.uid = 2

    mock_replay = mock_replays()
    mock_replay_builder.side_effect = [mock_replay, mock_replay]
    replays = Replays(mock_replay_builder)

    await replays.handle_connection(writer)
    f = asyncio.ensure_future(replays.stop())
    await asyncio.sleep(0.1)    # FIXME - sensitive to races

    with pytest.raises(CannotAcceptConnectionError):
        await replays.handle_connection(writer_2)

    mock_replay._manual_replay_end.set()
    await f
