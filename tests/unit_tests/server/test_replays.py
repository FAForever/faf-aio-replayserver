import pytest
import asynctest
from tests import timeout
from asyncio.locks import Event

from replayserver.server.connection import Connection
from replayserver.server.replays import Replays


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
    await replays.handle_connection(connection)
    mock_replay_builder.assert_not_called()


@pytest.mark.asyncio
@timeout(1)
async def test_readers_successful_connection(
        mock_replays, mock_replay_builder, mock_connections, mocker):
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
