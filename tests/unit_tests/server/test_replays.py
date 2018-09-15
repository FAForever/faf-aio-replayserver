import pytest
import asynctest
from tests import timeout

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

    return asynctest.Mock(spec=R)


@pytest.fixture
def mock_replay_builder():
    return mock_replay


@pytest.mark.asyncio
@timeout(1)
async def test_reader_with_no_existing_replay_is_not_processed(
        mock_replay_builder, mock_connections, mocker):
    connection = mock_connections(None, None)
    connection.type = Connection.Type.READER
    connection.uid = 1
    replay_builder = mocker.Mock()

    replays = Replays(replay_builder)
    await replays.handle_connection(connection)
    replay_builder.assert_not_called()
