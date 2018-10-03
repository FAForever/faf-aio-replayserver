import pytest
import asyncio
import asynctest
from tests import timeout

from replayserver.server.connections import Connections
from replayserver.errors import BadConnectionError


@pytest.fixture
def mock_replays():
    class R:
        async def handle_connection():
            pass

        async def start():
            pass

        async def stop():
            pass

    return asynctest.Mock(spec=R)


@pytest.fixture
def mock_header_read():
    return asynctest.CoroutineMock()


def test_connections_init(mock_replays, mock_header_read):
    Connections(mock_header_read, mock_replays)


@pytest.mark.asyncio
@timeout(0.1)
async def test_connections_handle_connection(mock_replays, mock_header_read,
                                             mock_connections):
    connection = mock_connections()
    conns = Connections(mock_header_read, mock_replays)
    mock_header = "foo"
    mock_header_read.return_value = mock_header

    await conns.handle_connection(connection)
    mock_header_read.assert_awaited()
    mock_replays.handle_connection.assert_awaited()
    connection.close.assert_called()
    await conns.wait_until_empty()


@pytest.mark.asyncio
@timeout(0.1)
async def test_connections_close_all(mock_replays, mock_header_read,
                                     mock_connections):
    read_header_called = asyncio.locks.Event()
    verified = asyncio.locks.Event()

    connection = mock_connections()

    async def at_header_read(conn):
        read_header_called.set()
        await verified.wait()
        return "foo"

    mock_header_read.side_effect = at_header_read

    conns = Connections(mock_header_read, mock_replays)

    f = asyncio.ensure_future(conns.handle_connection(connection))
    await read_header_called.wait()
    conns.close_all()

    connection.close.assert_called()
    verified.set()
    await f
    await conns.wait_until_empty()


@pytest.mark.asyncio
@timeout(0.1)
async def test_connections_error_closes_connection(
        mock_replays, mock_header_read, mock_connections):

    connection = mock_connections()

    async def at_header_read(conn):
        raise BadConnectionError
    mock_header_read.side_effect = at_header_read

    conns = Connections(mock_header_read, mock_replays)
    await conns.handle_connection(connection)
    connection.close.assert_called()
    await conns.wait_until_empty()
