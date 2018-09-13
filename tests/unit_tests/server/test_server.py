import pytest
import decorator
import asyncio
import asynctest
from replayserver.server.server import Server


@pytest.fixture
def mock_replays():
    class R:
        async def handle_connection():
            pass

        async def stop():
            pass

    return asynctest.Mock(spec=R)


class MockServer:
    class S:
        def close():
            pass

        async def wait_closed():
            pass

    def __init__(self):
        self.server = None

    async def __call__(self, callback):
        self.server = asynctest.Mock(spec=self.S, _callback=callback)
        return self.server


@pytest.fixture
def mock_server_maker():
    class S:
        def close():
            pass

        async def wait_closed():
            pass

    mock_server = asynctest.Mock(spec=S)
    mock_builder = asynctest.CoroutineMock(return_value=mock_server)
    return mock_builder


class MockConnectionBuilder:
    def __init__(self, items):
        self._items = items

    def __call__(self, reader, writer):
        item = self._items.pop(0)
        item.configure_mock(_reader=reader, _writer=writer)
        return item


def mock_connection(reader, writer):
    class C:
        async def read_header():
            pass

        async def read():
            pass

        async def write():
            pass

        def close():
            pass

    return asynctest.Mock(spec=C, _reader=reader, _writer=writer)


@pytest.fixture
def mock_connections():
    def build(reader, writer):
        return mock_connection(reader, writer)
    return build


def timeout(time):
    def deco(coro):
        async def wrapper_function(coro, *args, **kwargs):
            return (await asyncio.wait_for(coro(*args, **kwargs), time))
        # Needed for fixtures to work
        return decorator.decorator(wrapper_function, coro)
    return deco


@pytest.mark.asyncio
@timeout(1)
async def test_server_start(mock_replays, mock_server_maker, mock_connections):
    connection = mock_connections(None, None)
    mock_connection_builder = MockConnectionBuilder([connection])
    server = Server(mock_server_maker, mock_replays, mock_connection_builder)
    await server.start()
    mock_server_maker.assert_awaited_with(server.handle_connection)


@pytest.mark.asyncio
@timeout(1)
async def test_server_successful_connection(mock_replays, mock_server_maker,
                                            mock_connections):
    connection = mock_connections(None, None)
    mock_connection_builder = MockConnectionBuilder([connection])
    server = Server(mock_server_maker, mock_replays, mock_connection_builder)

    await server.start()
    await server.handle_connection("reader", "writer")
    assert connection._reader == "reader"
    assert connection._writer == "writer"
    connection.read_header.assert_awaited()
    mock_replays.handle_connection.assert_awaited()
    connection.close.assert_called()


@pytest.mark.asyncio
@timeout(1)
async def test_server_stopping(mock_replays, mock_server_maker,
                               mock_connections):
    read_header_called = asyncio.locks.Event()
    verified = asyncio.locks.Event()

    connection = mock_connections(None, None)
    mock_connection_builder = MockConnectionBuilder([connection])

    async def at_header_read():
        read_header_called.set()
        await verified.wait()
    connection.read_header.side_effect = at_header_read

    server = Server(mock_server_maker, mock_replays, mock_connection_builder)

    await server.start()
    asyncio.ensure_future(server.handle_connection("reader", "writer"))
    await read_header_called.wait()
    await server.stop()

    mock_server = mock_server_maker.return_value
    mock_server.close.assert_called()
    mock_server.wait_closed.assert_awaited()
    connection.close.assert_called()
    mock_replays.stop.assert_awaited()
    verified.set()
