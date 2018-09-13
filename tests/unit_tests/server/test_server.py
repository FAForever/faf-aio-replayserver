import pytest
import asyncio
from unittest.mock import Mock
from replayserver.server.server import Server


class AsyncMock(Mock):
    def __await__(self, *args, **kwargs):
        async def do_nothing():
            pass
        self(*args, **kwargs)   # FIXME - doesn't catch missing 'await'
        return do_nothing().__await__()


@pytest.fixture
def mock_replays():
    return AsyncMock(spec=["handle_connection", "stop"])


@pytest.fixture
def mock_server():
    async def server_maker(callback):
        return AsyncMock(spec=["close", "wait_closed"], _callback=callback)
    return server_maker


class MockConnectionBuilder:
    def __init__(self, items):
        self._items = items

    def __call__(self, reader, writer):
        item = self._items.pop(0)
        item.configure_mock(_reader=reader, _writer=writer)
        return item


def mock_connection(reader, writer):
    return AsyncMock(spec=["read_header", "read", "write", "close"],
                     _reader=reader, _writer=writer)


@pytest.fixture
def mock_connections():
    def build(reader, writer):
        return mock_connection(reader, writer)
    return build


@pytest.fixture
def coroutine_runner():
    def run(coro):
        f = asyncio.ensure_future(coro)
        return asyncio.get_event_loop().run_until_complete(f)
    return run


def test_server_successful_connection(mock_replays, mock_server,
                                      mock_connections, coroutine_runner):
    connection = mock_connections(None, None)
    mock_connection_builder = MockConnectionBuilder([connection])
    server = Server(mock_server, mock_replays, mock_connection_builder)

    async def continue_test():
        await server.start()
        await server.handle_connection("reader", "writer")
        assert connection._reader == "reader"
        assert connection._writer == "writer"
        connection.read_header.assert_called()
        mock_replays.handle_connection.assert_called()
        connection.close.assert_called()

    coroutine_runner(continue_test())
