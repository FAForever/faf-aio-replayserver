import pytest
import asynctest
from asyncio.locks import Event
from replayserver.stream import ReplayStream


def mock_connection(reader, writer):
    class C:
        type = None
        uid = None

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


@pytest.fixture
def locked_mock_coroutines(event_loop):
    def get():
        manual_end = Event(loop=event_loop)

        async def manual_wait():
            await manual_end.wait()

        ended_wait_mock = asynctest.CoroutineMock(side_effect=manual_wait)
        return (manual_end, ended_wait_mock)

    return get


@pytest.fixture
def mock_replay_stream():
    stream = ReplayStream()
    return asynctest.Mock(spec=stream, autospec=True)


@pytest.fixture
def mock_outside_source_stream(mock_replay_stream):
    class OS:
        def set_header():
            pass

        def feed_data():
            pass

        def finish():
            pass

    mock_replay_stream.mock_add_spec(OS)
    return mock_replay_stream
