import pytest
import asynctest
from asyncio.locks import Event
from tests import TimeSkipper


pytest_plugins = ['tests.fixtures.connection', 'tests.fixtures.stream',
                  'tests.fixtures.database']


@pytest.fixture
def locked_mock_coroutines(event_loop):
    def get():
        manual_end = Event(loop=event_loop)

        async def manual_wait(*args, **kwargs):
            await manual_end.wait()

        ended_wait_mock = asynctest.CoroutineMock(side_effect=manual_wait)
        return (manual_end, ended_wait_mock)

    return get


@pytest.fixture
def mock_bookkeeper():
    class C:
        async def save_replay():
            pass

    return asynctest.Mock(spec=C)


@pytest.fixture
def mock_replay_headers(mocker):
    def build(raw_replay=None):
        m = mocker.Mock(spec=["header", "data"])
        if raw_replay is None:
            m.header = None
            m.data = b""
        else:
            m.header.mock_add_spec(["header", "data"])
            m.header.configure_mock(
                header=raw_replay.header,
                data=raw_replay.data[:raw_replay.header_size],
                )
            m.data = raw_replay.data[raw_replay.header_size:]
        return m

    return build


@pytest.fixture
def time_skipper(event_loop):
    return TimeSkipper(event_loop)
