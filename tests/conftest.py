import pytest
import asynctest
from asyncio.locks import Event


pytest_plugins = ['tests.fixtures.connection', 'tests.fixtures.stream',
                  'tests.fixtures.database']


@pytest.fixture
def blockable_coroutines(event_loop):
    def get():
        manual_end = Event(loop=event_loop)

        async def manual_wait(*args, **kwargs):
            await manual_end.wait()

        blockable = asynctest.CoroutineMock(side_effect=manual_wait,
                                            _lock=manual_end)
        return blockable

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
        m = mocker.Mock(spec=["data", "struct"])
        if raw_replay is not None:
            m.configure_mock(
                struct=raw_replay.header,
                data=raw_replay.header_data,
            )
        return m

    return build
