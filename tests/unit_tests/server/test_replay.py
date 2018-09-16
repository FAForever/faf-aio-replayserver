import pytest
import asynctest
import asyncio
from replayserver.server.replay import Replay
from tests import timeout


@pytest.fixture
def mock_merger(locked_mock_coroutines):
    class M:
        canonical_stream = None

        async def handle_connection():
            pass

        def close():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=M, _manual_end=replay_end,
                          wait_for_ended=ended_wait)


@pytest.fixture
def mock_sender(locked_mock_coroutines):
    class S:
        async def handle_connection():
            pass

        def close():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=S, _manual_end=replay_end,
                          wait_for_ended=ended_wait)


@pytest.fixture
def mock_bookkeeper():
    class B:
        async def save_replay():
            pass

    return asynctest.Mock(spec=B)


@pytest.mark.asyncio
@timeout(1)
async def test_replay_closes_after_timeout(
        mock_merger, mock_sender, mock_bookkeeper):
    timeout = 0.1
    replay = Replay(mock_merger, mock_sender, mock_bookkeeper, timeout)
    mock_merger.close.assert_not_called()
    mock_sender.close.assert_not_called()
    await asyncio.sleep(0.2)
    mock_merger.close.assert_called()
    mock_sender.close.assert_called()

    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await replay.wait_for_ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_close_cancels_timeout(
        mock_merger, mock_sender, mock_bookkeeper):
    pass
