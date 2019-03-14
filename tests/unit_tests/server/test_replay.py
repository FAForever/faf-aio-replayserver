import pytest
import asynctest
import asyncio
from asynctest.helpers import exhaust_callbacks

from tests import timeout, fast_forward_time
from replayserver.server.replay import Replay
from replayserver.server.connection import ConnectionHeader
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_merger(locked_mock_coroutines):
    class M:
        canonical_stream = None

        async def handle_connection():
            pass

        def stop_accepting_connections():
            pass

        async def no_connections_for():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    no_conns, conns_wait = locked_mock_coroutines()
    conn_handled, handled_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=M,
                          _manual_end=replay_end,
                          wait_for_ended=ended_wait,
                          _manual_no_conns=no_conns,
                          no_connections_for=conns_wait,
                          _manual_handle_conn=conn_handled,
                          handle_connection=handled_wait)


@pytest.fixture
def mock_sender(locked_mock_coroutines):
    class S:
        async def handle_connection():
            pass

        def stop_accepting_connections():
            pass

        async def wait_for_ended():
            pass

    replay_end, ended_wait = locked_mock_coroutines()
    conn_handled, handled_wait = locked_mock_coroutines()
    return asynctest.Mock(spec=S, _manual_end=replay_end,
                          wait_for_ended=ended_wait,
                          _manual_handle_conn=conn_handled,
                          handle_connection=handled_wait)


@pytest.fixture
def mock_offline_merger(mocker):
    class M:
        def add_replay():
            pass

        def get_best_replay():
            pass

    return mocker.Mock(spec=M)


@pytest.fixture
def replay_deps(mock_merger, mock_sender, mock_offline_merger,
                mock_bookkeeper):
    return (mock_merger, mock_sender, mock_offline_merger, mock_bookkeeper)


class MockReplayConfig:
    def __init__(self, forced_end_time, grace_period):
        self.forced_end_time = forced_end_time
        self.grace_period = grace_period


@pytest.mark.asyncio
@fast_forward_time(1, 40)
@timeout(30)
async def test_replay_closes_after_timeout(
        event_loop, replay_deps, mock_conn_plus_head):
    mock_merger, mock_sender, _, _ = replay_deps
    conf = MockReplayConfig(15, 100)
    replay = Replay(*replay_deps, conf, 1)
    mock_merger.stop_accepting_connections.assert_not_called()
    mock_sender.stop_accepting_connections.assert_not_called()

    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)
    wf = asyncio.ensure_future(replay.handle_connection(*writer))

    await asyncio.sleep(20)
    exhaust_callbacks(event_loop)
    mock_merger.stop_accepting_connections.assert_called()
    mock_sender.stop_accepting_connections.assert_called()
    writer[1].close.assert_called()

    mock_merger._manual_handle_conn.set()
    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await wf
    await replay.wait_for_ended()


@pytest.mark.asyncio
@fast_forward_time(1, 40)
@timeout(30)
async def test_replay_ending_cancels_timeouts(event_loop, replay_deps):
    mock_merger, mock_sender, _, _ = replay_deps
    conf = MockReplayConfig(15, 100)
    replay = Replay(*replay_deps, conf, 1)
    exhaust_callbacks(event_loop)
    replay.close()
    mock_merger.stop_accepting_connections.assert_called()
    mock_sender.stop_accepting_connections.assert_called()
    mock_merger.stop_accepting_connections.reset_mock()
    mock_sender.stop_accepting_connections.reset_mock()

    # Replay expects these to end after calling close
    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await replay.wait_for_ended()

    # Did replay stop waiting until merger says there aren't more connections?
    await asyncio.sleep(1)
    mock_merger._manual_no_conns.set()

    # Did replay stop waiting for its own timeout?
    await asyncio.sleep(19)
    exhaust_callbacks(event_loop)
    mock_merger.stop_accepting_connections.assert_not_called()
    mock_sender.stop_accepting_connections.assert_not_called()


@pytest.mark.asyncio
@fast_forward_time(1, 40)
@timeout(30)
async def test_replay_timeouts_while_ending_dont_explode(
        event_loop, replay_deps):
    mock_merger, mock_sender, _, _ = replay_deps
    conf = MockReplayConfig(15, 100)
    replay = Replay(*replay_deps, conf, 1)
    exhaust_callbacks(event_loop)

    # Won't finish until we tell mock merger & sender to end
    f = asyncio.ensure_future(replay.wait_for_ended())

    await asyncio.sleep(1)
    mock_merger._manual_no_conns.set()

    await asyncio.sleep(19)
    exhaust_callbacks(event_loop)

    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await f


@pytest.mark.asyncio
@fast_forward_time(1, 40)
@timeout(30)
async def test_replay_forwarding_connections(event_loop, replay_deps,
                                             mock_conn_plus_head):
    mock_merger, mock_sender, _, _ = replay_deps
    reader = mock_conn_plus_head(ConnectionHeader.Type.READER, 1)
    writer = mock_conn_plus_head(ConnectionHeader.Type.WRITER, 1)
    invalid = mock_conn_plus_head(17, 1)
    conf = MockReplayConfig(15, 100)
    replay = Replay(*replay_deps, conf, 1)

    mock_sender._manual_handle_conn.set()
    mock_merger._manual_handle_conn.set()

    await replay.handle_connection(*reader)
    mock_merger.handle_connection.assert_not_awaited()
    mock_sender.handle_connection.assert_awaited_with(reader[1])
    mock_sender.handle_connection.reset_mock()

    await replay.handle_connection(*writer)
    mock_sender.handle_connection.assert_not_awaited()
    mock_merger.handle_connection.assert_awaited_with(writer[1])
    mock_merger.handle_connection.reset_mock()

    with pytest.raises(MalformedDataError):
        await replay.handle_connection(*invalid)
    mock_sender.handle_connection.assert_not_awaited()
    mock_merger.handle_connection.assert_not_awaited()

    mock_merger._manual_end.set()
    mock_sender._manual_end.set()
    await replay.wait_for_ended()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_keeps_proper_event_order(
        event_loop, mocker, replay_deps):
    merger, sender, offline_merger, bookkeeper = replay_deps
    mock_best_replay = mocker.Mock()
    offline_merger.get_best_replay.return_value = mock_best_replay

    async def bookkeeper_check(game_id, stream):
        assert stream is mock_best_replay
        # Merging has to end before bookkeeping starts
        merger.wait_for_ended.assert_awaited()
        # We shall not wait for stream sending to end before bookkeeping
        sender.wait_for_ended.assert_not_awaited()
        return

    bookkeeper.save_replay.side_effect = bookkeeper_check

    conf = MockReplayConfig(0.1, 100)
    replay = Replay(*replay_deps, conf, 1)
    await exhaust_callbacks(event_loop)
    merger._manual_end.set()
    await exhaust_callbacks(event_loop)
    sender._manual_end.set()
    await replay.wait_for_ended()
