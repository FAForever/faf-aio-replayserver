import pytest
import asyncio
from tests.replays import example_replay
from tests import fast_forward_time, timeout

from replayserver.receive.mergestrategy import MergeStrategies
from replayserver.receive.merger import Merger
from replayserver.server.connection import ConnectionHeader
from replayserver.errors import MalformedDataError, CannotAcceptConnectionError


config = {
    "config_merger_grace_period_time": 30,
    "config_replay_merge_strategy": MergeStrategies.GREEDY,
}


@pytest.fixture
def data_send_mixin():
    def build(mock_connection, replay_data, sleep_time, data_at_once):
        replay_data_pos = 0

        async def read_data(amount):
            nonlocal replay_data_pos
            new_pos = replay_data_pos + data_at_once
            data = replay_data[replay_data_pos:new_pos]
            replay_data_pos = new_pos
            await asyncio.sleep(sleep_time)
            return data

        mock_connection.read.side_effect = read_data
    return build


async def verify_merger_ending_with_data(merger, data):
    await merger.wait_for_ended()
    stream = merger.canonical_stream
    assert stream.ended()
    if data is None:
        assert stream.header is None
        assert stream.data.bytes() == b""
    else:
        assert stream.header.data + stream.data.bytes() == data


def test_merger_init():
    Merger.build(**config)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_successful_connection(event_loop, mock_connections,
                                            data_send_mixin):
    conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data
    data_send_mixin(conn, replay_data, 0.25, 200)

    merger = Merger.build(**config)
    await merger.handle_connection(conn)
    await verify_merger_ending_with_data(merger, replay_data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_incomplete_header(event_loop, mock_connections,
                                        data_send_mixin):
    conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data[:example_replay.header_size - 100]
    data_send_mixin(conn, replay_data, 0.25, 200)

    merger = Merger.build(**config)
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn)
    await verify_merger_ending_with_data(merger, None)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_incomplete_header_then_data(event_loop,
                                                  mock_connections,
                                                  data_send_mixin):
    conn_1 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    conn_2 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data[:example_replay.header_size - 100]
    data_send_mixin(conn_1, replay_data, 0.25, 200)
    data_send_mixin(conn_2, example_replay.data, 0.25, 200)

    merger = Merger.build(**config)
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn_1)
    await merger.handle_connection(conn_2)

    await verify_merger_ending_with_data(merger, example_replay.data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_two_connections(event_loop, mock_connections,
                                      data_send_mixin):
    conn_1 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    conn_2 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data

    # First has incomplete data, but sends faster
    data_send_mixin(conn_1, replay_data[:-100], 0.4, 160)
    data_send_mixin(conn_2, replay_data, 0.6, 160)

    merger = Merger.build(**config)
    f_1 = asyncio.ensure_future(merger.handle_connection(conn_1))
    f_2 = asyncio.ensure_future(merger.handle_connection(conn_2))
    await f_1
    await f_2
    await verify_merger_ending_with_data(merger, replay_data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 1000)
@timeout(500)
async def test_merger_sequential_connections(event_loop, mock_connections,
                                             data_send_mixin):
    conn_1 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    conn_2 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data

    # First has incomplete data, but sends faster
    data_send_mixin(conn_1, replay_data[:-100], 0.4, 160)
    data_send_mixin(conn_2, replay_data, 0.6, 160)

    merger = Merger.build(**config)
    await merger.handle_connection(conn_1)
    await asyncio.sleep(15)
    await merger.handle_connection(conn_2)
    await verify_merger_ending_with_data(merger, replay_data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 1000)
@timeout(500)
async def test_merger_ends(event_loop, mock_connections, data_send_mixin):
    conn_1 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    conn_2 = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data

    data_send_mixin(conn_1, replay_data, 0.6, 160)
    data_send_mixin(conn_2, replay_data, 0.6, 160)

    merger = Merger.build(**config)
    await merger.handle_connection(conn_1)
    await asyncio.sleep(45)
    with pytest.raises(CannotAcceptConnectionError):
        await merger.handle_connection(conn_2)
    await verify_merger_ending_with_data(merger, replay_data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 1000)
@timeout(500)
async def test_merger_closes_fast(event_loop, mock_connections,
                                  data_send_mixin):
    conn = mock_connections(ConnectionHeader.Type.WRITER, 1)
    replay_data = example_replay.data
    data_send_mixin(conn, replay_data, 0.6, 160)

    merger = Merger.build(**config)
    f = asyncio.ensure_future(merger.handle_connection(conn))
    await asyncio.sleep(45)
    conn.read.side_effect = lambda _: b""   # Simulate connection.close()
    merger.close()
    await asyncio.wait_for(merger.wait_for_ended(), 1)
    await asyncio.wait_for(f, 1)
